import argparse
import csv
import math
import random
from random import Random
import logging
from uuid import uuid4, UUID

from poisson_process import Event, poisson_process
from typing import List, Union, Sequence, Tuple
from dataclasses import dataclass

#set up logger
logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class PrimitiveEvent(Event):
    type: int
    node: int
    timestamp : float
    id: UUID

    def get_typename(self, alphabet: Sequence[str]) -> str:
        return alphabet[self.type]

    def get_timestamp_HHMMSS(self, seconds_per_time_unit : int) -> str:
        seconds = math.floor(self.timestamp * seconds_per_time_unit)
        hrs = seconds // 3600
        seconds -= hrs * 3600
        mins = seconds // 60
        seconds -= mins * 60

        return f"{hrs:02d}:{mins:02d}:{seconds:02d}"

    def to_csv_row(self, alphabet : Sequence[str], seconds_per_time_unit : int) -> Tuple[str, str, str]:
        return (self.get_timestamp_HHMMSS(seconds_per_time_unit), self.get_typename(alphabet), str(self.id)[0:8]) #FIXME?: truncated uuid

# The type of a network specification
# First tuple entry:
# NxM matrix of rates (as float).  NET[i][j] is the rate at which event type j is generated at node i.
# N... number of nodes       (nodes are 0, 1, ...)
# M... number of event types (types are A, B, ...)
#Second tuple entry:
# A 1xM matrix of booleans indicating, for each event type, whether it is "broadcasted". This means that whenever such
# an event is generated at a node, copies (with the same ID) appear at all other nodes that can generate it (i.e. nonzero rate).
Net = Tuple[List[List[float]], List[bool]]

def read_rate_matrix(filename : str) -> Net :
    #read file into strings, one per line, until first line with ---
    with open(filename, "r") as file:
        lines = []
        for line in file:
            line = line.strip()
            if line.endswith("---"):
                break
            lines.append(line)
        #split lines into number-strings, convert each to float
        rates= [ [float(x) for x in l.split() ] for l in lines ]

        #read one more line after ---, expecting "B" if it is broadcast, else a "P"
        is_broadcast = []
        for value in file.readline().split():
            if not (value == "B" or value == "P"):
                raise ValueError("First row after --- must contain a B or P for each event type")
            is_broadcast.append(value == "B")
        if not (len(rates) == 0 or len(rates[0]) == len(is_broadcast)):
            raise ValueError("First row after --- must contain a B or P for each event type")

        result = (rates, is_broadcast)
        assert_valid(result)
        return result
def assert_valid(net : Net):
    rates, is_broadcast = net
    for etype in range(len(is_broadcast)):
        if is_broadcast[etype]:
            my_rates = [rates[node][etype] for node in range(len(rates)) if rates[node][etype] != 0]
            if len(my_rates) > 1 and not all((rate == my_rates[0] for rate in my_rates)):
                raise ValueError(f"Broadcast event type {etype} occurs at two different nonzero rates.")

def main():
    # Get cmdline args
    args = parse_args()
    logging.basicConfig(level=args.log_level, format='[%(levelname)s] %(message)s')

    #seed Random number generator
    RNG = create_rng(args.seed)

    # get the primitive event rates from the input
    r, is_broadcast = read_rate_matrix(args.filename)

    N = len(r)
    """Number of rows (nodes) in input"""
    M = len(r[0]) if N>0 else 0
    """Number of columns (event types) in input"""

    #open N files, one per node, for output
    files = [open(f"{args.output}_{node}.csv", "w") for node in range(N)]
    """One csv file per node (local trace)"""
    writers = [csv.writer(files[node]) for node in range(N)]
    """One csv writer per node that will accept primitive events (as tuples) and write them into the file"""

    # generate events with poisson process
    R = sum( [sum(row) for row in r] )
    """Total rate of all primitive events"""
    R *= args.multiply_all_rates_by_factor
    for e in poisson_process(R, args.tmax, RNG):
        #determine type and node randomly, where an event of type j being generated at node i has probability r[i][j] / R
        #Justified by https://stats.stackexchange.com/questions/122022/is-the-sum-of-several-poisson-processes-a-poisson-process
        node = RNG.choices(range(N), weights=[sum(row) for row in r]).pop()
        etype = RNG.choices(range(M), weights=r[node]).pop()
        ev = PrimitiveEvent(etype, node, e.timestamp, uuid4())
        """sourced, typed event"""
        if (not is_broadcast[etype]):
            writers[node].writerow(ev.to_csv_row(args.alphabet, args.timeunit))
        else:
            all_sources_generating_etype = (node for node in range(N) if r[node][etype] > 0)
            for node in all_sources_generating_etype:
                writers[node].writerow(ev.to_csv_row(args.alphabet, args.timeunit))

    #cleanup
    del writers
    for file in files:
        file.flush()
        file.close()
    return 0


def parse_args():
    parser = argparse.ArgumentParser(description='Generate global trace of primitive events for sigmod24 paper, using poisson processes. Events have IDs that are unique except in case of broadcasting (see below)')
    parser.add_argument("filename", help="Path to file with a NxM matrix of event rates, where N... number of nodes, M... number of prim event types. Last line must contain for each event a B if it is broadcast (anytime the event occurs it is copied to all sources) or a P (if not)", type=str)
    parser.add_argument("tmax", help="Stop generating events after this many units of time. Set unit with --timeunit. ", type=float, default=math.inf)
    parser.add_argument("-s", "--seed", help="Seed for RNG", required=False, default=None)
    parser.add_argument("-t", "--timeunit", type=int, default=60, help="The number of seconds per time unit, e.g for unit 'minute' give 60, for hour 3600 etc. Only used to convert timestamps to seconds for output purposes (timestamps in output are in the format HH:MM:SS regardless of timeunit, as required by ambrosia)")
    parser.add_argument("-a", "--alphabet", type=str, default="ABCDEFGHIJKLMNOPRSTUVWXYZ", help="A string that gives names of events to use. e.g. ABCDEFGH will mean event type 0 is A, type 1 is B etc")
    parser.add_argument("-o", "--output", help="File name prefix for output files", default="trace")
    parser.add_argument("-M", "--multiply-all-rates-by-factor", default=1, type=float, help="Multiply all rates by this factor")
    parser.add_argument("-l", "--log-level", default="INFO", help="Set log level. Supported values (in order of increasing verbosity): WARNING, INFO")
    args = parser.parse_args()
    return args


def create_rng(seed : Union[None, int, float, str, bytes, bytearray]) -> Random:
    """Set up RNG. If seed is one, creates seed and logs it to stderr as a side effect"""
    if seed is None:
        seed = random.randint(0, 1 << 255 - 1)
        logger.info(f"RNG seed: {seed} (generated)")
    rng = random.Random()
    rng.seed(seed)
    return rng


if __name__ == "__main__":
    main()
