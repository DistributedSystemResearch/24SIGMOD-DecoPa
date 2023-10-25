import argparse
from contextlib import ExitStack
import csv
import math
import os
import random
import re
import string
from functools import lru_cache
from random import Random
import logging
from uuid import uuid4, UUID

from poisson_process import Event, poisson_process
from typing import List, Union, Sequence, Tuple, Dict, TypeAlias
from dataclasses import dataclass

# set up logger
logger = logging.getLogger(__name__)


@dataclass(unsafe_hash=True)
class PrimitiveEvent(Event):
    type: int
    timestamp: float
    id: UUID
    attribute_values: Tuple[str]

    def get_typename(self, alphabet: Sequence[str]) -> str:
        return alphabet[self.type]

    def get_timestamp_hhmmssuuuuuu(self) -> str:
        microseconds = round(self.timestamp * 1_000_000)

        seconds = microseconds // 1_000_000
        microseconds %= 1_000_000

        mins = seconds // 60
        seconds %= 60

        hrs = mins // 60
        mins %= 60

        return f"{hrs:02d}:{mins:02d}:{seconds:02d}:{microseconds:06d}"

    def to_csv_row(self, alphabet: Sequence[str]):
        return (self.get_timestamp_hhmmssuuuuuu(), self.get_typename(alphabet), str(self.id)[0:8]) + self.attribute_values  # FIXME?: truncated uuid

    def __init__(self, etype: int, timestamp: float, event_id: UUID, attribute_values : Sequence[str]):
        super().__init__(timestamp)
        self.type = etype
        self.id = event_id
        self.attribute_values = tuple(attribute_values)

    TIMESTAMP_RE = re.compile(r"(\d+):(\d\d?):(\d\d?)(?:[:.](\d{1,6}))?(?:.*)?")

    @classmethod
    def parse_ts(cls, timestamp: str) -> float:
        timestamp = timestamp.strip()
        ts_decoded = re.match(cls.TIMESTAMP_RE, timestamp)
        if ts_decoded is None:
            raise ValueError(f"Invalid timestamp: {timestamp} - does not mach RE {cls.TIMESTAMP_RE}")
        hours, minutes, seconds, microseconds = map(lambda s : int(s) if s is not None else 0, ts_decoded.groups())

        if minutes > 60 or seconds > 60:
            raise ValueError(f"Invalid timestamp: {timestamp} (mins or secs above 60)")
        return hours * 3600.0 + minutes * 60.0 + seconds + microseconds / 1_000_000.0


NetSpec : TypeAlias = Tuple[Tuple[float], Tuple[frozenset[frozenset[int]]]]
"""
The type of a network specification
A tuple of two parallel arrays, of length equal to the number of event types that can occur.

First array:    The global rate, as float, for each event type.
Second array:   A set of disjoint sets of node IDs, called the event type's set of 'subscribed groups', for each event type.

Each subscribed group receives all events of the type. Within each group the events are partitioned across the members.
"""


def read_net_spec(filename: str) -> NetSpec:
    # read file into strings, one per line, until first line with ---
    # for readability
    subscribed_groups_per_event_type = []  # type:List[frozenset[frozenset[int]]]

    with open(filename, "r") as file:
        # read all lines until seperator
        for line in file:
            line = line.strip().rstrip(";" + string.whitespace)
            if line.endswith("---"):
                break
            elif line == "":
                subscribed_groups_per_event_type.append(frozenset())
            else:
                if not re.match(r"^\d+(?:\s+\d+)*\s*(?:;\s*\d+(?:\s+\d+)*\s*)*$", line):
                    logger.warning(f"Unexpected format of input line in file {file.name}: {line}")
                subscribed_groups_per_event_type.append(
                    frozenset(frozenset(int(node_id.strip()) for node_id in ids.strip().split()) for ids in line.split(";"))
                    # type:frozenset[frozenset[int]]
                )

        # read one more line after ---, expecting a list of rates
        last_line = file.readline().strip()
        try:
            rates_per_event_type = tuple(float(value.strip()) for value in last_line.split())
        except ValueError as e:
            raise ValueError(f"Last line of {file.name} must contain a list of global rates. Error: {e}")

        if not (len(rates_per_event_type) == len(subscribed_groups_per_event_type)):
            raise ValueError(f"Last line must contain a global rate for each line above the seperator. Lines above: "
                             f"{len(subscribed_groups_per_event_type)} Columns below: {len(rates_per_event_type)}")

        result = (rates_per_event_type, tuple(subscribed_groups_per_event_type))
        assert_valid(result)
        return result


def assert_valid(net: NetSpec):
    rate, subscribed_groups = net
    assert (len(rate) == len(subscribed_groups))

    for event_type in range(len(subscribed_groups)):
        # check that the subscribed groups of node IDs are pairwise disjoint
        union = set()
        for group in subscribed_groups[event_type]:
            if union.intersection(group) != set():
                raise ValueError(
                    f"These node ids occur in multiple partitoning groups for event type {event_type}: {union.intersection(group)}. "
                    f"The groups should be disjoint to prevent either from getting an event twice")
            union.update(group)


@dataclass(frozen=True)
class Net:
    """Convenience wrapper around net spec"""
    spec: NetSpec

    @property
    def R_event_type(self) -> Sequence[float]:
        """Global rate for each event type. Broadcasted events are counted only once."""
        return self.spec[0]

    @property
    def subscribed_groups_per_event_type(self) -> Sequence[frozenset[frozenset[int]]]:
        return self.spec[1]

    def get_subscribed_groups(self, event_type: int) -> frozenset[frozenset[int]]:
        """
        Get the set of groups of node ids that receive a given event type
        :param event_type:
        :return:
        """
        return self.subscribed_groups_per_event_type[event_type]

    def get_rate(self, event_type: int) -> float:
        """
        Get the rate of an event type
        """
        return self.R_event_type[event_type]

    @property
    @lru_cache(1)
    def num_event_types(self) -> int:
        """Number of event types"""
        return len(self.R_event_type)

    @property
    @lru_cache(1)
    def num_nodes(self) -> int:
        """Maximum node id +1"""
        result = -1
        subscribed_groups_array = self.spec[1]
        for subscribed_groups in subscribed_groups_array:
            for subscribed_group in subscribed_groups:
                for nodeId in subscribed_group:
                    result = max(result, nodeId)
        return result + 1

    @classmethod
    def from_file(cls, filename: str):
        """Read a net from a file"""
        return cls(read_net_spec(filename))


def main():
    # Get cmdline args
    args = parse_args()
    logging.basicConfig(level=args.log_level, format='[%(levelname)s] %(message)s')

    # seed Random number generator
    RNG = create_rng(args.seed)

    # Read all the rate files
    nets = [Net.from_file(filename) for filename in args.filename]

    # Verify that they are consistent, i.e. that the global rates of all event types are the same
    for net_index in range(1, len(nets)):
        if not nets[net_index].num_event_types == nets[0].num_event_types:
            raise ValueError(f"All input files must have the same number of event types. The 0-th file has "
                             f"{nets[0].num_event_types} types but the {net_index}-th file has {nets[net_index].num_event_types}")
        if args.trace_input_file is None:
            for j in range(nets[0].num_event_types):
                if abs(nets[net_index].R_event_type[j] - nets[0].R_event_type[j]) > 0.0001: #FIXME: It should be the same! I am doing this b/c of floating point issues
                    raise ValueError(
                        f"All input files must agree about the global rates of each event type. Event type {j} is generated "
                        f"at rate {nets[0].R_event_type[j]} in the input 0, but at rate {nets[net_index].R_event_type[j]} in input {net_index}.")

    def generated_global_trace(R_event_type: Sequence[float]):
        """
        Generate events with poisson process
        """
        num_event_types = len(R_event_type)
        R_total = sum(R_event_type)
        R_total *= args.multiply_all_rates_by_factor / args.timeunit
        for e in poisson_process(R_total, args.tmax, RNG):
            # Determine type of event randomly, with probability R_event_type[j] / R_total for each event type j.
            # Justified by https://stats.stackexchange.com/questions/122022/is-the-sum-of-several-poisson-processes-a-poisson-process
            etype = RNG.choices(range(num_event_types), weights=R_event_type).pop()
            yield PrimitiveEvent(etype, e.timestamp, uuid4(), [])

    def read_global_trace(filename: str, multiply_all_rates_by_factor : float):
        """
        Generate events by reading them from a file
        """
        with open(filename, "r") as file:
            inverse_alphabet = {args.alphabet[i]: i for i in range(len(args.alphabet))}  # type:Dict[str,int]
            reader = csv.reader(file)
            last_ts = -math.inf
            for row in reader:
                timestamp, event_type, *attribute_values = row  # type: str,str,List[str]
                event_type = event_type.strip()
                if event_type not in args.alphabet:
                    continue       
                if int(timestamp) < 101479641801 or int(timestamp) >  107326305934: # AND(DICA), AND(KL(D),I,B)
                #if int(timestamp) < 101479641801 or int(timestamp) >  104044647350: # AND(DICA), AND(KL(D),I,B)
                ##if int(timestamp) < 101479641801 or int(timestamp) >  104014647350:        
                #if int(timestamp) < 170007896601 or int(timestamp) >  170934951825:# 33 timewindows;AND(SEQ(C,E,A),G)
                #if int(timestamp) < 170007896601 or int(timestamp) >  170291578684:# 33 timewindows;AND(SEQ(C,E,A),G) more aggressive
                
              #  if int(timestamp) < 103884647300 or int(timestamp) > 104004647360 :    #KLDIG   
                #if int(timestamp) < 101422915019 or int(timestamp) > 101562915019 :   bhea
                
                    print(int(timestamp))
                    continue
                else:
                   timestamp = int(timestamp) - 101462526355 # AND(DICA), AND(KL(D),I,B)
                   #timestamp = int(timestamp) - 170001850321 # AND(SEQ(C,E,A),G)
                   #timestamp = int(timestamp) - 101438868739 # bhea
			#raise ValueError(
                    #    f"Unknown event type {event_type} in row {row} - type must be in {args.alphabet}")
                etype = inverse_alphabet[event_type]
                if etype >= nets[0].num_event_types:
                    logger.info(f"trace file {file.name} contains event {event_type} ({etype}) at ts={timestamp} that "
                                f"isn't mentioned in the input files - ignoring event")
                    continue

                try:
                    try:
                        ts_float = int(timestamp) / 1_000_000.0 #support integer microsecond timestamps
                    except ValueError:
                        ts_float = PrimitiveEvent.parse_ts(timestamp) #support ts in the style this tool writes (string with optional microseconds)
                    ts_float/=5
                    ts_float /= multiply_all_rates_by_factor
                    event = PrimitiveEvent(inverse_alphabet[event_type],
                                           ts_float,
                                           event_id=uuid4(),                    # fixme?: Any ids in the file not be used here. And end up as the first attribute value. We basically expect inputs without id. It would be saner to expet inputs with id (and not tranform it - same with ts). I'm leaving this as-is for compatibility with old inputs only (in case the program is to be used on old input files)
                                           attribute_values=attribute_values)
                    print(event)
                except ValueError as e:
                    logger.error(f"Failed to parse event from inputfile: {row}, Error: {e}. Ignoring event.")
                    continue

                if event.timestamp < last_ts:
                    logger.warning(
                        f"Timestamp of event out of order: {row} (ts {event.timestamp} smaller than {last_ts}). Processing event.")

                last_ts = max(last_ts, event.timestamp)
                yield event

    # Open output files
    # Create a directory for each output configuration
    if len(nets) > 1:
        output_directory = [f"{args.output_directory}/{netIndex}" for netIndex in range(len(nets))]
    else:
        output_directory = [args.output_directory]
    for path in output_directory:
        os.makedirs(path, exist_ok=True)

    with ExitStack() as stack:
        # Open an CSV file for each node
        files = [[stack.enter_context(open(f"{output_directory[netIndex]}/{args.output}_{nodeId}.csv", "w"))
                  for nodeId in range(nets[netIndex].num_nodes)]
                 for netIndex in range(len(nets))]

        writers = [[csv.writer(files[netIndex][nodeId])
                    for nodeId in range(nets[netIndex].num_nodes)]
                   for netIndex in range(len(nets))]
        """One csv writer per node that will accept primitive events (as tuples) and write them into the file"""

        global_trace = read_global_trace(args.trace_input_file, args.multiply_all_rates_by_factor) if args.trace_input_file is not None \
            else generated_global_trace(nets[0].R_event_type)

        for event in global_trace:
            # All subscribed groups receive the same event (events are broadcast to all groups).
            # Within each group a random node receives it.
            for net_index in range(len(nets)):
                net = nets[net_index]
                for subscribed_group in net.get_subscribed_groups(event.type):
                    node_id = RNG.choices(list(subscribed_group)).pop()  # uniform weights

                    writers[net_index][node_id].writerow(event.to_csv_row(args.alphabet))
        return 0


def parse_args():
    parser = argparse.ArgumentParser(
        description='Generate global trace of primitive events, using poisson processes or by splitting an existing input file. '
                    'Events have unique IDs  but may be delivered to more than one node as copies (see below)')
    parser.add_argument("filename",
                        help="Path to file whose first M lines, where M is the number of event types, contain a list of groups of NodeIds called 'subscribed groups' for that event type. Each group of nodes will receive all events of the type, and split them amongs the members randomly (poisson process). Semicolons seperate groups and whitespace seperates members. After M lines the program expects '---', and  the next line must contain the list of event rates for all M event types (whitespace sperated). If multiple files are given, they will receive the same global trace",
                        nargs="+", type=str)
    parser.add_argument("-s", "--seed", help="Seed for RNG", required=False, default=None)
    parser.add_argument("-l", "--log-level", default="INFO",
                        help="Set log level. Supported values (in order of increasing verbosity): WARNING, INFO")
    parser.add_argument("-a", "--alphabet", type=str, default=string.ascii_uppercase,
                        help="A string that gives names of event types to use, and to expect in the input files. e.g. ABCDEFGH will mean event type 0 is A, type 1 is B etc")
    parser.add_argument("-o", "--output",
                        help="File name prefix for output files. For the i-th node, a seperate output file named OUTPUT_i is generated.",
                        default="trace")
    parser.add_argument("-d", "--output-directory",
                        help="Path to directory to save outputs in. Note if multiple input filenames are given, the k-th output will go in to the directory OUTPUT_DIRECTORY/k",
                        default="traces")
    parser.add_argument("tmax",
                        help="Stop generating events after this many units of time. Set unit with --timeunit. Ignored if --trace-input-file is given",
                        type=float, default=math.inf)
    parser.add_argument("-M", "--multiply-all-rates-by-factor", default=1, type=float,
                                  help="Multiply all rates by this factor. If -f is given, timestamps are assumed to start with 0, and divided by this factor to speed up/slow down data.")
    mutex_group = parser.add_mutually_exclusive_group()
    mutex_group.add_argument("-t", "--timeunit", type=int, default=60,
                        help="The number of seconds per time unit, e.g for unit 'minute' give 60, for hour 3600 etc. This only affects how the rates are interpreted. A setting of 1 means rates are 'per second', a setting of 60 means rates are 'per minute', a setting of 3600 means rates are 'per hour'. Ignored when using -f")
    mutex_group.add_argument("-f", "--trace-input-file", type=str, default=None,
                                     help="Read global trace from CSV instead of generating it. This option will cause the global rates in the matrix inputs to be ignored. The provided trace file is"
                                          "randomly distributed across the nodes according to the input")

    args = parser.parse_args()
    return args


def create_rng(seed: Union[None, int, float, str, bytes, bytearray]) -> Random:
    """Set up RNG. If seed is one, creates seed and logs it to stderr as a side effect"""
    if seed is None:
        seed = random.randint(0, 1 << 255 - 1)
        logger.info(f"RNG seed: {seed} (generated)")
    rng = random.Random()
    rng.seed(seed)
    return rng


if __name__ == "__main__":
    main()
