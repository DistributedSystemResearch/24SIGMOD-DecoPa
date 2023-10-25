from typing import Iterable
from poisson_process import Event, poisson_process


def count(events: Iterable[Event]) -> int:
    return sum((1 for _ in events))


if __name__ == "__main__":
    import random, math, argparse
    from statistics import mean, stdev

    LAMBDA_DEFAULT = 2
    DURATION_DEFAULT = 3600
    TRIALS_DEFAULT = 200
    parser = argparse.ArgumentParser(description='Simulate poisson process and count the number of events over time.')
    parser.add_argument("--lambda", dest="LAMBDA", type=float, help='Poisson rate parameter, events/sec',
                        default=LAMBDA_DEFAULT, required=False)
    parser.add_argument("--duration", dest='DURATION', type=int, help='Duration of simulation, sec',
                        default=DURATION_DEFAULT, required=False)
    parser.add_argument("--trials", dest='TRIALS', type=int, help='Number of times to repeat simulation',
                        default=TRIALS_DEFAULT, required=False)
    args = parser.parse_args()

    LAMBDA, DURATION, TRIALS = args.LAMBDA, args.DURATION, args.TRIALS
    RNG = random.Random()

    print(f"Running one simulation with λ={args.LAMBDA} events/s with duration {DURATION}s")
    number_of_events = count(poisson_process(LAMBDA, DURATION, RNG))
    rate = number_of_events / DURATION
    print(f"Events: {number_of_events}, Rate: {rate:.3f} events/sec\n")

    # Let's repeat the simulation a few times
    streams = [poisson_process(LAMBDA, DURATION, random.Random()) for _ in range(TRIALS)]
    print(f"Running {TRIALS} simulations, each of duration {args.DURATION}s with λ={args.LAMBDA} events/s ...")

    counts = list(map(count, streams))
    rates = [count / DURATION for count in counts]
    avg_count = mean(counts)
    std_count = stdev(counts)
    avg_rate = mean(rates)
    std_rate = stdev(rates)

    print(
        f"Number of events per trial: Average {avg_count} (expected: {DURATION * LAMBDA}), Stddev: {std_count :.3f} (expected: {math.sqrt(DURATION * LAMBDA) : .3f})")
    print(
        f"Number of events per trial / length of trial (rate): Average {avg_rate : .3f} (expected: {LAMBDA}), Stddev: {std_rate : .3f}")
