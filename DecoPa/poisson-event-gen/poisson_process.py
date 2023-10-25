from random import Random


class Event:
    def __init__(self, timestamp):
        self.timestamp = timestamp


def poisson_process(rate, t_max: float, random: Random, t_start: float = 0):
    """
    Yield all events from a poisson process with rate rate that occur before T_max
    :param rate:    The poisson rate parameter lambda
    :param t_max:   The maximum simulation time
    :param random:     A python random number generator
    :param t_start: The starting simulation time
    :return: An iterator over the events
    """
    t = t_start
    while True:
        delta_t = random.expovariate(rate)  # compute time until next event
        t += delta_t  # increment the simulation time
        if t <= t_max:
            yield Event(t)
        else:
            break
