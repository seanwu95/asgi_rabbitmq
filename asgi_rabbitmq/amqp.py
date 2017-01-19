import atexit
import statistics
import time
from functools import wraps
from multiprocessing import Manager
from operator import itemgetter

from pika import SelectConnection
from pika.channel import Channel
from pika.connection import LOGGER
from tabulate import tabulate

manager = Manager()
amqp_stats = manager.dict()
layer_stats = manager.dict()


def print_stats():

    headers = ['method', 'calls', 'mean', 'median', 'stdev']
    for num, stats in enumerate([amqp_stats, layer_stats], start=1):
        if stats:
            data = []
            for method, latencies in stats.items():
                data.append([
                    method,
                    len(latencies),
                    statistics.mean(latencies),
                    statistics.median(latencies),
                    statistics.stdev(latencies)
                    if len(latencies) > 1 else None,
                ])
            data = sorted(data, key=itemgetter(1), reverse=True)
            print(tabulate(data, headers))
        else:
            print("%d) No statistic available" % num)


atexit.register(print_stats)


def bench(f):
    """Collect function call duration statistics."""

    @wraps(f)
    def wrapper(*args, **kwargs):

        start = time.time()
        result = f(*args, **kwargs)
        latency = time.time() - start
        layer_stats.setdefault(f.__name__, manager.list())
        layer_stats[f.__name__] += [latency]
        return result

    return wrapper


def wrap(method, callback):

    start = time.time()

    def wrapper(method_frame):
        latency = time.time() - start
        amqp_stats.setdefault(method, manager.list())
        amqp_stats[method] += [latency]
        if callback:
            callback(method_frame)

    return wrapper


class DebugConnection(SelectConnection):
    """Collect statistics about RabbitMQ methods usage on connection."""

    def _create_channel(self, channel_number, on_open_callback):

        LOGGER.debug('Creating channel %s', channel_number)
        return DebugChannel(self, channel_number, on_open_callback)


class DebugChannel(Channel):
    """Collect statistics about RabbitMQ methods usage on channel."""

    def basic_ack(self, *args, **kwargs):
        return super(DebugChannel, self).basic_ack(*args, **kwargs)

    def basic_cancel(self, callback=None, *args, **kwargs):
        return super(DebugChannel, self).basic_cancel(
            wrap('basic_cancel', callback), *args, **kwargs)

    def basic_consume(self, *args, **kwargs):
        return super(DebugChannel, self).basic_consume(*args, **kwargs)

    def basic_publish(self, *args, **kwargs):
        return super(DebugChannel, self).basic_publish(*args, **kwargs)

    def exchange_bind(self, callback=None, *args, **kwargs):
        return super(DebugChannel, self).exchange_bind(
            wrap('exchange_bind', callback), *args, **kwargs)

    def exchange_declare(self, callback=None, *args, **kwargs):
        return super(DebugChannel, self).exchange_declare(
            wrap('exchange_declare', callback), *args, **kwargs)

    def exchange_delete(self, callback=None, *args, **kwargs):
        return super(DebugChannel, self).exchange_delete(
            wrap('exchange_delete', callback), *args, **kwargs)

    def exchange_unbind(self, callback=None, *args, **kwargs):
        return super(DebugChannel, self).exchange_unbind(
            wrap('exchange_unbind', callback), *args, **kwargs)

    def queue_bind(self, callback, *args, **kwargs):
        return super(DebugChannel, self).queue_bind(
            wrap('queue_bind', callback), *args, **kwargs)

    def queue_declare(self, callback, *args, **kwargs):
        return super(DebugChannel, self).queue_declare(
            wrap('queue_declare', callback), *args, **kwargs)
