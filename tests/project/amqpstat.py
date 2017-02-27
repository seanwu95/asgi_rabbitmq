from __future__ import print_function

import glob
import json
import os
import random
import statistics
import time
from functools import wraps
from operator import itemgetter

import asgi_rabbitmq
from pika.channel import Channel
from pika.spec import Basic
from tabulate import tabulate

amqp_stats = {}
layer_stats = {}
consumers = {}

BENCHMARK = os.environ.get('BENCHMARK', 'False') == 'True'


def maybe_monkeypatch():

    if BENCHMARK:
        monkeypatch_all()


def maybe_print_stats(fromdir):

    if BENCHMARK:
        print_stats(fromdir)


def monkeypatch_all():
    monkeypatch_connection()
    monkeypatch_layer()


def monkeypatch_connection():

    asgi_rabbitmq.core.RabbitmqConnection.Connection = DebugConnection


def monkeypatch_layer():

    layer = asgi_rabbitmq.core.RabbitmqChannelLayer
    layer.send = bench(layer.send)
    layer.receive = bench(layer.receive)
    layer.new_channel = bench(layer.new_channel)
    layer.group_add = bench(layer.group_add)
    layer.group_discard = bench(layer.group_discard)
    layer.send_group = bench(layer.send_group, count=True)


def percentile(values, fraction):
    """
    Returns a percentile value (e.g. fraction = 0.95 -> 95th percentile)
    """
    values = sorted(values)
    stopat = int(len(values) * fraction)
    if stopat == len(values):
        stopat -= 1
    return values[stopat]


def print_stats(fromdir):
    for statfile in glob.glob('%s/*.dump' % fromdir):
        with open(statfile) as f:
            statblob = f.read()
        statdata = json.loads(statblob)
        for num, stat in enumerate([amqp_stats, layer_stats]):
            for k, v in statdata[num].items():
                if isinstance(v, list):
                    stat.setdefault(k, [])
                    stat[k].extend(v)
                else:
                    stat.setdefault(k, 0)
                    stat[k] += v
    headers = ['method', 'calls', 'mean', 'median', 'stdev', '95%', '99%']
    for num, stats in enumerate([amqp_stats, layer_stats], start=1):
        if stats:
            data = []
            for method, latencies in stats.items():
                if isinstance(latencies, list):
                    data.append([
                        method,
                        len(latencies),
                        statistics.mean(latencies),
                        statistics.median(latencies),
                        statistics.stdev(latencies)
                        if len(latencies) > 1 else None,
                        percentile(latencies, 0.95),
                        percentile(latencies, 0.99),
                    ])
                elif isinstance(latencies, int):
                    data.append(
                        [method, latencies, None, None, None, None, None],
                    )
                else:
                    raise Exception(
                        'Stat(%d) was currupted at method %s' % (num, method),
                    )
            data = sorted(data, key=itemgetter(1), reverse=True)
            print()
            print(tabulate(data, headers))
        else:
            print("%d) No statistic available" % num)


def save_stats(todir):

    statdata = [amqp_stats, layer_stats]
    statblob = json.dumps(statdata)
    path = os.path.join(todir, '%d.dump' % random.randint(0, 100))
    with open(path, 'w') as f:
        f.write(statblob)


def bench(f, count=False):
    """Collect function call duration statistics."""

    if count:

        @wraps(f)
        def wrapper(*args, **kwargs):
            layer_stats.setdefault(f.__name__, 0)
            layer_stats[f.__name__] += 1
            return f(*args, **kwargs)
    else:

        @wraps(f)
        def wrapper(*args, **kwargs):

            start = time.time()
            result = f(*args, **kwargs)
            latency = time.time() - start
            layer_stats.setdefault(f.__name__, [])
            layer_stats[f.__name__] += [latency]
            return result

    return wrapper


def wrap(method, callback):

    start = time.time()

    def wrapper(*args):
        latency = time.time() - start
        amqp_stats.setdefault(method, [])
        amqp_stats[method] += [latency]
        if callback:
            callback(*args)

    return wrapper


class DebugConnection(asgi_rabbitmq.core.LayerConnection):

    def _create_channel(self, channel_number, on_open_callback):

        return DebugChannel(self, channel_number, on_open_callback)


class DebugChannel(Channel):
    """Collect statistics about RabbitMQ methods usage on channel."""

    def basic_ack(self, *args, **kwargs):
        amqp_stats.setdefault('basic_ack', 0)
        amqp_stats['basic_ack'] += 1
        return super(DebugChannel, self).basic_ack(*args, **kwargs)

    def basic_cancel(self, callback=None, *args, **kwargs):
        return super(DebugChannel, self).basic_cancel(
            wrap('basic_cancel', callback), *args, **kwargs)

    def basic_consume(self, *args, **kwargs):

        start = time.time()
        consumer_tag = super(DebugChannel, self).basic_consume(*args, **kwargs)
        consumers[consumer_tag] = start
        return consumer_tag

    def _on_eventok(self, method_frame):

        end = time.time()
        if isinstance(method_frame.method, Basic.ConsumeOk):
            start = consumers.pop(method_frame.method.consumer_tag)
            latency = end - start
            amqp_stats.setdefault('basic_consume', [])
            amqp_stats['basic_consume'] += [latency]
            return
        return super(DebugChannel, self)._on_eventok(method_frame)

    def basic_get(self, callback=None, *args, **kwargs):
        # TODO: Measure latency for Get-Empty responses.
        return super(DebugChannel, self).basic_get(
            wrap('basic_get', callback), *args, **kwargs)

    def basic_publish(self, *args, **kwargs):
        amqp_stats.setdefault('basic_publish', 0)
        amqp_stats['basic_publish'] += 1
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
