import multiprocessing
import os
import signal
import time
from functools import partial
from itertools import count

import amqpstat
import django
import pytest
from asgi_rabbitmq import RabbitmqChannelLayer
from channels.asgi import ChannelLayerWrapper
from channels.worker import Worker, WorkerGroup
from daphne.server import Server


@pytest.fixture(params=[1, 4])
def asgi_server(request, rabbitmq_url, statdir):
    """Daphne Live Server."""

    worker_process = WorkerProcess(
        url=rabbitmq_url, threads=request.param, todir=statdir)
    worker_process.start()
    server_process = DaphneProcess(url=rabbitmq_url, todir=statdir)
    server_process.start()
    if server_process.host != '0.0.0.0':
        host = server_process.host
    else:
        host = '127.0.0.1'
    time.sleep(5)
    yield host, server_process.port
    server_process.terminate()
    server_process.join()
    worker_process.terminate()
    worker_process.join()


class DaphneProcess(multiprocessing.Process):

    host = '0.0.0.0'
    port_factory = count(8000)

    def __init__(self, *args, **kwargs):

        self.url = kwargs.pop('url')
        self.todir = kwargs.pop('todir')
        super(DaphneProcess, self).__init__(*args, **kwargs)
        self.daemon = True
        self.port = next(self.port_factory)

    def run(self):

        amqpstat.maybe_monkeypatch()
        signal.signal(signal.SIGTERM, partial(at_exit, self.todir))
        django.setup(**{'set_prefix': False} if django.VERSION[1] > 9 else {})
        asgi_layer = RabbitmqChannelLayer(url=self.url)
        channel_layer = ChannelLayerWrapper(
            channel_layer=asgi_layer,
            alias='default',
            routing='demo.routing.routes',
        )
        server = Server(
            channel_layer=channel_layer,
            endpoints=['tcp:port=%d:interface=%s' % (self.port, self.host)],
            signal_handlers=False,
        )
        server.run()


class WorkerProcess(multiprocessing.Process):

    def __init__(self, *args, **kwargs):

        self.url = kwargs.pop('url')
        self.threads = kwargs.pop('threads')
        self.todir = kwargs.pop('todir')
        super(WorkerProcess, self).__init__(*args, **kwargs)
        self.daemon = True

    def run(self):

        amqpstat.maybe_monkeypatch()
        signal.signal(signal.SIGTERM, partial(at_exit, self.todir))
        django.setup(**{'set_prefix': False} if django.VERSION[1] > 9 else {})
        asgi_layer = RabbitmqChannelLayer(url=self.url)
        channel_layer = ChannelLayerWrapper(
            channel_layer=asgi_layer,
            alias='default',
            routing='demo.routing.routes',
        )
        channel_layer.router.check_default()
        if self.threads == 1:
            worker = Worker(
                channel_layer=channel_layer,
                signal_handlers=False,
            )
        else:
            worker = WorkerGroup(
                channel_layer=channel_layer,
                signal_handlers=False,
                n_threads=self.threads,
            )
        worker.ready()
        worker.run()


def at_exit(todir, signum, frame):

    if amqpstat.BENCHMARK:
        amqpstat.save_stats(todir)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    os.kill(os.getpid(), signum)
