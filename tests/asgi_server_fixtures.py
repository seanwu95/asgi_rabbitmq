import multiprocessing
import time

import django
import pytest
from asgi_rabbitmq import RabbitmqChannelLayer
from asgi_rabbitmq.core import BENCHMARK
from channels.asgi import ChannelLayerWrapper
from channels.worker import Worker, WorkerGroup
from daphne.server import Server

if BENCHMARK:
    import asgi_rabbitmq.amqp
    from asgi_rabbitmq.amqp import manager, amqp_stats, layer_stats
else:
    manager = amqp_stats = layer_stats = None


@pytest.fixture(params=[1, 4])
def asgi_server(request, rabbitmq_url):
    """Daphne Live Server."""

    worker_process = WorkerProcess(
        url=rabbitmq_url,
        threads=request.param,
        manager=manager,
        amqp_stats=amqp_stats,
        layer_stats=layer_stats,
    )
    worker_process.start()
    server_process = DaphneProcess(
        url=rabbitmq_url,
        manager=manager,
        amqp_stats=amqp_stats,
        layer_stats=layer_stats,
    )
    server_process.start()
    if server_process.host != '0.0.0.0':
        host = server_process.host
    else:
        host = '127.0.0.1'
    time.sleep(2)
    yield host, server_process.port
    server_process.terminate()
    server_process.join()
    worker_process.terminate()
    worker_process.join()


class DaphneProcess(multiprocessing.Process):

    host = '0.0.0.0'
    port = 8000

    def __init__(self, *args, **kwargs):

        if BENCHMARK:
            self.manager = kwargs.pop('manager')
            self.amqp_stats = kwargs.pop('amqp_stats')
            self.layer_stats = kwargs.pop('layer_stats')
        self.url = kwargs.pop('url')
        super(DaphneProcess, self).__init__(*args, **kwargs)
        self.daemon = True

    def run(self):

        if BENCHMARK:
            asgi_rabbitmq.amqp.manager = self.manager
            asgi_rabbitmq.amqp.amqp_stats = self.amqp_stats
            asgi_rabbitmq.amqp.layer_stats = self.layer_stats
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

        if BENCHMARK:
            self.manager = kwargs.pop('manager')
            self.amqp_stats = kwargs.pop('amqp_stats')
            self.layer_stats = kwargs.pop('layer_stats')
        self.url = kwargs.pop('url')
        self.threads = kwargs.pop('threads')
        super(WorkerProcess, self).__init__(*args, **kwargs)
        self.daemon = True

    def run(self):

        if BENCHMARK:
            asgi_rabbitmq.amqp.manager = self.manager
            asgi_rabbitmq.amqp.amqp_stats = self.amqp_stats
            asgi_rabbitmq.amqp.layer_stats = self.layer_stats
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
