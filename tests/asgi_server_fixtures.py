import threading

import pytest
from channels import DEFAULT_CHANNEL_LAYER, channel_layers
from channels.worker import Worker
from daphne.server import Server


@pytest.fixture(scope='session')
def asgi_server():
    """Daphne Live Server."""

    channel_layer = channel_layers[DEFAULT_CHANNEL_LAYER]

    worker_thread = WorkerThread(channel_layer)
    worker_thread.daemon = True
    worker_thread.start()

    daphne_thread = DaphneThread(channel_layer)
    daphne_thread.daemon = True
    daphne_thread.start()

    return daphne_thread.server_addr


class DaphneThread(threading.Thread):

    def __init__(self, channel_layer):

        self.channel_layer = channel_layer
        self.server_addr = None
        super(DaphneThread, self).__init__()

    def run(self):

        server = Server(
            channel_layer=self.channel_layer,
            signal_handlers=False,
        )
        self.server_addr = server.host, server.port
        server.run()


class WorkerThread(threading.Thread):

    def __init__(self, channel_layer):
        self.channel_layer = channel_layer
        super(WorkerThread, self).__init__()

    def run(self):

        worker = Worker(
            channel_layer=self.channel_layer,
            signal_handlers=False,
        )
        worker.ready()
        worker.run()
