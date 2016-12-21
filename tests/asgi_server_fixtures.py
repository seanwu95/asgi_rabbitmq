import multiprocessing

import pytest
from channels import DEFAULT_CHANNEL_LAYER, channel_layers
from channels.worker import Worker
from daphne.server import Server

from demo.asgi import channel_layer


@pytest.fixture(scope='session')
def asgi_server():
    """Daphne Live Server."""

    worker_process = WorkerProcess()
    worker_process.start()
    server_process = DaphneProcess()
    server_process.start()
    return server_process.host, server_process.port


class DaphneProcess(multiprocessing.Process):

    host = '0.0.0.0'
    port = 8000

    def __init__(self, *args, **kwargs):

        super(DaphneProcess, self).__init__(*args, **kwargs)
        self.daemon = True

    def run(self):

        server = Server(
            channel_layer=channel_layer,
            host=self.host,
            port=self.port,
            signal_handlers=False,
        )
        server.run()


class WorkerProcess(multiprocessing.Process):

    def __init__(self, *args, **kwargs):

        super(WorkerProcess, self).__init__(*args, **kwargs)
        self.daemon = True

    def run(self):

        worker = Worker(
            channel_layer=channel_layers[DEFAULT_CHANNEL_LAYER],
            signal_handlers=False,
        )
        worker.ready()
        worker.run()
