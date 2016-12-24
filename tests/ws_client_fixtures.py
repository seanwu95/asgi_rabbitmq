from multiprocessing import Process, Queue

import pytest
from autobahn.twisted.websocket import (WebSocketClientFactory,
                                        WebSocketClientProtocol)
from twisted.internet import reactor, threads


@pytest.yield_fixture(scope='session')
def ws_client(asgi_server):
    """WebSocket Client which can send messages to the Live Server."""

    # FIXME: some generated id based mechanism.
    requests, responses = Queue(), Queue()

    def client(message):

        assert type(message) is bytes
        requests.put(message)
        return responses.get(timeout=30)

    host, port = asgi_server
    ws_process = WSClientProcess(requests, responses, host, port)
    ws_process.start()
    yield client
    ws_process.terminate()


class WSClientProcess(Process):
    """
    Process holding Twisted reactor with WebSocket client.

    Necessary for ping/pong frames to work.
    """

    def __init__(self, requests, responses, host, port):

        self.requests = requests
        self.responses = responses
        self.host = host
        self.port = port
        super(WSClientProcess, self).__init__()
        self.daemon = True

    def run(self):

        factory = WebSocketClientFactory()
        factory.protocol = type('BoundFixtureProtocol', (FixtureProtocol,), {
            'requests': self.requests,
            'responses': self.responses,
        })
        reactor.connectTCP(self.host, self.port, factory)
        reactor.run(installSignalHandlers=False)

    def terminate(self):

        reactor.callFromThread(reactor.stop)


class FixtureProtocol(WebSocketClientProtocol):

    def onOpen(self):

        threads.deferToThread(self.requests.get).addCallback(self.sendMessage)

    def onMessage(self, payload, isBinary):

        self.responses.put(payload)
