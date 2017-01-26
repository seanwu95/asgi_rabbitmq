from __future__ import print_function

import benchmark
import requests
import websocket
from twisted.internet import reactor


def test_http_request(asgi_server):
    """Test the ability to send http requests and receive responses."""

    url = 'http://%s:%d/' % asgi_server
    response = requests.get(url)
    assert response.status_code == 404


def test_websocket_message(asgi_server):
    """Test the ability to send and receive messages over WebSocket."""

    url = 'ws://%s:%d/' % asgi_server
    ws = websocket.create_connection(url)
    ws.send('test')
    response = ws.recv()
    ws.close()
    assert 'test' == response


# asgi_server is a parametized fixture, but twisted reactor was
# designed to run only ones.  This is the dirty hack to make
# test_benchmark function idempotent.
run_benchmark = True


def test_benchmark(asgi_server):

    global run_benchmark
    if run_benchmark:
        benchmarker = benchmark.Benchmarker(
            url='ws://%s:%d' % asgi_server,
            num=100,
            concurrency=10,
            rate=1,
            messages=5,
            spawn=30,
        )
        benchmarker.loop()
        reactor.run(installSignalHandlers=False)
        run_benchmark = False
