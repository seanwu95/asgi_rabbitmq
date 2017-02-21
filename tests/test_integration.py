import multiprocessing

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


def test_benchmark(asgi_server):

    proc = multiprocessing.Process(target=run_benchmark, args=(asgi_server,))
    proc.daemon = True
    proc.start()
    proc.join(timeout=90)
    proc.terminate()
    proc.join()
    assert proc.exitcode == 0


def run_benchmark(asgi_server):

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
