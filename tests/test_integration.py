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


class Benchmarker(benchmark.Benchmarker):

    def print_progress(self):

        print('.', end='', flush=True)
        open_protocols = len([x for x in benchmark.stats.values() if not x])
        if open_protocols == 0 and len(benchmark.stats) >= self.num:
            reactor.stop()
            self.print_stats()


def test_benchmark(asgi_server):

    benchmarker = Benchmarker(
        url='ws://%s:%d' % asgi_server,
        num=100,
        concurrency=10,
        rate=1,
        messages=5,
        spawn=30,
    )
    benchmarker.loop()
    reactor.run()
