import requests
import websocket


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
