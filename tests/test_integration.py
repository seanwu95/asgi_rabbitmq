def test_websocket_message(ws_client):
    """Test the ability to send and receive messages over WebSocket."""

    message = 'test'.encode()
    response = ws_client(message)
    assert message == response
