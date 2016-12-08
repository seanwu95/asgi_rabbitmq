from channels.routing import route

from .consumers import ws_message

routes = [route('websocket.receive', ws_message)]
