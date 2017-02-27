from .settings import *

CHANNEL_LAYERS = {
    'default': {
        'BACKEND': 'asgi_rabbitmq.RabbitmqChannelLayer',
        'ROUTING': 'demo.routing.routes',
        'CONFIG': {
            'url': 'amqp://guest:guest@rabbitmq:5672/%2F',
        },
    },
}
