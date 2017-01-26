import multiprocessing

try:
    multiprocessing.set_start_method('spawn')
except AttributeError:
    pass

pytest_plugins = [
    'management_fixtures',
    'asgi_server_fixtures',
]
