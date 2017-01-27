import multiprocessing

try:
    multiprocessing.set_start_method('spawn')
except AttributeError:
    pass

pytest_plugins = [
    'asgi_server_fixtures',
    'benchmark_fixtures',
    'management_fixtures',
]
