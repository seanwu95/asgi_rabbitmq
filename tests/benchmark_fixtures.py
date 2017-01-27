import pytest
from asgi_rabbitmq import amqp


@pytest.fixture(scope='session', autouse=True)
def statistics(statdir):
    """Print benchmark statistics table."""

    amqp.maybe_monkeypatch()
    yield
    amqp.maybe_print_stats(statdir)


@pytest.fixture(scope='session')
def statdir(tmpdir_factory):

    d = tmpdir_factory.mktemp('stats', numbered=True)
    return str(d)
