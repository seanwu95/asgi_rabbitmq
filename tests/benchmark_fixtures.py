import amqpstat
import pytest


@pytest.fixture(scope='session', autouse=True)
def statistics(statdir):
    """Print benchmark statistics table."""

    amqpstat.maybe_monkeypatch()
    yield
    amqpstat.maybe_print_stats(statdir)


@pytest.fixture(scope='session')
def statdir(tmpdir_factory):

    d = tmpdir_factory.mktemp('stats', numbered=True)
    return str(d)
