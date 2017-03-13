import multiprocessing
import os
import sys

import pytest

try:
    multiprocessing.set_start_method('spawn')
except AttributeError:
    pass

sys.path.append(
    os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        'libs',
    ),
)


@pytest.fixture(scope='session', autouse=True)
def statistics(statdir):
    """Print benchmark statistics table."""

    import amqpstat
    amqpstat.maybe_monkeypatch(statdir)
    yield
    amqpstat.maybe_print_stats(statdir)


@pytest.fixture(scope='session')
def statdir(tmpdir_factory):

    d = tmpdir_factory.mktemp('stats', numbered=True)
    return str(d)
