import os
import random
import string

import pytest
from rabbitmq_admin import AdminAPI


@pytest.fixture(scope='session')
def management():
    """RabbitMQ Management Client."""

    hostname = os.environ.get('RABBITMQ_HOST', 'localhost')
    port = os.environ.get('RABBITMQ_MANAGEMENT_PORT', '15672')
    user = os.environ.get('RABBITMQ_MANAGEMENT_USER', 'guest')
    password = os.environ.get('RABBITMQ_MANAGEMENT_PASSWORD', 'guest')
    return AdminAPI('http://%s:%s' % (hostname, port), (user, password))


@pytest.yield_fixture(scope='session', autouse=True)
def remove_vhost(management):
    """
    Remove all virtual hosts in the RabbitMQ which was created during
    test run.
    """

    vhosts = []
    yield vhosts
    for vhost in vhosts:
        management.delete_vhost(vhost)


@pytest.fixture
def vhost(management, remove_vhost):
    """Create random named virtual host."""

    name = ''.join(random.choice(string.ascii_letters) for i in range(8))
    management.create_vhost(name)
    remove_vhost.append(name)
    return name
