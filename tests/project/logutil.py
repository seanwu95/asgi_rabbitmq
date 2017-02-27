import logging
import os
import sys

from daphne.access import AccessLogGenerator
from twisted.python.log import PythonLoggingObserver

DEBUGLOG = os.environ.get('DEBUGLOG', 'False') == 'True'
ACTION_LOGGER = AccessLogGenerator(sys.stdout) if DEBUGLOG else None


def setup_logger(name):

    if DEBUGLOG:
        logging.basicConfig(
            level=logging.DEBUG,
            format=name + ' %(asctime)-15s %(levelname)-8s %(message)s',
        )
        for logger in ['pika']:
            logging.getLogger(logger).setLevel(logging.WARNING)
        observer = PythonLoggingObserver(loggerName='twisted')
        observer.start()
