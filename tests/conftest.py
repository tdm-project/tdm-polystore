

import json
import logging
import multiprocessing
import os
import pytest
import random
import requests
import signal
import socket
import string
import time

from collections import defaultdict

from tdmq import create_app
import tdmq.db


@pytest.fixture(scope="session")
def source_data():
    root = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(root, 'data/sources.json')) as f:
        sources = json.load(f)['sources']
    with open(os.path.join(root, 'data/records.json')) as f:
        records = json.load(f)['records']

    records_by_source = defaultdict(list)
    for r in records:
        records_by_source[r['source']].append(r)

    return dict(sources=sources, records=records,
                records_by_source=records_by_source)


@pytest.fixture(scope="session")
def db_connection_config():
    return {
        'user': 'postgres',
        'password': 'foobar',
        'dbname': 'tdmqtest-{}'.format(_rand_str(6)),
        'host': 'timescaledb',
        'port': 5432,
    }


@pytest.fixture(scope="session")
def db(db_connection_config):
    """Create and connect to the database"""

    tdmq.db.init_db(db_connection_config)
    connection = tdmq.db.db_connect(db_connection_config)

    try:
        yield connection
    finally:
        logging.info(f"Tearing down DB connection)")
        connection.close()
        logging.info("Dropping test DB %s", db_connection_config['dbname'])
        tdmq.db.drop_db(db_connection_config)


@pytest.fixture
def clean_db(db):
    with db:
        with db.cursor() as curs:
            curs.execute("DELETE FROM source;")

    yield db

    with db:
        with db.cursor() as curs:
            logging.debug("Deleting sources from DB")
            curs.execute("DELETE FROM source;")


@pytest.fixture
def app(db_connection_config, caplog):
    caplog.set_level(logging.DEBUG)
    """Create and configure a new app instance for each test."""
    # The database is created and dropped with each run
    logging.debug("Creating new app")
    app = create_app({
        'TESTING': True,
        'DB_HOST': db_connection_config['host'],
        'DB_PORT': db_connection_config['port'],
        'DB_NAME': db_connection_config['dbname'],
        'DB_USER': db_connection_config['user'],
        'DB_PASSWORD': db_connection_config['password'],
        'LOG_LEVEL': 'DEBUG'
    })

    app.testing = True
    with app.app_context():
        yield app
        tdmq.db.close_db()


@pytest.fixture
def flask_client(app):
    """A test client for the app."""
    return app.test_client()


@pytest.fixture
def runner(app):
    """A test runner for the app's Click commands."""
    return app.test_cli_runner()


@pytest.fixture
def db_data(clean_db, source_data):
    tdmq.db.load_sources_conn(clean_db, source_data['sources'])
    tdmq.db.load_records_conn(clean_db, source_data['records'])

    return clean_db  # return the DB connection


def _rand_str(length=6):
    if length < 1:
        raise ValueError(f"Length must be >= 1 (got {length})")
    return ''.join([ random.choice(string.ascii_lowercase) for _ in range(length) ])


#
# Used by in testing tdmq.client
#
@pytest.fixture(scope="session")
def tdmq_config():
    return {
        'tdmq_base_url': 'http://web:8000/api/v0.0',
        'tiledb_config': {'vfs.hdfs.username': 'root'},
        'tiledb_hdfs_root': 'hdfs://namenode:8020/arrays'
    }


@pytest.fixture
def clean_hdfs(tdmq_config):
    # FIXME make it configurable
    url = tdmq_config['tiledb_hdfs_root']
    hdfs_cmd = f'HADOOP_USER_NAME=root hdfs dfs -rm -r -f {url}'
    os.system(hdfs_cmd)


@pytest.fixture
def reset_db(tdmq_config):
    requests.get(f'{tdmq_config["tdmq_base_url"]}/init_db')

############################################################

#  Code taken from pytest-flask version 0.15.
#  FIXME:  contribute PR to fix LiveServer.stop()


try:
    from urllib2 import URLError, urlopen
except ImportError:
    from urllib.error import URLError
    from urllib.request import urlopen


class LiveServer(object):
    """The helper class uses to manage live server. Handles creation and
    stopping application in a separate process.

    :param app: The application to run.
    :param host: The host where to listen (default localhost).
    :param port: The port to run application.
    """

    def __init__(self, app, host, port, clean_stop=False):
        self.app = app
        self.port = port
        self.host = host
        self.clean_stop = clean_stop
        self._process = None

    def start(self):
        """Start application in a separate process."""
        def worker(app, host, port):
            logging.info("Starting live server (in separate process)")
            app.run(host=host, port=port, use_reloader=False, threaded=False, debug=True, use_evalex=False)
        self._process = multiprocessing.Process(
            target=worker,
            args=(self.app, self.host, self.port)
        )
        self._process.start()

        # We must wait for the server to start listening with a maximum
        # timeout of 5 seconds.
        timeout = 5
        while timeout > 0:
            time.sleep(1)
            try:
                urlopen(self.url())
                timeout = 0
            except URLError:
                timeout -= 1

    def url(self, url=''):
        """Returns the complete url based on server options."""
        return 'http://%s:%d%s' % (self.host, self.port, url)

    def stop(self):
        """Stop application process."""
        if self._process:
            if self.clean_stop and self._stop_cleanly():
                logging.info("Stopped live server cleanly")
                return

            if self._process.is_alive():
                logging.info("Live server process %s didn't exit with SIGINT. Terminating forcefully", self._process.pid)
                self._process.terminate()
                self._process.join(2)
                if self._process.is_alive():
                    logging.debug("Process won't die.  Killing")
                    os.kill(self._process.pid, signal.SIGKILL)
                    self._process.join(2)
                    if self._process.is_alive():
                        logging.error("Live server process %s still alive...giving up trying to kill it!", self._process.pid)

    def _stop_cleanly(self, timeout=3):
        """Attempts to stop the server cleanly by sending a SIGINT signal and waiting for
        ``timeout`` seconds.

        :return: True if the server was cleanly stopped, False otherwise.
        """
        logging.debug("Trying to stop live server cleanly...")
        try:
            os.kill(self._process.pid, signal.SIGINT)
            logging.debug("sent SIGINT.  Joining with timeout %s", timeout)
            self._process.join(timeout)
            if self._process.exitcode is not None:
                logging.debug("joined!!  Exitcode is %s", self._process.exitcode)
                return True
            else:
                logging.warning("clean stop of live server timed out")
                return False
        except Exception as ex:
            logging.error('Failed to join the live server process: %r', ex)
            return False

    def __repr__(self):
        return '<LiveServer listening at %s>' % self.url()


def _rewrite_server_name(server_name, new_port):
    """Rewrite server port in ``server_name`` with ``new_port`` value."""
    sep = ':'
    if sep in server_name:
        server_name, port = server_name.split(sep, 1)
    return sep.join((server_name, new_port))


# FIXME:  Can we get this to run for the entire session?
@pytest.fixture
def live_app(request, app, monkeypatch, pytestconfig):
    """Run application in a separate process.

    When the ``live_server`` fixture is applied, the ``url_for`` function
    works as expected::

        def test_server_is_up_and_running(live_server):
            index_url = url_for('index', _external=True)
            assert index_url == 'http://localhost:5000/'

            res = urllib2.urlopen(index_url)
            assert res.code == 200

    """
    port = pytestconfig.getvalue('live_server_port')

    if port == 0:
        # Bind to an open port
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', 0))
        port = s.getsockname()[1]
        s.close()

    host = pytestconfig.getvalue('live_server_host')

    # Explicitly set application ``SERVER_NAME`` for test suite
    # and restore original value on test teardown.
    server_name = app.config['SERVER_NAME'] or 'localhost'
    monkeypatch.setitem(app.config, 'SERVER_NAME',
                        _rewrite_server_name(server_name, str(port)))

    clean_stop = request.config.getvalue('live_server_clean_stop')
    server = LiveServer(app, host, port, clean_stop)
    if request.config.getvalue('start_live_server'):
        server.start()
    try:
        yield server
    finally:
        server.stop()
