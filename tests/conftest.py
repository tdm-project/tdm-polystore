import copy
import json
import logging
import os
import random
import signal
import socket
import string
import subprocess
import tempfile
import time
from collections import defaultdict

import pytest
import tdmq.db
import tdmq.db_manager as db_manager
from tdmq.app import create_app


@pytest.fixture(scope="session")
def auth_token():
    return 'supersecret'


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
def public_source_data(source_data):
    sources = [s for s in source_data['sources'] if s.get('public') is True ]
    records = [r for r in source_data['records'] if r['source'] in [s['id'] for s in sources]]
    records_by_source = defaultdict(list)
    for r in records:
        records_by_source[r['source']].append(r)

    return dict(sources=sources, records=records, records_by_source=records_by_source)

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

    db_manager.create_db(db_connection_config)
    db_manager.alembic_run_migrations(db_connection_config)
    connection = db_manager.db_connect(db_connection_config)

    try:
        yield connection
    finally:
        logging.info(f"Tearing down DB connection)")
        connection.close()
        logging.info("Dropping test DB %s", db_connection_config['dbname'])
        db_manager.drop_db(db_connection_config)


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
def app(db_connection_config, auth_token):
    """Create and configure a new app instance for each test."""
    app = create_app({
        'TESTING': True,
        'DB_HOST': db_connection_config['host'],
        'DB_PORT': db_connection_config['port'],
        'DB_NAME': db_connection_config['dbname'],
        'DB_USER': db_connection_config['user'],
        'DB_PASSWORD': db_connection_config['password'],
        'LOG_LEVEL': 'DEBUG',
        'PROMETHEUS_REGISTRY': True,
        'APP_PREFIX': '',
        'AUTH_TOKEN': auth_token
    })

    app.testing = True
    with app.app_context():
        yield app
        tdmq.db.close_db()


@pytest.fixture
def flask_client(app):
    """
    A test client for the app.
    You can retrieve the authentication token through the `auth_token` attribute.
    """
    client = app.test_client()
    client.auth_token = app.config['AUTH_TOKEN']
    return client


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
    return ''.join([random.choice(string.ascii_lowercase) for _ in range(length)])


#
# Used by in testing tdmq.client
#
@pytest.fixture(scope="session")
def tdmq_s3_credentials():
    return {
        "vfs.s3.aws_access_key_id": "tdm-user",
        "vfs.s3.aws_secret_access_key": "tdm-user-s3",
    }


@pytest.fixture(scope="session")
def tdmq_s3_service_info():
    return {
        'version' : '0.1',
        'tiledb' : {
            'storage.root' : 's3://firstbucket/',
            'config': {
                "vfs.s3.endpoint_override": "minio:9000",
                "vfs.s3.scheme": "http",
                "vfs.s3.region": "",
                "vfs.s3.verify_ssl": "false",
                "vfs.s3.use_virtual_addressing": "false",
                "vfs.s3.use_multipart_upload": "false",
                #"vfs.s3.logging_level": 'TRACE'
                }
            }
        }


@pytest.fixture(scope="session")
def tdmq_s3_service_info_with_creds(tdmq_s3_service_info, tdmq_s3_credentials):
    conf = copy.deepcopy(tdmq_s3_service_info)
    conf['tiledb']['config'].update(tdmq_s3_credentials)
    return conf


@pytest.fixture
def clean_s3(tdmq_s3_service_info_with_creds):
    import tiledb
    bucket = tdmq_s3_service_info_with_creds['tiledb']['storage.root']
    assert bucket.startswith('s3://')
    config = tiledb.Config(params=tdmq_s3_service_info_with_creds['tiledb']['config'])
    ctx = tiledb.Ctx(config=config)
    vfs = tiledb.VFS(ctx=ctx)
    if vfs.is_bucket(bucket):
        vfs.empty_bucket(bucket)
    else:
        vfs.create_bucket(bucket)
    return tdmq_s3_service_info_with_creds


@pytest.fixture(scope="session")
def tdmq_hdfs_credentials():
    return {'vfs.hdfs.username': 'root'}


@pytest.fixture(scope="session")
def tdmq_hdfs_service_info():
    return {
        'version' : '0.1',
        'tiledb' : {
            'storage.root': 'hdfs://namenode:8020/arrays',
            'config': {},
            }
        }


@pytest.fixture(scope="session")
def tdmq_hdfs_service_info_with_creds(tdmq_hdfs_service_info, tdmq_hdfs_credentials):
    conf = copy.deepcopy(tdmq_hdfs_service_info)
    conf['tiledb']['config'].update(tdmq_hdfs_credentials)
    return conf


@pytest.fixture
def clean_hdfs(tdmq_hdfs_service_info_with_creds):
    import tiledb
    array_root = tdmq_hdfs_service_info_with_creds['tiledb']['storage.root']
    assert array_root.startswith("hdfs://")
    config = tiledb.Config(params=tdmq_hdfs_service_info_with_creds['tiledb']['config'])
    ctx = tiledb.Ctx(config=config)
    vfs = tiledb.VFS(ctx=ctx)
    if vfs.is_dir(array_root):
        vfs.remove_dir(array_root)

    return tdmq_hdfs_service_info_with_creds


@pytest.fixture
def clean_storage(clean_db, clean_s3, clean_hdfs):
    """
    Combines cleaning actions for all storage fixtures.
    """
    pass


############################################################

#  Code in part taken from pytest-flask version 0.15.
#  FIXME:  contribute PR to fix LiveServer.stop()
try:
    from urllib2 import URLError, urlopen
except ImportError:
    from urllib.error import URLError
    from urllib.request import urlopen


class SubprocessLiveServer(object):
    """The helper class uses to manage live server. Handles creation and
    stopping application in a separate process.

    :param host: The host where to listen (default localhost).
    :param port: The port to run application.
    """

    def __init__(self, cfg_file, app_path, host, port, prefix=None):
        self.cfg_file = cfg_file
        self.app_path = app_path
        self.port = port
        self.host = host
        self.prefix = prefix
        self._process = None

    def start(self):
        """Start application in a separate process."""
        proc_env = os.environ.copy()
        proc_env.update({
            "FLASK_APP": self.app_path,
            "FLASK_ENV": "development",
            "FLASK_RUN_PORT": str(self.port),
            "FLASK_RUN_HOST": self.host,
            "TDMQ_FLASK_CONFIG": self.cfg_file
        })

        self._process = subprocess.Popen(["/usr/local/bin/flask", "run"], env=proc_env)

        # We must wait for the server to start listening with a maximum
        # timeout of 5 seconds.
        timeout = 5
        success = False
        url = self.url('/')
        while timeout > 0 and not success:
            time.sleep(1)
            try:
                urlopen(url)
                success = True
            except URLError as e:
                logging.debug("Service didn't reply successfully to url %s: %s", url, e)
                timeout -= 1

        if not success:
            self._process.kill()  # Just in case it started after the timeout
            raise RuntimeError("live_app: failed to start Flask application")

    def url(self, url=''):
        """Returns the complete url based on server options."""
        base = 'http://{}:{}'.format(self.host, self.port)
        if self.prefix:
            base += self.prefix
        if url:
            base += url
        return base

    def stop(self):
        """Stop application process."""
        if self._process:
            if self._stop_cleanly():
                logging.info("Stopped live server cleanly")
                return

            if self._process.poll() is None:
                logging.info("Live server process %s didn't exit with SIGINT. Terminating forcefully", self._process.pid)
                self._process.terminate()
                try:
                    self._process.wait(2)
                    return
                except subprocess.TimeoutExpired:
                    pass

                logging.debug("Process won't die.  Killing")
                self._process.kill()
                try:
                    self._process.wait(2)
                except subprocess.TimeoutExpired:
                    logging.error("Live server process %s still alive...giving up trying to kill it!", self._process.pid)

    def _stop_cleanly(self, timeout=3):
        """Attempts to stop the server cleanly by sending a SIGINT signal and waiting for
        ``timeout`` seconds.

        :return: True if the server was cleanly stopped, False otherwise.
        """
        logging.debug("Trying to stop live server cleanly...")
        if self._process.poll() is not None:
            logging.debug("Process already exited.  Exit code: %s", self._process.returncode)
            return True

        self._process.send_signal(signal.SIGINT)
        logging.debug("sent SIGINT.  Waiting up to %s seconds", timeout)
        try:
            retcode = self._process.wait(timeout)
            logging.debug("Exited!!  Exit code is %s", retcode)
            return True
        except subprocess.TimeoutExpired:
            logging.warning("clean stop of live server timed out")
            return False

    def __repr__(self):
        return '<SubprocessLiveServer listening at %s>' % self.url()


@pytest.fixture(scope="session", params=["s3", "hdfs"])
def live_app(request, db, db_connection_config, pytestconfig, auth_token,
             tdmq_s3_service_info, tdmq_s3_credentials,
             tdmq_hdfs_service_info, tdmq_hdfs_credentials):
    """Run application in a separate process.

       Get the URL with live_app.url().
    """
    import tdmq.wsgi as wsgi
    port = pytestconfig.getvalue('live_server_port')
    if request.param == 'hdfs':
        service_info = tdmq_hdfs_service_info
        credentials = tdmq_hdfs_credentials
    elif request.param == 's3':
        service_info = tdmq_s3_service_info
        credentials = tdmq_s3_credentials
    else:
        raise RuntimeError(f"Unrecognized parameter {request.param}")

    if port == 0:
        # Bind to an open port
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', 0))
        port = s.getsockname()[1]
        s.close()

    host = pytestconfig.getvalue('live_server_host')

    cfg = f"""
SECRET_KEY = "dev"
TESTING = True
DB_HOST = "{db_connection_config['host']}"
DB_PORT = "{db_connection_config['port']}"
DB_NAME = "{db_connection_config['dbname']}"
DB_USER = "{db_connection_config['user']}"
DB_PASSWORD = "{db_connection_config['password']}"
LOG_LEVEL = "DEBUG"
PROMETHEUS_REGISTRY = True
TILEDB_VFS_ROOT = "{service_info['tiledb']['storage.root']}"
TILEDB_VFS_CONFIG = {service_info['tiledb']['config']}
TILEDB_VFS_CREDENTIALS = {credentials}
APP_PREFIX = ''
AUTH_TOKEN = '{auth_token}'
    """

    application_path = os.path.abspath(os.path.splitext(wsgi.__file__)[0])
    logging.debug("Running tdmq application at path %s", application_path)

    with tempfile.NamedTemporaryFile(mode='w') as f:
        f.write(cfg)
        f.flush()
        server = SubprocessLiveServer(f.name, application_path, host, port, '')
        server.start()
        try:
            yield server
        finally:
            server.stop()
