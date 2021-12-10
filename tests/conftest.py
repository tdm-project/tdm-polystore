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

import prometheus_client
import pytest
import geojson
import shapely.geometry as sg

import tdmq.db
import tdmq.db_manager as db_manager
from tdmq.app import create_app


def _rand_str(length=6):
    if length < 1:
        raise ValueError(f"Length must be >= 1 (got {length})")
    return ''.join([random.choice(string.ascii_lowercase) for _ in range(length)])


@pytest.fixture(autouse=True)
def enable_all_loggers():
    # For some reason the client loggers end up being disabled when we run our tests.
    # Until we figure out why they're getting disabled, this fixture brutally
    # enables all existing loggers.
    for _, logger in logging.root.manager.loggerDict.items():
        if hasattr(logger, 'disabled'):
            logger.disabled = False


# Data fixtures

@pytest.fixture(scope="session")
def local_zone_db():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "test_zone_db.tar.gz")


@pytest.fixture()
def a_geojson_feature():
    return geojson.GeoJSON({
        "type": "Feature",
        "properties": {'name': 'CRS4, main offices'},
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [ 8.934352397918701, 38.98895230694548 ],
                    [ 8.937184810638428, 38.98895230694548 ],
                    [ 8.937184810638428, 38.99110378097014 ],
                    [ 8.934352397918701, 38.99110378097014 ],
                    [ 8.934352397918701, 38.98895230694548 ]
                    ]
                ]
            }
        })


@pytest.fixture
def a_geojson_geometry(a_geojson_feature):
    return a_geojson_feature.geometry


@pytest.fixture
def a_shapely_geometry(a_geojson_feature):
    return sg.shape(a_geojson_feature.geometry)


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

# DB fixtures


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
        logging.info("Tearing down DB connection")
        connection.close()
        logging.info("Dropping test DB %s", db_connection_config['dbname'])
        db_manager.drop_db(db_connection_config)


@pytest.fixture
def clean_db(db):
    with db:
        with db.cursor() as curs:
            curs.execute("DELETE FROM source;")

    try:
        yield db
    finally:
        with db:
            with db.cursor() as curs:
                logging.debug("Deleting sources from DB")
                curs.execute("DELETE FROM source;")


@pytest.fixture
def db_data(clean_db, source_data):
    tdmq.db.load_sources_conn(clean_db, source_data['sources'])
    tdmq.db.load_records_conn(clean_db, source_data['records'])

    return clean_db  # return the DB connection


@pytest.fixture
def public_db_data(clean_db, public_source_data):
    tdmq.db.load_sources_conn(clean_db, public_source_data['sources'])
    tdmq.db.load_records_conn(clean_db, public_source_data['records'])

    return clean_db  # return the DB connection


# Flask app fixtures

@pytest.fixture
def app(db_connection_config, auth_token, local_zone_db):
    """Create and configure a new app instance for each test."""
    app = create_app(
        test_config={
            'TESTING': True,
            'DB_HOST': db_connection_config['host'],
            'DB_PORT': db_connection_config['port'],
            'DB_NAME': db_connection_config['dbname'],
            'DB_USER': db_connection_config['user'],
            'DB_PASSWORD': db_connection_config['password'],
            'LOG_LEVEL': 'DEBUG',
            'APP_PREFIX': '',
            'AUTH_TOKEN': auth_token,
            'LOC_ANONYMIZER_DB': local_zone_db,
            'EXTERNAL_HOST_DOMAIN': 'jicsardegna.it',
        },
        prom_registry=prometheus_client.CollectorRegistry())

    app.testing = True
    try:
        with app.app_context():
            yield app
    finally:
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


# Storage fixtures


@pytest.fixture(scope="session", params=['hdfs', 's3'])
def storage_type(request):
    return request.param


@pytest.fixture(scope="session")
def storage_credentials(storage_type):
    if storage_type == "s3":
        return s3_credentials()
    if storage_type == "hdfs":
        return hdfs_credentials()
    raise ValueError(f"Unrecognized storage type {storage_type}")


@pytest.fixture(scope="session")
def service_info(storage_type):
    if storage_type == "s3":
        return s3_service_info()
    if storage_type == "hdfs":
        return hdfs_service_info()
    raise ValueError(f"Unrecognized storage type {storage_type}")


def s3_credentials():
    return {
        "vfs.s3.aws_access_key_id": "tdm-user",
        "vfs.s3.aws_secret_access_key": "tdm-user-s3",
    }


def s3_service_info():
    return {
        'version': '0.1',
        'tiledb': {
            'storage.root': 's3://test-bucket-{}/'.format(_rand_str(6)),
            'config': {
                "vfs.s3.endpoint_override": "minio:9000",
                "vfs.s3.scheme": "http",
                "vfs.s3.region": "",
                "vfs.s3.verify_ssl": "false",
                "vfs.s3.use_virtual_addressing": "false",
                "vfs.s3.use_multipart_upload": "false",
                # "vfs.s3.logging_level": 'TRACE'
                }
            }
        }


def hdfs_credentials():
    return {'vfs.hdfs.username': 'root'}


def hdfs_service_info():
    return {
        'version': '0.1',
        'tiledb': {
            'storage.root': 'hdfs://namenode:8020/test-arrays-{}'.format(_rand_str(6)),
            'config': {},
            }
        }


@pytest.fixture(scope="session")
def service_info_with_creds(service_info, storage_credentials):
    conf = copy.deepcopy(service_info)
    conf['tiledb']['config'].update(storage_credentials)
    return conf


def _wait_for(max_seconds, test_fn, error_msg="Timeout waiting"):
    for _ in range(max_seconds):
        if test_fn():
            logging.debug("Wait succeeded!")
            break
        time.sleep(1)
        logging.debug("Not yet.  Sleeping")
    else:
        raise RuntimeError(error_msg)


def s3_fixture(vfs, storage_root):
    assert storage_root.startswith("s3")
    if vfs.is_bucket(storage_root):
        vfs.empty_bucket(storage_root)
    else:
        vfs.create_bucket(storage_root)

    _wait_for(5, lambda: vfs.is_bucket(storage_root), "Timeout waiting for bucket to be created")

    try:
        yield
    finally:
        vfs.empty_bucket(storage_root)
        vfs.remove_bucket(storage_root)
        _wait_for(5, lambda: not vfs.is_bucket(storage_root), "Timeout waiting for bucket to be deleted")


def hdfs_fixture(vfs, storage_root):
    assert storage_root.startswith("hdfs")
    if vfs.is_dir(storage_root):
        vfs.remove_dir(storage_root)

    try:
        yield
    finally:
        if vfs.is_dir(storage_root):
            vfs.remove_dir(storage_root)


@pytest.fixture
def clean_storage(clean_db, service_info_with_creds):
    """
    Combines cleaning actions for all storage fixtures.
    """
    import tiledb
    storage_root = service_info_with_creds['tiledb']['storage.root']

    config = tiledb.Config(params=service_info_with_creds['tiledb']['config'])
    ctx = tiledb.Ctx(config=config)
    vfs = tiledb.VFS(ctx=ctx)

    if storage_root.startswith("s3"):
        print("Setting up s3 storage fixture")
        yield from s3_fixture(vfs, storage_root)
    elif storage_root.startswith("hdfs"):
        print("Setting up hdfs storage fixture")
        yield from hdfs_fixture(vfs, storage_root)
    else:
        raise ValueError(f"Unrecognized storage type {storage_root}")


# Live app fixture

############################################################

#  Code in part taken from pytest-flask version 0.15.
#  FIXME:  contribute PR to fix LiveServer.stop()
try:
    from urllib2 import URLError, urlopen
except ImportError:
    from urllib.error import URLError
    from urllib.request import urlopen


class SubprocessLiveServer:
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

    @staticmethod
    def _find_exec(exec_name):
        for p in os.environ.get('PATH', '').split(os.pathsep):
            full_path = os.path.join(p, exec_name)
            if os.path.exists(full_path) and os.access(full_path, os.X_OK):
                return full_path
        return None

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

        flask_path = self._find_exec('flask')
        if not flask_path:
            raise RuntimeError("Couldn't find flask executable in PATH")
        self._process = subprocess.Popen([flask_path, "run"], env=proc_env)

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
############################################################


@pytest.fixture(scope="session")
def auth_token():
    return 'supersecret'


@pytest.fixture(scope="session")
def live_app(db, db_connection_config, pytestconfig, auth_token, local_zone_db,
             service_info, storage_credentials):
    """Run application in a separate process.

       Get the URL with live_app.url().
    """
    import tdmq.wsgi as wsgi
    port = pytestconfig.getvalue('live_server_port')

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
EXTERNAL_HOST_DOMAIN = None
TILEDB_INTERNAL_VFS = {{
    'storage.root': "{service_info['tiledb']['storage.root']}",
    'config': {service_info['tiledb']['config']},
    'credentials': {storage_credentials}
}}
APP_PREFIX = ''
AUTH_TOKEN = '{auth_token}'
LOC_ANONYMIZER_DB = '{local_zone_db}'
    """

    application_path = os.path.abspath(os.path.splitext(wsgi.__file__)[0])
    logging.debug("Running tdmq application at path %s", application_path)

    with tempfile.NamedTemporaryFile(mode='w') as f:
        f.write(cfg)
        f.flush()
        server = SubprocessLiveServer(f.name, application_path, host, port, '')
        server.start()
        server.auth_token = auth_token
        try:
            yield server
        finally:
            server.stop()
