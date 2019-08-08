

import logging
import json
import os
import psycopg2 as psy
import pytest
import random
import string
from collections import defaultdict


from tdmq import create_app
from tdmq.db import close_db, get_db, init_db, load_sources, load_records


@pytest.fixture(scope="session")
def source_data():
    root = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(root, 'data/sources.json')) as f:
        sources = json.load(f)['sources']
    with open(os.path.join(root, 'data/records.json')) as f:
        records = json.load(f)['records']

    records_by_source = defaultdict(list)
    for r in records:
        records_by_source[ r['source'] ].append(r)

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
def app(db_connection_config):
    """Create and configure a new app instance for each test."""
    # The database is created and dropped with each run
    app = create_app({
        'TESTING': True,
        'DB_HOST': db_connection_config['host'],
        'DB_PORT': db_connection_config['port'],
        'DB_NAME': db_connection_config['dbname'],
        'DB_USER': db_connection_config['user'],
        'DB_PASSWORD': db_connection_config['password']
    })

    with app.app_context():
        yield app


@pytest.fixture
def client(app):
    """A test client for the app."""
    return app.test_client()


@pytest.fixture
def runner(app):
    """A test runner for the app's Click commands."""
    return app.test_cli_runner()


@pytest.fixture(scope="session")
def db(app):
    """Get an handle to the underlying db."""
    init_db()
    connection = get_db()

    try:
        yield connection
    finally:
        logging.info(f"Tearing down DB connection)")
        close_db()
        logging.info(f"Dropping test DB {app.config['DB_NAME']})")
        _drop_db(app)


@pytest.fixture
def clean_db(db):
    connection = get_db()

    with connection.cursor() as curs:
        curs.execute("DELETE FROM source;")

    yield connection

    with connection.cursor() as curs:
        logging.debug("Deleting sources from DB")
        curs.execute("DELETE FROM source;")


@pytest.fixture
def db_data(clean_db, source_data):
    connection = get_db()

    load_sources(connection, source_data['sources'])
    load_records(connection, source_data['records'])

    yield connection


def _rand_str(length=6):
    if length < 1:
        raise ValueError(f"Length must be >= 1 (got {length})")
    return ''.join([ random.choice(string.ascii_lowercase) for _ in range(length) ])


def _drop_db(app):
    db_config = {
        'user': app.config['DB_USER'],
        'password': app.config['DB_PASSWORD'],
        'host': app.config['DB_HOST'],
        'port': app.config.get('DB_PORT'),
        'dbname': 'postgres'
    }

    drop_cmd = psy.sql.SQL("DROP DATABASE ") + psy.sql.Identifier(app.config['DB_NAME'])
    with psy.connect(**db_config) as conn:
        conn.set_isolation_level(psy.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as curs:
            curs.execute(drop_cmd)
