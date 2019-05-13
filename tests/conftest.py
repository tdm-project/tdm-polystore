import psycopg2 as psy
import pytest
from tdmq import create_app


@pytest.fixture
def app():
    """Create and configure a new app instance for each test."""
    app = create_app({
        'TESTING': True,
        'DB_HOST': 'localhost',
        'DB_NAME': 'tdmqtest',
        'DB_USER': 'postgres',
        'DB_PASSWORD': 'foobar',
    })
    yield app


@pytest.fixture
def client(app):
    """A test client for the app."""
    return app.test_client()


@pytest.fixture
def runner(app):
    """A test runner for the app's Click commands."""
    return app.test_cli_runner()


@pytest.fixture
def db(app):
    """Get an handle to the underlying db."""
    db_settings = {
        'user': app.config['DB_USER'],
        'password': app.config['DB_PASSWORD'],
        'host': app.config['DB_HOST'],
        'dbname': app.config['DB_NAME'],
    }
    return psy.connect(**db_settings)
