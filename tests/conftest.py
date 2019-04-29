import os

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
