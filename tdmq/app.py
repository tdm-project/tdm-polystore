import os
from flask import Flask

from tdmq.db import add_db_cli
from tdmq.api import add_routes


def create_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        # FIXME this is not working as expected
        APPLICATION_ROOT='/tdmq/v0.1',
        SECRET_KEY='dev',
        DB_HOST='timescaledb',
        DB_NAME='tdmqtest',
        DB_USER='postgres',
        DB_PASSWORD='foobar',
    )
    if test_config is None:
        app.config.from_pyfile('config.py', silent=True)
    else:
        app.config.from_mapping(test_config)

    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    add_db_cli(app)
    add_routes(app)

    return app
