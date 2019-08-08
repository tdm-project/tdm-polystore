import os
from flask import Flask

from tdmq.db import add_db_cli, close_db
from tdmq.api import add_routes
from tdmq.api import DuplicateItemException


def create_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DB_HOST='timescaledb',
        DB_NAME='tdmqtest',
        DB_USER='postgres',
        DB_PASSWORD='foobar',
    )
    if test_config is None:
        app.config.from_envvar('TDMQ_FLASK_CONFIG', silent=True)
    else:
        app.config.from_mapping(test_config)

    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    add_db_cli(app)
    add_routes(app)

    @app.errorhandler(DuplicateItemException)
    def handle_duplicate(e):
        import logging
        logging.error('duplicate item exception %s', e.args)
        return f'{e.args}', 512

    @app.teardown_appcontext
    def teardown_db(arg):
        import logging
        logging.info("teardown_db:  here are the args: %s", arg)
        close_db()

    return app
