

import logging
import os
import flask

from tdmq.db import add_db_cli, close_db
from tdmq.api import add_routes
from tdmq.api import DuplicateItemException


def configure_logging(app):
    level_str = app.config.get('LOG_LEVEL', 'INFO')
    error = False
    try:
        level_value = getattr(logging, level_str)
    except AttributeError:
        level_value = logging.INFO
        error = True

    logging.basicConfig(level=level_value)
    if error:
        app.logger.error("LOG_LEVEL value %s is invalid. Defaulting to INFO", level_str)

    app.logger.info('Logging is active. Log level: %s', logging.getLevelName(app.logger.getEffectiveLevel()))


class DefaultConfig(object):
    SECRET_KEY = 'dev'

    DB_HOST = 'timescaledb'
    DB_NAME = 'tdmqtest'
    DB_USER = 'postgres'
    DB_PASSWORD = 'foobar'

    LOG_LEVEL = "INFO"

    TILEDB_HDFS_ROOT = 'hdfs://namenode:8020/arrays'
    TILEDB_HDFS_USERNAME = 'root'


def create_app(test_config=None):
    app = flask.Flask(__name__, instance_relative_config=True)
    app.config.from_object(DefaultConfig)

    if test_config is not None:
        app.config.from_mapping(test_config)
    elif 'TDMQ_FLASK_CONFIG' in os.environ:
        app.config.from_envvar('TDMQ_FLASK_CONFIG')

    configure_logging(app)

    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    add_db_cli(app)
    add_routes(app)

    @app.errorhandler(DuplicateItemException)
    def handle_duplicate(e):
        app.logger.error('duplicate item exception %s', e.args)
        return f'{e.args}', 512

    @app.teardown_appcontext
    def teardown_db(arg):
        close_db()

    return app
