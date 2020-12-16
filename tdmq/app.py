

import atexit
import logging
import os
import secrets

import flask
from flask.json import jsonify
import werkzeug.exceptions as wex
from prometheus_client.registry import REGISTRY, CollectorRegistry
from prometheus_client.metrics import Histogram
from prometheus_client.utils import INF

from tdmq.api import tdmq_bp
from tdmq.db import add_db_cli, close_db
from .loc_anonymizer import loc_anonymizer

## This is the best way I've found to close the DB connect when the application exits.
atexit.register(close_db)

DEFAULT_PREFIX = '/api/v0.0'


ERROR_CODES = {
    400: "bad_request",
    401: "unauthorized",
    403: "forbidden",
    404: "not_found",
    405: "method_not_allowed",
    409: "duplicated_resource",
    500: "error_retrieving_data"
}


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


def configure_prometheus_registry(app):
    if app.config.get('PROMETHEUS_REGISTRY', False) is True:
        registry = CollectorRegistry(auto_describe=True)
    else:
        registry = REGISTRY

    app.http_request_prom = Histogram('tdmq_http_requests_seconds', 'Elapsed time to process http requests',
                                      ['method', 'endpoint'], buckets=[.5, 1, 5, 10, 20, INF], registry=registry)
    app.http_response_prom = Histogram('tdmq_http_response_bytes', 'Size in bytes of an http response',
                                       ['method', 'endpoint'], buckets=[1, 2, 5, 10, 20, INF], registry=registry)


class DefaultConfig(object):
    SECRET_KEY = 'dev'

    APP_PREFIX = DEFAULT_PREFIX

    DB_HOST = 'timescaledb'
    DB_NAME = 'tdmqtest'
    DB_USER = 'postgres'
    DB_PASSWORD = 'foobar'

    LOG_LEVEL = "INFO"

    TILEDB_VFS_ROOT = "s3://tdm-public/"
    TILEDB_VFS_CONFIG = {
        "vfs.s3.endpoint_override": "minio:9000",
        "vfs.s3.scheme": "http",
        "vfs.s3.region": "",
        "vfs.s3.verify_ssl": "false",
        "vfs.s3.use_virtual_addressing": "false",
        "vfs.s3.use_multipart_upload": "false",
    }
    TILEDB_VFS_CREDENTIALS = {
        "vfs.s3.aws_access_key_id": "tdm-user",
        "vfs.s3.aws_secret_access_key": "tdm-user-s3",
    }

    AUTH_TOKEN = secrets.token_urlsafe(32)

    # TileDB has tons of client configuration properties.
    # See https://docs.tiledb.com/main/solutions/tiledb-embedded/examples/configuration-parameters
    # for a full list.
    #
    # TileDB configuration properties relevant to S3.
    # See https://docs.tiledb.com/main/solutions/tiledb-embedded/backends/s3
    # for the docs, but you'll need to print out a tiledb configuration
    # (e.g., on the python shell) to see the full list.
    #
    # "vfs.s3.aws_access_key_id"
    # "vfs.s3.aws_secret_access_key"
    #
    # Basic access:
    # "vfs.s3.scheme" "https"
    # "vfs.s3.region" "us-east-1"
    # "vfs.s3.endpoint_override" ""
    # "vfs.s3.use_virtual_addressing" "true"
    #
    # Temporary security token:
    # "vfs.s3.aws_session_token" (session token corresponding to the configured key/secret pair)
    #
    # For debugging purposes it is possible to disable SSL/TLS certificate verification:
    # "vfs.s3.verify_ssl" ["false"], "true"


def create_app(test_config=None):
    app = flask.Flask(__name__, instance_relative_config=True)

    app.config.from_object(DefaultConfig)

    if test_config is not None:
        app.config.from_mapping(test_config)
    elif 'TDMQ_FLASK_CONFIG' in os.environ:
        app.config.from_envvar('TDMQ_FLASK_CONFIG')

    # Strip any trailing slashes from the prefix
    app.config['APP_PREFIX'] = app.config['APP_PREFIX'].rstrip('/')

    configure_logging(app)

    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    if 'TDMQ_AUTH_TOKEN' in os.environ:
        if os.environ['TDMQ_AUTH_TOKEN']:
            app.config['AUTH_TOKEN'] = os.environ['TDMQ_AUTH_TOKEN']
            app.logger.info("Setting TDM-q access token from environment variable TDMQ_AUTH_TOKEN")
        else:
            app.logger.warning("Ignoring empty token value in TDMQ_AUTH_TOKEN environment variable")

    app.logger.info("The access token is %s", app.config['AUTH_TOKEN'])

    add_db_cli(app)
    configure_prometheus_registry(app)
    loc_anonymizer.init_app(app)

    app.register_blueprint(tdmq_bp, url_prefix=app.config['APP_PREFIX'])

    @app.errorhandler(wex.HTTPException)
    def handle_errors(e):
        return jsonify({"error": ERROR_CODES.get(e.code)}), e.code

    return app
