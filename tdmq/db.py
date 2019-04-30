import click
from flask import current_app, g
from flask.cli import AppGroup

import psycopg2 as psy
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from datetime import datetime, timedelta

import logging


# FIXME build a better logging infrastructure
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info('Logging is active.')


def drop_and_create_db():
    logger.debug('drop_and_create_db:init')
    db_settings = {
        'user': current_app.config['DB_USER'],
        'password': current_app.config['DB_PASSWORD'],
        'host': current_app.config['DB_HOST'],
        'dbname': 'postgres'
        }
    logger.debug('drop_and_create_db:db_setting: {}'.format(db_settings))
    con = psy.connect(**db_settings)
    # FIXME we are breaking one of psycopg2 cardinal rules. However,
    # since we are creating the database, it should be ok.  To be
    # paranoid, we chould check if db_name does not contain SQL
    # commands.
    db_name = current_app.config['DB_NAME']
    logger.debug('drop_and_create_db:db_name: {}'.format(db_name))
    with con:
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with con.cursor() as cur:
            cur.execute('DROP DATABASE IF EXISTS {};'.format(db_name))
            cur.execute('CREATE DATABASE {}'.format(db_name))
    con.close()
    logger.debug('drop_and_create_db:done.')


def add_extensions(db):
    SQL = """
    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
    CREATE EXTENSION IF NOT EXISTS postgis;
    """
    with db:
        with db.cursor() as cur:
            cur.execute(SQL)


def add_tables(db):
    SQL = """
    CREATE TABLE  sensor_types (
           code        SERIAL PRIMARY KEY,
           id          UUID UNIQUE,
           description JSONB);
    CREATE TABLE  nodes (
           code        SERIAL PRIMARY KEY,
           id          UUID UNIQUE,
           stationcode INT4 NOT NULL,
           geom        GEOMETRY,
           description JSONB);
    CREATE TABLE sensors (
           code        SERIAL PRIMARY KEY,
           id          UUID UNIQUE,
           stypecode   INT4 NOT NULL,
           nodecode    INT4 NOT NULL,
           geom        GEOMETRY,
           description JSONB);
    CREATE TABLE measures (
           time       TIMESTAMPTZ NOT NULL,
           sensorcode INT4        NOT NULL,
           value      REAL,
           url        TEXT,
           indx       INT4);
    SELECT create_hypertable('measures', 'time');
    CREATE INDEX measures_sensor_index on measures(sensorcode);
    """
    with db:
        with db.cursor() as cur:
            cur.execute(SQL)


def get_db():
    """Connect to the application's configured database. The connection
    is unique for each request and will be reused if this is called
    again.
    """
    if 'db' not in g:
        db_settings = {
            'user': current_app.config['DB_USER'],
            'password': current_app.config['DB_PASSWORD'],
            'host': current_app.config['DB_HOST'],
            'dbname': current_app.config['DB_NAME'],
        }
        logger.debug('get_db:db_setting: {}'.format(db_settings))
        g.db = psy.connect(**db_settings)
    return g.db


def close_db(e=None):
    """If this request is connected to the database, close the
    connection.
    """
    db = g.pop('db', None)

    if db is not None:
        db.close()


def init_db():
    """Clear existing data and create new tables."""
    logger.debug('init_db: start')
    drop_and_create_db()
    logger.debug('init_db: db_created')
    db = get_db()
    add_extensions(db)
    add_tables(db)
    close_db()
    logger.debug('init_db: done')


def load_file(filename):
    """Load objects from a json file."""
    logger.debug('load_file: start')
    logger.debug('load_file: done.')


def list_sensor_types():
    """List known sensor_types"""
    pass


def list_sensors(args):
    """List known sensor_types"""
    pass


def get_sensor(sid):
    """Provide sensor sid description """
    pass


def get_timeseries(sid, args):
    """Provide  timeseries for sensor sid"""
    pass


def add_db_cli(app):
    db_cli = AppGroup('db')

    @db_cli.command('init')
    def db_init():
        click.echo('Starting initialization process.')
        init_db()
        click.echo('Initialized the database.')

    @db_cli.command('load')
    @click.argument('filename', type=click.Path(exists=True))
    def db_load(filename):
        msg = 'Loading from {}.'.format(click.format_filename(filename))
        click.echo(msg)
        stats = load_file(filename)
        click.echo('Loaded {}'.format(str(stats)))

    app.cli.add_command(db_cli)


def init_app(app):
    app.teardown_appcontext(close_db)
    add_db_cli(app)
