import click
import json
from flask import current_app, g
from flask.cli import AppGroup

import psycopg2 as psy
import itertools as it
import psycopg2.sql as sql
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
           code        UUID PRIMARY KEY,
           description JSONB);
    CREATE TABLE  nodes (
           code        UUID PRIMARY KEY,
           stationcode UUID NOT NULL,
           geom        GEOMETRY,
           description JSONB);
    CREATE TABLE sensors (
           code        UUID PRIMARY KEY,
           stypecode   UUID NOT NULL,
           nodecode    UUID NOT NULL,
           geom        GEOMETRY,
           description JSONB);
    CREATE TABLE measures (
           time       TIMESTAMPTZ NOT NULL,
           sensorcode UUID        NOT NULL,
           value      REAL,
           url        TEXT,
           indx       INT4);
    SELECT create_hypertable('measures', 'time');
    CREATE INDEX measures_sensor_index on measures(sensorcode);
    """
    with db:
        with db.cursor() as cur:
            cur.execute(SQL)


def take_by_n(a, n):
    c = it.cycle(range(2*n))
    for k, g in it.groupby(a, lambda _: next(c) < n):
        yield [_ for _ in g]


def format_to_sql_tuple(t):
    "Convert tuple t to an SQL.Composable."
    return sql.SQL("({})").format(sql.SQL(', ').join(
        sql.Literal(v) for v in t))


def load_data_by_chunks(db, data, chunk_size, into, format_to_sql_tuple):
    values = take_by_n(data, chunk_size)
    with db:
        with db.cursor() as cur:
            for v in values:
                s = sql.SQL(into) + sql.SQL(' VALUES ')
                s += sql.SQL(', ').join(format_to_sql_tuple(_) for _ in v)
                cur.execute(s)


def load_sensor_types(db, data, validate=False, chunk_size=10000):
    """
    Load sensor_types objects.
    """
    def fix_json(d):
        return format_to_sql_tuple((d['uuid'], json.dumps(d)))
    logger.debug('load_sensor_types: start loading %d sensor_types', len(data))
    into = "INSERT INTO sensor_types (code, description)"
    load_data_by_chunks(db, data, chunk_size, into, fix_json)
    logger.debug('load_sensor_types: done.')
    return len(data)


def load_sensors(db, data, validate=False, chunk_size=10000):
    """
    Load sensors objects.
    """
    def fix_geom_and_json(d):
        return sql.SQL("({})").format(sql.SQL(', ').join([
            sql.Literal(d['uuid']),
            sql.Literal(d['stypecode']), sql.Literal(d['nodecode']),
            sql.SQL(
                "ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326), 3003)"
                % json.dumps(d['geometry'])),
            sql.Literal(json.dumps(d))]))
    into = "INSERT INTO sensors (code, stypecode, nodecode, geom, description)"
    logger.debug('load_sensors: start loading %d sensors', len(data))
    load_data_by_chunks(db, data, chunk_size, into, fix_geom_and_json)
    logger.debug('load_sensors: done.')
    return len(data)


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
    stats = {}
    with open(filename) as f:
        data = json.load(f)
    db = get_db()
    if 'sensor_types' in data:
        n = load_sensor_types(db, data['sensor_types'])
        stats['sensor_types'] = n
    if 'sensors' in data:
        n = load_sensors(db, data['sensors'])
        stats['sensors'] = n
    logger.debug('load_file: done.')
    return stats


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
        path = click.format_filename(filename)
        msg = 'Loading from {}.'.format(path)
        click.echo(msg)
        stats = load_file(path)
        click.echo('Loaded {}'.format(str(stats)))

    app.cli.add_command(db_cli)


def init_app(app):
    app.teardown_appcontext(close_db)
    add_db_cli(app)
