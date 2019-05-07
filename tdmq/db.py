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

from tdmq.query_builder import select_sensors
from tdmq.query_builder import gather_scalar_timeseries

# FIXME build a better logging infrastructure
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.info('Logging is active.')


# FIXME move all of this to appropriate classes
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
           index      INT4);
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


def dump_table(db, tname, path, itersize=100000):
    # FIXME
    SQL = 'SELECT row_to_json({}) from {};'.format(tname, tname)
    first = True
    counter = 0
    with open(path, 'w') as f:
        f.write('{"%s": [\n' % tname)
        with db:
            with db.cursor('dump_cursor') as cur:
                cur.itersize = itersize
                cur.execute(SQL)
                for r in cur:
                    if first:
                        first = False
                    else:
                        f.write(',\n')
                    f.write(json.dumps(r[0]))
                    counter += 1
        f.write(']}\n')
    return counter


def load_sensor_types(db, data, validate=False, chunk_size=10000):
    """
    Load sensor_types objects.
    """
    def fix_json(d):
        return format_to_sql_tuple((d['code'], json.dumps(d)))
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
            sql.Literal(d['code']),
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


def load_measures(db, data, validate=False, chunk_size=10000):
    """
    Load measures.

    {"time": "2019-02-21T11:32:08Z",
     "sensorcode": "98359c6d-863a-4c94-a997-d0e5446a489f",
     "measure": {"value": 0.333}},
    {"time": "2019-02-21T11:34:08Z",
     "sensorcode": "98359c6d-863a-4c94-a997-d0e5446a489f",
     "measure": {"reference": "hdfs://xxxx", "index": 22}}
    """
    def fix_geom_and_json(d):
        gf = "ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326), 3003)"
        return sql.SQL("({})").format(sql.SQL(', ').join([
            sql.Literal(d['code']),
            sql.Literal(d['stypecode']), sql.Literal(d['nodecode']),
            sql.SQL(gf % json.dumps(d['geometry'])),
            sql.Literal(json.dumps(d))]))

    def fix_value(d):
        def fix_measure(m):
            # FIXME brute force rendering
            if 'reference' in m and m['reference'] is not None:
                return (None, m['reference'], m['index'])
            else:
                return (m['value'], None, None)
        return format_to_sql_tuple((d['time'], d['sensorcode']) +
                                   fix_measure(d['measure']))
    into = "INSERT INTO measures (time, sensorcode, value, url, index)"
    logger.debug('load_measures: start loading %d measures', len(data))
    load_data_by_chunks(db, data, chunk_size, into, fix_value)
    logger.debug('load_measures: done.')
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


loader = {}
loader['sensor_types'] = load_sensor_types
loader['sensors'] = load_sensors
loader['measures'] = load_measures


def load_file(filename):
    """Load objects from a json file."""
    logger.debug('load_file: start')
    stats = {}
    with open(filename) as f:
        data = json.load(f)
    db = get_db()
    for k in loader.keys():
        if k in data:
            n = loader[k](db, data[k])
            stats[k] = n
    logger.debug('load_file: done.')
    return stats


def dump_field(field, path):
    """Dump all record of field to file path"""
    db = get_db()
    return dump_table(db, field, path, itersize=100000)


def list_descriptions_in_table(db, tname):
    SQL = 'SELECT description from {};'.format(tname)
    # FIXME
    with db:
        with db.cursor() as cur:
            cur.execute(SQL)
            return [_[0] for _ in cur.fetchall()]


def list_sensor_types():
    """List known sensor_types"""
    db = get_db()
    return list_descriptions_in_table(db, 'sensor_types')


def list_sensors_in_db(db, args):
    if args is None:
        return list_descriptions_in_table(db, 'sensors')
    else:
        return list_sensors_in_cylinder(db, args)


def list_sensors(args):
    """Return all sensors that have reported an event in a
       given spatio-temporal region."""
    db = get_db()
    return list_sensors_in_db(db, args)


def list_sensors_in_cylinder(db, args):
    SQL = select_sensors(args)
    with db:
        with db.cursor() as cur:
            cur.execute(SQL)
            # FIXME
            return [_[0] for _ in cur.fetchall()]


def get_object(db, tname, oid):
    SQL = """SELECT description FROM {} t
             WHERE t.code = '{}';""".format(tname, oid)
    with db:
        with db.cursor() as cur:
            cur.execute(SQL)
            # FIXME
            return cur.fetchall()[0][0]


def get_sensor(sid):
    """Provide sensor sid description """
    db = get_db()
    return get_object(db, 'sensors', sid)


def get_sensor_type(sid):
    """Provide sensor_type sid description """
    db = get_db()
    return get_object(db, 'sensor_type', sid)


def get_scalar_timeseries_data(db, args):
    assert 'code' in args
    assert 'after' in args
    assert 'before' in args
    result = {'timebase': args['after'],
              'timedelta': [],
              'data': []}
    SQL = gather_scalar_timeseries(args)
    with db:
        with db.cursor() as cur:
            cur.execute(SQL, args)
            # FIXME
            tuples = cur.fetchall()
            for t in tuples:
                result['timedelta'].append(t[0].total_seconds())
                result['data'].append(t[1])
    return result


def get_timeseries(code, args=None):
    """
     {'timebase': '2019-02-21T11:03:25Z',
      'timedelta':[0.11, 0.22, 0.33, 0.44],
      'data': [12000, 12100, 12200, 12300]}
     :query after: consider only sensors reporting strictly after
                   this time, e.g., '2019-02-21T11:03:25Z'

     :query before: consider only sensors reporting strictly before
                    this time, e.g., '2019-02-22T11:03:25Z'

     :query bucket: time bucket for data aggregation, e.g., '20 min'

     :query op: aggregation operation on data contained in bucket,
                e.g., `sum`,  `average`, `count` FIXME.

    """
    db = get_db()
    sensor = get_object(db, 'sensors', code)
    args['code'] = code
    if sensor['geometry']['type'] == 'Point':
        return get_scalar_timeseries_data(db, args)
    else:
        raise ValueError(
            'timeseries on sensor {} are not supported'.format(code))


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

    @db_cli.command('dump')
    @click.argument('field')
    @click.argument('filename', type=click.Path(exists=False))
    def db_dump(field, filename):
        msg = 'Dumping {} to {}.'.format(field, filename)
        path = click.format_filename(filename)
        click.echo(msg)
        n = dump_field(field, path)
        click.echo('Dumped {} records'.format(n))

    app.cli.add_command(db_cli)


def init_app(app):
    app.teardown_appcontext(close_db)
    add_db_cli(app)
