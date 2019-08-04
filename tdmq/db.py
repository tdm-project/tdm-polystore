import itertools as it
import json
import logging
import uuid

import click
import psycopg2 as psy
import psycopg2.extras
import psycopg2.sql as sql
from flask import current_app, g
from flask.cli import AppGroup
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import tdmq.errors
import tdmq.query_builder as qb

from tdmq.query_builder import gather_nonscalar_timeseries
from tdmq.query_builder import gather_scalar_timeseries
from tdmq.query_builder import select_sensors_by_roi

# FIXME build a better logging infrastructure
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.info('Logging is active.')

# need to register_uuid to enable psycopg2's conversion from Python UUID
# type to PgSQL
psycopg2.extras.register_uuid()

NAMESPACE_TDMQ = uuid.UUID('6cb10168-c65b-48fa-af9b-a3ca6d03156d')


def query_db_all(q, args=(), fetch=True, one=False, cursor_factory=None):
    with get_db() as db:
        with db.cursor(cursor_factory=cursor_factory) as cur:
            cur.execute(q, (args,))
            result = cur.fetchall() if fetch else None

    if one and result is not None:
        return result[0]
    else:
        return result


def list_sources(args):
    """
    args:
        'id'
        'entity_category'
        'entity_type'
        'tdmq_id'
        'controlledProperties'
        'after'
        'before'
        'roi'
    """
    query = qb.select_sources_helper(args)
    return query_db_all(query, cursor_factory=psycopg2.extras.RealDictCursor)


def get_sources(list_of_tdmq_ids):
    q = sql.SQL("""
        SELECT
            tdmq_id,
            external_id,
            ST_AsGeoJSON(ST_Transform(default_footprint, 4326))::json
                as default_footprint,
            stationary,
            entity_category,
            entity_type,
            description
        FROM source
        WHERE tdmq_id IN %s""")

    return query_db_all(q, tuple(list_of_tdmq_ids), cursor_factory=psycopg2.extras.RealDictCursor)


def delete_sources(list_of_tdmq_ids):
    q = sql.SQL("""
        DELETE FROM source
        WHERE tdmq_id IN %s""")
    query_db_all(q, tuple(list_of_tdmq_ids), fetch=False)
    return list_of_tdmq_ids


def list_entity_catories(category_start=None):
    q = sql.SQL("""
      SELECT entity_category
      FROM entity_category""")

    if category_start:
        starts_with = sql.SQL("starts_with(lower(entity_category), {}))").format(sql.Literal(category_start.lower()))
        q = q + sql.SQL(" WHERE ") + starts_with

    return query_db_all(q, cursor_factory=psycopg2.extras.RealDictCursor)


def list_entity_types(category_start=None, type_start=None):
    q = sql.SQL("""
      SELECT
        entity_category,
        entity_type,
        schema
      FROM entity_type""")

    where = []

    if category_start:
        where.append(sql.SQL("starts_with(lower(entity_category), {}))").format(sql.Literal(category_start.lower())))
    if type_start:
        where.append(sql.SQL("starts_with(lower(entity_type), {}))").format(sql.Literal(type_start.lower())))

    if where:
        q = q + sql.SQL(" WHERE ") + sql.SQL(' AND ').join(where)

    return query_db_all(q, cursor_factory=psycopg2.extras.RealDictCursor)


# FIXME move all of this to appropriate classes
def create_db(drop=False):
    logger.debug('drop_and_create_db:init')
    db_settings = {
        'user': current_app.config['DB_USER'],
        'password': current_app.config['DB_PASSWORD'],
        'host': current_app.config['DB_HOST'],
        'dbname': 'postgres'
    }
    logger.debug('drop_and_create_db:db_settings: %s', db_settings)
    con = psy.connect(**db_settings)
    db_name = sql.Identifier(current_app.config['DB_NAME'])
    logger.debug('drop_and_create_db:db_name: %s', db_name.string)
    with con:
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with con.cursor() as cur:
            if drop:
                cur.execute(
                    sql.SQL('DROP DATABASE IF EXISTS {}').format(db_name))
                cur.execute(sql.SQL('CREATE DATABASE {}').format(db_name))
            else:
                cur.execute(
                    'SELECT count(*) FROM pg_catalog.pg_database '
                    'WHERE datname = %s',
                    [current_app.config['DB_NAME']])
                if not cur.fetchone()[0]:
                    cur.execute(sql.SQL('CREATE DATABASE {}').format(db_name))

    con.close()
    logger.debug('drop_and_create_db:done.')


def add_extensions(db):
    SQL = """
    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
    CREATE EXTENSION IF NOT EXISTS postgis;
    CREATE EXTENSION IF NOT EXISTS citext;
    """
    with db:
        with db.cursor() as cur:
            cur.execute(SQL)


def add_tables(db):
    SQL = """
      CREATE TABLE entity_category (
          entity_category CITEXT PRIMARY KEY
      );

      CREATE TABLE entity_type (
          entity_category CITEXT REFERENCES entity_category(entity_category),
          entity_type CITEXT,
          schema JSONB,
          PRIMARY KEY (entity_category, entity_type)
      );

      CREATE TABLE source (
          tdmq_id UUID,
          external_id TEXT NOT NULL UNIQUE,
          default_footprint GEOMETRY NOT NULL,
          stationary BOOLEAN NOT NULL DEFAULT TRUE, -- source.stationary is true => record.geom is NULL
          entity_category CITEXT NOT NULL,
          entity_type CITEXT NOT NULL,
          description JSONB,
          PRIMARY KEY (tdmq_id),
          FOREIGN KEY (entity_category, entity_type) REFERENCES entity_type(entity_category, entity_type)
      );

      CREATE TABLE record (
          time TIMESTAMP(3) NOT NULL,
          source_id UUID NOT NULL REFERENCES source(tdmq_id) ON DELETE CASCADE,
          footprint GEOMETRY, -- source.stationary is true => record.footprint is NULL
          data JSONB NOT NULL
      );

      -- create the hypertable on record. Rather than using the default index, we create an
      -- index on (source_id, time DESC) as suggested by TimescaleDB best practices:
      -- https://docs.timescale.com/v1.2/using-timescaledb/schema-management#indexing-best-practices
      SELECT create_hypertable('record', 'time', create_default_indexes => FALSE, if_not_exists => TRUE);
      CREATE INDEX ON record (source_id, time DESC);
      CREATE INDEX ON source USING GIST (default_footprint);

      INSERT INTO entity_category VALUES
          ('Radar'),
          ('Satellite'),
          ('Simulation'),
          ('Station');

      INSERT INTO entity_type VALUES
          ('Radar', 'MeteoRadarMosaic'),
          ('Station', 'PointWeatherObserver'),
          ('Station', 'TemperatureMosaic'),
          ('Station', 'EnergyConsumptionMonitor')
          ;
    """
    with db:
        with db.cursor() as cur:
            cur.execute(SQL)


def take_by_n(a, n):
    c = it.cycle(range(2 * n))
    for k, g_ in it.groupby(a, lambda _: next(c) < n):
        yield [_ for _ in g_]


def format_to_sql_tuple(t):
    "Convert tuple t to an SQL.Composable."
    return sql.SQL("({})").format(sql.SQL(', ').join(
        sql.Literal(v) for v in t))


def dump_table(db, tname, path, itersize=100000):
    query = sql.SQL('SELECT row_to_json({0}) from {0}').format(
        sql.Identifier(tname)
    )
    first = True
    counter = 0
    with open(path, 'w') as f:
        f.write('{"%s": [\n' % tname)
        with db:
            with db.cursor('dump_cursor') as cur:
                cur.itersize = itersize
                cur.execute(query)
                for r in cur:
                    if first:
                        first = False
                    else:
                        f.write(',\n')
                    f.write(json.dumps(r[0]))
                    counter += 1
        f.write(']}\n')
    return counter


def load_sources(db, data, validate=False, chunk_size=500):
    """
    Load sensors objects.

    Return the list of UUIDs assigned to each object.
    """

    def gen_source_tuple(d):
        tdmq_id = uuid.uuid5(NAMESPACE_TDMQ, d['id'])
        external_id = d['id']
        entity_type = d['entity_type']
        entity_cat = d['entity_category']
        footprint = d['default_footprint']
        stationary = d.get('stationary', True)
        return ( tdmq_id, external_id, psycopg2.extras.Json(footprint), stationary, entity_cat, entity_type, psycopg2.extras.Json(d) )

    logger.debug('load_sources: start loading %d sources', len(data))
    tuples = [ gen_source_tuple(t) for t in data ]
    sql = """
          INSERT INTO source
              (tdmq_id, external_id, default_footprint, stationary, entity_category, entity_type, description)
              VALUES %s"""
    template = "(%s, %s, ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), 3003), %s, %s, %s, %s)"
    try:
        with db:
            with db.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, tuples, template=template, page_size=chunk_size)
                db.commit()
    except psycopg2.errors.UniqueViolation as e:
        logger.debug(e)
        raise tdmq.errors.DuplicateItemException(f"{e.pgerror}\n{e.diag.message_detail}")

    logger.debug('load_sources: done.')
    return [ t[0] for t in tuples ]


def load_records(db, records, validate=False, chunk_size=500):
    """
    Load records.

    Return the number of loaded objects.

    {"time": "2019-02-21T11:32:08Z",
     "source": "sensor_0",
     "data": {"prop1": 0.333}},
    {"time": "2019-02-21T11:34:08Z",
     "source": "sensor_1",
     "data": {"reference": "hdfs://xxxx", "index": 22}},
    {"time": "2019-02-21T12:14:01Z",
     "source": "sensor_3",
     "footprint": {"type": "Point", "coordinates": [9.222, 30.003]},
     "data": {"something": 42 }
    """
    def add_internal_source_ids(cursor, data):
        external_ids = tuple(set(d['source'] for d in data if 'tdmq_id' not in d))
        #  If we get a lot of external_ids, using the IN clause might not be so efficient
        q = "SELECT external_id, tdmq_id FROM source WHERE external_id IN %s"
        cursor.execute(q, (external_ids,))
        # The following `fetchall` and transforming the result to a dict is also at risk
        # of explosion
        map_external_to_tdm_id = dict(cur.fetchall())
        logging.debug("Fetched map_external_to_tdm_id: %s", map_external_to_tdm_id)
        for d in data:
            if 'tdmq_id' not in d:
                d['tdmq_id'] = map_external_to_tdm_id[ d['source'] ]
        return data

    def gen_record_tuple(d):
        s_time = d['time']
        tdmq_id = d['tdmq_id']
        return (s_time, tdmq_id, json.dumps(d.get('footprint')) if d.get('footprint') else None, psycopg2.extras.Json(d['data']))

    sql = "INSERT INTO record (time, source_id, footprint, data) VALUES %s"
    template = "(%s, %s, ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), 3003), %s)"
    with db:
        with db.cursor() as cur:
            tuples = [ gen_record_tuple(t) for t in add_internal_source_ids(cur, records) ]
            logger.debug('load_records: start loading %d records', len(records))
            psycopg2.extras.execute_values(cur, sql, tuples, template=template, page_size=chunk_size)
            db.commit()

    logger.debug('load_records: done.')
    return len(records)


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
        logger.debug('get_db:db_setting: %s', db_settings)
        g.db = psy.connect(**db_settings)
    return g.db


def close_db(e=None):
    """If this request is connected to the database, close the
    connection.
    """
    db = g.pop('db', None)

    if db is not None:
        db.close()


def init_db(drop=False):
    """Clear existing data and create new tables."""
    logger.debug(f'init_db: start drop {drop}')
    create_db(drop)
    logger.debug('init_db: db_created')
    db = get_db()
    add_extensions(db)
    add_tables(db)
    close_db()
    logger.debug('init_db: done')


loader = {}
loader['sources'] = load_sources
loader['records'] = load_records


def load_file(filename):
    """Load objects from a json file."""
    logger.debug('load_file: start')
    stats = {}
    with open(filename) as f:
        data = json.load(f)
    db = get_db()
    for k in loader.keys():
        if k in data:
            rval = loader[k](db, data[k])
            try:
                n = len(rval)
            except TypeError:
                n = rval  # records
            stats[k] = n
    logger.debug('load_file: done.')
    return stats


def dump_field(field, path):
    """Dump all record of field to file path"""
    db = get_db()
    return dump_table(db, field, path, itersize=100000)


def list_sensors_in_cylinder(db, args):
    """Return all sensors that have reported an event in a
       given spatio-temporal region."""
    query, data = select_sensors_by_roi(args)
    with db:
        with db.cursor() as cur:
            cur.execute(query, data)
            return cur.fetchall()


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


def get_tiledb_timeseries_data(db, args):
    assert 'code' in args
    assert 'after' in args
    assert 'before' in args
    result = {'timebase': args['after'],
              'timedelta': [],
              'data': []}
    SQL = gather_nonscalar_timeseries(args)
    with db.cursor() as cur:
        cur.execute(SQL, args)
        # FIXME in principle, it could blow up, but it is unlikely
        tuples = cur.fetchall()
    for t in tuples:
        result['timedelta'].append(t[0].total_seconds())
        result['data'].append(t[1:])
    return result


supported_bucket_ops = {
    "avg",
    "count_records",
    "count_values",
    "max",
    "min",
    "sum",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "string_agg",
    "variance",
    "var_pop",
    "var_samp"
}


def _get_source_description(tdmq_id):
    q = sql.SQL("""
        SELECT
            source.description
        FROM source
        WHERE tdmq_id = %s""")
    row = query_db_all(q, args=(tdmq_id,), one=True)
    if row is None:
        raise tdmq.errors.ItemNotFoundException(f"tdmq_id {tdmq_id} not found in DB")

    return row[0]


def _timeseries_select(properties):
    select_list = [ sql.SQL("EXTRACT(epoch FROM record.time), record.footprint") ]
    # select_list.append( sql.SQL("record.time, record.footprint") )
    select_list.extend(
        [ sql.SQL("data->{} AS {}").format(sql.Literal(field), sql.Identifier(field))
            for field in properties ])

    grouping_clause = sql.SQL(" ORDER BY record.time ASC ")

    return dict(select_list=sql.SQL(", ").join(select_list), grouping_clause=grouping_clause)


def _bucketed_timeseries_select(properties, bucket_interval, bucket_op):
    select_list = []
    # select_list.append( sql.SQL("time_bucket({}, record.time) AS time_bucket").format(sql.Literal(bucket_interval)) )
    select_list.append( sql.SQL("EXTRACT(epoch FROM time_bucket({}, record.time)) AS time_bucket").format(sql.Literal(bucket_interval)) )
    select_list.append( sql.SQL("ST_Collect(record.footprint) AS footprint_centroid") )

    if bucket_op == 'string_agg':
        operation_args = "(data->>{}), ','"
    elif bucket_op == 'jsonb_agg':
        operation_args = "(data->{})"
    else:
        operation_args = "(data->{})::real"
    access_template = "{}( " + operation_args + " ) AS {}"

    select_list.extend(
        [ sql.SQL(access_template).format(
            sql.Identifier(bucket_op),
            sql.Literal(field),
            sql.Identifier(f"{bucket_op}_{field}"))
          for field in properties ] )

    grouping_clause = sql.SQL("""
        GROUP BY time_bucket
        ORDER BY time_bucket ASC""")

    return dict(select_list=sql.SQL(", ").join(select_list), grouping_clause=grouping_clause)


def get_timeseries(tdmq_id, args=None):
    """
     :query after: consider only sensors reporting strictly after
                   this time, e.g., '2019-02-21T11:03:25Z'

     :query before: consider only sensors reporting strictly before
                    this time, e.g., '2019-02-22T11:03:25Z'

     :query bucket: time bucket for data aggregation, e.g., '20 min'

     :query op: aggregation operation on data contained in bucket. One of the
                values in supported_bucket_ops

     :query fields: list of controlledProperties from the source,
                    or nothing to select all of them.

     :returns: array of arrays: time, footprint, field+
               Fields are in the same order as specified in args.
    """

    info = _get_source_description(tdmq_id)

    if info.get('shape'):
        properties = ['tiledb_index']
    else:
        properties = info['controlledProperties']
        if args.get('fields', []):
            fields = set(args['fields'])
            properties = set(properties) & fields
            if fields != properties:
                unknown_fields = ', '.join(fields - properties)
                raise tdmq.errors.RequestException(f"The following field(s) requested for source do not exist: {unknown_fields}")

    query_template = sql.SQL("""
        SELECT {select_list}
            FROM record
            WHERE {where_clause}
            {grouping_clause}""")

    if args.get('bucket'):
        bucket_interval = args['bucket']

        if info.get('shape'):
            bucket_op = 'jsonb_agg'
        else:
            bucket_op = args['op']
            if bucket_op not in supported_bucket_ops:
                raise tdmq.errors.RequestException(f"Unsupported bucketing operation '{bucket_op}'")

        clauses = _bucketed_timeseries_select(properties, bucket_interval, bucket_op)
    else:
        bucket_op = None
        clauses = _timeseries_select(properties)

    where = [ sql.SQL("source_id = {}").format(sql.Literal(tdmq_id)) ]

    if args.get('after'):
        where.append( sql.SQL("record.time >= {}").format(sql.Literal(args['after'])) )
    if args.get('before'):
        where.append( sql.SQL("record.time < {}").format(sql.Literal(args['before'])) )

    clauses['where_clause'] = sql.SQL(" AND ").join(where)

    query = query_template.format(**clauses)

    return info, properties, query_db_all(query)


def add_db_cli(app):
    db_cli = AppGroup('db')

    @db_cli.command('init')
    @click.option('--drop', default=False, is_flag=True)
    def db_init(drop):
        click.echo('Starting initialization process.')
        init_db(drop)
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
