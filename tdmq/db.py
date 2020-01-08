
import json
import logging
import psycopg2.extras
import psycopg2.sql as sql
import uuid

from psycopg2.sql import SQL

import tdmq.errors
import tdmq.db_manager

logger = logging.getLogger(__name__)

# need to register_uuid to enable psycopg2's conversion from Python UUID
# type to PgSQL
psycopg2.extras.register_uuid()

NAMESPACE_TDMQ = uuid.UUID('6cb10168-c65b-48fa-af9b-a3ca6d03156d')


def get_db():
    """
    Requires active application context.

    Connect to the application's configured database. The connection
    is unique for each request and will be reused if this is called
    again.
    """
    import flask
    if 'db' not in flask.g:
        db_settings = {
            'user': flask.current_app.config['DB_USER'],
            'password': flask.current_app.config['DB_PASSWORD'],
            'host': flask.current_app.config['DB_HOST'],
            'dbname': flask.current_app.config['DB_NAME'],
        }
        flask.g.db = tdmq.db_manager.db_connect(db_settings)
    return flask.g.db


def close_db():
    """
    If this request is connected to the database, close the
    connection.
    """
    import flask
    db = flask.g.pop('db', None)

    if db is not None:
        db.close()


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
    Possible args:
        'id'
        'entity_category'
        'entity_type'
        'tdmq_id'
        'controlledProperties'
        'after'
        'before'
        'roi'
        'limit'
        'offset'

    All applied conditions must match for an element to be returned.

    controlledProperties: value is an array, or None.  All specified
                          elements must be in the controlledProperties of the source.

    after and before:  specify temporal interal.  Specify any combination of the two.

    roi: value is GeoJSON with `center` and `radius`.  Tests on source.default_footprint.

    All other arguments are tested for equality.
    """
    select = SQL("""
        SELECT
            source.tdmq_id,
            source.external_id,
            ST_AsGeoJSON(ST_Transform(source.default_footprint, 4326))::json
                as default_footprint,
            source.entity_category,
            source.entity_type
        FROM source""")

    where = []
    limit = None
    offset = None

    # where clauses
    def add_where_lit(column, condition, literal):
        where.append(SQL(" ").join((SQL(column), SQL(condition), sql.Literal(literal))))

    if 'id' in args:
        add_where_lit('source.external_id', '=', args.pop('id'))
    if 'entity_category' in args:
        add_where_lit('source.entity_category', '=', args.pop('entity_category'))
    if 'entity_type' in args:
        add_where_lit('source.entity_type', '=', args.pop('entity_type'))
    if 'tdmq_id' in args:
        add_where_lit('source.tdmq_id', '=', args.pop('tdmq_id'))
    if 'stationary' in args:
        add_where_lit('source.stationary', 'is', (args.pop('stationary').lower() in {'t', 'true'}))
    if 'controlledProperties' in args:
        # require that all these exist in the controlledProperties array
        # This is the PgSQL operator: ?&  text[]   Do all of these array strings exist as top-level keys?
        required_properties = args.pop('controlledProperties')
        assert isinstance(required_properties, list)
        where.append(
            SQL("source.description->'controlledProperties' ?& array[ {} ]").format(
                SQL(', ').join([sql.Literal(p) for p in required_properties])))
    if 'roi' in args:
        fp = args.pop('roi')
        where.append(SQL(
            """
            ST_DWithin(
                source.default_footprint,
                ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON({}), 4326), 3003),
            {})""").format(
                sql.Literal(json.dumps(fp['center'])),
                sql.Literal(fp['radius'])))

    if args.keys() & {'after', 'before'}:  # actually, for mobile sensors we'll also have to add 'footprint'
        in_subquery = SQL("""
          source.tdmq_id IN (
            SELECT record.source_id
            FROM record
            WHERE {}
          )""")
        interval = []
        if 'after' in args:
            interval.append(SQL("record.time >= {}").format(sql.Literal(args.pop('after'))))
        if 'before' in args:
            interval.append(SQL("record.time < {}").format(sql.Literal(args.pop('before'))))

        where.append(in_subquery.format(SQL(" AND ").join(interval)))

    if 'limit' in args:
        limit = sql.Literal(args.pop('limit'))
    if 'offset' in args:
        offset = sql.Literal(args.pop('offset'))

    if args:  # not empty, so we have additional filtering attributes to apply to description
        logger.debug("Left over args for JSON query: %s", args)
        for k, v in args.items():
            term = {"description": {k: v}}
            where.append(SQL('source.description @> {}::jsonb').format(sql.Literal(json.dumps(term))))

    query = select
    if where:
        query += SQL(' WHERE ') + SQL(' AND ').join(where)

    if limit or offset:
        query += SQL(' ORDER BY source.tdmq_id ')
        if limit:
            query += SQL(' LIMIT ') + limit
        if offset:
            query += SQL(' OFFSET ') + offset

    try:
        return query_db_all(query, cursor_factory=psycopg2.extras.RealDictCursor)
    except psycopg2.OperationalError:
        raise tdmq.errors.DBOperationalError


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


def list_entity_categories(category_start=None):
    q = sql.SQL("""
      SELECT entity_category
      FROM entity_category""")

    if category_start:
        starts_with = sql.SQL("starts_with(lower(entity_category), {})").format(sql.Literal(category_start.lower()))
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
        where.append(sql.SQL("starts_with(lower(entity_category), {})").format(sql.Literal(category_start.lower())))
    if type_start:
        where.append(sql.SQL("starts_with(lower(entity_type), {})").format(sql.Literal(type_start.lower())))

    if where:
        q = q + sql.SQL(" WHERE ") + sql.SQL(' AND ').join(where)

    return query_db_all(q, cursor_factory=psycopg2.extras.RealDictCursor)


def dump_table(conn, tname, path, itersize=100000):
    query = sql.SQL('SELECT row_to_json({0}) from {0}').format(
        sql.Identifier(tname)
    )
    first = True
    counter = 0
    with open(path, 'w') as f:
        f.write('{"%s": [\n' % tname)
        with conn:
            with conn.cursor('dump_cursor') as cur:
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


def load_sources(data, validate=False, chunk_size=500):
    return load_sources_conn(get_db(), data, validate, chunk_size)


def load_sources_conn(conn, data, validate=False, chunk_size=500):
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
        return (tdmq_id, external_id, psycopg2.extras.Json(footprint), stationary, entity_cat, entity_type, psycopg2.extras.Json(d))

    logger.debug('load_sources: start loading %d sources', len(data))
    tuples = [gen_source_tuple(t) for t in data]
    sql = """
          INSERT INTO source
              (tdmq_id, external_id, default_footprint, stationary, entity_category, entity_type, description)
              VALUES %s"""
    template = "(%s, %s, ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), 3003), %s, %s, %s, %s)"
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, tuples, template=template, page_size=chunk_size)
    except psycopg2.errors.UniqueViolation as e:
        logger.debug(e)
        raise tdmq.errors.DuplicateItemException(f"{e.pgerror}\n{e.diag.message_detail}")

    logger.debug('load_sources: done.')
    return [t[0] for t in tuples]


def load_records(records, validate=False, chunk_size=500):
    return load_records_conn(get_db(), records, validate, chunk_size)


def load_records_conn(conn, records, validate=False, chunk_size=500):
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
    def get_required_internal_source_id_map(cursor, data):
        external_ids = tuple(set(d['source'] for d in data if 'tdmq_id' not in d))
        if external_ids:
            #  If we get a lot of external_ids, using the IN clause might not be so efficient
            q = "SELECT external_id, tdmq_id FROM source WHERE external_id IN %s"
            cursor.execute(q, (external_ids,))
            # The following `fetchall` and transforming the result to a dict is also at risk
            # of explosion
            map_external_to_tdm_id = dict(cur.fetchall())  # creates a mapping external_ids -> tdmq_id
        else:
            map_external_to_tdm_id = dict()

        return map_external_to_tdm_id

    def gen_record_tuple(d, id_to_tdmq_id):
        s_time = d['time']
        tdmq_id = d['tdmq_id'] if 'tdmq_id' in d else id_to_tdmq_id[d['source']]
        footprint = json.dumps(d.get('footprint')) if d.get('footprint') else None

        return (s_time, tdmq_id, footprint, psycopg2.extras.Json(d['data']))

    sql = "INSERT INTO record (time, source_id, footprint, data) VALUES %s"
    template = "(%s, %s, ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), 3003), %s)"
    with conn:
        with conn.cursor() as cur:
            id_to_tdmq_id = get_required_internal_source_id_map(cur, records)
            tuples = [gen_record_tuple(t, id_to_tdmq_id) for t in records]
            logger.debug('load_records: start loading %d records', len(records))
            psycopg2.extras.execute_values(cur, sql, tuples, template=template, page_size=chunk_size)

    logger.debug('load_records: done.')
    return len(records)


loader = {}
loader['sources'] = load_sources
loader['records'] = load_records


def load_file(filename):
    """Load objects from a json file."""
    logger.debug('load_file: start')
    stats = {}

    with open(filename) as f:
        data = json.load(f)

    for k in loader.keys():
        if k in data:
            rval = loader[k](data[k])
            try:
                n = len(rval)
            except TypeError:
                n = rval  # records
            stats[k] = n
    logger.debug('load_file: done.')
    return stats


def dump_field(field, path):
    """Dump all record of field to file path"""
    conn = get_db()
    return dump_table(conn, field, path, itersize=100000)


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
    select_list = [sql.SQL("EXTRACT(epoch FROM record.time), record.footprint")]
    # select_list.append( sql.SQL("record.time, record.footprint") )
    select_list.extend(
        [sql.SQL("data->{} AS {}").format(sql.Literal(field), sql.Identifier(field))
            for field in properties])

    grouping_clause = sql.SQL(" ORDER BY record.time ASC ")

    return dict(select_list=sql.SQL(", ").join(select_list), grouping_clause=grouping_clause)


def _bucketed_timeseries_select(properties, bucket_interval, bucket_op):
    select_list = []
    # select_list.append( sql.SQL("time_bucket({}, record.time) AS time_bucket").format(sql.Literal(bucket_interval)) )
    select_list.append(sql.SQL("EXTRACT(epoch FROM time_bucket({}, record.time)) AS time_bucket").format(sql.Literal(bucket_interval)))
    select_list.append(sql.SQL("ST_Collect(record.footprint) AS footprint_centroid"))

    if bucket_op == 'string_agg':
        operation_args = "(data->>{}), ','"
    elif bucket_op == 'jsonb_agg':
        operation_args = "(data->{})"
    else:
        operation_args = "(data->{})::real"
    access_template = "{}( " + operation_args + " ) AS {}"

    select_list.extend(
        [sql.SQL(access_template).format(
            sql.Identifier(bucket_op),
            sql.Literal(field),
            sql.Identifier(f"{bucket_op}_{field}"))
         for field in properties])

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
        if args and args.get('fields', []):
            fields = args['fields']
            # keep the order specified in fields
            properties = [f for f in fields if f in properties]
            if fields != properties:
                unknown_fields = ', '.join(set(fields).difference(properties))
                raise tdmq.errors.RequestException(f"The following field(s) requested for source do not exist: {unknown_fields}")
            # Convert properties from a set to a list, since order is important
            properties = list(properties)

    query_template = sql.SQL("""
        SELECT {select_list}
            FROM record
            WHERE {where_clause}
            {grouping_clause}""")

    if args and args.get('bucket'):
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

    where = [sql.SQL("source_id = {}").format(sql.Literal(tdmq_id))]

    if args and args.get('after'):
        where.append(sql.SQL("record.time >= {}").format(sql.Literal(args['after'])))
    if args and args.get('before'):
        where.append(sql.SQL("record.time < {}").format(sql.Literal(args['before'])))

    clauses['where_clause'] = sql.SQL(" AND ").join(where)

    query = query_template.format(**clauses)

    return dict(source_info=info, properties=properties, rows=query_db_all(query))


def add_db_cli(app):
    import flask
    import click
    db_cli = flask.cli.AppGroup('db')

    def conn_params():
        return {
            'host': flask.current_app.config.get('DB_HOST'),
            'port': flask.current_app.config.get('DB_PORT'),
            'user': flask.current_app.config['DB_USER'],
            'password': flask.current_app.config['DB_PASSWORD'],
            'dbname': flask.current_app.config['DB_NAME']
        }

    @db_cli.command('init')
    @click.option('--drop', default=False, is_flag=True)
    def db_init(drop):
        click.echo('Starting initialization process.')
        tdmq.db_manager.create_db(conn_params(), drop)
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
