import datetime
import json

import psycopg2.sql as sql
from psycopg2.sql import SQL
from psycopg2.extras import Json

import logging
logger = logging.getLogger(__name__)


def select_sensors_by_footprint(args):
    assert args['footprint']['type'] in ['circle']
    if args['footprint']['type'] == 'circle':
        return select_sensors_in_circle(args)


def filter_by_description(table_name, args):
    """\
    E.g., dict(brandName="Acme", controlledProperty="humidity,temperature")
    """
    qstart = sql.SQL("SELECT code, description FROM {} WHERE").format(
        sql.Identifier(table_name))
    query = qstart + sql.SQL(" AND ").join(
        sql.SQL("(description->{} @> {}::jsonb)").format(
            sql.Placeholder(), sql.Placeholder())
        for _ in args)
    data = []
    for k, v in args.items():
        data.append(k)
        v = v.split(",")
        if len(v) == 1:
            v = v[0]
        data.append(Json(v))
    return query, data


def select_sensor_types(args):
    return filter_by_description('sensor_types', args)


def select_sources_helper(db, args):
    """
    Possible args:
        'id'
        'category'
        'type'
        'tdmq_id'
        'controlledProperties'
        'after'
        'before'
        'footprint'

    All applied conditions must match for an element to be returned.

    controlledProperties: value is an array.  All specified elements my be in the controlledProperties
    of the source.

    after and before:  specify temporal interal.  Specify any combination of the two.

    footprint: value is GeoJSON with `center` and `radius`.  Tests on source.default_footprint.

    All other arguments are tested for equality.
    """
    select = SQL("""
        SELECT
            source.tdmq_id,
            source.external_id,
            source.default_footprint,
            source.entity_category,
            source.entity_type
        FROM source""")

    where = []

    # where clauses
    def add_where_lit(column, condition, literal):
        where.append(SQL(" ").join( (SQL(column), SQL(condition), sql.Literal(literal)) ))

    if 'id' in args:
        add_where_lit('source.external_id', '=', args.pop('id'))
    if 'category' in args:
        add_where_lit('source.entity_category', '=', args.pop('category'))
    if 'type' in args:
        add_where_lit('source.entity_type', '=', args.pop('type'))
    if 'tdmq_id' in args:
        add_where_lit('source.tdmq_id', '=', args.pop('tdmq_id'))
    if 'stationary' in args:
        add_where_lit('source.stationary', 'is', (args.pop('stationary').lower() in { 't', 'true' }))
    if 'controlledProperties' in args:
        # require that all these exist in the controlledProperties array
        # This is the PgSQL operator: ?&  text[]   Do all of these array strings exist as top-level keys?
        required_properties = args.pop('controlledProperties')
        logger.debug("required_properties is %s", required_properties)
        assert isinstance(required_properties, list)
        where.append(
            SQL("source.description->'controlledProperties' ?& array[ {} ]").format(
                SQL(', ').join([ sql.Literal(p) for p in required_properties ])))
    if 'footprint' in args:
        fp = args.pop('footprint')
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

        where.append(in_subquery.format( SQL(" AND ").join(interval) ))

    if args:  # not empty, so we have additional filtering attributes to apply to description
        logger.debug("Left over args for JSON query: %s", args)
        for k, v in args.items():
            term = { "description": { k: v } }
            where.append(SQL('source.description @> {}::jsonb').format(sql.Literal(json.dumps(term))))

    query = select
    if where:
        query += SQL(' WHERE ') + SQL(' AND ').join(where)

    return query


def select_sensors_in_circle(args):
    query = """
    WITH temporal AS (
      SELECT DISTINCT on (sensorcode) sensorcode FROM measures m
      WHERE m.time >= %s AND m.time < %s
    )
    SELECT code, description FROM sensors t
    WHERE ST_DWithin(
      t.geom, ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), 3003), %s)
    AND code IN (SELECT sensorcode FROM temporal)
    """
    data = [
        args['after'],
        args['before'],
        json.dumps(args['footprint']['center']),
        args['footprint']['radius'],
    ]
    return query, data


def gather_scalar_timeseries(args):
    assert args['code'] is not None
    assert args['after'] is not None
    assert args['before'] is not None

    if args['bucket'] is not None:
        assert isinstance(args['bucket'], datetime.timedelta)
        assert args['op'] is not None
        select = sql.SQL(
            "SELECT time_bucket({}, time) - {} as dt, {}(value) as v").format(
            sql.Placeholder(name='bucket'), sql.Placeholder(name='after'),
            sql.Identifier(args['op']))
        group_by = sql.SQL("GROUP BY dt ORDER BY dt")
    else:
        select = sql.SQL(
            "SELECT time - {} as dt, value as v").format(
            sql.Placeholder(name='after'))
        group_by = sql.SQL(" ")
    return sql.SQL(' ').join([select,
                              sql.SQL(
                                  """FROM measures m
                                  WHERE m.sensorcode = {}
                                  AND m.time >= {} AND m.time < {}""").format(
                                  sql.Placeholder(name='code'),
                                  sql.Placeholder(name='after'),
                                  sql.Placeholder(name='before')),
                              group_by])


def gather_nonscalar_timeseries(args):
    assert args['code'] is not None
    assert args['after'] is not None
    assert args['before'] is not None

    if args['bucket'] is not None:
        assert isinstance(args['bucket'], datetime.timedelta)
        qstr = ("SELECT time_bucket({}, time) - {} as dt, "
                "array_agg(url) as u, array_agg(index) as i")
        select = sql.SQL(qstr).format(
            sql.Placeholder(name='bucket'), sql.Placeholder(name='after')
        )
        group_by = sql.SQL("GROUP BY dt ORDER BY dt")
    else:
        select = sql.SQL(
            "SELECT time - {} as dt, url as u, index as i").format(
                sql.Placeholder(name='after'))
        group_by = sql.SQL(" ")
    return sql.SQL(' ').join([select,
                              sql.SQL(
                                  """FROM measures m
                                  WHERE m.sensorcode = {}
                                  AND m.time >= {} AND m.time < {}""").format(
                                      sql.Placeholder(name='code'),
                                      sql.Placeholder(name='after'),
                                      sql.Placeholder(name='before')),
                              group_by])
