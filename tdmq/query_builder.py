import datetime
import json

import psycopg2.sql as sql
from psycopg2.extras import Json


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


def select_sensors(args):
    return filter_by_description('sensors', args)


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
        select = sql.SQL(
            "SELECT time_bucket({}, time) - {} as dt, array_agg(url) as u, array_agg(index) as i").format(
                sql.Placeholder(name='bucket'), sql.Placeholder(name='after'))
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


def gather_nonscalar_timeseries(args):
    assert args['code'] is not None
    assert args['after'] is not None
    assert args['before'] is not None

    if args['bucket'] is not None:
        assert isinstance(args['bucket'], datetime.timedelta)
        select = sql.SQL(
            "SELECT time_bucket({}, time) - {} as dt, array_agg(url) as u, array_agg(index) as i").format(
                sql.Placeholder(name='bucket'), sql.Placeholder(name='after'))
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
