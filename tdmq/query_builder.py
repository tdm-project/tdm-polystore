import json
import datetime
import psycopg2.sql as sql
from psycopg2.extras import Json


def select_sensors(args):
    assert args['footprint']['type'] in ['circle']
    if args['footprint']['type'] == 'circle':
        return select_sensors_in_circle(args)


def select_sensor_types(args):
    """\
    E.g., dict(brandName="Acme", controlledProperty="humidity,temperature")
    """
    query = "".join([
        "SELECT description FROM sensor_types WHERE",
        " AND ".join(
            "(description->%s @> %s::jsonb)" for _ in args
        ),
    ])
    data = []
    for k, v in args.items():
        data.append(k)
        v = v.split(",")
        if len(v) == 1:
            v = v[0]
        data.append(Json(v))
    return query, data


def select_sensors_in_circle(args):
    SQL = """
    WITH spatial AS (
         SELECT code, stypecode, geom
         FROM sensors
         WHERE ST_DWithin(
                   geom,
                   ST_Transform(
                       ST_SetSRID(ST_GeomFromGeoJSON('%s'), 4326),
                       3003
                   ),
                   %s)
         ),
         selected AS (
         SELECT DISTINCT on (sensorcode) sensorcode FROM measures m
         WHERE m.time >= '%s' AND m.time < '%s'
               AND m.sensorcode IN (SELECT code FROM spatial)
    )
    SELECT row_to_json(t)
    FROM (
       SELECT spatial.code, spatial.stypecode,
           ST_AsGeoJSON(ST_Transform(spatial.geom, 4326)) geometry
       FROM spatial
       WHERE  spatial.code in (SELECT sensorcode FROM selected)
    ) t
    """ % (json.dumps(args['footprint']['center']),
           args['footprint']['radius'],
           args['after'], args['before'])
    return SQL


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
