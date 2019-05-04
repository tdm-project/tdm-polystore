def select_sensors(args):
    assert args['footprint']['type'] in ['circle']
    if args['footprint']['type'] == 'circle':
        return select_sensors_in_circle(args)


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
         WHERE m.time > %s AND m.time < %s
               AND m.sensorcode IN (SELECT code FROM spatial)
    )
    SELECT spatial.code, spatial.stypecode,
           ST_GeoJSONFromGeom(ST_Transform(spatial.geom, 4326))
    FROM spatial
    WHERE  spatial.code in (SELECT sensorcode FROM selected);
    """.format(args['footprint']['center'], args['footprint']['radius'],
               args['after'], args['before'])
    return SQL
