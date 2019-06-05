from flask import request
from flask import url_for
from flask import jsonify
from datetime import timedelta
import tdmq.db as db
from tdmq.utils import convert_footprint


def add_routes(app):
    @app.route('/')
    def index():
        return 'The URL for this page is {}'.format(url_for('index'))

    @app.route('/sensor_types')
    def sensor_types():
        """Return a list of sensor types.

        With no parameters, return all sensor types. Parameters can be used to
        filter sensor types according to one or more attributes.

        .. :quickref: Get collection of sensor types.

        **Example request**::

          GET /sensor_types?controlledProperty=temperature HTTP/1.1

        **Example response**:

        .. sourcecode:: http

          HTTP/1.1 200 OK
          Content-Type: application/json

          [
           {
             "uuid": "0fd67c67-c9be-45c6-9719-4c4eada4be65",
             "type": "TemperatureSensorDTH11",
             "name": "temperature sensor in DHT11",
             "brandName": "Acme",
             "modelName": "Acme multisensor DHT11",
             "manufacturerName": "Acme Inc.",
             "category": ["sensor"],
             "function": ["sensing"],
             "controlledProperty": ["temperature"]
           }
          ]

        :resheader Content-Type: application/json
        :status 200: no error
        :returns: list of sensor types
        """
        res = db.list_sensor_types(request.args)
        return jsonify(res)

    @app.route('/sensors')
    def sensors():
        """Return the collection of sensor that have reported an event in a
           given spatio-temporal region.

           The spatio-temporal domain is expressed as a cylinder with
           a given geometrical footprint and a time interval.

           Calling without arguments will return all available sensors.

        .. :quickref: Get collection of reporting sensors.

        **Example request**::

          GET /sensors?footprint=circle((9.2, 33.0), 1000) HTTP/1.1

        **Example response**:

        .. sourcecode:: http

          HTTP/1.1 200 OK
          Content-Type: application/json

          [
           {"code": "0fd67c67-c9be-45c6-9719-4c4eada4becc",
            "stypecode": "0fd67c67-c9be-45c6-9719-4c4eada4be65",
            "geometry": {"type": "Point", "coordinates": [9.3, 30.0]},
           },
           {"code": "0fd67c67-c9be-45c6-9719-4c4eada4beff",
            "stypecode": "0fd67c67-c9be-45c6-9719-4c4eada4bebe",
            "geometry": {"type": "Point", "coordinates": [9.2, 31.0]},
           }
          ]

        :resheader Content-Type: application/json

        :query footprint: consider only sensors within footprint
                          e.g., 'circle((9.3, 32), 1000)'

        :query after: consider only sensors reporting  after (included)
                      this time, e.g., '2019-02-21T11:03:25Z'

        :query before: consider only sensors reporting strictly before
                      this time, e.g., '2019-02-22T11:03:25Z'

        :query type: consider only sensors of this type (filter by stypecode)

        :status 200: no error
        :returns: list of sensors
        """
        args = {k: v for k, v in request.args.items()}
        if 'footprint' in args:
            args['footprint'] = convert_footprint(args['footprint'])
        res = db.list_sensors(args)
        return jsonify(res)

    @app.route('/sensors/<uuid:code>')
    def sensor(code):
        """Return description of sensor with uuid ``code``.

        .. :quickref: Get description of sensor[code]

        **Example request**::

          GET /sensors/1 HTTP/1.1

        **Example response**:

        .. sourcecode:: http

          HTTP/1.1 200 OK
          Content-Type: application/json

          {"uuid": "0fd67c67-c9be-45c6-9719-4c4eada4becc",
           "stypecode": "0fd67c67-c9be-45c6-9719-4c4eada4be65",
           "geometry": {"type": "Point", "coordinates": [9.3, 30.0]},
          }

        :resheader Content-Type: application/json
        :status 200: no error
        :returns: sensor description
        """
        res = db.get_sensor(str(code))
        return jsonify(res)

    @app.route('/sensors/<uuid:code>/timeseries')
    def timeseries(code):
        """Return timeseries for sensor ``code``.

        Will return the measures and the related timedeltas array, the
        latter expressed as seconds from the `after` time, if `after`
        is defined, otherwise as FIXME ISO-XX datetime.

        .. :quickref: Get time series of data of sensor[code].


        **Example request**::

          GET /sensors/0fd67c67-c9be-45c6-9719-4c4eada4becc/
              timeseries?after=2019-02-21T11:03:25Z HTTP/1.1

        **Example response**:

        .. sourcecode:: http

          HTTP/1.1 200 OK
          Content-Type: application/json

          {"timebase": "2019-02-21T11:03:25Z",
           "timedelta": [0.11, 0.22, 0.33, 0.44],
           "data": [12000, 12100, 12200, 12300]}

        :resheader Content-Type: application/json

        :query after: consider only sensors reporting after (included)
                      this time, e.g., '2019-02-21T11:03:25Z'

        :query before: consider only sensors reporting strictly before
                      this time, e.g., '2019-02-22T11:03:25Z'

        :query bucket: time bucket for data aggregation, in seconds,
                       e.g., 10.33

        :query op: aggregation operation on data contained in bucket,
                   e.g., `sum`,  `average`, `count` FIXME.

        :status 200: no errors
        :returns: list of sensors
        """
        rargs = request.args
        args = dict((k, rargs.get(k, None))
                    for k in ['after', 'before', 'bucket', 'op'])
        if args['bucket'] is not None:
            assert args['op'] is not None
            args['bucket'] = timedelta(seconds=float(args['bucket']))

        res = db.get_timeseries(str(code), args)
        return jsonify(res)
