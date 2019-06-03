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
        """Return known sensor types.

        .. :quickref: Get collection of sensor types.

        **Example request**:

        .. sourcecode:: http

          GET /sensor_types/ HTTP/1.1
          Host: example.com
          Accept: application/json

        **Example response**:

        .. sourcecode:: http

          HTTP/1.1 200 OK
          Vary: Accept
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
           },
           {
             "uuid": "0fd67c67-c9be-45c6-9719-4c4eada4bebe",
             "type": "HumiditySensorDHT11",
             "name": "Humidity sensor in DHT11",
             "brandName": "Acme",
             "modelName": "Acme multisensor DHT11",
             "manufacturerName": "Acme Inc.",
             "category": ["sensor"],
             "function": ["sensing"],
             "controlledProperty": ["humidity"]
           },
          ]
        :resheader Content-Type: application/json
        :status 200: list[SensorType] found
        :returns: :class:`list[tdmq.objects.SensorType]`
        """
        res = db.list_sensor_types(request.args)
        return jsonify(res)

    @app.route('/sensors')
    def sensors():
        """Return the collection of sensor that have reported an event in a
           given spatio-temporal region.

           The spatio-temporal domain is expressed as a cylinder with
           a given geometrical footprint and a time interval.

           Calling withour arguments will return all available sensors.


        .. :quickref: Get collection of reporting sensors.

        **Example request**:

        .. sourcecode:: http

          GET /sensors?footprint=circle((9.2 33.0), 1000) HTTP/1.1
          Host: example.com
          Accept: application/json


        **Example response**:

        .. sourcecode:: http

          HTTP/1.1 200 OK
          Vary: Accept
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

        :query selector: consider only sensors such that this
                         predicate is true,
                         e.g., 'temperature in sensor_type.controlledProperty'

        :status 200: list[Sensor] found
        :returns: :class:`list[tdmq.objects.Sensor]`

        """
        rargs = request.args
        if not rargs:
            args = None
        else:
            args = dict((k, rargs.get(k, None))
                        for k in ['footprint', 'after', 'before', 'selector'])
            if 'footprint' in args:
                args['footprint'] = convert_footprint(args['footprint'])
        res = db.list_sensors(args)
        return jsonify(res)

    @app.route('/sensors/<uuid:code>')
    def sensor(code):
        """Return description of sensor with uuid code

        .. :quickref: Get description of sensor[code]

        **Example request**:

        .. sourcecode:: http

          GET /sensors/1 HTTP/1.1
          Host: example.com
          Accept: application/json


        **Example response**:

        .. sourcecode:: http

          HTTP/1.1 200 OK
          Vary: Accept
          Content-Type: application/json

          {"uuid": "0fd67c67-c9be-45c6-9719-4c4eada4becc",
           "stypecode": "0fd67c67-c9be-45c6-9719-4c4eada4be65",
           "geometry": {"type": "Point", "coordinates": [9.3, 30.0]},
          }

        :resheader Content-Type: application/json
        :status 200: Sensor[code] found
        :returns: :class:`tdmq.objects.Sensor`

        """
        res = db.get_sensor(str(code))
        return jsonify(res)

    @app.route('/sensors/<uuid:code>/timeseries')
    def timeseries(code):
        """Return timeseries for sensor[code].

        Will return the measures and the related timedeltas array, the
        latter expressed as seconds from the `after` time, if `after`
        is defined, otherwise as FIXME ISO-XX datetime.

        .. :quickref: Get time series of data of sensor[code].


        **Example request**:

        .. sourcecode:: http

          GET /sensors/0fd67c67-c9be-45c6-9719-4c4eada4becc/
              timeseries?after=2019-02-21T11:03:25Z HTTP/1.1
          Host: example.com
          Accept: application/json


        **Example response**:

        .. sourcecode:: http

          HTTP/1.1 200 OK
          Vary: Accept
          Content-Type: application/json

          {'timebase': '2019-02-21T11:03:25Z',
           'timedelta':[0.11, 0.22, 0.33, 0.44],
           'data': [12000, 12100, 12200, 12300]}

        :resheader Content-Type: application/json

        :query after: consider only sensors reporting after (included)
                      this time, e.g., '2019-02-21T11:03:25Z'

        :query before: consider only sensors reporting strictly before
                      this time, e.g., '2019-02-22T11:03:25Z'

        :query bucket: time bucket for data aggregation, in seconds,
                       e.g., 10.33

        :query op: aggregation operation on data contained in bucket,
                   e.g., `sum`,  `average`, `count` FIXME.

        :status 200: list[Sensor] found
        :returns: :class:`list[tdmq.objects.Sensor]`

        """
        rargs = request.args
        args = dict((k, rargs.get(k, None))
                    for k in ['after', 'before', 'bucket', 'op'])
        if args['bucket'] is not None:
            assert args['op'] is not None
            args['bucket'] = timedelta(seconds=float(args['bucket']))

        res = db.get_timeseries(str(code), args)
        return jsonify(res)
