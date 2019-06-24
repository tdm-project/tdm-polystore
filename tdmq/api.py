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

    @app.route('/sensor_types', methods=['GET', 'POST'])
    def sensor_types():
        """Return a list of sensor types.

        .. :quickref: Get sensor types

        With no parameters, return all sensor types. Parameters can be used to
        filter sensor types according to one or more attributes.

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

        :query {attribute}: select sensors whose description has the specified
          value(s) for the chosen attribute (top-level JSON key, e.g.,
          name=SensorPro; controlledProperty=humidity,temperature)

        :resheader Content-Type: application/json
        :status 200: no error
        :returns: list of sensor types
        """
        if request.method == "GET":
            res = db.list_sensor_types(request.args)
            return jsonify(res)
        else:
            data = request.json
            db.load_sensor_types(db.get_db(), data)
            return jsonify({"loaded": len(data)})

    @app.route('/sensors', methods=['GET', 'POST'])
    def sensors():
        """Return a list of sensors.

        .. :quickref: Get sensors

        With no parameters, return all sensors. With ``type={uuid}``, select
        sensors of the specified type. When ``footprint``, ``after`` and
        ``before`` are specified, return all sensors that have reported an
        event in the corresponding spatio-temporal region. Sensors can also be filtered
        by generic attributes stored in the description field.

        Note: currently queries by footprint, type or attributes are mutually exclusive,
        i.e. they cannot be combined in a single query.



        **Example request**::

          GET /sensors?footprint=circle((9.22, 30.0), 1000)
                      &after=2019-05-02T11:00:00Z
                      &before=2019-05-02T11:50:25Z HTTP/1.1

        (unencoded URL)

        **Example response**:

        .. sourcecode:: http

          HTTP/1.1 200 OK
          Content-Type: application/json

          [{"code": "0fd67c67-c9be-45c6-9719-4c4eada4becc",
            "geometry": {
              "coordinates": [9.22100000642642, 30.0000000019687],
              "type": "Point"
            },
            "stypecode": "0fd67c67-c9be-45c6-9719-4c4eada4be65"},
           {"code": "0fd67c67-c9be-45c6-9719-4c4eada4beff",
            "geometry": {
              "coordinates": [9.22200000642623, 30.0030000019686],
              "type": "Point"
            },
            "stypecode": "0fd67c67-c9be-45c6-9719-4c4eada4bebe"}]

        :resheader Content-Type: application/json

        :query footprint: consider only sensors within footprint
          e.g., ``circle((9.3, 32), 1000)``

        :query after: consider only sensors reporting  after (included)
          this time, e.g., ``2019-02-21T11:03:25Z``

        :query before: consider only sensors reporting strictly before
          this time, e.g., ``2019-02-22T11:03:25Z``

        :query type: consider only sensors of this type (filter by stypecode)
        :query {attribute}: select sensors whose description has the specified value(s)
            for the chosen attribute (top-level JSON key, e.g., name=SensorName; controlledProperty=humidity,temperature)

        :status 200: no error
        :returns: list of sensors
        """
        if request.method == "GET":
            args = {k: v for k, v in request.args.items()}
            if 'footprint' in args:
                args['footprint'] = convert_footprint(args['footprint'])
            res = db.list_sensors(args)
            return jsonify(res)
        else:
            data = request.json
            db.load_sensors(db.get_db(), data)
            return jsonify({"loaded": len(data)})

    @app.route('/sensors/<uuid:code>')
    def sensor(code):
        """Return description of sensor with uuid ``code``.

        .. :quickref: Get sensor description

        **Example request**::

          GET /sensors/0fd67c67-c9be-45c6-9719-4c4eada4becc HTTP/1.1

        **Example response**:

        .. sourcecode:: http

          HTTP/1.1 200 OK
          Content-Type: application/json

          {"code": "0fd67c67-c9be-45c6-9719-4c4eada4becc",
           "geometry": {"coordinates": [9.221, 30.0], "type": "Point"},
           "nodecode": "0fd67ccc-c9be-45c6-9719-4c4eada4beaa",
           "stypecode": "0fd67c67-c9be-45c6-9719-4c4eada4be65"}

        :resheader Content-Type: application/json
        :status 200: no error
        :returns: sensor description
        """
        res = db.get_sensor(str(code))
        return jsonify(res)

    @app.route('/sensors/<uuid:code>/timeseries')
    def timeseries(code):
        """Return timeseries for sensor ``code``.

        .. :quickref: Get time series data for sensor

        For the specified sensor and time interval, return all measures and
        the corresponding timedeltas array (expressed in seconds from the
        initial time). Also returns the initial time as "timebase".

        **Example request**::

          GET /sensors/0fd67c67-c9be-45c6-9719-4c4eada4becc/
              timeseries?after=2019-02-21T11:03:25Z
                        &before=2019-05-02T11:50:25Z HTTP/1.1

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
                   e.g., `sum`, `count`.

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

    @app.route('/measures', methods=["POST"])
    def measures():
        data = request.json
        db.load_measures(db.get_db(), data)
        return jsonify({"loaded": len(data)})
