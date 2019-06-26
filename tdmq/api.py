from datetime import timedelta

from flask import jsonify
from flask import request
from flask import url_for

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
             "brandName": "Acme",
             "category": ["sensor"],
             "code": "c7afa96b-ca9a-5561-b57b-5187ad005d75",
             "controlledProperty": ["temperature"],
             "function": ["sensing"],
             "manufacturerName": "Acme Inc.",
             "modelName": "Acme multisensor DHT11",
             "name": "sensor_type_0",
             "type": "TemperatureSensorDTH11"
           }
          ]

        :query {attribute}: select sensors whose description has the specified
          value(s) for the chosen attribute (top-level JSON key, e.g.,
          brandName=Acme; controlledProperty=humidity,temperature)

        :resheader Content-Type: application/json
        :status 200: no error
        :returns: list of sensor types
        """
        if request.method == "GET":
            res = []
            for code, descr in db.list_sensor_types(request.args):
                descr["code"] = code
                res.append(descr)
            return jsonify(res)
        else:
            data = request.json
            codes = db.load_sensor_types(db.get_db(), data)
            return jsonify(codes)

    @app.route('/sensors', methods=['GET', 'POST'])
    def sensors():
        """Return a list of sensors.

        .. :quickref: Get sensors

        With no parameters, return all sensors. When ``footprint``, ``after``
        and ``before`` are specified, return all sensors that have reported an
        event in the corresponding spatio-temporal region. Sensors can also be
        filtered by generic attributes stored in the description field.

        Note: currently queries by footprint or by attributes are mutually
        exclusive, i.e. they cannot be combined in a single query.

        **Example request**::

          GET /sensors?footprint=circle((9.22, 30.0), 1000)
                      &after=2019-05-02T11:00:00Z
                      &before=2019-05-02T11:50:25Z HTTP/1.1

        (unencoded URL)

        **Example response**:

        .. sourcecode:: http

          HTTP/1.1 200 OK
          Content-Type: application/json

          [{"code": "c034f147-8e54-50bd-97bb-9db1addcdc5a",
            "geometry": {"coordinates": [9.221, 30.0], "type": "Point"},
            "name": "sensor_0",
            "node": "node_0",
            "type": "sensor_type_0"},
           {"code": "c932ff51-6eec-5b73-abe1-4163f9e72cb3",
            "geometry": {"coordinates": [9.222, 30.003], "type": "Point"},
            "name": "sensor_1",
            "node": "node_0",
            "type": "sensor_type_1"}]

        :resheader Content-Type: application/json

        :query footprint: consider only sensors within footprint
          e.g., ``circle((9.3, 32), 1000)``

        :query after: consider only sensors reporting  after (included)
          this time, e.g., ``2019-02-21T11:03:25Z``

        :query before: consider only sensors reporting strictly before
          this time, e.g., ``2019-02-22T11:03:25Z``

        :query {attribute}: select sensors whose description has the
            specified value(s) for the chosen attribute
            (top-level JSON key, e.g., type=sensor_type_1)

        :status 200: no error
        :returns: list of sensors
        """
        if request.method == "GET":
            args = {k: v for k, v in request.args.items()}
            if 'footprint' in args:
                args['footprint'] = convert_footprint(args['footprint'])
            res = []
            for code, descr in db.list_sensors(args):
                descr["code"] = code
                res.append(descr)
            return jsonify(res)
        else:
            data = request.json
            codes = db.load_sensors(db.get_db(), data)
            return jsonify(codes)

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

          {"code": "c034f147-8e54-50bd-97bb-9db1addcdc5a",
           "geometry": {"coordinates": [9.221, 30.0], "type": "Point"},
           "name": "sensor_0",
           "node": "node_0",
           "type": "sensor_type_0"}

        :resheader Content-Type: application/json
        :status 200: no error
        :returns: sensor description
        """
        res = db.get_sensor(str(code))
        res["code"] = code
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
        n = db.load_measures(db.get_db(), data)
        return jsonify({"loaded": n})
