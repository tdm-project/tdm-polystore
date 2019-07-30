from datetime import timedelta

from flask import jsonify
from flask import request
from flask import url_for

import tdmq.db as db
from tdmq.utils import convert_footprint

import logging
logger = logging.getLogger(__name__)


def add_routes(app):
    @app.route('/')
    def index():
        return 'The URL for this page is {}'.format(url_for('index'))

    @app.route('/sources', methods=['GET', 'POST'])
    def sources():
        """Return a list of sources.

        .. :quickref: Get sources

        With no parameters, return all sources. When ``footprint``, ``after``
        and ``before`` are specified, return all sources that have reported an
        event in the corresponding spatio-temporal region. Sources can also be
        filtered by generic attributes stored in the description field.

        The footprint should be specified using one of the following:
         - circle((center_lon, center_lat), radius_in_meters)
         - FIXME: rectangle?
         - FIXME: arbitrary GeoJson?


        **Example request**::

          GET /sources?footprint=circle((9.22, 30.0), 1000)
                      &after=2019-05-02T11:00:00Z
                      &before=2019-05-02T11:50:25Z HTTP/1.1

          GET /sources?controlledProperties=temperature,humidity
        (unencoded URL)

        **Example response**:

        .. sourcecode:: http

          HTTP/1.1 200 OK
          Content-Type: application/json

          [{"tdmq_id": "c034f147-8e54-50bd-97bb-9db1addcdc5a",
            "id": "source_0",
            "geometry_type": "Point",
            "entity_type": "entity_type0},
           {"tdmq_id": "c034f147-8e54-50bd-97bb-9db1addcdc5b",
            "id": "source_1",
            "geometry_type": "Point",
            "entity_type": "entity_type_1"}]

        :resheader Content-Type: application/json

        :query footprint: consider only sources within footprint
          e.g., ``circle((9.3, 32), 1000)``

        :query after: consider only sources reporting  after (included)
          this time, e.g., ``2019-02-21T11:03:25Z``

        :query before: consider only sources reporting strictly before
          this time, e.g., ``2019-02-22T11:03:25Z``

        :query {attribute}: select sources whose description has the
            specified value(s) for the chosen attribute
            (top-level JSON key, e.g., controlledProperties=temperature)
        :status 200: no error
        :returns: list of sources
        """
        if request.method == "GET":
            args = {k: v for k, v in request.args.items()}
            logger.debug("source:  args is %s", args)
            if 'footprint' in args:
                args['footprint'] = convert_footprint(args['footprint'])
            if 'controlledProperties' in args:
                args['controlledProperties'] = args['controlledProperties'].split(',')
            res = db.list_sources(args)
            return jsonify(res)
        else:
            data = request.json
            tdmq_ids = db.load_sources(db.get_db(), data)
            return jsonify(tdmq_ids)

    @app.route('/sources/<uuid:tdmq_id>')
    def source(tdmq_id):
        """Return description of source with uuid ``tdmq_id``.

        .. :quickref: Get source description

        **Example request**::

          GET /sources/0fd67c67-c9be-45c6-9719-4c4eada4becc HTTP/1.1

        **Example response**:

        .. sourcecode:: http

          HTTP/1.1 200 OK
          Content-Type: application/json

          {"tdmq_id": "c034f147-8e54-50bd-97bb-9db1addcdc5a",
           "id": "source_0",
           "geometry_ype": "Point",
           "entity_type": "entity_type_0"}

        :resheader Content-Type: application/json
        :status 200: no error
        :returns: source description
        """
        res = db.get_source(str(tdmq_id))
        res["tdmq_id"] = tdmq_id
        return jsonify(res)

    @app.route('/sources/<uuid:tdmq_id>/timeseries')
    def timeseries(tdmq_id):
        """Return timeseries for source ``tdmq_id``.

        .. :quickref: Get time series data for source

        For the specified source and time interval, return all records and
        the corresponding timedeltas array (expressed in seconds from the
        initial time). Also returns the initial time as "timebase".

        **Example request**::

          GET /sources/0fd67c67-c9be-45c6-9719-4c4eada4becc/
              timeseries?after=2019-02-21T11:03:25Z
                        &before=2019-05-02T11:50:25Z HTTP/1.1

        **Example response**:

        .. sourcecode:: http

          HTTP/1.1 200 OK
          Content-Type: application/json

          {"timebase": "2019-02-21T11:03:25Z",
           "timedelta": [0.11, 0.22, 0.33, 0.44],
           "data": {'temperature':[12000, 12100, 12200, 12300]}}

        :resheader Content-Type: application/json

        :query after: consider only sources reporting after (included)
                      this time, e.g., '2019-02-21T11:03:25Z'

        :query before: consider only sources reporting strictly before
                      this time, e.g., '2019-02-22T11:03:25Z'

        :query bucket: time bucket for data aggregation, in seconds,
                       e.g., 10.33

        :query op: aggregation operation on data contained in bucket,
                   e.g., `sum`, `count`.

        :status 200: no errors
        :returns: list of sources
        """
        rargs = request.args
        args = dict((k, rargs.get(k, None))
                    for k in ['after', 'before', 'bucket', 'op'])
        if args['bucket'] is not None:
            assert args['op'] is not None
            args['bucket'] = timedelta(seconds=float(args['bucket']))

        res = db.get_timeseries(str(tdmq_id), args)
        return jsonify(res)

    @app.route('/records', methods=["POST"])
    def records():
        data = request.json
        n = db.load_records(db.get_db(), data)
        return jsonify({"loaded": n})
