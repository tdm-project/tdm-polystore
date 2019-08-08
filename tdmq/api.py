from datetime import timedelta

from flask import jsonify
from flask import request
from flask import url_for

import werkzeug.exceptions

import tdmq.db as db
import tdmq.errors
from tdmq.utils import convert_roi


import logging
logger = logging.getLogger(__name__)


class DuplicateItemException(werkzeug.exceptions.HTTPException):
    code = 512
    description = 'Attemp to duplicate unique field.'


def restructure_timeseries(res, properties):
    # The arrays time and footprint define the scaffolding on which
    # the actual data (properties) are defined.
    result = {'coords': None, 'data': None}
    t = zip(*res) if len(res) > 0 else iter([[]] * (2 + len(properties)))
    result['coords'] = dict((p, next(t)) for p in ['time', 'footprint'])
    result['data'] = dict((p, next(t)) for p in properties)
    return result


def add_routes(app):
    @app.before_request
    def print_args():
        logger.debug("request.args: %s", request.args)

    @app.route('/')
    def index():
        return 'The URL for this page is {}'.format(url_for('index'))

    @app.route('/entity_types')
    def entity_types():
        return jsonify(db.list_entity_types())

    @app.route('/entity_categories')
    def entity_categories():
        return jsonify(db.list_entity_categories())

    @app.route('/sources', methods=['GET', 'POST'])
    def sources():
        """Return a list of sources.

        .. :quickref: Get sources

        With no parameters, return all sources. When ``roi``,
        ``after`` and ``before`` are specified, return all sources
        that have reported an event that intesect the corresponding
        spatio-temporal region. It is
        also possible to filter by any of the following:

          * entity_type;
          * entity_category;
          * stationary True/False.

        Moreover, sources can also be filtered by generic attributes
        stored in their description field.

        The roi should be specified using one of the following:
         - circle((center_lon, center_lat), radius_in_meters)
         - FIXME: rectangle?
         - FIXME: arbitrary GeoJson?

        **Example request**::

          GET /sources?roi=circle((9.22, 30.0), 1000)
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

        :query roi: consider only sources with footprint intersecting
          the given roi e.g., ``circle((9.3, 32), 1000)``

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
            if 'roi' in args:
                args['roi'] = convert_roi(args['roi'])
            if 'controlledProperties' in args:
                args['controlledProperties'] = \
                    args['controlledProperties'].split(',')
            res = db.list_sources(args)
            return jsonify(res)
        elif request.method == "POST":
            data = request.json
            try:
                tdmq_ids = db.load_sources(db.get_db(), data)
            except tdmq.errors.DuplicateItemException as e:
                raise DuplicateItemException(e.args)
            return jsonify(tdmq_ids)
        else:
            raise NotImplementedError(
                f"{request.method} not supported by this endpoint")

    @app.route('/sources/<uuid:tdmq_id>', methods=['GET', 'DELETE'])
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
        if request.method == "DELETE":
            result = db.delete_sources([str(tdmq_id)])
        else:
            sources = db.get_sources([str(tdmq_id)])
            if len(sources) == 1:
                result = sources[0]
            elif len(sources) == 0:
                result = None
            else:
                raise RuntimeError(
                    f"Got more than one source for tdmq_id {tdmq_id}")
        return jsonify(result)

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

          {
            "source_id": "...",
            "default_footprint": {...},
            "shape": [...],
            "bucket": null,
          <oppure>
            "bucket": { "interval": 10, "op": "avg" },

            "coords": {
                "footprint": [...],
                "time": [...]
            },
            "data": {
              "humidity": [...],
              "temperature": [...],
            }
          }

        :resheader Content-Type: application/json

        :query after: consider only sources reporting after (included)
                      this time, e.g., '2019-02-21T11:03:25Z'

        :query before: consider only sources reporting strictly before
                      this time, e.g., '2019-02-22T11:03:25Z'

        :query bucket: time bucket for data aggregation, in seconds,
                       e.g., 10.33

        :query op: aggregation operation on data contained in bucket,
                   e.g., `sum`, `count`.

         :query fields: comma-separated controlledProperties from the source,
                    or nothing to select all of them.

        :status 200: no errors
        :returns: list of sources
        """
        rargs = request.args
        args = dict((k, rargs.get(k, None))
                    for k in ['after', 'before', 'bucket', 'fields', 'op'])
        if args['bucket'] is not None:
            args['bucket'] = timedelta(seconds=float(args['bucket']))
        if args['fields'] is not None:
            args['fields'] = args['fields'].split(',')

        try:
            result = db.get_timeseries(tdmq_id, args)
        except tdmq.errors.RequestException as e:
            return str(e), 400  # BAD_REQUEST

        res = restructure_timeseries(result['rows'], result['properties'])

        res["tdmq_id"] = tdmq_id
        res["default_footprint"] = result['source_info']['default_footprint']
        res["shape"] = result['source_info']['shape']
        if args['bucket']:
            res["bucket"] = {"interval": args['bucket'].total_seconds(),
                             "op": args.get("op")}
        else:
            res['bucket'] = None

        return jsonify(res)

    @app.route('/records', methods=["POST"])
    def records():
        data = request.json
        n = db.load_records(db.get_db(), data)
        return jsonify({"loaded": n})

    @app.route('/client_info')
    def client_info():
        response = {
            'version': '0.0',
            'tiledb': {
                'hdfs_root': 'hdfs://namenode:8020/arrays',
                'vfs.hdfs.username': 'root'
            }
        }

        return jsonify(response)
