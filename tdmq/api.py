import logging
import sys
from datetime import timedelta

import werkzeug.exceptions as wex
from flask import Blueprint, current_app, jsonify, request, url_for

import tdmq.db as db
import tdmq.errors
from tdmq.utils import convert_roi

logger = logging.getLogger(__name__)

tdmq_bp = Blueprint('tdmq', __name__)


def _restructure_timeseries(res, properties):
    # The arrays time and footprint define the scaffolding on which
    # the actual data (properties) are defined.
    result = {'coords': None, 'data': None}
    t = zip(*res) if len(res) > 0 else iter([[]] * (2 + len(properties)))
    result['coords'] = dict((p, next(t)) for p in ['time', 'footprint'])
    result['data'] = dict((p, next(t)) for p in properties)
    return result


@tdmq_bp.before_request
def print_args():
    logger.debug("request.args: %s", request.args)

@tdmq_bp.route('/')
def index():
    return 'The URL for this page is {}'.format(url_for('tdmq.index'))

@tdmq_bp.route('/entity_types')
def entity_types():
    with current_app.http_request_prom.labels(method='get', endpoint='entity_types').time():
        types = db.list_entity_types()
        res = jsonify({'entity_types': types})
        current_app.http_response_prom.labels(method='get', endpoint='entity_types').observe(
            sys.getsizeof(res.data))
        return res

@tdmq_bp.route('/entity_categories')
def entity_categories():
    with current_app.http_request_prom.labels(method='get', endpoint='entity_categories').time():
        categories = db.list_entity_categories()
        res = jsonify({'entity_categories': categories})
        current_app.http_response_prom.labels(method='get', endpoint='entity_categories').observe(
            sys.getsizeof(res.data))
        return res

@tdmq_bp.route('/sources', methods=['GET', 'POST'])
def sources():
    """
    Return a list of sources.
    See spec for documentation.
    """
    if request.method == "GET":
        with current_app.http_request_prom.labels(method='get', endpoint='sources').time():
            args = {k: v for k, v in request.args.items()}
            logger.debug("source: args is %s", args)
            if 'roi' in args:
                args['roi'] = convert_roi(args['roi'])
            if 'controlledProperties' in args:
                args['controlledProperties'] = \
                    args['controlledProperties'].split(',')
            try:
                args['include_private'] = 'false'
                res = db.list_sources(args)
            except tdmq.errors.DBOperationalError:
                raise wex.InternalServerError()
            res = jsonify(res)
            current_app.http_response_prom.labels(method='get', endpoint='sources').observe(
                sys.getsizeof(res.data))
            return res
    elif request.method == "POST":
        data = request.json
        try:
            tdmq_ids = db.load_sources(data)
        except tdmq.errors.DuplicateItemException:
            raise wex.Conflict()
        return jsonify(tdmq_ids)
    else:
        raise wex.MethodNotAllowed()

@tdmq_bp.route('/sources/<uuid:tdmq_id>', methods=['GET', 'DELETE'])
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
        result = db.delete_sources([tdmq_id])
    else:
        srcs = db.get_sources([tdmq_id], include_private=False)
        if len(srcs) == 1:
            result = srcs[0]
        elif len(srcs) == 0:
            raise wex.NotFound()
        else:
            raise RuntimeError(
                f"Got more than one source for tdmq_id {tdmq_id}")
    return jsonify(result)

@tdmq_bp.route('/sources/<uuid:tdmq_id>/timeseries')
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

    with current_app.http_request_prom.labels(method='get', endpoint='timeseries').time():
        rargs = request.args
        args = dict((k, rargs.get(k, None))
                    for k in ['after', 'before', 'bucket', 'fields', 'op'])
        if args['bucket'] is not None:
            args['bucket'] = timedelta(seconds=float(args['bucket']))
        if args['fields'] is not None:
            args['fields'] = args['fields'].split(',')

        # Forces returning data only for private_sources
        args['include_private'] = False

        try:
            result = db.get_timeseries(tdmq_id, args)
        except tdmq.errors.RequestException:
            logger.error("Bad request getting timeseries")
            raise wex.BadRequest()

        if len(result["rows"]) == 0:
            logger.error(
                "did not find any timeseries corresponding to required args")
            raise wex.NotFound()

        res = _restructure_timeseries(result['rows'], result['properties'])

        res["tdmq_id"] = tdmq_id
        res["default_footprint"] = result['source_info']['default_footprint']
        res["shape"] = result['source_info']['shape']
        if args['bucket']:
            res["bucket"] = {
                "interval": args['bucket'].total_seconds(), "op": args.get("op")}
        else:
            res['bucket'] = None
        res = jsonify(res)
        current_app.http_response_prom.labels(method='get', endpoint='timeseries').observe(
            sys.getsizeof(res.data))
        return res

@tdmq_bp.route('/records', methods=["POST"])
def records():
    data = request.json
    n = db.load_records(data)
    return jsonify({"loaded": n})

@tdmq_bp.route('/service_info')
def client_info():
    response = {
        'version': '0.1'
    }

    if current_app.config.get('TILEDB_VFS_ROOT'):
        tiledb_conf = {
            'storage.root': current_app.config['TILEDB_VFS_ROOT']
        }

        if 'TILEDB_VFS_CONFIG' in current_app.config:
            tiledb_conf['config'] = current_app.config.get('TILEDB_VFS_CONFIG')

        response['tiledb'] = tiledb_conf
    return jsonify(response)
