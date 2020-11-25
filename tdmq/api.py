import copy
import logging
import sys
from datetime import timedelta
from functools import wraps

from flask import Blueprint, current_app, jsonify, request, url_for
import werkzeug.exceptions as wex

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


def _request_authorized():
    auth_header = request.headers.get('Authorization')
    return f"Bearer {current_app.config['AUTH_TOKEN']}" == auth_header


def auth_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not request.headers.get('Authorization'):
            raise wex.Unauthorized("Access token required")

        auth_header = request.headers.get('Authorization')

        if not auth_header.startswith('Bearer'):
            raise wex.Unauthorized('Only Bearer token authentication is supported')
        if f"Bearer {current_app.config['AUTH_TOKEN']}" != auth_header:
            raise wex.Unauthorized("Invalid access token")

        return f(*args, **kwargs)

    return decorated_function


@tdmq_bp.before_request
def print_args():
    logger.debug("request.args: %s", request.args)


@tdmq_bp.route('/')
def index():
    return 'The URL for this page is {}'.format(url_for('tdmq.index'))


@tdmq_bp.route('/entity_types')
def entity_types_get():
    with current_app.http_request_prom.labels(method='get', endpoint='entity_types').time():
        types = db.list_entity_types()
        res = jsonify({'entity_types': types})
        current_app.http_response_prom.labels(method='get', endpoint='entity_types').observe(
            sys.getsizeof(res.data))
        return res


@tdmq_bp.route('/entity_categories')
def entity_categories_get():
    with current_app.http_request_prom.labels(method='get', endpoint='entity_categories').time():
        categories = db.list_entity_categories()
        res = jsonify({'entity_categories': categories})
        current_app.http_response_prom.labels(method='get', endpoint='entity_categories').observe(
            sys.getsizeof(res.data))
        return res

@tdmq_bp.route('/sources', methods=['GET'])
def sources_get():
    """
    Return a list of sources.
    See spec for documentation.
    """
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


@tdmq_bp.route('/sources', methods=['POST'])
@auth_required
def sources_post():
    data = request.json
    try:
        tdmq_ids = db.load_sources(data)
    except tdmq.errors.DuplicateItemException as e:
        logger.debug("Duplicate item exception")
        logger.debug(e)
        raise wex.Conflict("Duplicate source id")
    return jsonify(tdmq_ids)


@tdmq_bp.route('/sources/<uuid:tdmq_id>')
def sources_get_one(tdmq_id):
    srcs = db.get_sources([tdmq_id], include_private=False)
    if len(srcs) == 1:
        result = srcs[0]
        return jsonify(result)
    elif len(srcs) == 0:
        raise wex.NotFound()
    else:
        raise RuntimeError(
            f"Got more than one source for tdmq_id {tdmq_id}")


@tdmq_bp.route('/sources/<uuid:tdmq_id>', methods=['DELETE'])
@auth_required
def sources_delete(tdmq_id):
    result = db.delete_sources([tdmq_id])
    return jsonify(result)


@tdmq_bp.route('/sources/<uuid:tdmq_id>/timeseries')
def timeseries_get(tdmq_id):
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
@auth_required
def records_post():
    data = request.json
    n = db.load_records(data)
    return jsonify({"loaded": n})


@tdmq_bp.route('/service_info')
def service_info_get():
    response = {
        'version': '0.1'
    }

    if current_app.config.get('TILEDB_VFS_ROOT'):
        tiledb_conf = {
            'storage.root': current_app.config['TILEDB_VFS_ROOT']
        }

        if 'TILEDB_VFS_CONFIG' in current_app.config:
            tiledb_conf['config'] = current_app.config.get('TILEDB_VFS_CONFIG')

        if 'TILEDB_VFS_CREDENTIALS' in current_app.config and _request_authorized():
            # We're recycling the configuration object in the response.  Copy
            # it before merging in the credentials to avoid modifying it.
            tiledb_conf['config'] = copy.deepcopy(tiledb_conf['config'])
            tiledb_conf['config'].update(current_app.config.get('TILEDB_VFS_CREDENTIALS'))

        response['tiledb'] = tiledb_conf
    return jsonify(response)
