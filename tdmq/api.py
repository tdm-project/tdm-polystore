import copy
import logging
import sys
from datetime import timedelta
from functools import wraps

from flask import Blueprint, current_app, jsonify, request, url_for
import geojson
import werkzeug.exceptions as wex

import tdmq.db as db
import tdmq.errors
from tdmq.utils import convert_roi, str_to_bool

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


def _should_anonymize_private_data(source_is_public, private_data_requested=False):
    return source_is_public or (_request_authorized() and private_data_requested)


def _anonymize_source(src_dict):
    safe_keys = (
        'entity_category',
        'entity_type',
        'public',
        'stationary',
        'tdmq_id')
    safe_description_keys = (
        'controlledProperties',
        'description',
        'entity_category',
        'entity_type',
        'shape',
        'stationary')
    sanitized = dict()
    for k in safe_description_keys:
        if k in src_dict['description']:
            sanitized[k] = src_dict['description'][k]

    sanitized = dict(description=sanitized)
    for k in safe_keys:
        if k in src_dict:
            sanitized[k] = src_dict[k]

    # Round off default_footprint to nearest 0.05
    # Around Italy this equates to placing the point on a grid with
    # 5 km resolution.
    geo = geojson.GeoJSON(src_dict['default_footprint'])
    low_precision = geojson.utils.map_coords(lambda coord: round(coord * 20) / 20, geo)
    sanitized['default_footprint'] = low_precision

    return sanitized


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

        private_requested = str_to_bool(request.args.get('include_private'))
        if private_requested and not _request_authorized():
            raise wex.Unauthorized("Unauthorized request for private data")

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
    except tdmq.errors.DuplicateItemException:
        raise wex.Conflict()
    return jsonify(tdmq_ids)


@tdmq_bp.route('/sources/<uuid:tdmq_id>')
def sources_get_one(tdmq_id):
    private_requested = str_to_bool(request.args.get('include_private'))
    if private_requested and not _request_authorized():
        raise wex.Unauthorized("Unauthorized request for private data")

    srcs = db.get_sources([tdmq_id], include_private=True)
    if len(srcs) == 1:
        source = srcs[0]
        if not _should_anonymize_private_data(source_is_public=source.get('public', False),
                                              private_data_requested=private_requested):
            source = _anonymize_source(source)
        return jsonify(source)
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

        private_requested = str_to_bool(rargs.get('include_private'))
        if private_requested and not _request_authorized():
            raise wex.Unauthorized("Unauthorized request for private data")

        args = dict((k, rargs.get(k, None))
                    for k in ['after', 'before', 'bucket', 'fields', 'op'])
        if args['bucket'] is not None:
            args['bucket'] = timedelta(seconds=float(args['bucket']))
        if args['fields'] is not None:
            args['fields'] = args['fields'].split(',')

        # Pass 'include_private' = True to the DB query or it will not return
        # data from private sources.  We filter the data at this level.
        args['include_private'] = True
        try:
            result = db.get_timeseries(tdmq_id, args)
        except tdmq.errors.RequestException:
            logger.error("Bad request getting timeseries")
            raise wex.BadRequest()

        res = _restructure_timeseries(result['rows'], result['properties'])

        res["tdmq_id"] = tdmq_id
        res["shape"] = result['source_info']['shape']
        if args['bucket']:
            res["bucket"] = {
                "interval": args['bucket'].total_seconds(), "op": args.get("op")}
        else:
            res['bucket'] = None

        # If private data is to be returned, we leave location data in the result: i.e.,
        # the default_footprint and the timestamped footprint.
        # Otherwise, we erase the mobile footprint from the result by replacing it with nulls.
        if _should_anonymize_private_data(result['public'], private_requested):
            res["default_footprint"] = result['source_info']['default_footprint']
        else:
            res["coords"]["footprint"] = [ None ] * len(res["coords"]["footprint"])

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
