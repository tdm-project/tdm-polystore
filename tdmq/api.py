import copy
import logging
import sys
from datetime import timedelta
from functools import wraps
from http import HTTPStatus

import werkzeug.exceptions as wex
from flask import Blueprint, current_app, jsonify, request, url_for

import tdmq.errors
from .model import EntityType, EntityCategory, Source, Timeseries
from .utils import convert_roi, str_to_bool

logger = logging.getLogger(__name__)
tdmq_bp = Blueprint('tdmq', __name__)


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
        types = EntityType.get_entity_types()
        res = jsonify({'entity_types': types})
        current_app.http_response_prom.labels(method='get', endpoint='entity_types').observe(
            sys.getsizeof(res.data))
        return res


@tdmq_bp.route('/entity_categories')
def entity_categories_get():
    with current_app.http_request_prom.labels(method='get', endpoint='entity_categories').time():
        categories = EntityCategory.get_entity_categories()
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
        rargs = {k: v for k, v in request.args.items()}
        logger.debug("source: args is %s", rargs)

        private_requested = str_to_bool(rargs.pop('include_private', None))
        if private_requested and not _request_authorized():
            raise wex.Unauthorized("Unauthorized request for private data")
        anonymize_private = not private_requested

        # preprocess controlledProperties and roi arguments
        if 'controlledProperties' in rargs:
            rargs['controlledProperties'] = \
                rargs['controlledProperties'].split(',')
        if 'public' in rargs:
            rargs['public'] = str_to_bool(rargs['public'])
        if 'roi' in rargs:
            rargs['roi'] = convert_roi(rargs['roi'])
            if rargs['roi']['type'] != 'Circle':
                raise NotImplementedError()
            if rargs['roi']['radius'] <= 0:
                raise wex.BadRequest("ROI radius must be > 0")
        if 'stationary' in rargs:
            rargs['stationary'] = str_to_bool(rargs['stationary'])

        search_args = dict((k, rargs.pop(k)) for k in Source.AcceptedSearchKeys if k in rargs)

        limit = rargs.pop('limit', None)
        if limit:
            limit = int(limit)
        offset = rargs.pop('offset', None)
        if offset:
            offset = int(offset)

        match_attr = rargs # everything that hasn't been popped

        try:
            items = Source.search(search_args, match_attr, anonymize_private, limit, offset)
        except tdmq.errors.DBOperationalError:
            raise wex.InternalServerError()

        res = jsonify(items)
        current_app.http_response_prom.labels(method='get', endpoint='sources').observe(
            sys.getsizeof(res.data))
        return res


@tdmq_bp.route('/sources', methods=['POST'])
@auth_required
def sources_post():
    data = request.json
    try:
        tdmq_ids = Source.store_new(data)
    except tdmq.errors.DuplicateItemException:
        raise wex.Conflict()
    return jsonify(tdmq_ids)


@tdmq_bp.route('/sources/<uuid:tdmq_id>')
def sources_get_one(tdmq_id):
    include_private = str_to_bool(request.args.get('include_private'))
    if include_private and not _request_authorized():
        raise wex.Unauthorized("Unauthorized request for private data")
    anonymize_private = not include_private

    source = Source.get_one(tdmq_id, anonymize_private)
    if source is None:
        raise wex.NotFound()
    return jsonify(source)


@tdmq_bp.route('/sources/<uuid:tdmq_id>', methods=['DELETE'])
@auth_required
def sources_delete(tdmq_id):
    Source.delete_one(tdmq_id)
    return ('', HTTPStatus.NO_CONTENT)


@tdmq_bp.route('/sources/<uuid:tdmq_id>/timeseries')
def timeseries_get(tdmq_id):
    with current_app.http_request_prom.labels(method='get', endpoint='timeseries').time():
        rargs = request.args

        private_requested = str_to_bool(rargs.get('include_private'))
        if private_requested and not _request_authorized():
            raise wex.Unauthorized("Unauthorized request for private data")

        anonymize_private = not private_requested

        args = dict((k, rargs.get(k, None))
                    for k in ['after', 'before', 'bucket', 'fields', 'op'])
        if args['bucket'] is not None:
            args['bucket'] = timedelta(seconds=float(args['bucket']))
        if args['fields'] is not None:
            args['fields'] = args['fields'].split(',')

        result = Timeseries.get_one(tdmq_id, anonymize_private, args)
        jres = jsonify(result)
        current_app.http_response_prom.labels(method='get', endpoint='timeseries').observe(
            sys.getsizeof(jres.data))
        return jres


@tdmq_bp.route('/records', methods=["POST"])
@auth_required
def records_post():
    data = request.json
    n = Timeseries.store_new_records(data)
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
