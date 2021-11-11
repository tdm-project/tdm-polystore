import copy
import json
import logging
from datetime import timedelta
from functools import wraps
from http import HTTPStatus
from typing import List

import werkzeug.exceptions as wex
from flask import Blueprint, current_app, jsonify, request, url_for

import tdmq.errors
from .model import EntityType, EntityCategory, Source, Timeseries
from .utils import convert_roi, str_to_bool

logger = logging.getLogger(__name__)
tdmq_bp = Blueprint('tdmq', __name__)


ERROR_CODES = {
    400: "bad_request",
    401: "unauthorized",
    403: "forbidden",
    404: "not_found",
    405: "method_not_allowed",
    409: "duplicated_resource",
    413: "payload_too_large",
    500: "error_retrieving_data"
}


@tdmq_bp.app_errorhandler(wex.HTTPException)
def handle_http_exception(e):
    response_logger = logging.getLogger("response")
    response_logger.exception(e)
    struct = {
        "error": ERROR_CODES.get(e.code),
        "description": e.description
    }
    return jsonify(struct), e.code


@tdmq_bp.app_errorhandler(tdmq.errors.QueryTooLargeException)
def handle_query_too_large_exception(e):
    response_logger = logging.getLogger("response")
    response_logger.exception(e)
    struct = {
        "error": "query_too_large",
        "description": e.description
    }
    return jsonify(struct), 413


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


@tdmq_bp.route('/')
def index():
    return 'The URL for this page is {}'.format(url_for('tdmq.index'))


@tdmq_bp.route('/entity_types')
def entity_types_get():
    types = EntityType.get_entity_types()
    res = jsonify({'entity_types': types})
    return res


@tdmq_bp.route('/entity_categories')
def entity_categories_get():
    categories = EntityCategory.get_entity_categories()
    res = jsonify({'entity_categories': categories})
    return res


@tdmq_bp.route('/sources', methods=['GET'])
def sources_get():
    """
    Return a list of sources.
    See spec for documentation.
    """
    rargs = {k: v for k, v in request.args.items()}
    logger.debug("source: args is %s", rargs)

    anonymize_private = str_to_bool(rargs.pop('anonymized', 'true'))
    if not anonymize_private and not _request_authorized():
        raise wex.Unauthorized("Unauthorized request for unanonymized private data")

    # preprocess controlledProperties and roi arguments
    if 'controlledProperties' in rargs:
        rargs['controlledProperties'] = \
            rargs['controlledProperties'].split(',')
    if 'public' in rargs:
        rargs['public'] = str_to_bool(rargs['public'])

    if 'only_public' in rargs:
        if 'public' in rargs:
            raise wex.BadRequest("Cannot specify both 'only_public' and 'public' query attributes")
        only_public = str_to_bool(rargs.pop('only_public'))
        if only_public:
            rargs['public'] = True
    else:
        rargs['public'] = rargs.get('public', True)

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

    match_attr = rargs  # everything that hasn't been popped

    try:
        items = Source.search(search_args, match_attr, anonymize_private, limit, offset)
    except tdmq.errors.DBOperationalError:
        raise wex.InternalServerError()

    res = jsonify(items)
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
    anonymize_private = str_to_bool(request.args.get('anonymized', 'true'))
    if not anonymize_private and not _request_authorized():
        raise wex.Unauthorized("Unauthorized request for unanonymized private data")

    source = Source.get_one(tdmq_id, anonymize_private)
    if source is None:
        raise wex.NotFound(f"source {tdmq_id} does not exist")
    return jsonify(source)


@tdmq_bp.route('/sources/<uuid:tdmq_id>', methods=['DELETE'])
@auth_required
def sources_delete(tdmq_id):
    Source.delete_one(tdmq_id)
    return ('', HTTPStatus.NO_CONTENT)


@tdmq_bp.route('/sources/<uuid:tdmq_id>/timeseries')
def timeseries_get_stream(tdmq_id):
    rargs = request.args

    anonymize_private = str_to_bool(rargs.get('anonymized', 'true'))
    if not anonymize_private and not _request_authorized():
        raise wex.Unauthorized("Unauthorized request for unanonymized private data")

    args = dict((k, rargs.get(k, None))
                for k in ['after', 'before', 'bucket', 'fields', 'op'])
    if args['bucket'] is not None:
        args['bucket'] = timedelta(seconds=float(args['bucket']))
    if args['fields'] is not None:
        args['fields'] = args['fields'].split(',')
    if rargs.get('sparse'):
        sparse_format = str_to_bool(rargs['sparse'])
    else:
        # By default, use a sparse format if only a subset of the possible fields is 
        # requested by the query.
        sparse_format = bool(args['fields'])

    batch_size = int(rargs.get('batch_size', -1))
    if batch_size <= 0:
        batch_size = None
    else:
        logger.debug("GET requested batch_size of %s", batch_size)

    result = Timeseries.get_one_by_batch(tdmq_id, anonymize_private, batch_size, args)

    def format_sparse_row(row: List) -> str:
        assert len(row) == len(result.fields)
        return json.dumps(dict(zip(result.fields, row)))

    def format_dense_row(row: List) -> str:
        return json.dumps(row)

    def generate(row_format_fn):
        first_batch = True
        response_opening = \
            f'{{"tdmq_id": {json.dumps(str(tdmq_id))},'\
            f'"shape": {json.dumps(result.shape)},'\
            f'"bucket": {json.dumps(result.bucket)},'\
            f'"fields": {json.dumps(result.fields)},'\
            f'"sparse": {json.dumps(sparse_format)},'
        if result.default_footprint:
            response_opening += f'"default_footprint": {json.dumps(result.default_footprint)},'
        response_opening += '"items": ['
        yield response_opening
        for batch in result:
            if not first_batch:  # First batch does not need pre-pending the comma
                yield ','
                first_batch = False
            yield ','.join(row_format_fn(row) for row in batch)
        yield ']}'  # response closing
    return current_app.response_class(
        generate(format_sparse_row if sparse_format else format_dense_row),
        content_type='application/json')


def timeseries_get(tdmq_id):
    """
    Old implementation of GET /timeseries that retrieves and returns the entire query set at once.
    """
    rargs = request.args

    anonymize_private = str_to_bool(rargs.get('anonymized', 'true'))
    if not anonymize_private and not _request_authorized():
        raise wex.Unauthorized("Unauthorized request for unanonymized private data")

    args = dict((k, rargs.get(k, None))
                for k in ['after', 'before', 'bucket', 'fields', 'op'])
    if args['bucket'] is not None:
        args['bucket'] = timedelta(seconds=float(args['bucket']))
    if args['fields'] is not None:
        args['fields'] = args['fields'].split(',')

    result = Timeseries.get_one(tdmq_id, anonymize_private, args)
    jres = jsonify(result)
    return jres


@tdmq_bp.route('/sources/<uuid:tdmq_id>/activity/latest')
def source_activity_latest(tdmq_id):
    result = Source.get_latest_activity(tdmq_id)
    if result is None:
        raise wex.NotFound(f"source {tdmq_id} does not exist")
    jres = jsonify(result)
    return jres


@tdmq_bp.route('/records', methods=["POST"])
@auth_required
def records_post():
    def validate_record(record):
        if not all(k in record for k in ('time', 'data')) or \
           not any(k in record for k in ('tdmq_id', 'source')):
            raise wex.BadRequest(
                "Missing fields in request.  "
                "Mandatory fields: 'time', 'data', ('tdmq_id' or 'source')")

    data = request.json
    for record in data:
        validate_record(record)
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
