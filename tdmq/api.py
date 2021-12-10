import copy
import json
import logging
from datetime import timedelta
from functools import wraps
from http import HTTPStatus
from typing import List

import werkzeug.exceptions as wex
from flask import Blueprint, current_app, jsonify, request
from flask import render_template

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
    if e.code >= 500:
        response_logger.exception(e)
    elif response_logger.isEnabledFor(logging.DEBUG):
        response_logger.exception(e)
    struct = {
        "error": ERROR_CODES.get(e.code),
        "description": e.description
    }
    return jsonify(struct), e.code


@tdmq_bp.app_errorhandler(tdmq.errors.TdmqError)
def handle_tdmq_error(e):
    response_logger = logging.getLogger("response")
    if e.status >= 500:
        response_logger.exception(e)
    elif response_logger.isEnabledFor(logging.DEBUG):
        response_logger.exception(e)
    struct = {
        "error": e.title,
        "code": e.status,
        "description": e.detail
    }
    return jsonify(struct), e.status


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
        # If neither 'public' nor 'only_public' have been specified, default to public=True
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


@tdmq_bp.route('/sources/<uuid:tdmq_id>/timeseries_stream')
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
        # By default, use a dense format if only a subset of the fields is
        # requested by the query.
        sparse_format = not bool(args['fields'])

    data_format = rargs.get('format', 'json')
    if data_format not in ('json', 'csv'):
        raise wex.BadRequest(f"Unknown/unsupported format {data_format}")

    batch_size = int(rargs.get('batch_size', 2500))
    assert batch_size > 0
    logger.debug("GET using batch_size of %s", batch_size)

    result = Timeseries.get_one_by_batch(tdmq_id, anonymize_private, batch_size, args)

    if data_format == 'json':
        response = current_app.response_class(
            generate_ts_json(result, sparse_format),
            content_type='application/json')
    else:
        response = current_app.response_class(
            generate_ts_csv(result), content_type='text/csv')
        response.headers["Content-Disposition"] = f"attachment;filename={result.tdmq_id}.csv"
    return response


def generate_ts_json(resultset, sparse_format: bool):
    def format_sparse_row(row: List) -> str:
        assert len(row) == len(resultset.fields)
        d = {field_name: value for field_name, value in zip(resultset.fields, row) if value is not None}
        return json.dumps(d)

    def format_dense_row(row: List) -> str:
        return json.dumps(row)

    row_format_fn = format_sparse_row if sparse_format else format_dense_row

    logger.debug("Generating JSON timeseries output")
    response_opening = \
        f'{{"tdmq_id": {json.dumps(str(resultset.tdmq_id))},'\
        f'"shape": {json.dumps(resultset.shape)},'\
        f'"bucket": {json.dumps(resultset.bucket)},'\
        f'"fields": {json.dumps(resultset.fields)},'\
        f'"sparse": {json.dumps(sparse_format)},'
    if resultset.default_footprint:
        response_opening += f'"default_footprint": {json.dumps(resultset.default_footprint)},'
    response_opening += '"items": ['
    yield response_opening
    first_batch = True
    for batch in resultset:
        logger.debug("Timeseries: sending %s records", len(batch))
        if not first_batch:  # First batch does not need pre-pending the comma
            yield ','
        yield ','.join((row_format_fn(row) for row in batch))
        first_batch = False
    yield ']}'  # response closing


def generate_ts_csv(resultset):
    def format_row(row: List) -> str:
        return ','.join( (str(v if v is not None else '') for v in row) )

    logger.debug("Generating CSV timeseries output")
    # header row
    yield ','.join(resultset.fields) + "\n"
    # content
    for batch in resultset:
        logger.debug("Timeseries: sending %s records", len(batch))
        yield '\n'.join((format_row(row) for row in batch))


@tdmq_bp.route('/sources/<uuid:tdmq_id>/timeseries')
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
        if not all(record.get(k) for k in ('time', 'data')) or \
           not any(record.get(k) for k in ('tdmq_id', 'source')):
            raise wex.BadRequest(
                "Missing fields in POSTed timeseries record.  "
                "Mandatory fields: 'time', 'data', ('tdmq_id' or 'source').  "
                f"Received keys: {record.keys()}")

    data = request.json
    for record in data:
        validate_record(record)
    n = Timeseries.store_new_records(data)
    return jsonify({"loaded": n})


@tdmq_bp.route('/')
@tdmq_bp.route('/service_info')
def service_info_get():
    response = {
        'version': '0.1'
    }

    # Check whether the client is inside or outside the local network.  We do
    # this by checking whether the Host to which the request was addressed is in
    # the domain configured as "EXTERNAL_HOST_DOMAIN" (if it was configured).
    external_client = current_app.config.get('EXTERNAL_HOST_DOMAIN') and \
        request.headers.get('Host', '').endswith(current_app.config.get('EXTERNAL_HOST_DOMAIN'))
    response['client-origin'] = 'external' if external_client else 'internal'

    if request.headers.get('Authorization'):
        oauth2_conf = {
            'jwt_token': f"Authorization: {request.headers['Authorization']}"
        }

        if 'X-Forwarded-User' in request.headers:
            oauth2_conf['user_name'] = request.headers['X-Forwarded-User']

        if 'X-Forwarded-Email' in request.headers:
            oauth2_conf['user_email'] = request.headers['X-Forwarded-Email']

        response['oauth2'] = oauth2_conf

    if external_client and current_app.config.get('TILEDB_EXTERNAL_VFS'):
        vfs_config = current_app.config['TILEDB_EXTERNAL_VFS']
    else:
        vfs_config = current_app.config.get('TILEDB_INTERNAL_VFS')

    if vfs_config is not None:
        response_tiledb_conf = {
            'storage.root': vfs_config['storage.root'],
            'config': copy.deepcopy(vfs_config['config'])
        }

        if _request_authorized():
            response_tiledb_conf['config'].update(vfs_config.get('credentials', {}))

        response['tiledb'] = response_tiledb_conf

    if request.accept_mimetypes.accept_html:
        return render_template('service_info.html', data=response)
    return jsonify(response)
