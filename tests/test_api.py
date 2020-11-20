
import logging
import re
import tempfile
from contextlib import contextmanager
from datetime import datetime

import pytest
from tdmq.app import create_app

logger = logging.getLogger('test_api')


@contextmanager
def _create_new_app_test_client(config=None):
    if config is not None and 'TESTING' not in config:
        config = config.copy()
        config['TESTING'] = True
        config['LOG_LEVEL'] = 'DEBUG'
        config['PROMETHEUS_REGISTRY'] = True

    app = create_app(config)
    with app.app_context():
        yield app.test_client()


def _checkresp(response, table=None):
    assert response.status == '200 OK'
    assert response.is_json
    if table:
        result = response.get_json()
        assert len(result) == len(table)
        for r in result:
            assert "tdmq_id" in r
            tdmq_id = r.pop("tdmq_id")
            assert r == table[tdmq_id]


def _parse_datetime(s):
    d = datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
    return d


def _filter_records_in_time_range(records, after=None, before=None):
    if after:
        after = _parse_datetime(after)
    else:
        after = datetime.min

    if before:
        before = _parse_datetime(before)
    else:
        before = datetime.max

    return [r for r in records if
            _parse_datetime(r['time']) >= after and
            _parse_datetime(r['time']) < before]


def _filter_records_in_time_range_and_source(records, after=None, before=None, source_id=None):
    if source_id is not None:
        return [r for r in _filter_records_in_time_range(records, after, before) if r['source'] == source_id]
    else:
        return _filter_records_in_time_range(records, after, before)


def _get_active_sources_in_time_range(records, after=None, before=None):
    filtered_records = _filter_records_in_time_range(records, after, before)
    return set(r['source'] for r in filtered_records)


def _validate_ids(data, expected):
    assert set(s['external_id'] for s in data) == expected


def _create_auth_header(token):
    return {'Authorization': f'Bearer {token}'} if token is not None else {}


@pytest.mark.sources
def test_sources_db_error(flask_client):
    """
    Tests that, if a database error occurs when a client tries to get the sources, it returns a 500 error
    """
    # IMPORTANT: it must be left as first test in the file otherwise it fails since the other tests create the db
    response = flask_client.get('/sources')
    assert response.status == '500 INTERNAL SERVER ERROR'
    assert response.get_json() == {"error": "error_retrieving_data"}


@pytest.mark.sources
def test_source_types(flask_client, db_data):
    in_args = {"type": "multisource", "controlledProperties": "temperature"}
    q = "&".join(f"{k}={v}" for k, v in in_args.items())
    response = flask_client.get('/sources?{q}')
    _checkresp(response)
    data = response.get_json()

    for s in data:
        assert s.description['type'] == in_args['type']
        assert s.controlledProperties == in_args['controlledProperties'].split(
            ',')


def _create_source(flask_client):
    """
    Tests that, when a client tries to create a Source that already exist, a 409 CONFLICT error is returned
    """
    source_data = [{
        "id": "st1",
        "alias": "st1",
        "entity_type": "WeatherObserver",
        "entity_category": "Station",
        "default_footprint": {
            "type": "Point",
            "coordinates": [1.1, 2.2]
        },
        "stationary": True,
        "controlledProperties": ["windDirection", "windSpeed"],
        "shape": [],
        "description": {}
    }]
    headers = {
        'Authorization': 'Bearer supersecret'
    }
    return flask_client.post('/sources', json=source_data, headers=headers)


@pytest.mark.sources
def test_source_create(flask_client, db_data):
    response = _create_source(flask_client)
    _checkresp(response)


@pytest.mark.sources
def test_source_delete(flask_client, db_data):
    response = _create_source(flask_client)
    tdmq_id = response.get_json()[0]
    headers = {
        'Authorization': 'Bearer supersecret'
    }
    response = flask_client.delete(f'/sources/{tdmq_id}', headers=headers)
    assert response.status == '200 OK'


@pytest.mark.sources
def test_source_delete_unauthorized(flask_client, db_data):
    response = _create_source(flask_client)
    tdmq_id = response.get_json()[0]
    response = flask_client.delete(f'/sources/{tdmq_id}')
    assert response.status == '401 UNAUTHORIZED'


@pytest.mark.sources
def test_source_create_duplicate(flask_client, db_data):
    """
    Tests that, when a client tries to create a Source that already exist, a 409 CONFLICT error is returned
    """
    source_data = [{
        "id": "st1",
        "alias": "st1",
        "entity_type": "WeatherObserver",
        "entity_category": "Station",
        "default_footprint": {
            "type": "Point",
            "coordinates": [1.1, 2.2]
        },
        "stationary": True,
        "controlledProperties": ["windDirection", "windSpeed"],
        "shape": [],
        "description": {}
    }]
    headers = {
        'Authorization': 'Bearer supersecret'
    }
    response = flask_client.post('/sources', json=source_data, headers=headers)
    _checkresp(response)
    response = flask_client.post('/sources', json=source_data, headers=headers)
    assert response.status == '409 CONFLICT'
    assert response.get_json() == {"error": "duplicated_resource"}


@pytest.mark.sources
def test_source_create_unauthorized(flask_client, db_data):
    """
    Tests that, when a client tries to create a Source that already exist, a 409 CONFLICT error is returned
    """
    source_data = [{
        "id": "st1",
        "alias": "st1",
        "entity_type": "WeatherObserver",
        "entity_category": "Station",
        "default_footprint": {
            "type": "Point",
            "coordinates": [1.1, 2.2]
        },
        "stationary": True,
        "controlledProperties": ["windDirection", "windSpeed"],
        "shape": [],
        "description": {}
    }]
    response = flask_client.post('/sources', json=source_data)
    assert response.status == '401 UNAUTHORIZED'
    assert response.get_json() == {"error": "unauthorized"}


@pytest.mark.sources
def test_sources_method_not_allowed(flask_client):
    """
    Tests that, if /sources is called using an http method different from POST and GET it returns a 405 METHOD NOT ALLOWED
    """
    response = flask_client.delete('/sources')
    assert response.status == '405 METHOD NOT ALLOWED'
    assert response.get_json() == {"error": "method_not_allowed"}


@pytest.mark.sources
def test_sources_get_no_args(flask_client, app, db_data, public_source_data):
    response = flask_client.get('/sources')
    _checkresp(response)
    data = response.get_json()
    assert isinstance(data, list)
    assert len(data) == len(public_source_data['sources'])
    _validate_ids(data, set(s['id'] for s in public_source_data['sources']))


@pytest.mark.sources
def test_sources_get_only_geom(flask_client, app, db_data, public_source_data):
    geom = 'circle((9.132, 39.248), 1000)'
    q = f'roi={geom}'
    response = flask_client.get(f'/sources?{q}')
    _checkresp(response)
    data = response.get_json()
    _validate_ids(data, {'tdm/sensor_3', 'tdm/tiledb_sensor_6'})


@pytest.mark.sources
def test_sources_get_active_after_before(flask_client, app, db_data, public_source_data):
    after, before = '2019-05-02T11:30:00Z', '2019-05-02T12:30:00Z'
    q = f'after={after}&before={before}'
    response = flask_client.get(f'/sources?{q}')
    _checkresp(response)
    #  expected = { 'tdm/tiledb_sensor_6' }
    expected = _get_active_sources_in_time_range(
        public_source_data['records'], after, before)
    _validate_ids(response.get_json(), expected)


@pytest.mark.sources
def test_sources_get_active_after(flask_client, app, db_data, public_source_data):
    after = '2019-05-02T11:00:22Z'
    q = f'after={after}'
    response = flask_client.get(f'/sources?{q}')
    _checkresp(response)
    #  expected = { 'tdm/sensor_0', 'tdm/sensor_1', 'tdm/tiledb_sensor_6' }
    expected = _get_active_sources_in_time_range(
        public_source_data['records'], after)
    _validate_ids(response.get_json(), expected)


@pytest.mark.sources
def test_sources_get_active_before(flask_client, app, db_data, public_source_data):
    before = '2019-05-02T11:00:00Z'
    q = f'before={before}'
    response = flask_client.get(f'/sources?{q}')
    _checkresp(response)
    #  expected = { 'tdm/sensor_0', 'tdm/sensor_1' }
    expected = _get_active_sources_in_time_range(
        public_source_data['records'], None, before)
    _validate_ids(response.get_json(), expected)


@pytest.mark.sources
def test_sources_get_active_after_geom(flask_client, app, db_data, public_source_data):
    geom = 'circle((8.93, 39.0), 10000)'  # point is near the town of Pula
    after = '2019-05-02T11:00:22Z'
    q = f'roi={geom}&after={after}'
    response = flask_client.get(f'/sources?{q}')
    _checkresp(response)
    #  expected = { 'tdm/sensor_0', 'tdm/tiledb_sensor_6' }
    expected = _get_active_sources_in_time_range(
        public_source_data['records'], after, None)
    # sensor_1 is out of the selected geographic region
    expected.remove('tdm/sensor_1')
    _validate_ids(response.get_json(), expected)


@pytest.mark.sources
def test_sources_get_fail(flask_client):
    geom = 'circle((9.2 33), 1000)'  # note the missing comma
    q = f'roi={geom}'
    with pytest.raises(ValueError) as ve:
        flask_client.get(f'/sources?{q}')
        assert "roi" in ve.value
        assert geom in ve.value


@pytest.mark.sources
def test_source_query_by_tdmq_id(flask_client, app, db_data, public_source_data):
    external_source_id = public_source_data['sources'][0]['id']
    response_with_id = flask_client.get(f'/sources?id={external_source_id}')
    item_with_id = response_with_id.get_json()[0]
    tdmq_id = item_with_id['tdmq_id']

    # query again using tdmq_id
    response_with_tdmq_id = flask_client.get(f"/sources/{tdmq_id}")

    assert item_with_id['external_id'] == response_with_tdmq_id.get_json()[
        'external_id']


@pytest.mark.timeseries
def test_timeseries_method_not_allowed(flask_client):
    # Test the records endpoint
    for method in ('delete', 'put', 'get'):
        response = getattr(flask_client, method)(f'/records')
        assert response.status == '405 METHOD NOT ALLOWED'

    # Test the sources/{tdmq_id}/timeseries endpoint
    response = _create_source(flask_client)
    tdmq_id = response.get_json()[0]

    for method in ('delete', 'put', 'post'):
        response = getattr(flask_client, method)(f'/sources/{tdmq_id}/timeseries')
        assert response.status == '405 METHOD NOT ALLOWED'


@pytest.mark.timeseries
def test_create_timeseries(flask_client, app, db_data):
    _create_source(flask_client)    
    timeseries_data = [{
        "time": "2019-05-02T10:50:00Z", 
        "source": "st1", 
        "data": {"temperature": 20}
    }]
    headers = {
        'Authorization': 'Bearer supersecret'
    }
    response = flask_client.post(
        '/records', json=timeseries_data, headers=headers)
    assert response.status == '200 OK'
    assert response.is_json
    assert response.get_json() == {'loaded': 1}


@pytest.mark.timeseries
def test_create_timeseries_unauthorized(flask_client, app, db_data):
    _create_source(flask_client)    
    timeseries_data = [{
        "source": "s1",
        "time": "2020-11-17T14:20:30Z",
        "footprint": {
            "type": "Point",
            "coordinates": [1, 0]
        },
        "data": {"temperature": 14.0}
    }]

    response = flask_client.post('/records', json=timeseries_data)
    assert response.status == '401 UNAUTHORIZED'
    assert response.get_json() == {"error": "unauthorized"}


@pytest.mark.timeseries
def test_get_timeseries(flask_client, app, db_data):
    source_id = 'tdm/sensor_1'
    response = flask_client.get(f'/sources?id={source_id}')
    tdmq_id = response.get_json()[0]['tdmq_id']

    bucket, op = 20 * 60, 'sum'
    q = f'bucket={bucket}&op={op}'
    response = flask_client.get(f'/sources/{tdmq_id}/timeseries?{q}')
    _checkresp(response)
    d = response.get_json()

    for attr in ("tdmq_id", "default_footprint", "shape", "bucket", "coords", "data"):
        assert attr in d

    assert 'time' in d['coords']
    assert 'footprint' in d['coords']

    assert d['tdmq_id'] == tdmq_id
    assert d['shape'] is None or len(d['shape']) == 0
    assert d['bucket'] is not None
    assert d['bucket']['op'] == op
    assert d['bucket']['interval'] == bucket
    assert 'temperature' in d['data'] and 'humidity' in d['data']


@pytest.mark.timeseries
def test_get_timeseries_for_private_sources(flask_client, app, db_data):
    source_id = 'tdm/sensor_7'
    response = flask_client.get(f'/sources?id={source_id}')
    assert response.status == '200 OK'
    _checkresp(response, [])


def test_entity_types_method_not_allowed(flask_client):

    for method in ('post', 'delete', 'put'):
        response = getattr(flask_client, method)(f'/entity_types', json={})
        assert response.status == '405 METHOD NOT ALLOWED'


def test_entity_categories_method_not_allowed(flask_client):

    for method in ('post', 'delete', 'put'):
        response = getattr(flask_client, method)(f'/entity_categories', json={})
        assert response.status == '405 METHOD NOT ALLOWED'


@pytest.mark.config
def test_get_service_info(flask_client):
    resp = flask_client.get(f'/service_info')
    _checkresp(resp)
    info = resp.json
    assert info.get('version') is not None
    assert re.fullmatch(r'(\d+\.){1,2}\d+', info['version'])
    assert 'tiledb' in info
    assert 'storage.root' in info['tiledb']
    assert 'config' in info['tiledb']
    assert info['tiledb']['storage.root'] is not None
    # By default, credentials for s3 storage are configured,
    # so we may have a key like vfs.s3.aws_secret_access_key.
    # This shouldn't be returned unless the token is provided.
    assert all(('secret' not in s for s in info['tiledb']['config']))


@pytest.mark.config
def test_get_service_info_authenticated(flask_client):
    resp = flask_client.get(f'/service_info',
                            headers=_create_auth_header(flask_client.auth_token))
    _checkresp(resp)
    info = resp.json
    assert info.get('version') is not None
    assert re.fullmatch(r'(\d+\.){1,2}\d+', info['version'])
    assert 'tiledb' in info
    assert 'vfs.s3.aws_access_key_id' in info['tiledb']['config']
    assert 'vfs.s3.aws_secret_access_key' in info['tiledb']['config']
    # Request again without authentication
    resp = flask_client.get(f'/service_info')
    _checkresp(resp)
    info = resp.json
    assert 'tiledb' in info
    assert 'vfs.s3.aws_access_key_id' not in info['tiledb']['config']
    assert 'vfs.s3.aws_secret_access_key' not in info['tiledb']['config']




@pytest.mark.config
def test_app_config_tiledb():
    hdfs_root = 'hdfs://someserver:8020/'
    k, v = 'vfs.hdfs.property', 'pippo'
    config = {
        'TILEDB_VFS_ROOT': hdfs_root,
        'TILEDB_VFS_CONFIG': {k: v},
        'APP_PREFIX': ''
    }

    with _create_new_app_test_client(config) as client:
        resp = client.get(f'/service_info')
        _checkresp(resp)
        info = resp.json
        assert 'tiledb' in info
        assert info['tiledb']['storage.root'] == hdfs_root
        assert info['tiledb']['config'][k] == v



@pytest.mark.config
def test_app_config_no_tiledb():
    config = {
        'TILEDB_VFS_ROOT': None,
        'APP_PREFIX': ''
    }

    with _create_new_app_test_client(config) as client:
        resp = client.get(f'/service_info')
        _checkresp(resp)
        info = resp.json
        assert 'tiledb' not in info


@pytest.mark.config
def test_app_config_from_file(monkeypatch):
    vfs_root = 's3://mybucket/'
    cfg = f"TILEDB_VFS_ROOT = '{vfs_root}'\n" \
        "TILEDB_VFS_CONFIG = { 'vfs.s3.property': 'bla' }\n" \
        "APP_PREFIX = ''"

    with tempfile.NamedTemporaryFile(mode='w') as f:
        f.write(cfg)
        f.flush()

        monkeypatch.setenv('TDMQ_FLASK_CONFIG', f.name)
        with _create_new_app_test_client() as client:
            resp = client.get(f'/service_info')
            _checkresp(resp)
            info = resp.json
            assert 'tiledb' in info
            assert info['tiledb']['storage.root'] == vfs_root
            assert info['tiledb']['config']['vfs.s3.property'] == 'bla'
