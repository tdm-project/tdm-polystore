
import pytest
import re
from datetime import datetime


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


def test_source_types(flask_client, app, db_data):
    in_args = {"type": "multisource", "controlledProperties": "temperature"}
    q = "&".join(f"{k}={v}" for k, v in in_args.items())
    response = flask_client.get(f'/sources?{q}')
    _checkresp(response)
    data = response.get_json()

    for s in data:
        assert s.description['type'] == in_args['type']
        assert s.controlledProperties == in_args['controlledProperties'].split(',')


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

    return [ r for r in records if
             _parse_datetime(r['time']) >= after and
             _parse_datetime(r['time']) < before ]


def _filter_records_in_time_range_and_source(records, after=None, before=None, source_id=None):
    if source_id is not None:
        return [ r for r in _filter_records_in_time_range(records, after, before) if r['source'] == source_id ]
    else:
        return _filter_records_in_time_range(records, after, before)


def _get_active_sources_in_time_range(records, after=None, before=None):
    filtered_records = _filter_records_in_time_range(records, after, before)
    return set( r['source'] for r in filtered_records )


def _validate_ids(data, expected):
    assert set( s['external_id'] for s in data ) == expected


def test_sources_no_args(flask_client, app, db_data, source_data):
    response = flask_client.get('/sources')
    _checkresp(response)
    data = response.get_json()
    assert isinstance(data, list)
    assert len(data) == len(source_data['sources'])
    _validate_ids(data, set( s['id'] for s in source_data['sources'] ))


def test_sources_only_geom(flask_client, app, db_data, source_data):
    geom = 'circle((9.132, 39.248), 1000)'
    q = f'roi={geom}'
    response = flask_client.get(f'/sources?{q}')
    _checkresp(response)
    data = response.get_json()
    _validate_ids(data, { 'tdm/sensor_3', 'tdm/tiledb_sensor_6' })


def test_sources_active_after_before(flask_client, app, db_data, source_data):
    after, before = '2019-05-02T11:30:00Z', '2019-05-02T12:30:00Z'
    q = f'after={after}&before={before}'
    response = flask_client.get(f'/sources?{q}')
    _checkresp(response)
    #  expected = { 'tdm/tiledb_sensor_6' }
    expected = _get_active_sources_in_time_range(source_data['records'], after, before)
    _validate_ids(response.get_json(), expected)


def test_sources_active_after(flask_client, app, db_data, source_data):
    after = '2019-05-02T11:00:22Z'
    q = f'after={after}'
    response = flask_client.get(f'/sources?{q}')
    _checkresp(response)
    #  expected = { 'tdm/sensor_0', 'tdm/sensor_1', 'tdm/tiledb_sensor_6' }
    expected = _get_active_sources_in_time_range(source_data['records'], after)
    _validate_ids(response.get_json(), expected)


def test_sources_active_before(flask_client, app, db_data, source_data):
    before = '2019-05-02T11:00:00Z'
    q = f'before={before}'
    response = flask_client.get(f'/sources?{q}')
    _checkresp(response)
    #  expected = { 'tdm/sensor_0', 'tdm/sensor_1' }
    expected = _get_active_sources_in_time_range(source_data['records'], None, before)
    _validate_ids(response.get_json(), expected)


def test_sources_active_after_geom(flask_client, app, db_data, source_data):
    geom = 'circle((8.93, 39.0), 10000)'  # point is near the town of Pula
    after = '2019-05-02T11:00:22Z'
    q = f'roi={geom}&after={after}'
    response = flask_client.get(f'/sources?{q}')
    _checkresp(response)
    #  expected = { 'tdm/sensor_0', 'tdm/tiledb_sensor_6' }
    expected = _get_active_sources_in_time_range(source_data['records'], after, None)
    expected.remove('tdm/sensor_1')  # sensor_1 is out of the selected geographic region
    _validate_ids(response.get_json(), expected)


def test_sources_fail(flask_client):
    geom = 'circle((9.2 33), 1000)'  # note the missing comma
    q = f'roi={geom}'
    with pytest.raises(ValueError) as ve:
        flask_client.get(f'/sources?{q}')
        assert "roi" in ve.value
        assert geom in ve.value


def test_source_query_by_tdmq_id(flask_client, app, db_data, source_data):
    external_source_id = source_data['sources'][0]['id']
    response_with_id = flask_client.get(f'/sources?id={external_source_id}')
    item_with_id = response_with_id.get_json()[0]
    tdmq_id = item_with_id['tdmq_id']

    # query again using tdmq_id
    response_with_tdmq_id = flask_client.get(f"/sources/{tdmq_id}")

    assert item_with_id['external_id'] == response_with_tdmq_id.get_json()['external_id']


def test_timeseries(flask_client, app, db_data):
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


def test_service_info(flask_client):
    resp = flask_client.get('service_info')
    _checkresp(resp)
    info = resp.json
    assert info.get('version') is not None
    assert re.fullmatch(r'(\d+\.){1,2}\d+', info['version'])
    if 'tiledb' in info:
        assert info['tiledb'].get('hdfs.root') is not None
