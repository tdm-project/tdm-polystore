

import copy
import operator as op
import pytest

import tdmq.db as db_query
from tdmq.errors import ItemNotFoundException
from test_api import _filter_records_in_time_range_and_source


def test_query_sources_simple(app, db_data, source_data):
    s1 = source_data['sources'][0]

    q_args = {'id': s1['id']}
    resultset = db_query.list_sources(q_args)

    assert len(resultset) == 1
    assert isinstance(resultset, list)
    assert isinstance(resultset[0], dict)

    row = resultset[0]
    expected_keys = {'tdmq_id', 'external_id', 'default_footprint', 'entity_category', 'entity_type'}
    assert set(row.keys()) >= expected_keys


def test_get_one_source(app, db_data, source_data):
    s1 = source_data['sources'][0]
    tdmq_id = db_query.list_sources({'id': s1['id']})[0]['tdmq_id']

    resultset = db_query.get_sources([tdmq_id])
    assert len(resultset) == 1
    assert isinstance(resultset, list)

    row = resultset[0]
    expected_keys = {"tdmq_id", "external_id", "default_footprint", "stationary", "entity_category", "entity_type", "description"}
    assert set(row.keys()) >= expected_keys
    assert row['tdmq_id'] == tdmq_id


def test_get_two_sources(app, db_data, source_data):
    source_ids = [s['id'] for s in source_data['sources'][0:2]]
    tdmq_ids = [
        db_query.list_sources({'id': source_ids[0]})[0]['tdmq_id'],
        db_query.list_sources({'id': source_ids[1]})[0]['tdmq_id']
    ]

    resultset = db_query.get_sources(tdmq_ids)
    assert len(resultset) == 2
    assert set(tdmq_ids) == set(r['tdmq_id'] for r in resultset)


def test_get_private_sources(app, db_data, source_data):
    private_sources = list(s for s in source_data['sources'] if s.get("public") is not True)
    results = db_query.list_sources({'id': private_sources[0]['id'] })
    assert len(results) == 1

    tdmq_id = results[0]['tdmq_id']
    resultset = db_query.get_sources([tdmq_id])
    assert len(resultset) == 1

    results = db_query.list_sources({'public': False })
    assert len(results) == len(private_sources)

    results = db_query.list_sources({'public': True })
    assert len(results) == len(source_data['sources']) - len(private_sources)


def test_query_source_offset_limit(app, db_data):
    src = db_query.list_sources({})
    assert len(src) > 2

    two_set = db_query.list_sources(limit=2)
    assert len(two_set) == 2

    first = db_query.list_sources(limit=1)
    assert len(first) == 1
    assert first[0]['tdmq_id'] == two_set[0]['tdmq_id']

    second = db_query.list_sources(limit=1, offset=1)
    assert len(second) == 1
    assert second[0]['tdmq_id'] == two_set[1]['tdmq_id']


def test_query_source_only_public(app, db_data):
    only_public_sources = db_query.list_sources({'public': True})
    for s in only_public_sources:
        assert s['public'] is True


def test_delete_source(app, db_data):
    src = db_query.list_sources(limit=1)

    db_query.delete_sources([src[0]['tdmq_id']])

    after_delete = db_query.get_sources([src[0]['tdmq_id']])
    assert after_delete == []


def test_list_categories(app, db):
    resultset = db_query.list_entity_categories("R")
    assert len(resultset) == 1
    assert resultset[0]['entity_category'].lower() == 'radar'

    resultset = db_query.list_entity_categories()
    assert len(resultset) > 1


def test_list_types(app, db):
    resultset = db_query.list_entity_types(type_start='Point')
    assert len(resultset) == 1
    assert isinstance(resultset[0], dict)
    assert resultset[0]['entity_type'].lower() == 'PointWeatherObserver'.lower()
    assert 'entity_category' in resultset[0]

    resultset = db_query.list_entity_types(type_start='point')
    assert len(resultset) == 1
    assert resultset[0]['entity_type'].lower() == 'PointWeatherObserver'.lower()

    resultset = db_query.list_entity_types(category_start='Radar', type_start='Point')
    assert resultset == []

    resultset = db_query.list_entity_types(category_start='Radar')
    assert len(resultset) == 1
    assert resultset[0]['entity_type'].lower() == 'MeteoRadarMosaic'.lower()


def test_get_timeseries_simple(app, db_data, source_data):
    our_source_id = 'tdm/sensor_0'
    all_src_recs = [r for r in source_data['records'] if r['source'] == our_source_id]

    src_from_db = db_query.list_sources({'id': our_source_id})[0]

    result = db_query.get_timeseries(src_from_db['tdmq_id'])

    assert set(result.keys()) >= {'source_info', 'public', 'properties', 'rows'}
    assert result['public'] is True
    assert set(result['properties']) >= set(all_src_recs[0]['data'].keys())
    assert len(result['rows']) == len(all_src_recs)
    assert len(result['rows'][0]) == 2 + len(result['properties'])


def test_get_timeseries_after(app, db_data, source_data):
    after = '2019-05-02T11:00:22Z'
    source_id = 'tdm/sensor_0'
    records = _filter_records_in_time_range_and_source(source_data['records'], after, source_id=source_id)

    tdmq_id = db_query.list_sources({'id': source_id})[0]['tdmq_id']

    result = db_query.get_timeseries(tdmq_id, {'after': after})

    assert \
        len(result['properties']) == \
        len(next(s for s in source_data['sources'] if s['id'] == source_id)['controlledProperties'])
    assert len(result['rows']) == len(records)


def test_get_timeseries_before(app, db_data, source_data):
    before = '2019-05-02T11:00:10Z'
    source_id = 'tdm/sensor_0'
    records = _filter_records_in_time_range_and_source(source_data['records'], before=before, source_id=source_id)

    tdmq_id = db_query.list_sources({'id': source_id})[0]['tdmq_id']

    result = db_query.get_timeseries(tdmq_id, {'before': before})

    assert \
        len(result['properties']) == \
        len(next(s for s in source_data['sources'] if s['id'] == source_id)['controlledProperties'])
    assert len(result['rows']) == len(records)


def test_get_timeseries_before_after(app, db_data, source_data):
    after = '2019-05-02T11:00:00Z'
    before = '2019-05-02T11:00:10Z'
    source_id = 'tdm/sensor_0'
    records = _filter_records_in_time_range_and_source(source_data['records'], after, before, source_id)

    tdmq_id = db_query.list_sources({'id': source_id})[0]['tdmq_id']
    result = db_query.get_timeseries(tdmq_id, {'after': after, 'before': before})

    assert \
        len(result['properties']) == \
        len(next(s for s in source_data['sources'] if s['id'] == source_id)['controlledProperties'])
    assert len(result['rows']) == len(records)


def test_get_timeseries_fields(app, db_data, source_data):
    source_id = 'tdm/sensor_0'
    tdmq_id = db_query.list_sources({'id': source_id})[0]['tdmq_id']
    original_source = next(s for s in source_data['sources'] if s['id'] == source_id)

    result = db_query.get_timeseries(tdmq_id)
    assert set(result['properties']) == set(original_source['controlledProperties'])

    result = db_query.get_timeseries(tdmq_id, {'fields': ['temperature']})
    assert result['properties'] == ['temperature']

    result = db_query.get_timeseries(tdmq_id, {'fields': ['relativeHumidity']})
    assert result['properties'] == ['relativeHumidity']

    result = db_query.get_timeseries(tdmq_id, {'fields': ['temperature', 'relativeHumidity']})
    assert result['properties'] == ['temperature', 'relativeHumidity']

    result = db_query.get_timeseries(tdmq_id, {'fields': ['relativeHumidity', 'temperature']})
    assert result['properties'] == ['relativeHumidity', 'temperature']


def test_get_timeseries_tdmq_id_not_found(app, db_data):
    with pytest.raises(ItemNotFoundException):
        db_query.get_timeseries('cc8d5c19-d269-4691-a692-9376223eb3d7')


def test_get_private_timeseries(app, db_data, source_data):
    sources = db_query.list_sources({'id': 'tdm/sensor_7'})
    assert len(sources) == 1
    private_source = sources[0]
    assert private_source['public'] is not True
    tdmq_id = private_source['tdmq_id']

    result = db_query.get_timeseries(tdmq_id)
    assert len(result['rows']) == 2
    assert result['public'] is not True


def test_get_shaped_timeseries(app, db_data, source_data):
    our_source_id = 'tdm/tiledb_sensor_6'
    all_src_recs = [r for r in source_data['records'] if r['source'] == our_source_id]

    tdmq_id = db_query.list_sources({'id': our_source_id})[0]['tdmq_id']

    result = db_query.get_timeseries(tdmq_id)

    assert len(result['source_info']['shape']) > 0
    assert result['properties'] == ['tiledb_index']
    assert len(result['rows'][0]) == 3
    assert len(result['rows']) == len(all_src_recs)


def test_get_bucketed_timeseries(app, db_data, source_data):
    our_source_id = 'tdm/sensor_0'
    all_src_recs = [r for r in source_data['records'] if r['source'] == our_source_id]

    tdmq_id = db_query.list_sources({'id': our_source_id})[0]['tdmq_id']

    result = db_query.get_timeseries(tdmq_id, {'fields': ['temperature'], 'bucket': '10', 'op': 'sum'})
    assert len(result['rows']) < len(all_src_recs)


def test_get_empty_timeseries(app, db_data, source_data):
    our_source_id = 'tdm/sensor_0'
    all_src_recs = sorted((r for r in source_data['records'] if r['source'] == our_source_id), key=op.itemgetter('time'))

    tdmq_id = db_query.list_sources({'id': our_source_id})[0]['tdmq_id']

    result = db_query.get_timeseries(tdmq_id, {'before': all_src_recs[0]['time']})
    assert len(result['rows']) == 0


def test_load_source(app, clean_db, source_data):
    results = db_query.list_sources({})
    assert len(results) == 0

    one_src = copy.deepcopy(source_data['sources'][0])

    tdmq_ids = db_query.load_sources([one_src])

    assert isinstance(tdmq_ids, list)
    assert len(tdmq_ids) == 1

    results = db_query.list_sources({})
    assert len(results) == 1
    assert results[0]['external_id'] == one_src['id']
    assert one_src == source_data['sources'][0]

    query_src = db_query.get_sources(tdmq_ids)
    assert len(query_src) == 1
    assert query_src[0]['tdmq_id'] == tdmq_ids[0]


def test_load_source_missing_public_attr(app, clean_db, source_data):
    one_src = copy.deepcopy(source_data['sources'][0])
    # remove the 'public attribute
    del one_src['public']
    assert 'public' not in one_src

    # If `public` isn't specified, the system should default to 'false'
    tdmq_ids = db_query.load_sources([one_src])
    retrieved = db_query.get_sources(tdmq_ids)
    assert len(retrieved) == 1
    item = retrieved[0]
    assert item['external_id'] == one_src['id']
    assert item['public'] is False


def test_load_records_one_src(app, clean_db, source_data):
    one_src = copy.deepcopy(source_data['sources'][0])
    records = copy.deepcopy(source_data['records_by_source'][one_src['id']])

    tdmq_id = db_query.load_sources([one_src])[0]

    n = db_query.load_records(records)
    assert n == len(records)
    # assert that load_records doesn't modify the `records` argument
    assert records == source_data['records_by_source'][one_src['id']]

    ts_data = db_query.get_timeseries(tdmq_id)
    assert len(ts_data['rows']) == n
    assert ts_data['source_info']['id'] == one_src['id']


def test_load_records_multiple_src(app, clean_db, source_data):
    tdmq_ids = db_query.load_sources(source_data['sources'])
    assert len(tdmq_ids) == len(source_data['sources'])
    n = db_query.load_records(source_data['records'])
    assert n == len(source_data['records'])

    records_by_source = source_data['records_by_source']
    for i in tdmq_ids:
        results = db_query.get_sources([i])
        assert len(results) == 1
        src = results[0]
        ts = db_query.get_timeseries(i)
        assert len(ts['rows']) == len(records_by_source[src['external_id']])
        assert ts['source_info']['id'] == src['external_id']
