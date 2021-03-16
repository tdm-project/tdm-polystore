
from collections import Counter

import pytest
from requests.exceptions import HTTPError
from tdmq.client import Client


def check_source(s, d):
    for k in ['entity_category', 'entity_type']:
        assert getattr(s, k) == d[k]
    sdf = s.default_footprint
    ddf = d['default_footprint']
    assert sdf['type'] == ddf['type']
    assert sdf['coordinates'][0] == pytest.approx(ddf['coordinates'][0])
    assert sdf['coordinates'][1] == pytest.approx(ddf['coordinates'][1])
    assert s.controlled_properties == d['controlledProperties']
    assert s.shape == ()


def register_sources(client, descs, check_source=check_source):
    srcs = []
    for d in descs:
        s = client.register_source(d, nslots=3600)
        check_source(s, d)
        srcs.append(s)
    return srcs


def is_scalar(d):
    return 'shape' not in d or len(d['shape']) == 0


def register_scalar_sources(client, source_data):
    return register_sources(client, [d for d in source_data['sources'] if is_scalar(d)])


def test_register_deregister_simple_source_as_admin(clean_storage, public_source_data, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    srcs = register_scalar_sources(c, public_source_data)
    sources = dict((_.tdmq_id, _) for _ in c.find_sources())
    tdmq_ids = []
    for s in srcs:
        assert s.tdmq_id in sources
        assert s.id == sources[s.tdmq_id].id
        assert s.external_id == sources[s.tdmq_id].id
        assert s.external_id == s.id
        assert s.tdmq_id == sources[s.tdmq_id].tdmq_id
        tdmq_id = s.tdmq_id
        c.deregister_source(s)
        tdmq_ids.append(tdmq_id)
    sources = dict((_.tdmq_id, _) for _ in c.find_sources())
    for tid in tdmq_ids:
        assert tid not in sources


def test_register_deregister_nonscalar_source_as_admin(clean_storage, public_source_data, live_app):
    import tiledb
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    source_def = next(s for s in public_source_data['sources'] if s['shape'])
    source = c.register_source(source_def)
    assert c.get_source(source.tdmq_id)
    # pylint: disable=protected-access
    assert tiledb.object_type(c._source_data_path(source.tdmq_id), ctx=c.tiledb_ctx) is not None
    c.deregister_source(source)

    with pytest.raises(HTTPError) as exc_info:
        c.get_source(source.tdmq_id)
    assert exc_info.value.response.status_code == 404
    assert tiledb.object_type(c._source_data_path(source.tdmq_id), ctx=c.tiledb_ctx) is None


def test_register_simple_source_as_user(clean_storage, public_source_data, live_app):
    c = Client(live_app.url())

    with pytest.raises(HTTPError) as exc_info:
        _ = register_scalar_sources(c, public_source_data)
    assert exc_info.value.response.status_code == 401


def test_deregister_simple_source_as_user(clean_storage, public_source_data, live_app):
    # first it creates a source with an admin client
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    srcs = register_scalar_sources(c, public_source_data)

    # then tries to deregister it with user client
    c = Client(live_app.url())
    with pytest.raises(HTTPError) as exc_info:
        c.deregister_source(srcs[0])
    assert exc_info.value.response.status_code == 401


def test_select_source_by_id(clean_storage, public_source_data, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    srcs = register_scalar_sources(c, public_source_data)
    for s in srcs:
        s2 = c.find_sources({'id': s.id})
        assert len(s2) == 1
        s2 = c.find_sources({'external_id': s.id})
        assert len(s2) == 1


def test_select_sources_by_entity_type(clean_storage, public_source_data, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    srcs = register_scalar_sources(c, public_source_data)
    counts = Counter([s.entity_type for s in srcs])
    for k in counts:
        s2 = c.find_sources({'entity_type': k})
        assert len(s2) == counts[k]


def test_access_attributes_scalar_source(clean_storage, db_data, public_source_data, live_app):
    c = Client(live_app.url())
    src_id = 'tdm/sensor_3'
    sources = c.find_sources(args={'id': src_id})
    assert len(sources) == 1
    s = sources[0]
    original = next(s for s in public_source_data['sources'] if s['id'] == src_id)
    assert s.id                         == original['id']
    assert s.is_stationary              == original.get('stationary', True)
    assert s.entity_category            == original['entity_category']
    assert s.entity_type                == original['entity_type']
    assert s.public                     == original['public']
    assert s.alias                      == original['alias']
    assert len(s.shape)                 == 0
    assert set(s.controlled_properties) == set(original['controlledProperties'])
    assert s.edge_id                    == original['description']['edge_id']
    assert s.station_id                 == original['description']['station_id']
    assert s.station_model              == original['description']['station_model']
    assert s.sensor_id                  == original['description']['sensor_id']
    assert s.registration_time          is not None


def test_access_attributes_scalar_source_private_unauthenticated(clean_storage, db_data, source_data, live_app):
    from tdmq.db import _compute_tdmq_id
    c = Client(live_app.url())
    src_id = 'tdm/sensor_7'
    tdmq_id = _compute_tdmq_id(src_id)
    s = c.get_source(tdmq_id)
    original = next(t for t in source_data['sources'] if t['id'] == src_id)
    assert s.id                         is None
    assert s.is_stationary              == original.get('stationary', True)
    assert s.entity_category            == original['entity_category']
    assert s.entity_type                == original['entity_type']
    assert s.public                     == original['public']
    assert s.alias                      is None
    assert len(s.shape)                 == 0
    assert set(s.controlled_properties) == set(original['controlledProperties'])
    assert s.edge_id                    == original['description']['edge_id']
    assert s.station_id                 == original['description']['station_id']
    assert s.station_model              == original['description']['station_model']
    assert s.sensor_id                  == original['description']['sensor_id']
    assert s.registration_time          is not None



def test_find_source_by_roi(clean_storage, db_data, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    geom = 'circle((9.132, 39.248), 1000)'
    results = c.find_sources(args={ 'roi': geom })
    external_ids = set( s.id for s in results )
    assert external_ids == { 'tdm/sensor_3', 'tdm/tiledb_sensor_6' }


def test_find_source_by_roi_as_user(clean_storage, db_data, live_app):
    c = Client(live_app.url())
    geom = 'circle((9.132, 39.248), 1000)'
    results = c.find_sources(args={ 'roi': geom })
    external_ids = set( s.id for s in results )
    assert external_ids == { 'tdm/sensor_3', 'tdm/tiledb_sensor_6' }


def test_find_anonymized_and_not_anonymized(clean_storage, db_data, source_data, live_app):
    c = Client(live_app.url())
    sources = c.find_sources(args={'only_public': 'false'})
    assert len(sources) == len(source_data['sources'])


def test_repr(clean_storage, db_data, live_app):
    c = Client(live_app.url())
    sources = c.find_sources(args={'only_public': 'false'})
    for s in sources:
        assert repr(s)
