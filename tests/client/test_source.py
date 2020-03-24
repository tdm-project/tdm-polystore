
import pytest

from collections import Counter

from tdmq.client import Client

# use the `live_server` fixture from pytest-flask to fire up the application
# so that we can sent real HTTP requests
# pytestmark = pytest.mark.usefixtures('live_server')


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


def test_register_deregister_simple_source(clean_hdfs, clean_db, public_source_data, live_app):
    c = Client(live_app.url())
    srcs = register_scalar_sources(c, public_source_data)
    sources = dict((_.tdmq_id, _) for _ in c.find_sources())
    tdmq_ids = []
    for s in srcs:
        assert s.tdmq_id in sources
        assert s.id == sources[s.tdmq_id].id
        assert s.tdmq_id == sources[s.tdmq_id].tdmq_id
        tdmq_id = s.tdmq_id
        c.deregister_source(s)
        tdmq_ids.append(tdmq_id)
    sources = dict((_.tdmq_id, _) for _ in c.find_sources())
    for tid in tdmq_ids:
        assert tid not in sources


def test_select_source_by_id(clean_hdfs, clean_db, public_source_data, live_app):
    c = Client(live_app.url())
    srcs = register_scalar_sources(c, public_source_data)
    for s in srcs:
        s2 = c.find_sources({'id': s.id})
        assert len(s2) == 1
    for s in srcs:
        c.deregister_source(s)


def test_select_sources_by_entity_type(clean_hdfs, clean_db, public_source_data, live_app):
    c = Client(live_app.url())
    srcs = register_scalar_sources(c, public_source_data)
    counts = Counter([s.entity_type for s in srcs])
    for k in counts:
        s2 = c.find_sources({'entity_type': k})
        assert len(s2) == counts[k]
    for s in srcs:
        c.deregister_source(s)
        # FIXME: add assertions


def test_find_source_by_roi(db_data, live_app):
    c = Client(live_app.url())
    geom = 'circle((9.132, 39.248), 1000)'
    results = c.find_sources(args={ 'roi': geom })
    external_ids = set( s.id for s in results )
    assert external_ids == { 'tdm/sensor_3', 'tdm/tiledb_sensor_6' }
