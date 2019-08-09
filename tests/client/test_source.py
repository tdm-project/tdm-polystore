import pytest
from tdmq.client import Client
from collections import Counter


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


def register_sources(c, descs, check_source=check_source):
    srcs = []
    for d in descs:
        s = c.register_source(d)
        check_source(s, d)
        srcs.append(s)
    return srcs


def is_scalar(d):
    return 'shape' not in d or len(d['shape']) == 0


def register_sources_here(c, source_data):
    return register_sources(c, [d for d in source_data['sources']
                                if is_scalar(d)])


def test_register_deregister_simple_source(clean_hdfs, reset_db, source_data):
    c = Client()
    srcs = register_sources_here(c, source_data)
    sources = dict((_.tdmq_id, _) for _ in c.get_sources())
    tdmq_ids = []
    for s in srcs:
        assert s.tdmq_id in sources
        assert s.id == sources[s.tdmq_id].id
        assert s.tdmq_id == sources[s.tdmq_id].tdmq_id
        assert s.tdmq_id in c.managed_objects
        assert s == c.managed_objects[s.tdmq_id]
        assert s == sources[s.tdmq_id]
        tdmq_id = s.tdmq_id
        c.deregister_source(s)
        tdmq_ids.append(tdmq_id)
    sources = dict((_.tdmq_id, _) for _ in c.get_sources())
    for tid in tdmq_ids:
        assert tid not in sources


def test_select_source_by_id(clean_hdfs, reset_db, source_data):
    c = Client()
    srcs = register_sources_here(c, source_data)
    for s in srcs:
        s2 = c.get_sources({'id': s.id})
        assert len(s2) == 1
        assert s == s2[0]
    for s in srcs:
        c.deregister_source(s)


def test_select_sources_by_entity_type(clean_hdfs, reset_db, source_data):
    c = Client()
    srcs = register_sources_here(c, source_data)
    counts = Counter([s.entity_type for s in srcs])
    for k in counts:
        s2 = c.get_sources({'entity_type': k})
        assert len(s2) == counts[k]
    for s in srcs:
        c.deregister_source(s)


def test_select_source_by_roi(clean_hdfs, reset_db, source_data):
    assert False
