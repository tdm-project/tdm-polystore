import pytest
import json
from tdmq.client.client import Client
import numpy as np


def check_source(s, d):
    for k in ['entity_category', 'entity_type']:
        assert getattr(s, k) == d[k]
    assert s.shape == tuple(d['shape'])
    sdf = s.default_footprint
    ddf = d['default_footprint']
    assert sdf['type'] == ddf['type']
    assert np.allclose(sdf['coordinates'], ddf['coordinates'])


def test_register_deregister_nonscalar_source():
    c = Client()
    with open('../data/sources.json') as f:
        descs = json.load(f)['sources']
    descs = [d for d in descs if 'shape' in d and len(d['shape']) > 0]
    srcs = []
    for d in descs:
        s = c.register_source(d)
        check_source(s, d)
        srcs.append(s)
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
