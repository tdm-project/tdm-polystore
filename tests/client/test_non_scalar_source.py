

import logging
import numpy as np
from datetime import datetime, timedelta

from tdmq.client import Client
from tdmq.client.sources import NonScalarSource
from test_source import register_sources, is_scalar


def create_data_frame(shape, properties, slot):
    data = {}
    for p in properties:
        data[p] = np.full(shape, slot, dtype=np.float32)
    return data


def ingest_records(s, N):
    now = datetime.now()
    t = now
    dt = timedelta(seconds=10)
    for slot in range(N):
        data = create_data_frame(s.shape, s.controlled_properties, slot)
        s.ingest(t, data, slot)
        t += dt
    return now, dt


def check_records(source, timebase, dt, N):
    ts = source.timeseries()
    assert len(ts) == N
    assert ts.get_shape() == (N,) + source.shape

    ts_times, series = ts[:]

    for p in source.controlled_properties:
        assert series[p].shape == (N,) + source.shape

    t = timebase
    for i in range(N):
        assert (ts_times[i] - t).total_seconds() < 1.0e-5
        t += dt
        for p in source.controlled_properties:
            assert series[p][i].min() == series[p][i].max()
            assert int(series[p][i].min()) == i


def check_source(s, d):
    for k in ['entity_category', 'entity_type']:
        assert getattr(s, k) == d[k]
    assert s.shape == tuple(d['shape'])
    assert s.controlled_properties == d['controlledProperties']
    sdf = s.default_footprint
    ddf = d['default_footprint']
    assert sdf['type'] == ddf['type']
    assert np.allclose(sdf['coordinates'], ddf['coordinates'])


def check_deallocation(c, tdmq_ids):
    sources = dict((_.tdmq_id, _) for _ in c.find_sources())
    for tid in tdmq_ids:
        assert tid not in sources


def register_nonscalar_sources(c, source_data):
    return register_sources(c, [d for d in source_data['sources']
                                if not is_scalar(d)],
                            check_source=check_source)


def test_nonscalar_source_register_deregister(clean_hdfs, clean_db, source_data, live_app):
    c = Client(live_app.url())
    srcs = register_nonscalar_sources(c, source_data)
    logging.debug("Registered %s sources", len(srcs))
    sources = dict((_.tdmq_id, _) for _ in c.find_sources())
    tdmq_ids = []
    for s in srcs:
        logging.debug("source: id %s, shape: %s; type: %s", s.id, s.shape, type(s))
        assert s.tdmq_id in sources
        assert s.id == sources[s.tdmq_id].id
        assert s.tdmq_id == sources[s.tdmq_id].tdmq_id
        assert isinstance(s, NonScalarSource)
        tdmq_id = s.tdmq_id
        c.deregister_source(s)
        tdmq_ids.append(tdmq_id)
    check_deallocation(c, tdmq_ids)


def test_add_record_to_one_nonscalar_source(clean_hdfs, clean_db, source_data, live_app):
    N = 1
    c = Client(live_app.url())

    mosaic_def = next(s for s in source_data['sources'] if s['id'] == "tdm/tiledb_sensor_6")
    source = c.register_source(mosaic_def, nslots=N)
    assert len(source.shape) == 2

    timebase, dt = create_and_ingest_records(source, N)
    check_records(source, timebase, dt, N)


def test_nonscalar_source_add_records(clean_hdfs, clean_db, source_data, live_app):
    N = 3
    c = Client(live_app.url())
    srcs = register_nonscalar_sources(c, source_data)
    logging.debug("Registered %s sources", len(srcs))
    tdmq_ids = []
    for s in srcs:
        logging.debug("source: id %s, shape: %s; type: %s", s.id, s.shape, type(s))
        assert len(s.shape) > 0
        timebase, dt = ingest_records(s, N)
        check_records(s, timebase, dt, N)
        tdmq_id = s.tdmq_id
        c.deregister_source(s)
        tdmq_ids.append(tdmq_id)
    check_deallocation(c, tdmq_ids)
