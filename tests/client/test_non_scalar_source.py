

import logging
import os
from datetime import datetime, timedelta

import numpy as np
import pytest
from requests.exceptions import HTTPError
from tdmq.client import Client
from tdmq.client.sources import NonScalarSource

from test_source import is_scalar, register_sources


def create_data_frame(shape, properties, fill_value):
    data = {}
    for p in properties:
        data[p] = np.full(shape, fill_value, dtype=np.float32)
    return data


def create_and_ingest_records(source, nrecs):
    now = datetime.now()
    t = now
    dt = timedelta(seconds=10)
    for slot in range(nrecs):
        frame = create_data_frame(source.shape, source.controlled_properties, slot)
        source.ingest(t, frame, slot)
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


def test_basic_tiledb_s3_operativity(clean_storage, service_info_with_creds):
    import tiledb

    storage_config = service_info_with_creds['tiledb']
    if not storage_config['storage.root'].startswith("s3"):
        return

    array_name = os.path.join(storage_config['storage.root'], 'basic-tiledb-s3-operativity')
    logging.debug("tiledb config parameters: %s", storage_config['config'])
    logging.debug("Array uri: %s", array_name)
    config = tiledb.Config(params=storage_config['config'])

    ctx = tiledb.Ctx(config=config)

    a = np.arange(5)
    logging.debug("trying to write array to s3: %s", array_name)
    schema = tiledb.schema_like(a, ctx=ctx)
    tiledb.DenseArray.create(array_name, schema)
    with tiledb.DenseArray(array_name, 'w', ctx=ctx) as T:
        T[:] = a

    logging.debug("reading back s3-backed array %s ", array_name)
    with tiledb.DenseArray(array_name, ctx=ctx) as t:
        assert (t[0:5] == a).all()


def test_nonscalar_source_register_deregister_as_admin(clean_storage, source_data, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
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


def test_nonscalar_source_register_as_user(clean_storage, source_data, live_app):
    c = Client(live_app.url())
    with pytest.raises(HTTPError) as ve:
        register_nonscalar_sources(c, source_data)
        assert ve.code == 401


def test_nonscalar_source_deregister_as_user(clean_storage, source_data, live_app):
    # first it creates a source with an admin client
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    srcs = register_nonscalar_sources(c, source_data)

    c = Client(live_app.url())
    with pytest.raises(HTTPError) as ve:
        c.deregister_source(srcs[0])
        assert ve.code == 401

def test_nonscalar_source_access_as_user(clean_storage, source_data, live_app):
    # first creates the source as admin
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    srcs = register_nonscalar_sources(c, source_data)
    logging.debug("Registered %s sources", len(srcs))

    # then access the sources with the user client
    c = Client(live_app.url())
    sources = dict((_.tdmq_id, _) for _ in c.find_sources())
    for s in srcs:
        logging.debug("source: id %s, shape: %s; type: %s", s.id, s.shape, type(s))
        assert s.tdmq_id in sources
        assert s.id == sources[s.tdmq_id].id
        assert s.tdmq_id == sources[s.tdmq_id].tdmq_id
        assert isinstance(s, NonScalarSource)


def test_add_record_to_one_nonscalar_source(clean_storage, source_data, live_app):
    N = 1
    c = Client(live_app.url(), auth_token=live_app.auth_token)

    mosaic_def = next(s for s in source_data['sources'] if s['id'] == "tdm/tiledb_sensor_6")
    source = c.register_source(mosaic_def, nslots=N)
    assert len(source.shape) == 2

    timebase, dt = create_and_ingest_records(source, N)
    check_records(source, timebase, dt, N)


def test_nonscalar_source_add_records(clean_storage, source_data, live_app):
    N = 3
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    srcs = register_nonscalar_sources(c, source_data)
    logging.debug("Registered %s sources", len(srcs))
    tdmq_ids = []
    for s in srcs:
        logging.debug("source: id %s, shape: %s; type: %s", s.id, s.shape, type(s))
        assert len(s.shape) > 0
        timebase, dt = create_and_ingest_records(s, N)
        check_records(s, timebase, dt, N)
        tdmq_id = s.tdmq_id
        c.deregister_source(s)
        tdmq_ids.append(tdmq_id)
    check_deallocation(c, tdmq_ids)


def test_nonscalar_source_array_context(clean_storage, source_data, live_app):
    N = 3
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    srcs = register_nonscalar_sources(c, source_data)
    logging.debug("Registered %s sources", len(srcs))
    for s in srcs:
        logging.debug("source: id %s, shape: %s; type: %s", s.id, s.shape, type(s))
        assert len(s.shape) > 0
        with s.array_context('w'):
            create_and_ingest_records(s, N)
        c.deregister_source(s)
