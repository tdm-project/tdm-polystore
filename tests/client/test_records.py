from tdmq.client import Client
from test_source import register_scalar_sources
from datetime import datetime
from datetime import timedelta
import numpy as np

# import pytest
# pytestmark = pytest.mark.skip(reason="not up-to-date with new pytest setup")

source_desc = {
    "id": "tdm/sensor_1/test_barfoo",
    "alias": "B221",
    "entity_type": "PointWeatherObserver",
    "entity_category": "Station",
    "default_footprint": {"type": "Point", "coordinates": [9.222, 30.003]},
    "controlledProperties": ["humidity", "temperature"],
    "shape": [],
    "description": {
        "type": "multisensor",
        "brandName": "ProSensor",
        "modelName": "R2D2",
        "manufacturerName": "CRS4"
    },
    "private": False
}


def test_add_scalar_records(clean_db, public_source_data, live_app):
    c = Client(live_app.url())
    srcs = register_scalar_sources(c, public_source_data)
    by_source = public_source_data['records_by_source']
    tdmq_ids = []
    for s in srcs:
        s.add_records(by_source[s.id])
        c.deregister_source(s)
        tdmq_ids.append(s.tdmq_id)
    sources = dict((_.tdmq_id, _) for _ in c.find_sources())
    for tid in tdmq_ids:
        assert tid not in sources


def test_add_scalar_record(clean_db, public_source_data, live_app):
    c = Client(live_app.url())
    srcs = register_scalar_sources(c, public_source_data)
    by_source = public_source_data['records_by_source']
    tdmq_ids = []
    for s in srcs:
        for r in by_source[s.id]:
            s.add_record(r)
        c.deregister_source(s)
        tdmq_ids.append(s.tdmq_id)
    sources = dict((_.tdmq_id, _) for _ in c.find_sources())
    for tid in tdmq_ids:
        assert tid not in sources


def test_ingest_scalar_record(clean_db, public_source_data, live_app):
    c = Client(live_app.url())
    srcs = register_scalar_sources(c, public_source_data)
    by_source = public_source_data['records_by_source']
    tdmq_ids = []
    for s in srcs:
        for r in by_source[s.id]:
            try:
                t = datetime.strptime(r['time'], c.TDMQ_DT_FMT)
            except ValueError:
                t = datetime.strptime(r['time'], c.TDMQ_DT_FMT_NO_MICRO)
            data = r['data']
            s.ingest(t, data)
        c.deregister_source(s)
        tdmq_ids.append(s.tdmq_id)
    sources = dict((_.tdmq_id, _) for _ in c.find_sources())
    for tid in tdmq_ids:
        assert tid not in sources


def test_check_timeseries(clean_db, live_app):
    c = Client(live_app.url())
    s = c.register_source(source_desc)
    N = 10
    now = datetime.now()
    time_base = datetime(now.year, now.month, now.day, now.hour)
    times = [time_base + timedelta(i) for i in range(N)]
    temps = [20 + i for i in range(N)]
    hums = [i / N for i in range(N)]
    for t, tv, th in zip(times, temps, hums):
        s.ingest(t, {'temperature': tv, 'humidity': th})
    ts = s.timeseries()
    ts_times, data = ts[:]
    assert np.array_equal(data['temperature'], temps)
    assert np.array_equal(data['humidity'], hums)
    assert np.array_equal(ts_times, times)
    tid = s.tdmq_id
    c.deregister_source(s)
    sources = dict((_.tdmq_id, _) for _ in c.find_sources())
    assert tid not in sources
