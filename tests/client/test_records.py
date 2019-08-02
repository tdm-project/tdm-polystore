import pytest
import json
import numpy as np
from tdmq.client.client import Client
from test_source import register_sources
from datetime import datetime
from datetime import timedelta


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
    }
}


def get_records():
    by_source = {}
    with open('../data/records.json') as f:
        records = json.load(f)['records']
    for r in records:
        by_source.setdefault(r['source'], []).append(r)
    return by_source


def test_add_scalar_records():
    c = Client()
    srcs = register_sources(c)
    by_source = get_records()
    tdmq_ids = []
    for s in srcs:
        s.add_records(by_source[s.id])
        c.deregister_source(s)
        tdmq_ids.append(s.tdmq_id)
    sources = dict((_.tdmq_id, _) for _ in c.get_sources())
    for tid in tdmq_ids:
        assert tid not in sources


def test_add_scalar_record():
    c = Client()
    srcs = register_sources(c)
    by_source = get_records()
    tdmq_ids = []
    for s in srcs:
        for r in by_source[s.id]:
            s.add_record(r)
        c.deregister_source(s)
        tdmq_ids.append(s.tdmq_id)
    sources = dict((_.tdmq_id, _) for _ in c.get_sources())
    for tid in tdmq_ids:
        assert tid not in sources


def test_check_timeseries():
    c = Client()
    s = c.register_source(source_desc)
    N = 10
    now = datetime.now()
    time_base = datetime(now.year, now.month, now.day, now.hour)
    times = [time_base + timedelta(i) for i in range(N)]
    temps = [20 + i for i in range(N)]
    hums = [i/N for i in range(N)]
    for t, tv, th in zip(times, temps, hums):
        s.add_record({'time': t.strftime('%Y-%m-%dT%H:%M:%SZ'),
                      'source': s.id,
                      'data': {'temperature': tv,
                               'humidity': th}})
    ts = s.timeseries()
    ts_times, data = ts[:]
    assert np.array_equal(data['temperature'], temps)
    assert np.array_equal(data['humidity'], hums)
    assert np.array_equal(ts_times, [t.timestamp() for t in times])
    tid = s.tdmq_id
    c.deregister_source(s)
    sources = dict((_.tdmq_id, _) for _ in c.get_sources())
    assert tid not in sources
