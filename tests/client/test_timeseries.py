
from datetime import datetime, timedelta
from urllib.error import HTTPError

import numpy as np
import pytest
import requests

from tdmq.client import Client

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
    "public": True
}


def test_check_timeseries_range(clean_db, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    s = c.register_source(source_desc)
    N = 10
    now = datetime.now()
    time_base = datetime(now.year, now.month, now.day, now.hour)
    times = [time_base + timedelta(i) for i in range(N)]
    temps = [20 + i for i in range(N)]
    hums = [i / N for i in range(N)]
    for t, tv, th in zip(times, temps, hums):
        s.add_record({'time': t.strftime('%Y-%m-%dT%H:%M:%SZ'),
                      'source': s.id,
                      'data': {'temperature': tv,
                               'humidity': th}})
    ts = s.timeseries()
    for u in range(N):
        for v in range(u, N):
            ts_times, data = ts[u:v]
            assert np.array_equal(data['temperature'], temps[u:v])
            assert np.array_equal(data['humidity'], hums[u:v])
            assert np.array_equal(ts_times, times[u:v])
    tid = s.tdmq_id
    c.deregister_source(s)
    sources = dict((_.tdmq_id, _) for _ in c.find_sources())
    assert tid not in sources


def test_check_timeseries_bucket(clean_db, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    s = c.register_source(source_desc)
    bucket = 10
    N = 10 * bucket
    now = datetime.now()
    time_base = datetime(now.year, now.month, now.day, now.hour)
    times = [time_base + timedelta(seconds=i) for i in range(N)]
    temps = [20 + i for i in range(N)]
    hums = [i / N for i in range(N)]
    for t, tv, th in zip(times, temps, hums):
        s.add_record({'time': t.strftime('%Y-%m-%dT%H:%M:%SZ'),
                      'source': s.id,
                      'data': {'temperature': tv,
                               'humidity': th}})
    ts = s.timeseries(bucket=10, op='sum')
    ts_times, data = ts[:]
    assert np.allclose(
        data['temperature'],
        np.reshape(np.array(temps, dtype=np.float32),
                   (-1, bucket)).sum(axis=1))
    assert np.allclose(
        data['humidity'],
        np.reshape(np.array(hums, dtype=np.float32),
                   (-1, bucket)).sum(axis=1))
    assert np.array_equal(
        ts_times,
        np.reshape(np.array(times), (-1, bucket)).min(axis=1))
    tid = s.tdmq_id
    c.deregister_source(s)
    sources = dict((_.tdmq_id, _) for _ in c.find_sources())
    assert tid not in sources


def test_empty_timeseries(clean_db, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    s = c.register_source(source_desc)
    # empty timeseries
    ts = s.timeseries()
    assert len(ts.get_shape()) == 1
    assert len(ts) == 0
    c.deregister_source(s)


def test_create_timeseries_range_as_user(clean_db, live_app):
    # First create a source
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    s = c.register_source(source_desc)

    # inject new user client
    s.client = Client(live_app.url())

    N = 1
    now = datetime.now()
    time_base = datetime(now.year, now.month, now.day, now.hour)
    times = [time_base + timedelta(i) for i in range(N)]
    temps = [20 + i for i in range(N)]
    hums = [i / N for i in range(N)]

    for t, tv, th in zip(times, temps, hums):
        with pytest.raises(requests.exceptions.HTTPError) as he:
            s.add_record({'time': t.strftime('%Y-%m-%dT%H:%M:%SZ'),
                          'source': s.id,
                          'data': {'temperature': tv, 'humidity': th}})
            assert he.status_code == '401'


def test_check_timeseries_range_as_user(clean_db, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    s = c.register_source(source_desc)
    bucket = 10
    N = 10 * bucket
    now = datetime.now()
    time_base = datetime(now.year, now.month, now.day, now.hour)
    times = [time_base + timedelta(seconds=i) for i in range(N)]
    temps = [20 + i for i in range(N)]
    hums = [i / N for i in range(N)]
    for t, tv, th in zip(times, temps, hums):
        s.add_record({'time': t.strftime('%Y-%m-%dT%H:%M:%SZ'),
                      'source': s.id,
                      'data': {'temperature': tv,
                               'humidity': th}})
    # inject new user client
    s.client = Client(live_app.url())
    ts = s.timeseries(bucket=10, op='sum')
    ts_times, data = ts[:]
    assert np.allclose(
        data['temperature'],
        np.reshape(np.array(temps, dtype=np.float32),
                   (-1, bucket)).sum(axis=1))
    assert np.allclose(
        data['humidity'],
        np.reshape(np.array(hums, dtype=np.float32),
                   (-1, bucket)).sum(axis=1))
    assert np.array_equal(
        ts_times,
        np.reshape(np.array(times), (-1, bucket)).min(axis=1))
    tid = s.tdmq_id
    c.deregister_source(s)
    sources = dict((_.tdmq_id, _) for _ in c.find_sources())
    assert tid not in sources
