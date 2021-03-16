
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest
import requests

from tdmq.client import Client

pytestmark = pytest.mark.timeseries

source_desc = {
    "id": "tdm/sensor_1/test_barfoo",
    "alias": "B221",
    "entity_type": "WeatherObserver",
    "entity_category": "Station",
    "default_footprint": {"type": "Point", "coordinates": [9.222, 30.003]},
    "controlledProperties": ["humidity", "temperature"],
    "shape": [],
    "description": {
        "brand_name": "ProSensor",
        "model_name": "R2D2",
        "operated_by": "CRS4",
        "reference": None
    },
    "public": True
}


def test_check_timeseries_range(clean_storage, clean_db, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    s = c.register_source(source_desc)
    N = 10
    time_base = datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
    times = [time_base + timedelta(i) for i in range(N)]
    temps = [20 + i for i in range(N)]
    hums = [i / N for i in range(N)]
    for t, tv, th in zip(times, temps, hums):
        s.ingest_one(t, {'temperature': tv, 'humidity': th})
    ts = s.timeseries()
    for u in range(N):
        for v in range(u, N):
            ts_times, data = ts[u:v]
            assert np.array_equal(data['temperature'], temps[u:v])
            assert np.array_equal(data['humidity'], hums[u:v])
            assert np.array_equal(ts_times, times[u:v])


def test_check_timeseries_ingest_many(clean_storage, clean_db, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    s = c.register_source(source_desc)
    N = 10
    time_base = datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
    times = [time_base + timedelta(i) for i in range(N)]
    temps = [20 + i for i in range(N)]
    hums = [i / N for i in range(N)]
    data = [{'temperature': tv, 'humidity': th} for tv, th in zip(temps, hums)]
    s.ingest_many(times, data)
    ts = s.timeseries()
    for u in range(N):
        for v in range(u, N):
            ts_times, data = ts[u:v]
            assert np.array_equal(data['temperature'], temps[u:v])
            assert np.array_equal(data['humidity'], hums[u:v])
            assert np.array_equal(ts_times, times[u:v])


def test_check_timeseries_bucket(clean_storage, clean_db, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    s = c.register_source(source_desc)
    bucket = 10
    N = 10 * bucket
    time_base = datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
    times = [time_base + timedelta(seconds=i) for i in range(N)]
    temps = [20 + i for i in range(N)]
    hums = [i / N for i in range(N)]
    for t, tv, th in zip(times, temps, hums):
        s.ingest_one(t,{'temperature': tv, 'humidity': th})
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


def test_empty_timeseries(clean_storage, clean_db, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    s = c.register_source(source_desc)
    # empty timeseries
    ts = s.timeseries()
    assert len(ts.get_shape()) == 1
    assert len(ts) == 0


def test_source_get_latest_activity(clean_storage, public_db_data, live_app):
    c = Client(live_app.url())
    s = c.find_sources(args={'id': 'tdm/sensor_1'})[0]
    assert s
    ts = s.get_latest_activity()
    assert len(ts) == 1
    timestamp = ts[-1][0]
    assert timestamp == datetime.fromisoformat("2019-05-02T11:20:00+00:00")


def test_create_timeseries_range_as_user(clean_storage, clean_db, live_app):
    # First create a source
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    s = c.register_source(source_desc)
    tdmq_id = s.tdmq_id

    # inject new user client
    c = Client(live_app.url())
    s = c.get_source(tdmq_id)

    N = 1
    time_base = datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
    times = [time_base + timedelta(i) for i in range(N)]
    temps = [20 + i for i in range(N)]
    hums = [i / N for i in range(N)]

    for t, tv, th in zip(times, temps, hums):
        with pytest.raises(requests.exceptions.HTTPError) as he:
            s.ingest_one(t, {'temperature': tv, 'humidity': th})
            assert he.status_code == '401'


def test_check_timeseries_range_as_user(clean_storage, clean_db, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    s = c.register_source(source_desc)
    bucket = 10
    N = 10 * bucket
    time_base = datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
    times = [time_base + timedelta(seconds=i) for i in range(N)]
    temps = [20 + i for i in range(N)]
    hums = [i / N for i in range(N)]
    for t, tv, th in zip(times, temps, hums):
        s.ingest_one(t, {'temperature': tv, 'humidity': th})
    # inject new user client
    tdmq_id = s.tdmq_id
    c = Client(live_app.url())
    s = c.get_source(tdmq_id)
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
