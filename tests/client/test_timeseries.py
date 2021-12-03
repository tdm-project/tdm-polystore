
import tempfile

from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from tdmq.client import Client
from tdmq.errors import UnauthorizedError

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
        s.ingest_one(t, {'temperature': tv, 'humidity': th})
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
        with pytest.raises(UnauthorizedError) as he:
            s.ingest_one(t, {'temperature': tv, 'humidity': th})
            assert he.status == '401'


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


def test_timeseries_none_arrays(clean_storage, db_data, live_app):
    c = Client(live_app.url())
    s = c.find_sources(args={'id': 'tdm/sensor_1'})[0]
    assert s
    ts = s.timeseries()
    # sensor_1 only container data for relativeHumidity and temperature
    from tdmq.client.timeseries import NoneArray
    assert isinstance(ts.series['CO'], NoneArray)
    assert isinstance(ts.series['temperature'], np.ndarray)
    none_array = ts.series['CO']
    assert len(ts) == len(none_array)
    assert len(ts) == len(ts.series['temperature'])
    # slicing
    assert len(none_array[1:3]) == 2
    assert all(x is None for x in none_array[1:3])
    assert none_array[1] is None
    # iterating
    assert [x for x in none_array] == [None]*len(ts)
    # contains
    assert None in none_array
    assert none_array.count(None) == len(ts)
    assert none_array.count(1) == 0
    assert none_array.index(None) == 0
    with pytest.raises(ValueError):
        none_array.index(1)


def test_timeseries_export_csv(clean_storage, db_data, live_app):
    c = Client(live_app.url())
    s = c.find_sources(args={'id': 'tdm/sensor_1'})[0]
    assert s
    ts = s.timeseries()
    with tempfile.TemporaryFile(mode="w+b") as f:
        ts.export(f)
        f.seek(0)
        contents = f.read().decode("utf-8")
    lines = contents.splitlines(keepends=False)
    assert len(lines) == 5
    table_heading = lines[0].split(',')
    assert set(table_heading) > { 'time', 'footprint', 'temperature', 'relativeHumidity' }
    table = [line.split(',') for line in lines[1:]]
    temp_index = table_heading.index('temperature')
    assert temp_index >= 0
    assert table[0][temp_index] == "20"


def test_timeseries_step_index(clean_storage, db_data, source_data, live_app):
    source_id = 'tdm/sensor_0'
    records = source_data['records_by_source'][source_id]

    c = Client(live_app.url())
    s = c.find_sources(args={'id': source_id})[0]
    assert s

    ts = s.timeseries()
    # extract each second tuple
    (times, _) = ts[0:-1:2]

    def parse_timestamp(time_str):
        return datetime.strptime(time_str, Client.TDMQ_DT_FMT_NO_MICRO).astimezone(timezone.utc)

    assert abs(times[0] - parse_timestamp(records[0]['time'])) < timedelta(seconds=1)
    assert abs(times[1] - parse_timestamp(records[2]['time'])) < timedelta(seconds=1)
    assert abs(times[2] - parse_timestamp(records[4]['time'])) < timedelta(seconds=1)


def test_timeseries_properties_subset(clean_storage, db_data, live_app):
    c = Client(live_app.url())
    s = c.find_sources(args={'id': 'tdm/sensor_1'})[0]
    ts = s.timeseries()
    assert len(ts.series.keys()) == len(s.controlled_properties)
    ts = s.timeseries(properties=['temperature'])
    assert len(ts.series.keys()) == 1
    assert 'temperature' in ts.series.keys()
