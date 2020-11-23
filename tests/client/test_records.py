from datetime import datetime, timedelta

import numpy as np
import pytest
from requests.exceptions import HTTPError
from tdmq.client import Client

from test_source import register_scalar_sources

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
    "public": True
}


def test_add_scalar_records_as_admin(clean_db, public_source_data, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
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


def test_add_scalar_records_as_user(clean_db, public_source_data, live_app):
    # first create the resource with admin client
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    srcs = register_scalar_sources(c, public_source_data)

    # then get the sources as user and tries to add records
    c = Client(live_app.url())    
    by_source = public_source_data['records_by_source']
    for s in c.find_sources():
        with pytest.raises(HTTPError) as ve:
            s.add_records(by_source[s.id])
            ve.code == 401


def test_add_scalar_record_as_admin(clean_db, public_source_data, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
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


def test_add_scalar_record_as_user(clean_db, public_source_data, live_app):
    # first create the resource with admin client
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    srcs = register_scalar_sources(c, public_source_data)

    # then get the sources as user and tries to add record
    c = Client(live_app.url())    
    by_source = public_source_data['records_by_source']
    for s in c.find_sources():
        with pytest.raises(HTTPError) as ve:
            for r in by_source[s.id]:
                s.add_record(r)
            ve.code == 401


def test_ingest_scalar_record_as_admin(clean_db, public_source_data, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
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


def test_ingest_scalar_record_as_user(clean_db, public_source_data, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    srcs = register_scalar_sources(c, public_source_data)
    by_source = public_source_data['records_by_source']
    tdmq_ids = []

    # then get the sources as user and tries to add record
    c = Client(live_app.url())
    for s in c.find_sources():
        for r in by_source[s.id]:
            try:
                t = datetime.strptime(r['time'], c.TDMQ_DT_FMT)
            except ValueError:
                t = datetime.strptime(r['time'], c.TDMQ_DT_FMT_NO_MICRO)
            data = r['data']
            with pytest.raises(HTTPError) as ve:
                s.ingest(t, data)
                ve.code = 401


def test_check_timeseries(clean_db, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
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
