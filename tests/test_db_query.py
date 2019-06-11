from tdmq.db import list_descriptions_in_table
from tdmq.db import get_object
from tdmq.db import list_sensors_in_db
from tdmq.db import get_scalar_timeseries_data
from tdmq.db import list_sensor_types_in_db

from datetime import datetime, timedelta
import json
import os
root = os.path.dirname(os.path.abspath(__file__))


def test_db_list_sensor_types(db):
    sensor_types = json.load(
        open(os.path.join(root, 'data/sensor_types.json')))['sensor_types']
    data = list_descriptions_in_table(db, 'sensor_types')
    assert len(sensor_types) == len(data)
    assert data == sensor_types


def test_list_sensor_types_with_args(db):
    sensor_types = json.load(
        open(os.path.join(root, 'data/sensor_types.json')))['sensor_types']
    args = {"controlledProperty": "temperature", "manufacturerName": "CRS4"}
    exp_res = [_ for _ in sensor_types if all((
        "temperature" in _["controlledProperty"],
        _["manufacturerName"] == "CRS4"
    ))]
    assert list_sensor_types_in_db(db, args) == exp_res
    args = {"controlledProperty": "temperature,humidity"}
    exp_res = [_ for _ in sensor_types if
               {"temperature", "humidity"}.issubset(_["controlledProperty"])]
    assert list_sensor_types_in_db(db, args) == exp_res


def test_list_sensors(db):
    sensors = json.load(
        open(os.path.join(root, 'data/sensors.json')))['sensors']
    data = list_descriptions_in_table(db, 'sensors')
    assert len(sensors) == len(data)
    assert data == sensors


def test_db_get_sensor(db):
    sensors = json.load(
        open(os.path.join(root, 'data/sensors.json')))['sensors']
    for s in sensors:
        assert s == get_object(db, 'sensors', s['code'])


def test_db_get_sensor_type(db):
    sensor_types = json.load(
        open(os.path.join(root, 'data/sensor_types.json')))['sensor_types']
    for s in sensor_types:
        assert s == get_object(db, 'sensor_types', s['code'])


def test_list_sensors_with_args(db):
    """\
    Sensor dist from [9.2215, 30.0015] (coord conversion with GDAL):
      "0fd67c67-c9be-45c6-9719-4c4eada4beff" 173.07071847340288
      "0fd67c67-c9be-45c6-9719-4c4eada4becc" 173.0708798144605
      "838407d9-9876-4226-a039-ff17ba833b2c" 44533.32174325444
      "d5307bae-76a9-4298-885c-05e7a4d521c2" 111412.41515372833
      "13c1cb32-486a-407f-b286-dc9ea8fef99f" 133329.30383038227
      "1f69d31c-a5ef-4ef4-902d-45f5e57923c6" 223714.63917666752
    """
    sensors = json.load(
        open(os.path.join(root, 'data/sensors.json')))['sensors']
    sensors_by_code = dict((s['code'], s) for s in sensors)
    center = [9.2215, 30.0015]
    expected_sensors_by_radius = {
        175: 2, 45000: 3, 115000: 4, 135000: 5, 225000: 6
    }
    args = {'after': '2019-05-02T11:00:00Z',
            'before': '2019-05-02T11:50:25Z'}
    for radius, exp_n in expected_sensors_by_radius.items():
        args['footprint'] = {
            'type': 'circle',
            'center':  {'type': 'Point', 'coordinates': center},
            'radius': radius
        }
        data = list_sensors_in_db(db, args)
        assert len(data) == exp_n
        for d in data:
            assert d['code'] in sensors_by_code
            assert d['stypecode'] == sensors_by_code[d['code']]['stypecode']
    # query by type
    t = sensors[0]["stypecode"]
    exp_res = [_ for _ in sensors if _["stypecode"] == t]
    res = list_sensors_in_db(db, {"type": t})
    assert res == exp_res


def test_get_scalar_timeseries_data(db):
    def to_dt(s):
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
    args = {}
    args['bucket'] = timedelta(seconds=1)
    args['op'] = 'sum'
    args['after'] = '2019-05-02T11:00:00Z'
    args['before'] = '2019-05-02T11:50:35Z'
    sensors = json.load(
        open(os.path.join(root, 'data/sensors.json')))['sensors']
    measures = json.load(
        open(os.path.join(root, 'data/measures.json')))['measures']
    sensors_by_code = dict((s['code'], s) for s in sensors)
    timebase = to_dt(args['after'])
    deltas_by_code = {}
    values_by_code = {}
    for m in measures:
        deltas_by_code.setdefault(m['sensorcode'], []).append(
            (to_dt(m['time']) - timebase).total_seconds())
        values_by_code.setdefault(m['sensorcode'], []).append(
            m['measure']['value'])
    for code in sensors_by_code:
        args['code'] = code
        result = get_scalar_timeseries_data(db, args)
        assert result['timebase'] == args['after']
        assert result['timedelta'] == deltas_by_code.get(code, [])
        assert result['data'] == values_by_code.get(code, [])


def test_get_scalar_timeseries_data_empty(db):
    sensors = json.load(
        open(os.path.join(root, 'data/sensors.json')))['sensors']
    sensors_by_code = dict((s['code'], s) for s in sensors)
    args = {}
    args['bucket'] = timedelta(seconds=3)
    args['op'] = 'sum'
    args['after'] = '2010-05-02T11:00:00Z'
    args['before'] = '2010-05-02T11:50:25Z'
    for code in sensors_by_code:
        args['code'] = code
        result = get_scalar_timeseries_data(db, args)
        assert result['timebase'] == args['after']
        assert result['timedelta'] == []
        assert result['data'] == []
