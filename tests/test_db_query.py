import json
import os
from datetime import datetime, timedelta

from tdmq.db import get_object
from tdmq.db import get_scalar_timeseries_data
from tdmq.db import list_descriptions_in_table
from tdmq.db import list_sensor_types_in_db
from tdmq.db import list_sensors_in_db

root = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(root, 'data/sensor_types.json')) as f:
    sensor_types = json.load(f)['sensor_types']
with open(os.path.join(root, 'data/sensors.json')) as f:
    sensors = json.load(f)['sensors']
with open(os.path.join(root, 'data/measures.json')) as f:
    measures = json.load(f)['measures']


def _getdesc(query_result):
    return [description for code, description in query_result]


def test_db_list_sensor_types(db):
    data = list_descriptions_in_table(db, 'sensor_types')
    assert len(sensor_types) == len(data)
    assert _getdesc(data) == sensor_types


def test_list_sensor_types_with_args(db):
    args = {"controlledProperty": "temperature", "manufacturerName": "CRS4"}
    exp_res = [_ for _ in sensor_types if all((
        "temperature" in _["controlledProperty"],
        _["manufacturerName"] == "CRS4"
    ))]
    assert _getdesc(list_sensor_types_in_db(db, args)) == exp_res
    args = {"controlledProperty": "temperature,humidity"}
    exp_res = [_ for _ in sensor_types if
               {"temperature", "humidity"}.issubset(_["controlledProperty"])]
    assert _getdesc(list_sensor_types_in_db(db, args)) == exp_res


def test_list_sensors(db):
    data = list_descriptions_in_table(db, 'sensors')
    assert len(sensors) == len(data)
    assert _getdesc(data) == sensors


def test_db_get_sensor(db):
    for s in sensors:
        code = list_sensors_in_db(db, {"name": s["name"]})[0][0]
        assert s == get_object(db, 'sensors', code)


def test_db_get_sensor_type(db):
    for s in sensor_types:
        code = list_sensor_types_in_db(db, {"name": s["name"]})[0][0]
        assert s == get_object(db, 'sensor_types', code)


def test_list_sensors_with_args(db):
    """\
    Sensor dist from [9.2215, 30.0015] (coord conversion with GDAL):
      "sensor_1" 173.07071847340288
      "sensor_0" 173.0708798144605
      "sensor_2" 44533.32174325444
      "sensor_5" 111412.41515372833
      "sensor_4" 133329.30383038227
      "sensor_3" 223714.63917666752
    """
    sensors_by_name = dict((s['name'], s) for s in sensors)
    center = [9.2215, 30.0015]
    expected_sensors_by_radius = {
        175: 2, 45000: 3, 115000: 4, 135000: 5, 225000: 6
    }
    args = {'after': '2019-05-02T11:00:00Z',
            'before': '2019-05-02T11:50:25Z'}
    for radius, exp_n in expected_sensors_by_radius.items():
        args['footprint'] = {
            'type': 'circle',
            'center': {'type': 'Point', 'coordinates': center},
            'radius': radius
        }
        data = list_sensors_in_db(db, args)
        assert len(data) == exp_n
        for _, d in data:
            assert d['name'] in sensors_by_name
            assert sensors_by_name[d['name']] == d
    # query by attribute
    t = sensors[0]["type"]
    exp_res = [_ for _ in sensors if _["type"] == t]
    res = list_sensors_in_db(db, {"type": t})
    assert _getdesc(res) == exp_res


def test_get_scalar_timeseries_data(db):
    def to_dt(s):
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")

    args = {}
    args['bucket'] = timedelta(seconds=1)
    args['op'] = 'sum'
    args['after'] = '2019-05-02T11:00:00Z'
    args['before'] = '2019-05-02T11:50:35Z'
    sensors_by_name = dict((s['name'], s) for s in sensors)
    timebase = to_dt(args['after'])
    deltas_by_name = {}
    values_by_name = {}
    for m in measures:
        deltas_by_name.setdefault(m['sensor'], []).append(
            (to_dt(m['time']) - timebase).total_seconds())
        values_by_name.setdefault(m['sensor'], []).append(
            m['measure']['value'])
    for name in sensors_by_name:
        code = list_sensors_in_db(db, {"name": name})[0][0]
        args['code'] = code
        result = get_scalar_timeseries_data(db, args)
        assert result['timebase'] == args['after']
        assert result['timedelta'] == deltas_by_name.get(name, [])
        assert result['data'] == values_by_name.get(name, [])


def test_get_scalar_timeseries_data_empty(db):
    sensors_by_name = dict((s['name'], s) for s in sensors)
    args = {}
    args['bucket'] = timedelta(seconds=3)
    args['op'] = 'sum'
    args['after'] = '2010-05-02T11:00:00Z'
    args['before'] = '2010-05-02T11:50:25Z'
    for name in sensors_by_name:
        code = list_sensors_in_db(db, {"name": name})[0][0]
        args['code'] = code
        result = get_scalar_timeseries_data(db, args)
        assert result['timebase'] == args['after']
        assert result['timedelta'] == []
        assert result['data'] == []
