import json
import os
import pytest
import uuid
from collections import OrderedDict
from copy import deepcopy
from datetime import datetime, timedelta
from tdmq.utils import convert_footprint

root = os.path.dirname(os.path.abspath(__file__))
sensor_types_fname = os.path.join(root, 'data/sensor_types.json')
sensors_fname = os.path.join(root, 'data/sensors.json')
measures_fname = os.path.join(root, 'data/measures.json')


# FIXME move it to a fixture?
class FakeDB:

    NS = uuid.UUID('6cb10168-c65b-48fa-af9b-a3ca6d03156d')

    def __load(self, fname):
        table = OrderedDict()
        with open(fname) as f:
            data = json.load(f)
        descriptions = next(iter(data.values()))
        for d in descriptions:
            code = str(uuid.uuid5(self.NS, d['name']))
            table[code] = d
        return table

    def __init__(self):
        self.called = {}
        self.sensor_types = self.__load(sensor_types_fname)
        self.sensors = self.__load(sensors_fname)
        measures = json.load(
            open(os.path.join(root, 'data/measures.json')))['measures']
        data = {}
        for m in measures:
            sensorcode = str(uuid.uuid5(self.NS, m['sensor']))
            data.setdefault(sensorcode, []).append(
                (datetime.strptime(m['time'], '%Y-%m-%dT%H:%M:%SZ'),
                 m['measure']['value']))
        self.timeseries = {}
        for k in data:
            ts = sorted(data[k])
            time_origin = ts[0][0]
            ts = [[(_[0] - time_origin).seconds, _[1]] for _ in ts]
            self.timeseries[k] = {
                'time_origin': time_origin.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'data': ts}

    def list_sensor_types(self, args):
        self.called['list_sensor_types'] = args
        return list(deepcopy(self.sensor_types).items())

    def list_sensors(self, args):
        self.called['list_sensors'] = args
        return list(deepcopy(self.sensors).items())

    def get_sensor(self, code):
        self.called['get_sensor'] = {'code': code}
        return deepcopy(self.sensors[code])

    def get_timeseries(self, code, args):
        args['code'] = code
        self.called['get_timeseries'] = args
        return self.timeseries[code]


def _checkresp(response, table=None):
    assert response.status == '200 OK'
    assert response.is_json
    if table:
        result = response.get_json()
        assert len(result) == len(table)
        for r in result:
            assert "code" in r
            code = r.pop("code")
            assert r == table[code]


def test_sensor_types_no_args(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.list_sensor_types', fakedb.list_sensor_types)
    response = client.get('/sensor_types')
    assert 'list_sensor_types' in fakedb.called
    _checkresp(response, table=fakedb.sensor_types)


def test_sensor_types(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.list_sensor_types', fakedb.list_sensor_types)
    in_args = {"type": "multisensor", "controlledProperty": "temperature"}
    q = "&".join(f"{k}={v}" for k, v in in_args.items())
    response = client.get(f'/sensor_types?{q}')
    assert 'list_sensor_types' in fakedb.called
    args = fakedb.called['list_sensor_types']
    assert {k: v for k, v in args.items()} == in_args
    _checkresp(response, table=fakedb.sensor_types)


def test_sensors_no_args(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.list_sensors', fakedb.list_sensors)
    response = client.get('/sensors')
    assert 'list_sensors' in fakedb.called
    _checkresp(response, table=fakedb.sensors)


def test_sensors(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.list_sensors', fakedb.list_sensors)
    footprint = 'circle((9.2, 33), 1000)'
    after, before = '2019-02-21T11:03:25Z', '2019-02-21T11:50:25Z'
    q = 'footprint={}&after={}&before={}'.format(footprint, after, before)
    response = client.get('/sensors?{}'.format(q))
    assert 'list_sensors' in fakedb.called
    args = fakedb.called['list_sensors']
    assert args['footprint'] == convert_footprint(footprint)
    assert args['after'] == after and args['before'] == before
    _checkresp(response, table=fakedb.sensors)


def test_sensors_fail(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.list_sensors', fakedb.list_sensors)
    footprint = 'circle((9.2 33), 1000)'  # note the missing comma
    after, before = '2019-02-21T11:03:25Z', '2019-02-21T11:50:25Z'
    type_ = next(iter(fakedb.sensor_types))
    q = 'footprint={}&after={}&before={}&type={}'.format(
        footprint, after, before, type_)
    with pytest.raises(ValueError) as ve:
        client.get('/sensors?{}'.format(q))
        assert "footprint" in ve.value
        assert footprint in ve.value


def test_sensor(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.get_sensor', fakedb.get_sensor)
    code = next(iter(fakedb.sensors))
    response = client.get('/sensors/{}'.format(code))
    args = fakedb.called['get_sensor']
    assert args['code'] == code
    assert 'get_sensor' in fakedb.called
    _checkresp(response)
    result = response.get_json()
    assert "code" in result
    code = result.pop("code")
    assert result == fakedb.sensors[code]


def test_timeseries(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.get_timeseries', fakedb.get_timeseries)
    code = next(iter(fakedb.sensors))
    # FIXME these timepoints are random
    after, before = '2019-02-21T11:03:25Z', '2019-02-21T11:50:25Z'
    bucket, op = 20.22, 'sum'
    q = 'after={}&before={}&bucket={}&op={}'.format(after, before, bucket, op)
    response = client.get('/sensors/{}/timeseries?{}'.format(code, q))
    assert 'get_timeseries' in fakedb.called
    assert response.status == '200 OK'
    assert response.is_json
    args = fakedb.called['get_timeseries']
    assert args['code'] == code
    assert args['after'] == after and args['before'] == before
    assert args['bucket'] == timedelta(seconds=bucket) and args['op'] == op
    assert response.get_json() == fakedb.timeseries[code]
