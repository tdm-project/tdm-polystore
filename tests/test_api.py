import json
import pytest
from collections import Counter
from collections import OrderedDict
from datetime import datetime, timedelta
from tdmq.utils import convert_footprint

import os
root = os.path.dirname(os.path.abspath(__file__))
sensor_types_fname = os.path.join(root, 'data/sensor_types.json')
sensors_fname = os.path.join(root, 'data/sensors.json')
measures_fname = os.path.join(root, 'data/measures.json')


# FIXME move it to a fixture?
class FakeDB:
    def __init__(self):
        self.initialized = False
        self.called = {}
        sensor_types = json.load(
            open(os.path.join(root, 'data/sensor_types.json')))['sensor_types']
        sensors = json.load(
            open(os.path.join(root, './data/sensors.json')))['sensors']
        self.sensor_types = OrderedDict((_['code'], _) for _ in sensor_types)
        self.sensors = OrderedDict((_['code'], _) for _ in sensors)
        self.sensors_no_args = dict(Counter(
            _['stypecode'] for _ in self.sensors.values()
        ))
        measures = json.load(
            open(os.path.join(root, 'data/measures.json')))['measures']
        data = {}
        for m in measures:
            data.setdefault(m['sensorcode'], []).append(
                (datetime.strptime(m['time'], '%Y-%m-%dT%H:%M:%SZ'),
                 m['measure']['value']))
        self.timeseries = {}
        for k in data.keys():
            ts = sorted(data[k])
            time_origin = ts[0][0]
            ts = [[(_[0] - time_origin).seconds, _[1]] for _ in ts]
            self.timeseries[k] = {
                'time_origin': time_origin.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'data': ts}

    def init(self):
        self.initialized = True

    def list_sensor_types(self):
        self.called['list_sensor_types'] = True
        return [_ for _ in self.sensor_types.values()]

    def list_sensors(self, args):
        self.called['list_sensors'] = args
        if args:
            return [_ for _ in self.sensors.values()]
        else:
            return self.sensors_no_args

    def get_sensor(self, code):
        self.called['get_sensor'] = {'code': code}
        return self.sensors[code]

    def get_timeseries(self, code, args):
        args['code'] = code
        self.called['get_timeseries'] = args
        return self.timeseries[code]


def test_sensor_types(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.list_sensor_types', fakedb.list_sensor_types)
    response = client.get('/sensor_types')
    assert 'list_sensor_types' in fakedb.called
    assert response.status == '200 OK'
    assert response.is_json
    assert response.get_json() == fakedb.list_sensor_types()


def test_sensors_no_args(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.list_sensors', fakedb.list_sensors)
    response = client.get('/sensors')
    assert 'list_sensors' in fakedb.called
    assert response.status == '200 OK'
    assert response.is_json
    assert response.get_json() == fakedb.sensors_no_args


def test_sensors(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.list_sensors', fakedb.list_sensors)
    footprint = 'circle((9.2, 33), 1000)'
    after, before = '2019-02-21T11:03:25Z', '2019-02-21T11:50:25Z'
    selector = "sensor_type.category=meteo"
    q = 'footprint={}&after={}&before={}&selector={}'.format(
        footprint, after, before, selector)
    response = client.get('/sensors?{}'.format(q))
    assert 'list_sensors' in fakedb.called
    assert response.status == '200 OK'
    assert response.is_json
    args = fakedb.called['list_sensors']
    assert args['footprint'] == convert_footprint(footprint)
    assert args['after'] == after and args['before'] == before
    assert args['selector'] == selector
    assert response.get_json() == fakedb.list_sensors(args)


def test_sensors_fail(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.list_sensors', fakedb.list_sensors)
    footprint = 'circle((9.2 33), 1000)'
    after, before = '2019-02-21T11:03:25Z', '2019-02-21T11:50:25Z'
    selector = "sensor_type.category=meteo"
    q = 'footprint={}&after={}&before={}&selector={}'.format(
        footprint, after, before, selector)
    with pytest.raises(ValueError) as ve:
        client.get('/sensors?{}'.format(q))
        assert "footprint" in ve.value
        assert footprint in ve.value


def test_sensor(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.get_sensor', fakedb.get_sensor)
    code = list(fakedb.sensors.keys())[0]
    response = client.get('/sensors/{}'.format(code))
    assert 'get_sensor' in fakedb.called
    assert response.status == '200 OK'
    assert response.is_json
    args = fakedb.called['get_sensor']
    assert args['code'] == code
    assert response.get_json() == fakedb.get_sensor(code)


def test_timeseries(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.get_timeseries', fakedb.get_timeseries)
    code = list(fakedb.sensors.keys())[0]
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
