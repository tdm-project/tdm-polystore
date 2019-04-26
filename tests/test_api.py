# FIXME move it to a fixture?
class FakeDB:
    def __init__(self):
        self.initialized = False
        self.called = {}
        self.sensor_types = [{'code': 1,
                              'description': {
                                  "id": "0fd67c67-c9be-45c6-9719-4c4eada4be65",
                                  "type": "TemperatureSensorModel",
                                  "name": "temperature sensor in DHT11",
                                  "brandName": "Acme",
                                  "modelName": "Acme multisensor DHT11",
                                  "manufacturerName": "Acme Inc.",
                                  "category": ["sensor"],
                                  "function": ["sensing"],
                                  "controlledProperty": ["temperature"]},
                              },
                             {"code": 2,
                              "description": {
                                  "id": "0fd67c67-c9be-45c6-9719-4c4eada4bebe",
                                  "type": "HumiditySensorModel",
                                  "name": "Humidity sensor in DHT11",
                                  "brandName": "Acme",
                                  "modelName": "Acme multisensor DHT11",
                                  "manufacturerName": "Acme Inc.",
                                  "category": ["sensor"],
                                  "function": ["sensing"],
                                  "controlledProperty": ["humidity"]}
                              }
                             ]
        self.sensors_no_args = {"0fd67c67-c9be-45c6-9719-4c4eada4be65": 1,
                                "0fd67c67-c9be-45c6-9719-4c4eada4bebe": 1}
        self.sensors = [
            {"code": 1,
             "stypecode": 1,
             "geometry": {"type": "Point", "coordinates": [9.3, 30.0]},
             "description": {"uuid": "0fd67c67-c9be-45c6-9719-4c4eada4becc"}
             },
            {"code": 2,
             "stypecode": 2,
             "geometry": {"type": "Point", "coordinates": [9.2, 31.0]},
             "description": {"uuid": "0fd67c67-c9be-45c6-9719-4c4eada4beff"}
             },
        ]
        self.timeseries = [[[0.11, 0.22, 0.33, 0.44],
                            [12000, 12100, 12200, 12300]],
                           [[1.11, 1.22, 1.33, 1.44],
                            [12000, 12100, 12200, 12300]]]

    def init(self):
        self.initialized = True

    def list_sensor_types(self):
        self.called['list_sensor_types'] = True
        return self.sensor_types

    def list_sensors(self, args):
        self.called['list_sensors'] = args
        if args:
            return self.sensors
        else:
            return self.sensors_no_args

    def get_sensor(self, code):
        self.called['get_sensor'] = {'code': code}
        return self.sensors[code - 1]

    def get_timeseries(self, code, args):
        args['code'] = code
        self.called['get_timeseries'] = args
        return self.timeseries[code - 1]


def test_sensor_types(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.list_sensor_types', fakedb.list_sensor_types)
    response = client.get('/sensor_types')
    assert 'list_sensor_types' in fakedb.called
    assert response.status == '200 OK'
    assert response.is_json
    assert response.get_json() == fakedb.sensor_types


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
    footprint = 'circle((9.2 33), 1000)'
    after, before = '2019-02-21T11:03:25Z', '2019-02-21T11:50:25Z'
    selector = "sensor_type.category=meteo"
    q = 'footprint={}&after={}&before={}&selector={}'.format(
        footprint, after, before, selector)
    response = client.get('/sensors?{}'.format(q))
    assert 'list_sensors' in fakedb.called
    assert response.status == '200 OK'
    assert response.is_json
    args = fakedb.called['list_sensors']
    assert args['footprint'] == footprint
    assert args['after'] == after and args['before'] == before
    assert args['selector'] == selector
    assert response.get_json() == fakedb.sensors


def test_sensor(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.get_sensor', fakedb.get_sensor)
    code = 1
    response = client.get('/sensors/{}'.format(code))
    assert 'get_sensor' in fakedb.called
    assert response.status == '200 OK'
    assert response.is_json
    args = fakedb.called['get_sensor']
    assert args['code'] == code
    assert response.get_json() == fakedb.sensors[code - 1]


def test_timeseries(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.get_timeseries', fakedb.get_timeseries)
    code = 1
    after, before = '2019-02-21T11:03:25Z', '2019-02-21T11:50:25Z'
    bucket, op = '20 min', 'sum'
    q = 'after={}&before={}&bucket={}&op={}'.format(after, before, bucket, op)
    response = client.get('/sensors/{}/timeseries?{}'.format(code, q))
    assert 'get_timeseries' in fakedb.called
    assert response.status == '200 OK'
    assert response.is_json
    args = fakedb.called['get_timeseries']
    assert args['code'] == code
    assert args['after'] == after and args['before'] == before
    assert args['bucket'] == bucket and args['op'] == op
    assert response.get_json() == fakedb.timeseries[code - 1]
