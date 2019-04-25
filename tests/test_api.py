# FIXME move it to a fixture?
class FakeDB:
    def __init__(self):
        self.initialized = False
        self.called = {}
        self.sensor_types = [[1, 'uuid00', 'aaa', {'key1': 'value1'}],
                             [2, 'uuid01', 'bbb', {'key1': 'value1'}]]
        self.sensors_no_args = {'uuid00': 1, 'uuid01': 1}
        self.sensors = [{'sid': 1,
                         'uuid': 'uuid02',
                         'stype': 'uuid00',
                         'geom': 'POINT(9.3 30.2)',
                         'description': {'key1': 'value1'}},
                        {'sid': 2,
                         'uuid': 'uuid03',
                         'stype': 'uuid01',
                         'geom': 'POINT(9.2 30.5)',
                         'description': {'key1': 'value1'}}]
        self.timeseries = [[0.22 * _ for _ in range(20)],
                           [0.10 * _ for _ in range(20)]]

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

    def get_sensor(self, sid):
        self.called['get_sensor'] = {'sid': sid}
        return self.sensors[sid - 1]

    def get_timeseries(self, sid, args):
        args['sid'] = sid
        self.called['get_timeseries'] = args
        return self.timeseries[sid - 1]


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
    center, radius = 'POINT(9.2 33)', 1000
    after, before = '2019-02-21T11:03:25Z', '2019-02-21T11:50:25Z'
    selector = "sensor_type.category=meteo"
    q = 'center={}&radius={}&after={}&before={}&selector={}'.format(
        center, radius, after, before, selector)
    response = client.get('/sensors?{}'.format(q))
    assert 'list_sensors' in fakedb.called
    assert response.status == '200 OK'
    assert response.is_json
    args = fakedb.called['list_sensors']
    assert args['center'] == center and args['radius'] == radius
    assert args['after'] == after and args['before'] == before
    assert args['selector'] == selector
    assert response.get_json() == fakedb.sensors


def test_sensor(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.get_sensor', fakedb.get_sensor)
    sid = 1
    response = client.get('/sensors/{}'.format(sid))
    assert 'get_sensor' in fakedb.called
    assert response.status == '200 OK'
    assert response.is_json
    args = fakedb.called['get_sensor']
    assert args['sid'] == sid
    assert response.get_json() == fakedb.sensors[sid - 1]


def test_timeseries(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.get_timeseries', fakedb.get_timeseries)
    sid = 1
    after, before = '2019-02-21T11:03:25Z', '2019-02-21T11:50:25Z'
    bucket, op = '20 min', 'sum'
    q = 'after={}&before={}&bucket={}&op={}'.format(after, before, bucket, op)
    response = client.get('/sensors/{}/timeseries?{}'.format(sid, q))
    assert 'get_timeseries' in fakedb.called
    assert response.status == '200 OK'
    assert response.is_json
    args = fakedb.called['get_timeseries']
    assert args['sid'] == sid
    assert args['after'] == after and args['before'] == before
    assert args['bucket'] == bucket and args['op'] == op
    assert response.get_json() == fakedb.timeseries[sid - 1]
