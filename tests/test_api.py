import json
import os
import pytest
import uuid
from collections import OrderedDict
from copy import deepcopy
from datetime import datetime, timedelta
from tdmq.utils import convert_roi

pytestmark = pytest.mark.skip(reason="not up-to-date with json model migration")

root = os.path.dirname(os.path.abspath(__file__))
sources_fname = os.path.join(root, 'data/sources.json')
records_fname = os.path.join(root, 'data/records.json')


# FIXME move it to a fixture?
class FakeDB:

    NS = uuid.UUID('6cb10168-c65b-48fa-af9b-a3ca6d03156d')

    def _load_sources(self, fname):
        table = OrderedDict()
        with open(fname) as f:
            data = json.load(f)
        descriptions = next(iter(data.values()))
        for d in descriptions:
            tdmq_id = str(uuid.uuid5(self.NS, d['id']))
            table[tdmq_id] = d
        self.sources = table

    def _load_records(self, fname):
        with open(fname) as f:
            records = json.load(f)['records']
        data = {}
        for m in records:
            tdmq_id = str(uuid.uuid5(self.NS, m['source']))
            data.setdefault(tdmq_id, []).append(
                [datetime.strptime(m['time'], '%Y-%m-%dT%H:%M:%SZ'),
                 # m['geometry'],
                 m['dataset']])
        self.timeseries = {}
        for k in data:
            ts = sorted(data[k])
            time_origin = ts[0][0]
            ts = [[(_[0] - time_origin).seconds, _[1:]] for _ in ts]
            self.timeseries[k] = {
                'time_origin': time_origin.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'data': ts}
        

    def __init__(self):
        self.called = {}
        self._load_sources(sources_fname)
        self._load_records(records_fname)
        self.entity_types = dict(('MeteoRadarMosaic', 'Radar'),
                                 ('PointWeatherObserver', 'Station'),
                                 ('TemperatureMosaic', 'Station'))
        
        self.geometry_types = [
            'Point', 'LineString', 'Polygon',
            'MultiPoint', 'MultiLineString', 'MultiPolygon',
            'GeometryCollection', 'CircularString', 'CompoundCurve',
            'CurvePolygon', 'MultiCurve', 'MultiSurface',
            'PolyhedralSurface', 'Triangle', 'Tin']

    def list_entity_types(self):
        self.called['list_entity_types'] = ()
        return list(self.entity_types.keys())
        
    def list_entity_categories(self):
        self.called['list_entity_categories'] = ()
        return list(self.entity_types.values())        
    
    def list_geometry_types(self):
        self.called['list_geometry_types'] = ()
        return self.geometry_types
        
    def list_sources(self, args):
        self.called['list_sources'] = args
        return list(deepcopy(self.sources).items())

    def get_source(self, tdmq_id):
        self.called['get_source'] = {'tdmq_id': tdmq_id}
        return deepcopy(self.sources[tdmq_id])

    def get_timeseries(self, tdmq_id, args):
        args['tdmq_id'] = tdmq_id
        self.called['get_timeseries'] = args
        return self.timeseries[tdmq_id]


def _checkresp(response, table=None):
    assert response.status == '200 OK'
    assert response.is_json
    if table:
        result = response.get_json()
        assert len(result) == len(table)
        for r in result:
            assert "tdmq_id" in r
            tdmq_id = r.pop("tdmq_id")
            assert r == table[tdmq_id]


# def test_source_types(client, monkeypatch):
#     fakedb = FakeDB()
#     monkeypatch.setattr('tdmq.db.list_source_types', fakedb.list_source_types)
#     in_args = {"type": "multisource", "controlledProperty": "temperature"}
#     q = "&".join(f"{k}={v}" for k, v in in_args.items())
#     response = client.get(f'/source_types?{q}')
#     assert 'list_source_types' in fakedb.called
#     args = fakedb.called['list_source_types']
#     assert {k: v for k, v in args.items()} == in_args
#     _checkresp(response, table=fakedb.source_types)


def test_sources_no_args(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.list_sources', fakedb.list_sources)
    response = client.get('/sources')
    assert 'list_sources' in fakedb.called
    _checkresp(response, table=fakedb.sources)


def test_sources(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.list_sources', fakedb.list_sources)
    geom = 'circle((9.2, 33), 1000)'
    after, before = '2019-02-21T11:03:25Z', '2019-02-21T11:50:25Z'
    q = f'roi={geom}&after={after}&before={before}'
    response = client.get(f'/sources?{q}')
    assert 'list_sources' in fakedb.called
    args = fakedb.called['list_sources']
    assert args['roi'] == convert_roi(geom)
    assert args['after'] == after and args['before'] == before
    _checkresp(response, table=fakedb.sources)


def test_sources_fail(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.list_sources', fakedb.list_sources)
    geom = 'circle((9.2 33), 1000)'  # note the missing comma
    after, before = '2019-02-21T11:03:25Z', '2019-02-21T11:50:25Z'
    type_ = 'foo'
    q = f'roi={geom}&after={after}&before={before}&type={type_}'
    with pytest.raises(ValueError) as ve:
        client.get(f'/sources?{q}')
        assert "roi" in ve.value
        assert geom in ve.value


def test_source(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.get_source', fakedb.get_source)
    tdmq_id = next(iter(fakedb.sources))
    response = client.get(f'/sources/{tdmq_id}')
    args = fakedb.called['get_source']
    assert args['tdmq_id'] == tdmq_id
    assert 'get_source' in fakedb.called
    _checkresp(response)
    result = response.get_json()
    assert "tdmq_id" in result
    tdmq_id = result.pop("tdmq_id")
    assert result == fakedb.sources[tdmq_id]


def test_timeseries(client, monkeypatch):
    fakedb = FakeDB()
    monkeypatch.setattr('tdmq.db.get_timeseries', fakedb.get_timeseries)
    tdmq_id = next(iter(fakedb.sources))
    # FIXME these timepoints are random
    after, before = '2019-02-21T11:03:25Z', '2019-02-21T11:50:25Z'
    bucket, op = 20.22, 'sum'
    q = f'after={after}&before={before}&bucket={bucket}&op={op}'
    response = client.get(f'/sources/{tdmq_id}/timeseries?{q}')
    assert 'get_timeseries' in fakedb.called
    assert response.status == '200 OK'
    assert response.is_json
    args = fakedb.called['get_timeseries']
    assert args['tdmq_id'] == tdmq_id
    assert args['after'] == after and args['before'] == before
    assert args['bucket'] == timedelta(seconds=bucket) and args['op'] == op
    assert response.get_json() == fakedb.timeseries[tdmq_id]
