

import pytest
import tempfile

pytestmark = pytest.mark.skip(reason="not up-to-date with json model migration")

def test_db_init(runner, monkeypatch):
    class Recorder:
        called = False

    def fake_init_db(drop=False):
        Recorder.called = True
    monkeypatch.setattr('tdmq.db.init_db', fake_init_db)
    result = runner.invoke(args=['db', 'init'])
    assert 'Initialized' in result.output
    assert Recorder.called


def test_db_load(runner, monkeypatch):
    class Recorder:
        called = False
        result = {'sources': 6, 'records': 17}

    def fake_load_file(fname):
        Recorder.called = fname
        return Recorder.result
    monkeypatch.setattr('tdmq.db.load_file', fake_load_file)
    with tempfile.NamedTemporaryFile() as tmpfile:
        result = runner.invoke(args=['db', 'load', tmpfile.name])
        assert Recorder.called == tmpfile.name
    assert str(Recorder.result) in result.output


def test_init_db(runner):
    result = runner.invoke(args=['db', 'init'])
    assert 'Initialized' in result.output


def assert_n_dumped(runner, _type, expected_n):
    with tempfile.NamedTemporaryFile("w") as f:
        result = runner.invoke(args=['db', 'dump', _type, f.name])
    assert "Dumped {}".format(expected_n) in result.output


def test_load_db(runner):
    runner.invoke(args=['db', 'init', '--drop'])
    result = runner.invoke(args=['db', 'load', sensors_fname])
    n = len(json.load(open(sensors_fname))['sensors'])
    assert_n_dumped(runner, 'sensors', n)
    assert "Loaded {'sensors': %d}" % n in result.output
    result = runner.invoke(args=['db', 'load', records_fname])
    n = len(json.load(open(records_fname))['records'])
    assert "Loaded {'records': %d}" % n in result.output
    n = len(json.load(open(records_fname))['records'])
    assert_n_dumped(runner, 'records', n)


def init_db_drop(runner):
    runner.invoke(args=['db', 'init', '--drop'])
    runner.invoke(args=['db', 'load', sensor_types_fname])
    n = len(json.load(open(sensor_types_fname))['sensor_types'])
    assert_n_dumped(runner, 'sensor_types', n)
    runner.invoke(args=['db', 'init'])
    assert_n_dumped(runner, 'sensor_types', n)
    runner.invoke(args=['db', 'init', '--drop'])
    assert_n_dumped(runner, 'sensor_types', 0)
