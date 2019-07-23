# FIXME: We are assuming that we have access to the DB mentioned in conftest.py

import json
# FIXME move to fixtures
import os
import tempfile

root = os.path.dirname(os.path.abspath(__file__))
sensor_types_fname = os.path.join(root, 'data/sensor_types.json')
sensors_fname = os.path.join(root, 'data/sensors.json')
measures_fname = os.path.join(root, 'data/measures.json')


def test_init_db(runner):
    result = runner.invoke(args=['db', 'init'])
    assert 'Initialized' in result.output


def assert_n_dumped(runner, _type, expected_n):
    with tempfile.NamedTemporaryFile("w") as f:
        result = runner.invoke(args=['db', 'dump', _type, f.name])
    assert "Dumped {}".format(expected_n) in result.output


def test_load_db(runner):
    runner.invoke(args=['db', 'init', '--drop'])
    result = runner.invoke(args=['db', 'load', sensor_types_fname])
    n = len(json.load(open(sensor_types_fname))['sensor_types'])
    assert "Loaded {'sensor_types': %d}" % n in result.output
    assert_n_dumped(runner, 'sensor_types', n)
    result = runner.invoke(args=['db', 'load', sensors_fname])
    n = len(json.load(open(sensors_fname))['sensors'])
    assert_n_dumped(runner, 'sensors', n)
    assert "Loaded {'sensors': %d}" % n in result.output
    result = runner.invoke(args=['db', 'load', measures_fname])
    n = len(json.load(open(measures_fname))['measures'])
    assert "Loaded {'measures': %d}" % n in result.output
    n = len(json.load(open(measures_fname))['measures'])
    assert_n_dumped(runner, 'measures', n)


def init_db_drop(runner):
    runner.invoke(args=['db', 'init', '--drop'])
    runner.invoke(args=['db', 'load', sensor_types_fname])
    n = len(json.load(open(sensor_types_fname))['sensor_types'])
    assert_n_dumped(runner, 'sensor_types', n)
    runner.invoke(args=['db', 'init'])
    assert_n_dumped(runner, 'sensor_types', n)
    runner.invoke(args=['db', 'init', '--drop'])
    assert_n_dumped(runner, 'sensor_types', 0)
