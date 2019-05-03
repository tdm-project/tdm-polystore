# FIXME: We are assuming that we have access to the DB mentioned in conftest.py

# FIXME move to fixtures
import os
import json
import tempfile


root = os.path.dirname(os.path.abspath(__file__))
sensor_types_fname = os.path.join(root, 'data/sensor_types.json')
sensors_fname = os.path.join(root, 'data/sensors.json')
measures_fname = os.path.join(root, 'data/measures.json')


def test_init_db(runner):
    result = runner.invoke(args=['db', 'init'])
    assert 'Initialized' in result.output


def test_load_db(runner):
    # FIXME it assumes that it will be run after test_init_db
    result = runner.invoke(args=['db', 'load', sensor_types_fname])
    n = len(json.load(open(sensor_types_fname))['sensor_types'])
    assert "Loaded {'sensor_types': %d}" % n in result.output
    result = runner.invoke(args=['db', 'load', sensors_fname])
    n = len(json.load(open(sensors_fname))['sensors'])
    assert "Loaded {'sensors': %d}" % n in result.output
    result = runner.invoke(args=['db', 'load', measures_fname])
    n = len(json.load(open(measures_fname))['measures'])
    assert "Loaded {'measures': %d}" % n in result.output


def test_dump_db(runner):
    # FIXME it assumes that it will be run after test_load_db
    n = len(json.load(open(measures_fname))['measures'])
    with tempfile.NamedTemporaryFile("w") as f:
        result = runner.invoke(args=['db', 'dump', 'measures', f.name])
    assert "Dumped {}".format(n) in result.output
