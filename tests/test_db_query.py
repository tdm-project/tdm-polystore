import json
from tdmq.db import list_descriptions_in_table
from tdmq.db import get_object

import os
root = os.path.dirname(os.path.abspath(__file__))


def test_db_list_sensor_types(db):
    sensor_types = json.load(
        open(os.path.join(root, 'data/sensor_types.json')))['sensor_types']
    data = list_descriptions_in_table(db, 'sensor_types')
    assert len(sensor_types) == len(data)
    assert data == sensor_types


def test_db_list_sensors(db):
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
