import tdmq.client.client as tdmqc
import numpy as np
import logging

# FIXME need to do this to patch a overzealous logging by urllib3
logger = logging.getLogger('urllib3.connectionpool')
logger.setLevel(logging.ERROR)
import requests

import os
import math
import tiledb
from datetime import datetime
# FIXME this is a patch for a missing handler.
from tdmq.client.client import sensor_classes
sensor_classes['temperatureSensorNetwork'] = sensor_classes['meteoRadar']




def ingest(sensor, t, data_fetcher):
    time_base = datetime.strptime(sensor.description['timebase'],
                                  '%Y-%m-%dT%H:%M:%SZ')
    time_delta = sensor.description['timedelta']
    i = math.floor((t - time_base).total_seconds() / time_delta)
    print(t, i)
    data = {}
    for fname in sensor.description['controlledProperty']:
        data[fname] = data_fetcher(sensor, t, fname)
    array_path = sensor.client.sensor_data_path(sensor.code)
    with tiledb.DenseArray(array_path, mode='w',
                           ctx=sensor.client.tiledb_ctx) as A:
        A[i:i+1, :, :] = data
    return sensor.client.register_measure(
        {'time': t.strftime('%Y-%m-%dT%H:%M:%SZ'),
         'sensor': sensor.description['name'],
         'measure': {'reference': array_path, 'index': i}})


class Client(tdmqc.Client):

    def __init__(self, tdmq_base_url, hdfs_url):
        self.hdfs_url = hdfs_url
        super().__init__(tdmq_base_url,
                         tiledb.Ctx({'vfs.hdfs.username': 'root'}))

    def sensor_data_path(self, code):
        return os.path.join(self.hdfs_url, code)

    def create_tiledb_array(self, n_slots, description):
        array_name = self.sensor_data_path(description['code'])
        if tiledb.object_type(array_name) is not None:
            raise ValueError('duplicate object with path %s' % array_name)
        shape = description['shape']
        assert len(shape) > 0 and n_slots > 0
        dims = [tiledb.Dim(name="delta_t",
                           domain=(0, n_slots),
                           tile=1, dtype=np.int32)]
        dims = dims + [tiledb.Dim(name=f"dim{i}", domain=(0, n - 1),
                                  tile=n, dtype=np.int32)
                       for i, n in enumerate(shape)]
        dom = tiledb.Domain(*dims, ctx=self.tiledb_ctx)
        attrs = [tiledb.Attr(name=aname, dtype=np.float32)
                 for aname in description['controlledProperty']]
        schema = tiledb.ArraySchema(domain=dom, sparse=False,
                                    attrs=attrs, ctx=self.tiledb_ctx)
        # Create the (empty) array on disk.
        tiledb.DenseArray.create(array_name, schema)
        return array_name

    def register_measure(self, measure):
        assert isinstance(measure, dict)
        # FIXME check if thing already exists and manage errors
        r = requests.post(f'{self.base_url}/measures', json=[measure])
        if r.status_code == 500:
            raise ValueError('Illegal value')
        return r.json()

    def register_thing(self, thing, description):
        assert isinstance(description, dict)
        r = requests.post(f'{self.base_url}/{thing}', json=[description])
        if r.status_code == 500:
            raise ValueError('Internal error')
        description['code'] = r.json()[0]
        return description

    def register_sensor_type(self, description):
        description = self.register_thing('sensor_types', description)
        self.update_sensor_types()
        return description

    def register_sensor(self, description, nslots=None):
        description = self.register_thing('sensors', description)
        if 'shape' in description and len(description['shape']) > 0:
            assert nslots is not None
            self.create_tiledb_array(nslots, description)
        return description
