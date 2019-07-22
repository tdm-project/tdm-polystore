import requests
import tiledb
import numpy as np

from tdmq.client.sensors import NonScalarSensor

sensor_classes = {
    'meteoRadar': NonScalarSensor,
}


class Client:
    def __init__(self, tdmq_base_url, tiledb_ctx=None):
        self.base_url = tdmq_base_url
        self.tiledb_ctx = tiledb_ctx
        self.sensor_types = None
        self.update_sensor_types()

    def update_sensor_types(self):
        stypes = requests.get(f'{self.base_url}/sensor_types').json()
        self.sensor_types = dict((st['name'], st) for st in stypes)

    def get_sensors(self, args):
        res = requests.get(f'{self.base_url}/sensors', params=args).json()
        return [self.get_sensor_proxy(r['code']) for r in res]

    def get_sensor_proxy(self, code):
        res = requests.get(f'{self.base_url}/sensors/{code}').json()
        assert res['code'] == code
        # FIXME we need to fix this 'type' thing
        stype = self.sensor_types[res['type']]
        return sensor_classes[stype['type']](
            self, code, stype, res)

    def get_timeseries(self, code, args):
        return requests.get(f'{self.base_url}/sensors/{code}/timeseries',
                            params=args).json()

    def fetch_data_block(self, block_of_refs, args):
        urls = set(r[0] for r in block_of_refs)
        # FIXME we support only trivial cases, for the time being
        assert len(urls) == 1
        indices = np.array([r[1] for r in block_of_refs], dtype=np.int32)
        assert len(indices) == 1 or np.all(indices[1:] - indices[:-1] == 1)
        url = urls.pop()
        if isinstance(args[0], slice):
            args = (slice(int(indices.min()),
                          int(indices.max()) + 1), ) + args[1:]
        else:
            assert len(indices) == 1
            args = (int(indices[0]),) + args[1:]
        with tiledb.DenseArray(url, mode='r', ctx=self.tiledb_ctx) as A:
            data = A[args]
        return data
