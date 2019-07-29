import requests
import tiledb
import os
import numpy as np

from tdmq.client.sources import NonScalarSource

source_classes = {
    'meteoRadar': NonScalarSource,
}


class Client:
    TILEDB_HDFS_ROOT = 'hdfs://hdfs:9000/arrays'
    TDMQ_BASE_URL = 'http://web:8000/api/v0.0'

    def __init__(self,
                 tdmq_base_url=None, tiledb_ctx=None, tiledb_hdfs_root=None):
        self.base_url = self.TDMQ_BASE_URL \
            if tdmq_base_url is None else tdmq_base_url
        self.tiledb_hdfs_root = self.TILEDB_HDFS_ROOT \
            if tiledb_hdfs_root is None else tiledb_hdfs_root
        self.tiledb_ctx = tiledb_ctx

    def source_data_path(self, tdmq_id):
        return os.path.join(self.tiledb_hdfs_root, tdmq_id)

    def get_entity_categories(self):
        return requests.get(f'{self.base_url}/entity_categories').json()

    def get_entity_types(self):
        return requests.get(f'{self.base_url}/entity_types').json()

    def get_geometry_types(self):
        return requests.get(f'{self.base_url}/geometry_types').json()

    def get_sources(self, args):
        res = requests.get(f'{self.base_url}/sources', params=args).json()
        return [self.get_source_proxy(r['tdmq_id']) for r in res]

    def get_source_proxy(self, tdmq_id):
        res = requests.get(f'{self.base_url}/sources/{tdmq_id}').json()
        assert res['tdmq_id'] == tdmq_id
        # FIXME we need to fix this 'type' thing
        stype = self.source_types[res['type']]
        return source_classes[stype['type']](
            self, code, stype, res)

    def get_timeseries(self, code, args):
        return requests.get(f'{self.base_url}/sources/{code}/timeseries',
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
