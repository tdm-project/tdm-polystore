

import logging
import numpy as np
import os
import requests
import tiledb

from tdmq.client.sources import ScalarSource
from tdmq.client.sources import NonScalarSource

from tdmq.errors import TdmqError
from tdmq.errors import DuplicateItemException
from tdmq.errors import UnsupportedFunctionality

# FIXME need to do this to patch a overzealous logging by urllib3
_logger = logging.getLogger('urllib3.connectionpool')
_logger.setLevel(logging.ERROR)

_logger = logging.getLogger(__name__)


def log_level():
    return _logger.getEffectiveLevel()


def set_log_level(level):
    _logger.setLevel(level)


class Client:
    DEFAULT_TDMQ_BASE_URL = 'http://web:8000/api/v0.0'

    TDMQ_DT_FMT = '%Y-%m-%dT%H:%M:%S.%fZ'
    TDMQ_DT_FMT_NO_MICRO = '%Y-%m-%dT%H:%M:%SZ'

    def __init__(self, tdmq_base_url=None):
        self.base_url = self.DEFAULT_TDMQ_BASE_URL \
            if tdmq_base_url is None else tdmq_base_url

        _logger.debug("New tdmq client object for %s", self.base_url)

        self.connected = False
        self.tiledb_hdfs_root = None
        self.tiledb_ctx = None
        self.tiledb_vfs = None

    def requires_connection(func):
        """
        Decorator for methods that require a connection to the tdmq service.
        """
        def wrapper_requires_connection(self, *args, **kwargs):
            if not self.connected:
                self.connect()
            return func(self, *args, **kwargs)

        return wrapper_requires_connection

    def connect(self):
        if self.connected:
            return

        service_info = self._do_get('service_info')
        _logger.debug("Service sent the following info: \n%s", service_info)

        if service_info['version'] != '0.0':
            raise NotImplementedError(f"This client isn't compatible with service version {service_info['version']}")
        if 'tiledb' in service_info:
            self.tiledb_hdfs_root = service_info['tiledb']['hdfs.root']
            self.tiledb_ctx = tiledb.Ctx(service_info['tiledb'].get('config'))
            self.tiledb_vfs = tiledb.VFS(config=self.tiledb_ctx.config(), ctx=self.tiledb_ctx)
            _logger.info("Configured TileDB context")
            _logger.debug("\t tiledb_hdfs_root: %s", self.tiledb_hdfs_root)
            _logger.debug("\t tiledb_config:\n%s", self.tiledb_ctx.config())

        self.connected = True
        _logger.info("Client connected to TDMQ service at %s", self.base_url)

    def _do_get(self, resource, params=None):
        r = requests.get(f'{self.base_url}/{resource}', params=params)
        r.raise_for_status()
        return r.json()

    def _destroy_source(self, tdmq_id):
        r = requests.delete(f'{self.base_url}/sources/{tdmq_id}')
        r.raise_for_status()
        array_name = self._source_data_path(tdmq_id)
        if tiledb.object_type(self._source_data_path(tdmq_id), ctx=self.tiledb_ctx) == 'array':
            tiledb.remove(array_name, ctx=self.tiledb_ctx)

    def _source_data_path(self, tdmq_id):
        return os.path.join(self.tiledb_hdfs_root, tdmq_id)

    @requires_connection
    def deregister_source(self, s):
        _logger.debug('deregistering %s %s', s.tdmq_id, s)
        self._destroy_source(s.tdmq_id)

    @requires_connection
    def register_source(self, definition, nslots=10*24*3600*365):
        """Register a new data source
        .. :quickref: Register a new data source

        :nslots: is the maximum expected number of slots that will be
        needed. Actual storage allocation will be done at
        ingestion. The default value is 10*24*3600*365
        """
        assert isinstance(definition, dict)
        _logger.debug('registering source id=%s', definition['id'])
        r = requests.post(f'{self.base_url}/sources', json=[definition])
        r.raise_for_status()
        tdmq_id = r.json()[0]
        if 'shape' in definition and len(definition['shape']) > 0:
            try:
                # FIXME add storage drivers
                if definition['storage'] != 'tiledb':
                    raise UnsupportedFunctionality(f'storage type {definition["storage"]} not supported.')
                self._create_tiledb_array(tdmq_id, definition['shape'], definition['controlledProperties'], nslots)
            except Exception as e:
                _logger.error('Failure in creating tiledb array: %s, cleaning up', e)
                self._destroy_source(tdmq_id)
                raise TdmqError(f"Internal failure in registering {definition.get('id', '(id unavailable)')}.")
        return self.get_source(tdmq_id)

    @requires_connection
    def add_records(self, records):
        r = requests.post(f'{self.base_url}/records', json=records)
        r.raise_for_status()

    @requires_connection
    def get_entity_categories(self):
        return self._do_get('entity_categories')

    @requires_connection
    def get_entity_types(self):
        return self._do_get('entity_types')

    @requires_connection
    def find_sources(self, args=None):
        res = self._do_get('sources', params=args)
        return [self.get_source(r['tdmq_id']) for r in res]

    @requires_connection
    def get_source(self, tdmq_id):
        res = self._do_get(f'sources/{tdmq_id}')
        assert res['tdmq_id'] == tdmq_id

        if res['description'].get('shape'):
            return NonScalarSource(self, tdmq_id, res)
        else:
            return ScalarSource(self, tdmq_id, res)

    @requires_connection
    def get_timeseries(self, code, args):
        args = dict((k, v) for k, v in args.items() if v is not None)
        _logger.debug('get_timeseries(%s, %s)', code, args)
        return self._do_get(f'sources/{code}/timeseries', params=args)

    @requires_connection
    def save_tiledb_frame(self, tdmq_id, slot, data):
        aname = self._source_data_path(tdmq_id)
        with tiledb.DenseArray(aname, mode='w', ctx=self.tiledb_ctx) as A:
            A[slot:slot+1] = data

    @requires_connection
    def fetch_data_block(self, tdmq_id, data, args):
        # FIXME hwired on tiledb
        tiledb_index = data['tiledb_index']
        block_of_indx = tiledb_index[args[0]]
        block_of_indx = block_of_indx \
            if isinstance(args[0], slice) else [block_of_indx]
        aname = self._source_data_path(tdmq_id)
        indices = np.array(block_of_indx, dtype=np.int32)
        assert len(indices) == 1 or np.all(indices[1:] - indices[:-1] == 1)
        if isinstance(args[0], slice):
            args = (slice(int(indices.min()),
                          int(indices.max()) + 1), ) + args[1:]
        else:
            assert len(indices) == 1
            args = (int(indices[0]),) + args[1:]
        with tiledb.DenseArray(aname, mode='r', ctx=self.tiledb_ctx) as A:
            data = A[args]
        return data

    def _create_tiledb_array(self, tdmq_id, shape, properties, n_slots):
        array_name = self._source_data_path(tdmq_id)
        _logger.debug('attempting creation of %s', array_name)
        if tiledb.object_type(array_name) is not None:
            raise DuplicateItemException(
                f'duplicate object with path {array_name}')
        assert len(shape) > 0 and n_slots > 0
        dims = [tiledb.Dim(name="slot",
                           domain=(0, n_slots),
                           tile=1, dtype=np.int32)]
        dims = dims + [tiledb.Dim(name=f"dim{i}", domain=(0, n - 1),
                                  tile=n, dtype=np.int32)
                       for i, n in enumerate(shape)]
        _logger.debug('trying domain creation for %s', array_name)
        dom = tiledb.Domain(*dims, ctx=self.tiledb_ctx)
        _logger.debug('trying attribute creation for %s', array_name)
        attrs = [tiledb.Attr(name=aname, dtype=np.float32) for aname in properties]
        _logger.debug('trying ArraySchema creation for %s', array_name)
        schema = tiledb.ArraySchema(domain=dom, sparse=False,
                                    attrs=attrs, ctx=self.tiledb_ctx)
        # Create the (empty) array on disk.
        _logger.debug('ensuring root HDFS directory exists: %s', self.tiledb_hdfs_root)
        self.tiledb_vfs.create_dir(self.tiledb_hdfs_root)
        _logger.debug('trying creation on disk of %s', array_name)
        tiledb.DenseArray.create(array_name, schema, ctx=self.tiledb_ctx)
        _logger.debug('%s successfully created.', array_name)
        return array_name
