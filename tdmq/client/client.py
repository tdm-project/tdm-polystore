
import logging
import os

import numpy as np
import requests

import tiledb
from tdmq.client.sources import NonScalarSource, ScalarSource
from tdmq.errors import (DuplicateItemException, TdmqError,
                         UnsupportedFunctionality)

# FIXME need to do this to patch a overzealous logging by urllib3
logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)

_logger = logging.getLogger(__name__)


def log_level():
    return _logger.getEffectiveLevel()


def set_log_level(level):
    _logger.setLevel(level)


class Client:
    DEFAULT_TDMQ_BASE_URL = 'http://web:8000/api/v0.0'

    TDMQ_DT_FMT = '%Y-%m-%dT%H:%M:%S.%fZ'
    TDMQ_DT_FMT_NO_MICRO = '%Y-%m-%dT%H:%M:%SZ'

    def __init__(self, tdmq_base_url=None, auth_token=None):
        self.base_url = (tdmq_base_url or
                         os.getenv('TDMQ_BASE_URL') or
                         self.DEFAULT_TDMQ_BASE_URL)
        # Strip any trailing slashes from the prefix
        self.base_url = self.base_url.rstrip('/')

        _logger.debug("New tdmq client object for %s", self.base_url)

        self.connected = False
        self.tiledb_storage_root = None
        self.tiledb_ctx = None
        self.tiledb_vfs = None
        self.headers = {'Authorization': f'Bearer {auth_token}'} if auth_token is not None else {}


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

        if service_info['version'] != '0.1':
            raise NotImplementedError(f"This client isn't compatible with service version {service_info['version']}")
        if 'tiledb' in service_info:
            self.tiledb_storage_root = service_info['tiledb']['storage.root']
            self.tiledb_ctx = tiledb.Ctx(service_info['tiledb'].get('config'))
            self.tiledb_vfs = tiledb.VFS(config=self.tiledb_ctx.config(), ctx=self.tiledb_ctx)
            _logger.info("Configured TileDB context")
            _logger.debug("\t tiledb_storage_root: %s", self.tiledb_storage_root)
            _logger.debug("\t tiledb_config:\n%s", self.tiledb_ctx.config())
            if self.tiledb_storage_root.startswith("s3:"):
                if not self.tiledb_vfs.is_bucket(self.tiledb_storage_root):
                    raise RuntimeError(f"The storage root bucket {self.tiledb_storage_root} does not exist!")

        self.connected = True
        _logger.info("Client connected to TDMQ service at %s", self.base_url)

    def _do_get(self, resource, params=None):
        r = requests.get(f'{self.base_url}/{resource}', params=params, headers=self.headers)
        r.raise_for_status()
        return r.json()

    def _destroy_source(self, tdmq_id):
        r = requests.delete(f'{self.base_url}/sources/{tdmq_id}', headers=self.headers)
        r.raise_for_status()
        array_name = self._source_data_path(tdmq_id)
        if tiledb.object_type(self._source_data_path(tdmq_id), ctx=self.tiledb_ctx) == 'array':
            tiledb.remove(array_name, ctx=self.tiledb_ctx)

    def _source_data_path(self, tdmq_id):
        return os.path.join(self.tiledb_storage_root, tdmq_id)

    @requires_connection
    def deregister_source(self, s):
        _logger.debug('deregistering %s %s', s.tdmq_id, s)
        # FIXME: this is kind of ugly.  Should we add the concept of Array
        # to the top-most Source class?
        if hasattr(s, 'close_array'):
            s.close_array()
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
        r = requests.post(f'{self.base_url}/sources', json=[definition], headers=self.headers)
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
        r = requests.post(f'{self.base_url}/records', json=records, headers=self.headers)
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


    def open_array(self, tdmq_id, mode='r'):
        aname = self._source_data_path(tdmq_id)
        return tiledb.open(aname, mode=mode, ctx=self.tiledb_ctx)


    def close_array(self, tiledb_array):
        tiledb_array.close()


    @requires_connection
    def save_tiledb_frame(self, tiledb_array, slot, data):
        tiledb_array[slot:slot + 1] = data


    @requires_connection
    def fetch_non_scalar_slice(self, tiledb_array, tiledb_indices, args):
        block_of_indx = tiledb_indices[args[0]]
        block_of_indx = block_of_indx \
            if isinstance(args[0], slice) else [block_of_indx]
        indices = np.array(block_of_indx, dtype=np.int32)
        assert len(indices) == 1 or np.all(indices[1:] - indices[:-1] == 1)
        if isinstance(args[0], slice):
            tiledb_i = (slice(int(indices.min()),
                              int(indices.max()) + 1), ) + args[1:]
        else:
            assert len(indices) == 1
            tiledb_i = (int(indices[0]),) + args[1:]
        data = tiledb_array[tiledb_i]
        return data


    def _create_tiledb_array(self, tdmq_id, shape, properties, n_slots):
        array_name = self._source_data_path(tdmq_id)
        _logger.debug('attempting creation of %s', array_name)
        if tiledb.object_type(array_name, self.tiledb_ctx) is not None:
            raise DuplicateItemException(f'duplicate object with path {array_name}')
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
        attrs = [tiledb.Attr(name=aname,
                             dtype=np.float32,
                             filters=tiledb.FilterList(
                             [tiledb.ZstdFilter()])) for aname in properties]
        _logger.debug('trying ArraySchema creation for %s', array_name)
        schema = tiledb.ArraySchema(domain=dom, sparse=False,
                                    attrs=attrs, ctx=self.tiledb_ctx)
        # Create the (empty) array on disk.
        _logger.debug('ensuring root storage directory exists: %s', self.tiledb_storage_root)
        self.tiledb_vfs.create_dir(self.tiledb_storage_root)
        _logger.debug('trying creation on disk of %s', array_name)
        tiledb.DenseArray.create(array_name, schema, ctx=self.tiledb_ctx)
        _logger.debug('%s successfully created.', array_name)
        return array_name
