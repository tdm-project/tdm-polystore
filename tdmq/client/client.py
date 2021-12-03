
import math
import logging
import os

from contextlib import contextmanager
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Dict, List
from urllib.parse import urlparse

import numpy as np
import requests

import tiledb
import tdmq.errors
from tdmq.client.sources import Source, NonScalarSource, ScalarSource

# FIXME need to do this to patch a overzealous logging by urllib3
logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)

_logger = logging.getLogger(__name__)


def log_level():
    return _logger.getEffectiveLevel()


def set_log_level(level):
    _logger.setLevel(level)


class Client:
    DEFAULT_TDMQ_BASE_URL = 'http://web:8000/api/v0.0'

    # The 'Z' at the end should stand for Zulu, which is the same as UTC
    TDMQ_DT_FMT = '%Y-%m-%dT%H:%M:%S.%fZ'
    TDMQ_DT_FMT_NO_MICRO = '%Y-%m-%dT%H:%M:%SZ'

    @staticmethod
    def _parse_timestamp(ts):
        """
        Create datetime object from a timestamp arriving from the API.
        Timestamps are specified in UTC time.
        """
        return datetime.fromtimestamp(ts, timezone.utc)

    @classmethod
    def _format_timestamp(cls, ts):
        """
        Format a datetime object `ts` in preparation to be sent to the API.
        If ts specifies a time zone, it is used to convert to UTC time.  Else
        **we assume that it is UTC time**.
        """
        if ts.tzinfo is not None:
            ts = ts.astimezone(timezone.utc)
        return ts.strftime(cls.TDMQ_DT_FMT)

    def __init__(self, tdmq_base_url=None, auth_token=None, verify_ssl=None):
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
        self._request_opts = dict()
        if not verify_ssl:  # either None or False
            self._request_opts['verify'] = False

        self.headers = {"User-Agent": "TDMq client/unknown"}
        if auth_token is not None:
            self.headers["Authorization"] = f"Bearer {auth_token}"
        self.headers["Accept"] = "application/json"

    def requires_connection(func):
        """
        Decorator for methods that require a connection to the tdmq service.
        """
        @wraps(func)
        def wrapper_requires_connection(self, *args, **kwargs):
            if not self.connected:
                self.connect()
            return func(self, *args, **kwargs)

        return wrapper_requires_connection

    def url_for(self, resource: str) -> str:
        return f'{self.base_url}/{resource}'

    @staticmethod
    def _raise_for_status(response: requests.Response) -> None:
        if response.status_code < 400:
            return
        if response.status_code == 401 or response.status_code == 403:
            raise tdmq.errors.UnauthorizedError(response.reason, response.status_code)
        if response.status_code == 404:
            raise tdmq.errors.ItemNotFoundException(response.reason)
        if response.status_code == 413:
            raise tdmq.errors.QueryTooLargeException(response.reason)
        if response.status_code == 500:
            raise tdmq.errors.InternalServerError(response.reason)
        if response.status_code == 501:
            raise tdmq.errors.UnsupportedFunctionality(response.reason)
        if response.status_code >= 400 and response.status_code < 500:
            raise tdmq.errors.TdmqBadRequestException(response.reason, response.status_code)
        raise tdmq.errors.TdmqError(title="Server error", status=response.status_code, detail=response.reason)

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

    def _check_if_authorized(self, response: requests.Response) -> None:
        if response.status_code in (401, 403):
            msg = f"Request not authorized ({response.status_code} {response.reason})"
            if "nginx" in response.headers.get("Server", "").lower():
                # This looks like our oauth2 proxy
                _logger.debug("Looks like our request was denied authorization by the TDMq oauth2 proxy")
                url_parts = urlparse(self.base_url)
                sign_in_uri = f"{url_parts.scheme}://{url_parts.netloc}/oauth2/sign_in"
                msg += f"\nPlease get an authentication token from {sign_in_uri}"
            _logger.error(msg)
            raise tdmq.errors.UnauthorizedError(msg, status=response.status_code)

    def _do_get(self, resource, params=None):
        r = requests.get(f'{self.base_url}/{resource}', params=params,
                         headers=self.headers, **self._request_opts)
        self._check_if_authorized(r)
        self._raise_for_status(r)
        return r.json()

    @contextmanager
    def _do_get_stream_ctx(self, resource, params=None):
        with requests.get(f'{self.base_url}/{resource}', stream=True, params=params,
                          headers=self.headers, **self._request_opts) as r:
            self._check_if_authorized(r)
            self._raise_for_status(r)
            yield r

    def _do_post(self, resource: str, json_obj) -> requests.Response:
        r = requests.post(resource, json=json_obj, headers=self.headers,
                          **self._request_opts)
        self._check_if_authorized(r)
        self._raise_for_status(r)
        return r

    def _destroy_source(self, tdmq_id):
        r = requests.delete(f'{self.base_url}/sources/{tdmq_id}', headers=self.headers,
                            **self._request_opts)
        self._check_if_authorized(r)
        self._raise_for_status(r)
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
    def register_source(self, definition, nslots=3600*24*365*10, **kwargs):
        """Register a new data source
        .. :quickref: Register a new data source

        :nslots: is the maximum expected number of slots that will be
        needed. Actual storage allocation will be done at
        ingestion. The default value is 10*24*3600*365

        :tiledb_extents: (optional) list of tile extent sizes.
                         First is for time dimension; successive ones apply to
                         each respective shape dimension.  E.g.,
                            shape = (576, 631); tiledb_extents = [ 100, 576, 631 ]


        :properties: (optional) Mapping of `property name` -> { storage attributes }.
                     The following storage attributes can be specified:
                       * `dtype` -> numpy.dtype
                       * `filters` -> tiledb.FilterList
                     A property may map to None if you don't want to customize settings.
                     E.g., properties={'temperature': None }

        """
        assert isinstance(definition, dict)
        # Try to validate arguments before registering the Source with tdmq
        unknown_args = set(kwargs.keys()) - {'tiledb_extents', 'properties'}
        if unknown_args:
            raise ValueError(f"Unknown kwargs {', '.join(unknown_args)}")

        if 'tiledb_extents' in kwargs:
            extents = kwargs['tiledb_extents']
            if len(extents) != len(definition.get('shape', [])) + 1:
                raise ValueError("Number of tile extents specified is incompatible with array shape "
                                 "(expected len(shape) + 1 == len(tiledb_extents))")
        if 'properties' in kwargs:
            unknown_properties = [ p for p in kwargs['properties'].keys() if p not in definition['controlledProperties'] ]
            if unknown_properties:
                raise ValueError("kwargs['properties'] references the following properties that are not "
                                 f"in the source's controlledProperties: {', '.join(unknown_properties)}")
            for k, v in kwargs['properties'].items():
                known_configs = ('dtype', 'filters')
                if any(k not in known_configs for k in v.keys()):
                    raise ValueError(f"Property configuration '{k}' contains invalid keys. "
                                     "Valid keys are {' and '.join(known_configs)}")

        _logger.debug("POSTing request to create new source with id '%s'", definition['id'])
        r = self._do_post(f'{self.base_url}/sources', json_obj=[definition])
        tdmq_id = r.json()[0]
        _logger.debug("POST successful. Registered source with tdmq_id %s", tdmq_id)
        if len(definition.get('shape', [])) > 0:
            try:
                _logger.debug("Source is NonScalar.  Creating tiledb array")
                extents = kwargs.get('tiledb_extents')
                properties = dict.fromkeys(definition['controlledProperties'])
                if kwargs.get('properties'):
                    properties.update(kwargs['properties'])

                self._create_tiledb_array(tdmq_id, definition['shape'], properties, nslots, extents)
            except Exception as e:
                _logger.exception(e)
                _logger.error('Failure in creating tiledb array: %s, cleaning up', e)
                self._destroy_source(tdmq_id)
                raise tdmq.errors.TdmqError(f"Error registering {definition.get('id', '(id unavailable)')}. {e}")
        return self.get_source(tdmq_id)

    @requires_connection
    def add_records(self, records) -> None:
        self._do_post(f'{self.base_url}/records', json_obj=records)

    @requires_connection
    def get_entity_categories(self):
        return self._do_get('entity_categories')

    @requires_connection
    def get_entity_types(self):
        return self._do_get('entity_types')

    def __source_factory(self, api_struct):
        if api_struct['description'].get('shape'):
            return NonScalarSource(self, api_struct['tdmq_id'], api_struct)
        # else
        return ScalarSource(self, api_struct['tdmq_id'], api_struct)

    @requires_connection
    def find_sources(self, args: Dict[str, Any] = None, **kwargs) -> List[Source]:
        """
        Gets the list of sources filtered using the provided args dictionary.  With
        no parameters, it returns all the sources.  Multiple arguments are combined
        with an AND condition (i.e., the intersection is returned).

        Arguments:

        only_public:
            Boolean. Default: True. Specify `only_public=False` to also select private sources.
        public:
            Boolean.  Select public (True) or private (False) sources.
        after:
            datetime.  Selects sources that have been active after (included) the specified time.
        before:
            datetime.  Selects sources that have been active before the specified time.
        roi:
            string in the format `circle((center_lon, center_lat), radius_in_meters)`.
            Selects sources in the specified region of interest.
            Longitude and Latitude are WGS coordinates.
        entity_type, entity_category:
            String. Select sources of the specified entity type and category, respectively.
        stationary:
            Boolean. Select stationary or mobile sources.
        controlledProperties:
            string.  Comma-separated list of controlled properties that the source must provide.
        id, external_id:
            string. External ID of source.
        other attributes:
            Any attributes not listed above are matched for equality against the Source
            description metadata.  *This will be work on public** sources.

            Recognized attributes:
                'registration_time',
                'tdmq_id'
                'brand_name',
                'edge_id',
                'model_name',
                'operated_by',
                'sensor_id',
                'shape',
                'station_id',
                'station_model',
                'type',
            WARNING: Any queries trying to match attributes not defined in this doc will be
            **limited to public sources**.
        """
        if args:
            if not isinstance(args, dict):
                raise TypeError("'args' argument, if specified, must be a dict")
            args = args.copy()
            args.update(kwargs)
        else:
            args = kwargs
        return [ self.__source_factory(s) for s in self._do_get('sources', params=args) ]

    @requires_connection
    def get_source(self, tdmq_id, anonymized=True):
        res = self._do_get(f'sources/{tdmq_id}', params={'anonymized': anonymized})
        assert res['tdmq_id'] == str(tdmq_id)
        return self.__source_factory(res)

    @requires_connection
    def get_timeseries(self, code, args, sparse: bool = None):
        args = dict((k, v) for k, v in args.items() if v is not None)
        if sparse is not None:
            args['sparse'] = sparse
        # for testing!  args['batch_size'] = 1
        _logger.debug('get_timeseries(%s, %s)', code, args)
        try:
            with self._do_get_stream_ctx(f'sources/{code}/timeseries_stream', params=args) as req:
                return req.json()
        except requests.exceptions.ChunkedEncodingError as e:
            if '0 bytes read' in str(e).lower():
                raise RuntimeError("Server took too long to respond.  Try reducing the size of the time series you're requesting")
            raise

    @requires_connection
    def export_timeseries(self, code, args, data_format: str = 'csv', chunk_size=16384):
        if data_format != 'csv':
            raise NotImplementedError("only CSV export is supported")
        _logger.debug('export_timeseries(%s, %s, data_format=%s, chunk_size=%s)',
                      code, args, data_format, chunk_size)
        args = dict((k, v) for k, v in args.items() if v is not None)
        args['format'] = data_format
        with self._do_get_stream_ctx(f'sources/{code}/timeseries_stream', params=args) as req:
            for chunk in req.iter_content(chunk_size=chunk_size):
                yield chunk

    @requires_connection
    def get_latest_source_activity(self, tdmq_id):
        _logger.debug("get_latest_source_activity(%s)", tdmq_id)
        r = self._do_get(f'sources/{tdmq_id}/activity/latest')
        if r['time'] is not None:
            r['time'] = self._parse_timestamp(r['time'])
        return r

    def open_array(self, tdmq_id, mode='r'):
        aname = self._source_data_path(tdmq_id)
        return tiledb.open(aname, mode=mode, ctx=self.tiledb_ctx)

    def close_array(self, tiledb_array):
        tiledb_array.close()

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

    def _create_tiledb_array(self, tdmq_id, shape, properties, n_slots, extent_sizes=None):
        array_name = self._source_data_path(tdmq_id)
        _logger.debug('attempting creation of %s', array_name)
        if tiledb.object_type(array_name, self.tiledb_ctx) is not None:
            raise tdmq.errors.DuplicateItemException(f'duplicate object with path {array_name}')
        assert len(shape) > 0 and n_slots > 0

        attr_defaults = dict(dtype=np.float32, filters=tiledb.FilterList([tiledb.ZstdFilter()]))

        def _attr_params(attr_name, attr_config=None):
            if not attr_config:
                attr_config = attr_defaults
            return dict(name=attr_name,
                        dtype=attr_config.get('dtype', attr_defaults['dtype']),
                        filters=attr_config.get('filters', attr_defaults['filters']))

        _logger.debug('Creating attributes for array %s', array_name)
        attrs = [tiledb.Attr(**_attr_params(aname, cfg)) for aname, cfg in properties.items()]

        if not extent_sizes:
            extent_sizes = [ min(n_slots, 100) ] + [ math.ceil(s / 3) for s in shape ]
        _logger.debug("Using extent sizes %s for shape %s", extent_sizes, shape)

        dims = [tiledb.Dim(name="slot",
                           domain=(0, n_slots),
                           tile=extent_sizes[0], dtype=np.int32)]
        dims = dims + [tiledb.Dim(name=f"dim{i}", domain=(0, n - 1),
                                  tile=extent_sizes[i+1], dtype=np.int32)
                       for i, n in enumerate(shape)]
        dom = tiledb.Domain(*dims, ctx=self.tiledb_ctx)
        schema = tiledb.ArraySchema(domain=dom, sparse=False,
                                    attrs=attrs, ctx=self.tiledb_ctx)
        _logger.debug("Array schema for %s:\n%s", array_name, schema)
        _logger.debug("Creating the array on storage...")
        _logger.debug('ensuring root storage directory exists: %s', self.tiledb_storage_root)
        self.tiledb_vfs.create_dir(self.tiledb_storage_root)
        _logger.debug('trying creation on disk of %s', array_name)
        tiledb.DenseArray.create(array_name, schema, ctx=self.tiledb_ctx)
        _logger.debug('%s successfully created.', array_name)
        return array_name
