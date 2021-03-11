
import abc
import logging
from collections.abc import Iterable
from collections.abc import Mapping
from contextlib import contextmanager

import tiledb
from tdmq.client.timeseries import ScalarTimeSeries
from tdmq.client.timeseries import NonScalarTimeSeries
from tdmq.errors import UnsupportedFunctionality
from tdmq.utils import timeit

_logger = logging.getLogger(__name__)


class Source(abc.ABC):
    def __init__(self, client, tdmq_id, desc):
        self.client = client
        self.tdmq_id = tdmq_id
        self._full_body = desc

    def _get_info(self):
        return self._full_body['description']

    @property
    def id(self):
        return self._full_body.get('external_id')

    @property
    def external_id(self):
        return self.id

    @property
    def default_footprint(self):
        return self._full_body['default_footprint']

    @property
    def is_stationary(self):
        return self._full_body['stationary']

    @property
    def entity_category(self):
        return self._full_body['entity_category']

    @property
    def entity_type(self):
        return self._full_body['entity_type']

    @property
    def registration_time(self):
        return self._full_body['registration_time']

    @property
    def public(self):
        return self._full_body.get('public', False)

    @property
    def alias(self):
        return self._get_info().get('alias')

    @property
    def shape(self):
        return tuple(self._get_info().get('shape', ()))

    @property
    def controlled_properties(self):
        return self._get_info()['controlledProperties']

    def __repr__(self):
        return repr({
            'tdmq_id ': self.tdmq_id,
            'id ': self.id,
            'entity_category ': self.entity_category,
            'entity_type ': self.entity_type,
            'default_footprint ': self.default_footprint,
            'is_stationary ': self.is_stationary,
            'shape ': self.shape,
            'controlled_properties ': self.controlled_properties })

    def get_timeseries(self, args):
        return self.client.get_timeseries(self.tdmq_id, args)

    @abc.abstractmethod
    def timeseries(self, after, before, bucket=None, op=None):
        pass

    def get_latest_activity(self):
        s = self.client.get_latest_source_activity(self.tdmq_id)
        return self.timeseries(after=s['time'], before=None)

    @abc.abstractmethod
    def ingest(self, t, data, slot=None):
        pass

    def add_record(self, record):
        assert isinstance(record, Mapping)
        return self.add_records([record])

    def add_records(self, records):
        assert isinstance(records, Iterable)
        sid = {'source': self.id}
        # sid will override potential pre-existing values for r['source']
        to_be_shipped = [{**r, **sid} for r in records]
        self.client.add_records(to_be_shipped)


class ScalarSource(Source):

    @property
    def sensor_id(self):
        return self._get_info().get('sensor_id')

    @property
    def station_id(self):
        return self._get_info().get('station_id')

    @property
    def station_model(self):
        return self._get_info().get('station_model')

    @property
    def edge_id(self):
        return self._get_info().get('edge_id')

    def __repr__(self):
        return repr({
            'tdmq_id ':               self.tdmq_id,
            'id ':                    self.id,
            'entity_category ':       self.entity_category,
            'entity_type ':           self.entity_type,
            'default_footprint ':     self.default_footprint,
            'is_stationary ':         self.is_stationary,
            'shape ':                 self.shape,
            'controlled_properties ': self.controlled_properties,
            'edge_id':                self.edge_id,
            'station_id':             self.station_id,
            'station_model':          self.station_model,
            'sensor_id':              self.sensor_id,
            })

    def timeseries(self, after=None, before=None, bucket=None, op=None):
        return ScalarTimeSeries(self, after, before, bucket, op)

    def ingest(self, t, data, slot=None):
        if slot:
            raise TypeError("Can't specity a slot to ingest a scalar record")
        self.add_record({'time': t.strftime(self.client.TDMQ_DT_FMT),
                         'data': data})


class NonScalarSource(Source):
    def __init__(self, client, tdmq_id, desc):
        super().__init__(client, tdmq_id, desc)
        self._tiledb_array = None


    def __del__(self):
        _logger.debug("NonScalarSource destructor")
        self.close_array()


    def close_array(self):
        if self._tiledb_array:
            _logger.debug("NonScalarSource: closing array")
            try:
                self._tiledb_array.close()
            finally:
                self._tiledb_array = None


    def open_array(self, mode='r'):
        if mode not in ('r', 'w'):
            raise ValueError(f"Invalid mode {mode}")

        if not self._tiledb_array:
            _logger.debug("NonScalarSource: opening array %s with mode %s", self.tdmq_id, mode)
            self._tiledb_array = self.client.open_array(self.tdmq_id, mode)
        elif mode not in self._tiledb_array.mode:
            _logger.debug("NonScalarSource: array %s opened in incompatible mode. Reopening with mode %s", self.tdmq_id, mode)
            self.client.close_array(self._tiledb_array)
            self._tiledb_array = None
            self._tiledb_array = self.client.open_array(self.tdmq_id, mode)


    @contextmanager
    def array_context(self, mode='r'):
        self.open_array(mode)
        try:
            yield
        finally:
            self.close_array()


    def get_array(self):
        if not self._tiledb_array:
            raise RuntimeError("Array not open!")
        return self._tiledb_array


    def timeseries(self, after=None, before=None, bucket=None, op=None):
        self.open_array(mode='r')
        return NonScalarTimeSeries(self, after, before, bucket, op)


    def ingest(self, t, data, slot=None):
        self.open_array(mode='w')

        if slot is None:
            raise UnsupportedFunctionality(f'No auto-slot support yet.')
        for p in self.controlled_properties:
            if p not in data:
                raise ValueError(f'data is missing field {p}')
        self.client.save_tiledb_frame(self.get_array(), slot, data)
        self.add_record({'time': t.strftime(self.client.TDMQ_DT_FMT),
                         'data': {'tiledb_index': slot}})


    def consolidate(self, mode=None, config_dict=None, vacuum=True):
        """
        The keys "sm.consolidation.mode" and "sm.vacuum.mode" in the
        configuration (both `config_dict` and in the Context) are ignored.
        By default this function will run for all consolidation
        (and vacuum) modes.  If you only want to run a specific mode, specify
        it with the `mode` parameter.
        """
        valid_modes = ('fragments', 'fragment_meta', 'array_meta')
        # pylint: disable=protected-access
        array_name = self.client._source_data_path(self.tdmq_id)
        _logger.debug("Ensuring client is connected...")
        self.client.connect()

        # As of 2021/03/10 to work around a bug in tiledb.consolidate
        # we have to explicitly pass the config to the function (rather
        # that implicitly using the configuration held by the context).
        # https://forum.tiledb.com/t/write-confirmation-question/305/12

        # Both options below generate an independent configuration object,
        # copied/disjoint from its original source (i.e., config_dict or tiledb_ctx)
        if config_dict:
            config = tiledb.Config(config_dict)
        else:
            config = self.client.tiledb_ctx.config()

        def _specific_consolidation(mode):
            config["sm.consolidation.mode"] = mode
            _logger.info("Executing %s consolidation on array %s", mode, array_name)
            with timeit(_logger.info):
                tiledb.consolidate(array_name, config=config, ctx=self.client.tiledb_ctx)

        if mode:
            if mode not in valid_modes:
                raise ValueError(f"Invalid tiledb consolidation mode '{mode}'. "
                                 "Valid modes are {', '.join(valid_modes)}")
            _specific_consolidation(mode)
        else:
            for m in valid_modes:
                _specific_consolidation(m)

        def _specific_vacuum(mode):
            config["sm.vacuum.mode"] = mode
            _logger.info("Executing %s vacuum on array %s", mode, array_name)
            with timeit(_logger.info):
                tiledb.vacuum(array_name, config=config, ctx=self.client.tiledb_ctx)

        if vacuum:
            _logger.info("Vacuuming tiledb array %s", array_name)
            # LP: I have found (empirically) that if I try to vacuum an array while it
            # is open the vacuum call will block.
            if self._tiledb_array and self._tiledb_array.isopen:
                _logger.debug("Array is open.  Must close it before proceeding")
                self.close_array()
            if mode:
                # mode already validated before consolidation
                _specific_vacuum(mode)
            else:
                #for m in valid_modes:  -> 'array_meta' vacuum mode fails in tests
                for m in ('fragments', 'fragment_meta'):
                    _specific_vacuum(m)
