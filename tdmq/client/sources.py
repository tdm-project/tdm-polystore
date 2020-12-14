
import abc
import logging
from collections.abc import Iterable
from collections.abc import Mapping
from contextlib import contextmanager

from tdmq.client.timeseries import ScalarTimeSeries
from tdmq.client.timeseries import NonScalarTimeSeries
from tdmq.errors import UnsupportedFunctionality

_logger = logging.getLogger(__name__)


class Source(abc.ABC):
    def __init__(self, client, tdmq_id, desc):
        self.client = client
        self.tdmq_id = tdmq_id
        self.id = desc['external_id']
        self.entity_category = desc['entity_category']
        self.entity_type = desc['entity_type']
        self.is_stationary = desc['stationary']
        self.default_footprint = desc['default_footprint']
        self.description = desc['description']
        self.shape = tuple(self.description.get('shape', ()))
        self.controlled_properties = self.description['controlledProperties']

    def get_timeseries(self, args):
        return self.client.get_timeseries(self.tdmq_id, args)

    @abc.abstractmethod
    def timeseries(self, after, before, bucket=None, op=None):
        pass

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
