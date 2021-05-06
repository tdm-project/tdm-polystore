
import abc
import itertools
import logging
from collections.abc import Sequence
from contextlib import contextmanager

import numpy as np
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
    def timeseries(self, after, before, bucket=None, op=None, properties=None):
        pass

    def get_latest_activity(self):
        """
        Get Timeseries starting at latest registered record's timestamp.
        """
        s = self.client.get_latest_source_activity(self.tdmq_id)
        return self.timeseries(after=s['time'], before=None)

    ### Ingestion ###
    ### Requires authentication
    ###
    @abc.abstractmethod
    def ingest_one(self, t, data, slot=None, footprint=None):
        """
        :param t: datetime object.  Assumed to be in UTC time.
        """

    def ingest(self, t, data, slot=None):
        """
        :param t: datetime object.  Assumed to be in UTC time.
        """
        self.ingest_one(t, data, slot)

    @abc.abstractmethod
    def ingest_many(self, times, data, initial_slot=None, footprint_iter=None):
        """
        :param times: Sequence of datetime objects.  Assumed to be in UTC time.
        """


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

    def timeseries(self, after=None, before=None, bucket=None, op=None, properties=None):
        return ScalarTimeSeries(self, after, before, bucket, op, properties)

    def _format_record(self, t, d, foot=None):
        record = {
            # pylint: disable=protected-access
            'time': self.client._format_timestamp(t),
            'data': d,
            'tdmq_id': self.tdmq_id }
        if foot:
            record['footprint'] = foot
        return record

    def ingest_one(self, t, data, slot=None, footprint=None):
        if footprint is None:
            footprint_it = None
        else:
            footprint_it = [footprint]
        self.ingest_many([t], [data], slot, footprint_it)

    def ingest_many(self, times, data, initial_slot=None, footprint_iter=None):
        if initial_slot:
            raise TypeError("Can't specity a slot to ingest a scalar record")
        if footprint_iter is None:
            records = [ self._format_record(t, d) for t, d in zip(times, data) ]
        else:
            records = [ self._format_record(t, d, f) for t, d, f in zip(times, data, footprint_iter) ]
        self.client.add_records(records)


class NonScalarSource(Source):
    def __init__(self, client, tdmq_id, desc):
        super().__init__(client, tdmq_id, desc)
        self._tiledb_array = None

    @property
    def comments(self):
        return self._get_info().get('comments')

    @property
    def reference(self):
        return self._get_info().get('reference')

    @property
    def brand_name(self):
        return self._get_info().get('brand_name')

    @property
    def model_name(self):
        return self._get_info().get('model_name')

    @property
    def operated_by(self):
        return self._get_info().get('operated_by')


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
        return self._tiledb_array


    @contextmanager
    def array_context(self, mode='r'):
        ary = self.open_array(mode)
        try:
            yield ary
        finally:
            self.close_array()


    def get_array(self):
        if not self._tiledb_array:
            raise RuntimeError("Array not open!")
        return self._tiledb_array


    def timeseries(self, after=None, before=None, bucket=None, op=None, properties=None):
        self.open_array(mode='r')
        return NonScalarTimeSeries(self, after, before, bucket, op)


    def _format_record(self, t, slot, foot=None):
        record = {
            'time': t.strftime(self.client.TDMQ_DT_FMT),
            'data': { 'tiledb_index': slot },
            'tdmq_id': self.tdmq_id }
        if foot:
            record['footprint'] = foot
        return record

    def _get_next_slot(self):
        _logger.debug("NonScalarSource._get_next_slot: getting latest activity")
        s = self.client.get_latest_source_activity(self.tdmq_id)
        if s['time'] is None:
            _logger.debug("No activity found. Starting from beginning")
            return 0 # first slot
        if 'tiledb_index' not in s['data']:
            raise RuntimeError(f"Activity record collected for {self.tdmq_id} does not contain `tiledb_index`!")
        most_recent_slot = s['data']['tiledb_index']
        _logger.debug("Found most recent slot %s. Returning %s + 1", most_recent_slot, most_recent_slot)
        return most_recent_slot + 1

    def ingest_one(self, t, data, slot=None, footprint=None):
        """
        auto-slot -> if you don't specify a slot, the client will retrieve the latest slot
        used for this source (as in most recent timestamp) and increment it by 1.

        Don't use this feature unless you consistently append to the time series.  I.e.,
        if the most recent record points to somewhere in the middle of the array, you'll
        end up overwriting data in the tiledb array.
        """
        ary = self.open_array(mode='w')

        if slot is None:
            slot = self._get_next_slot()
            _logger.debug("source %s: auto-slot: %s", self.tdmq_id, slot)

        for p in self.controlled_properties:
            if p not in data:
                raise ValueError(f'data is missing field {p}')
        record = self._format_record(t, slot, footprint)
        _logger.debug("Array %s: setting one slice in slot %s", self.tdmq_id, slot)
        with timeit(_logger.debug):
            ary[slot:slot + 1] = data
        _logger.debug("Registering record with tdmq")
        self.client.add_records([record])


    def ingest_many(self, times, data, initial_slot=None, footprint_iter=None):
        if initial_slot is None:
            initial_slot = self._get_next_slot()
            _logger.debug("source %s: auto-slot: %s", self.tdmq_id, initial_slot)

        for p in self.controlled_properties:
            if p not in data:
                raise ValueError(f'data is missing property {p}')

        if footprint_iter is None:
            footprint_iter = itertools.cycle([None])
        records = [
            self._format_record(t, s, f)
            for t, s, f in zip(times, itertools.count(initial_slot), footprint_iter) ]

        ary = self.open_array(mode='w')
        # Prepare the data.  The `data` argument must always be a mapping
        # property -> values to be ingested.
        # The values can be:
        # a) a sequence a np arrays, each one a slice for a single time slot;
        # b) a single np array with the same ndim as our array, where the
        #    first axis is the time dimension.
        struct = dict()
        for prop, value in data.items():
            if isinstance(value, Sequence):
                # The value should be a time sequence of np arrays, which we will stack
                new_data = np.stack(value)
            elif isinstance(value, np.ndarray):
                new_data = value

            if new_data.ndim != ary.ndim:
                raise ValueError(f"Array for property {prop} has {value.ndim} dimensions, while "
                                 "{ary.ndim} are expected")
            if new_data.shape[1:] != ary.shape[1:]:
                raise ValueError(f"Array for property {prop} has shape {value.shape} which "
                                 "is incompatible with {ary.shape}")
            # At this point, the array should be ok for assignment.
            struct[prop] = new_data

        _logger.debug("Array %s: setting %s slices in slots starting from %s",
                      self.tdmq_id, new_data.shape[0], initial_slot)
        with timeit(_logger.debug):
            ary[initial_slot:(initial_slot + new_data.shape[0])] = struct
        _logger.debug("Registering %s records with tdmq", len(records))
        self.client.add_records(records)


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
