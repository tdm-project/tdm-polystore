import abc
import collections.abc
import warnings
import numpy as np


class TimeSeries(abc.ABC):

    def __init__(self, source, after, before, bucket, op, properties=None):
        self.source = source
        self.after = after
        self.before = before
        self.bucket = bucket
        self.op = op
        self.time = []
        self.fetch(properties)

    def _pre_fetch(self, properties=None):
        """
        Fetch timeseries from tdmq web service and set the self.time array; returns tdmq
        timeseries data.

        The web service only returns data from timescaleDB.  Therefore, data
        for vector timeseries, stored in TileDB, is not fetched here.

        In this class, only series coordinates and some metatadata is handled.  The
        actual data series are returned so that they can be processed by specialized
        subclasses.

        :returns: dict mapping:  controlledProperty -> list
        """
        if not self.source.is_stationary:
            warnings.warn("Mobile data sources aren't implemented in the Client")

        args = {'after': self.after, 'before': self.before,
                'bucket': self.bucket, 'op': self.op}

        if properties:
            args['fields'] = ','.join(properties)

        res = self.source.get_timeseries(args)
        # pylint: disable=protected-access
        self.time = np.array([self.source.client._parse_timestamp(v) for v in res['coords']['time']])

        return res['data']

    @abc.abstractmethod
    def fetch(self, properties=None):
        pass

    @abc.abstractmethod
    def get_item(self, args):
        """
        Index into timeseries.  Accepts indexes and slices compatible with numpy arrays.

        Returns a tuple with two elements:
            1. np.ndarray of timestamps
            2. OrderedDict mapping property names to  np.ndarrays containing the actual data.
        """

    def __len__(self):
        return len(self.time)

    def __getitem__(self, indx):
        return self.get_item(np.index_exp[indx])

    def get_shape(self):
        return (len(self.time),) + self.source.shape


class NoneArray(collections.abc.Sequence):
    def __init__(self, length):
        if length < 0:
            raise ValueError("length < 0")
        self._length = length

    def __getitem__(self, s):
        is_tuple = False
        if isinstance(s, tuple):
            is_tuple = True
            if len(s) != 1:
                raise IndexError(f"Wrong number of indices for this array. Expected 1; got {len(s)}")
            s = s[0]
            # Continue into next `if`

        if isinstance(s, slice):
            start, stop, step = s.indices(self._length)
            if step == 0:
                raise ValueError("slice step cannot be zero")
            if start >= stop:
                return []
            selected_length = 1 + (stop - start - 1) // step
            return np.array([None] * selected_length)

        if isinstance(s, int):
            if -self._length <= s < self._length:
                return None
            raise IndexError(f"index {s} out of range")

        # else:
        error_msg = "list indices must be integers, slices or tuples of integers or slices "
        if is_tuple:
            error_msg += f"(index ({s},) is a tuple of {type(s)})"
        else:
            error_msg += f"(index {s} is a {type(s)})"
        raise TypeError(error_msg)

    def __len__(self):
        return self._length

    def __contains__(self, item):
        return self._length > 0 and item is None

    def __iter__(self):
        for _ in range(self._length):
            yield None

    def __reversed__(self):
        yield from self.__iter__()

    def index(self, value, start=0, stop=9223372036854775807):
        start, stop, _ = slice(start, stop).indices(self._length)
        if start < stop and value is None:
            return 0
        raise ValueError(f"{value} is not in list")

    def count(self, value):
        return self._length if value is None else 0


class ScalarTimeSeries(TimeSeries):

    """
    To the attributes defined by TimeSeries, this class adds self.series
    which contains a dict mapping property names to np_arrays of scalar data.
    """

    def __init__(self, source, after, before, bucket, op, properties=None):
        self.series = None
        super().__init__(source, after, before, bucket, op, properties)

    def fetch(self, properties=None):
        data = self._pre_fetch(properties)
        # convert the arrays returned by _pre_fetch into numpy arrays
        self.series = dict()
        for fname in data:
            if data[fname] is not None:
                self.series[fname] = np.array(data[fname])
            else:
                self.series[fname] = NoneArray(len(self))

    def get_item(self, args):
        assert len(args) == 1
        return (self.time[args], dict((propname, self.series[propname][args]) for propname in self.series))


class NonScalarTimeSeries(TimeSeries):
    def __init__(self, source, after, before, bucket, op):
        self.tiledb_indices = None
        if bucket:
            raise NotImplementedError("Bucketing is not yet implemented in the client for non-scalar timeseries")
        super().__init__(source, after, before, bucket, op)

    def fetch(self, properties=None):
        # NonScalarTimeSeries ignores any properties specified.  It only considers tiledb_index
        raw_data = self._pre_fetch()
        self.tiledb_indices = raw_data['tiledb_index']

    def fetch_data_block(self, args):
        if self.bucket is None:
            return self.source.client.fetch_non_scalar_slice(self.source.get_array(), self.tiledb_indices, args)
        raise ValueError('bucket not supported')

    def get_item(self, args):
        assert len(args) > 0
        time = self.time[args[0]]
        if isinstance(args[0], slice) and len(time) == 0:
            return (time, np.array([], dtype=np.int32))
        return (time, self.fetch_data_block(args))
