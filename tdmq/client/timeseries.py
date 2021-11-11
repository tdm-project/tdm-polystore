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
        self.time = None
        self.properties = properties
        self._fetch()

    def _fetch_ts_and_set_time(self, sparse: bool = None):
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

        if self.properties:
            args['fields'] = ','.join(self.properties)

        res = self.source.get_timeseries(args, sparse)
        # pylint: disable=protected-access
        assert res['fields'][0] == 'time'
        if res['sparse']:
            timestamps = (row['time'] for row in res['items'])
        else:
            timestamps = (row[0] for row in res['items'])

        self.time = np.array([self.source.client._parse_timestamp(t) for t in timestamps])

        return res

    @abc.abstractmethod
    def _fetch(self):
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

    def _fetch(self):
        api_response = self._fetch_ts_and_set_time()
        if api_response['sparse']:
            self._parse_sparse_response(api_response)
        else:
            self._parse_dense_response(api_response)

    def _parse_sparse_response(self, api_response):
        assert api_response['sparse']
        # Sparse representation is a list of dictionaries.  In each dictionary,
        # the fields are they keys.  We skip the first two fields
        # (time and footprint) which are handled elsewhere.
        self.series = dict()
        for f in api_response['fields'][2:]:
            # extract the value for the field. If the value was None on the
            # server side, it was not sent -- so we cannot assume that the key
            # will be in the dict.
            field_data = [row.get(f) for row in api_response['items']]
            # If all values are None, replace the array with a NoneArray;
            # else we store the data in a numpy array.
            if all(x is None for x in field_data):
                self.series[f] = NoneArray(len(self))
            else:
                self.series[f] = np.array(field_data)

    def _parse_dense_response(self, api_response):
        assert not api_response['sparse']
        # convert the arrays returned by _fetch_ts_and_set_time into numpy arrays
        if len(api_response['items']) > 0:
            transpose = list(zip(*api_response['items']))
        else:
            transpose = [[]] * len(api_response['fields'])

        self.series = dict()
        # iterate over fields, except for 'time' and 'footprint' (the first two)
        for idx in range(2, len(api_response['fields'])):
            field_name = api_response['fields'][idx]
            if all(x is None for x in transpose[idx]):
                self.series[field_name] = NoneArray(len(transpose[idx))
            else:
                self.series[field_name] = np.array(transpose[idx])

    def get_item(self, args):
        assert len(args) == 1
        return (self.time[args], dict((propname, self.series[propname][args]) for propname in self.series))


class NonScalarTimeSeries(TimeSeries):
    def __init__(self, source, after, before, bucket, op):
        self.tiledb_indices = None
        if bucket:
            raise NotImplementedError("Bucketing is not yet implemented in the client for non-scalar timeseries")
        super().__init__(source, after, before, bucket, op)

    def _fetch(self):
        # NonScalarTimeSeries ignores any properties specified.  It only considers tiledb_index
        api_response = self._fetch_ts_and_set_time(sparse=False)
        assert api_response['fields'][2] == 'tiledb_index'
        self.tiledb_indices = [row[2] for row in api_response['items']]

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
