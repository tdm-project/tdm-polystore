import abc
import numpy as np
from datetime import datetime


class TimeSeries(abc.ABC):

    def _pre_fetch(self):
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
            raise NotImplementedError("Mobile data sources aren't implemented in the Client")

        args = {'after': self.after, 'before': self.before,
                'bucket': self.bucket, 'op': self.op}

        res = self.source.get_timeseries(args)
        self.time = np.array([datetime.fromtimestamp(v) for v in res['coords']['time']])

        return res['data']

    @abc.abstractmethod
    def fetch(self):
        pass

    @abc.abstractmethod
    def get_item(self, args):
        """
        Index into timeseries.  Accepts indexes and slices compatible with numpy arrays.

        Returns a tuple with two elements:
            1. np.ndarray of timestamps
            2. OrderedDict mapping property names to  np.ndarrays containing the actual data.
        """
        pass

    def __len__(self):
        return len(self.time)

    def __getitem__(self, indx):
        return self.get_item(np.index_exp[indx])

    def __init__(self, source, after, before, bucket, op):
        self.source = source
        self.after = after
        self.before = before
        self.bucket = bucket
        self.op = op
        self.time = []
        self.fetch()

    def get_shape(self):
        return (len(self.time),) + self.source.shape


class ScalarTimeSeries(TimeSeries):

    """
    To the attributes defined by TimeSeries, this class adds self.series
    which contains a dict mapping property names to np_arrays of scalar data.
    """

    def fetch(self):
        data = self._pre_fetch()
        # convert the arrays returned by _pre_fetch into numpy arrays
        self.series = dict((fname, np.array(data[fname])) for fname in data)

    def get_item(self, args):
        assert len(args) == 1
        return (self.time[args], dict((propname, self.series[propname][args]) for propname in self.series))


class NonScalarTimeSeries(TimeSeries):
    def __init__(self, source, after, before, bucket, op):
        if bucket:
            raise NotImplementedError("Bucketing is not yet implemented in the client for non-scalar timeseries")
        super(NonScalarTimeSeries, self).__init__(source, after, before, bucket, op)

    def fetch(self):
        raw_data = self._pre_fetch()
        self.tiledb_indices = raw_data['tiledb_index']

    def fetch_data_block(self, args):
        if self.bucket is None:
            return self.source.client.fetch_non_scalar_slice(self.source.tdmq_id, self.tiledb_indices, args)
        else:
            raise ValueError('bucket not supported')

    def get_item(self, args):
        assert len(args) > 0
        time = self.time[args[0]]
        if isinstance(args[0], slice) and len(time) == 0:
            return (time, np.array([], dtype=np.int32))
        else:
            return (time, self.fetch_data_block(args))
