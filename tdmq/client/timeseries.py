import abc
import numpy as np
from datetime import datetime


class TimeSeries(abc.ABC):

    def pre_fetch(self):
        args = {'after': self.after, 'before': self.before,
                'bucket': self.bucket, 'op': self.op}
        res = self.source.get_timeseries(args)
        self.time = np.array([datetime.fromtimestamp(v)
                              for v in res['coords']['time']])
        # FIXME manage footprint depending on self.source.is_static
        return res['data']

    @abc.abstractmethod
    def fetch(self):
        pass

    @abc.abstractmethod
    def get_item(self, args):
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
    def fetch(self):
        data = self.pre_fetch()
        self.data = dict((fname, np.array(data[fname]))
                         for fname in data)

    def get_item(self, args):
        assert len(args) == 1
        return (self.time[args],
                dict((f, self.data[f][args]) for f in self.data))


class NonScalarTimeSeries(TimeSeries):
    def fetch(self):
        self.data = self.pre_fetch()

    def fetch_data_block(self, args):
        if self.bucket is None:
            return self.source.fetch_data_block(self.data, args)
        else:
            raise ValueError(f'bucket not supported')

    def get_item(self, args):
        assert len(args) > 0
        time = self.time[args[0]]
        if isinstance(args[0], slice) and len(time) == 0:
            return (time, np.array([], dtype=np.int32))
        else:
            return (time, self.fetch_data_block(args))
