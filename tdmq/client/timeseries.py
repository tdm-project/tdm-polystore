import abc
import numpy as np


class TimeSeries(abc.ABC):

    def pre_fetch(self):
        args = {'after': self.after, 'before': self.before,
                'bucket': self.bucket, 'op': self.op}
        res = self.source.get_timeseries(args)
        self.times = np.array(res['times'], dtype=np.float64)
        return res['data']

    @abc.abstractmethod
    def fetch(self):
        pass

    @abc.abstractmethod
    def get_item(self, args):
        pass

    def __getitem__(self, indx):
        return self.get_item(np.index_exp[indx])

    def __init__(self, source, after, before, bucket, op):
        self.source = source
        self.after = after
        self.before = before
        self.bucket = bucket
        self.op = op
        self.fetch()

    def get_shape(self):
        return (len(self.data),) + self.source.get_shape()


class ScalarTimeSeries(TimeSeries):
    def fetch(self):
        data = self.pre_fetch()
        self.data = dict((fname, np.array(data[fname]))
                         for fname in data)

    def get_item(self, args):
        assert len(args) == 1
        return (self.times[args],
                dict((f, self.data[f][args]) for f in self.data))


class NonScalarTimeSeries(TimeSeries):
    def fetch(self):
        self.data = self.pre_fetch()

    def fetch_data_block(self, args):
        if self.bucket is None:
            block_of_refs = self.data[args[0]]
            block_of_refs = block_of_refs \
                if isinstance(args[0], slice) else [block_of_refs]
            return self.source.fetch_data_block(
                block_of_refs, args)
        else:
            raise ValueError(f'bucket not supported')

    def get_item(self, args):
        assert len(args) > 0
        times = self.times[args[0]]
        if isinstance(args[0], slice) and len(times) == 0:
            return (times, np.array([], dtype=np.int32))
        else:
            return (times, self.fetch_data_block(args))
