import abc
import numpy as np


class TimeSeries(abc.ABC):

    def pre_fetch(self):
        args = {'after': self.after, 'before': self.before,
                'bucket': self.bucket, 'op': self.op}
        res = self.sensor.get_timeseries(args)
        self.timebase = res['timebase']
        self.timedelta = np.array(res['timedelta'])
        return res['data']

    @abc.abstractmethod
    def fetch(self):
        pass

    @abc.abstractmethod
    def get_item(self, args):
        pass

    def __getitem__(self, indx):
        return self.get_item(np.index_exp[indx])

    def __init__(self, sensor, after, before, bucket, op):
        self.sensor = sensor
        self.after = after
        self.before = before
        self.bucket = bucket
        self.op = op
        self.fetch()

    def get_shape(self):
        return (len(self.data),) + self.sensor.get_shape()

class ScalarTimeSeries(TimeSeries):
    def fetch(self):
        data = self.pre_fetch()
        # FIXME multi channel sensors would be supported using
        # something like
        # self.data = dict((fname,
        #                   np.array(res['data'][fname]
        #                   for fname in res['data'])
        self.data = np.array(data)

    def get_item(self, args):
        assert len(args) == 1
        return (self.timedelta[args], self.data[args])


class NonScalarTimeSeries(TimeSeries):
    def fetch(self):
        self.data = self.pre_fetch()

    def fetch_data_block(self, args):
        if self.bucket is None:
            block_of_refs = self.data[args[0]]
            block_of_refs = block_of_refs \
                if isinstance(args[0], slice) else [block_of_refs]
            return self.sensor.fetch_data_block(
                block_of_refs, args)
        else:
            raise ValueError(f'bucket not supported')

    def get_item(self, args):
        assert len(args) > 0
        timedeltas = self.timedelta[args[0]]
        if isinstance(args[0], slice) and len(timedeltas) == 0:
            return (timedeltas, np.array([], dtype=np.int32))
        else:
            return (timedeltas, self.fetch_data_block(args))
