from tdmq.client.timeseries import ScalarTimeSeries
from tdmq.client.timeseries import NonScalarTimeSeries
import abc


class Source(abc.ABC):
    def __init__(self, client, tdmq_id, source_entity_type, description):
        self.client = client
        self.tdmq_id = tdmq_id
        self.source_entity_type = source_entity_type
        self.description = description

    def geometry(self):
        return self.description['geometry']

    def get_timeseries(self, args):
        return self.client.get_timeseries(self.tdmq_id, args)

    def fetch_data_block(self, block_of_refs, args):
        return self.client.fetch_data_block(block_of_refs, args)

    @abc.abstractmethod
    def timeseries(self, after, before, bucket=None, op=None):
        pass

    @abc.abstractmethod
    def get_shape(self):
        "Returns the shape of the source record."
        pass


class ScalarSource(Source):
    def timeseries(self, after, before, bucket=None, op=None):
        return ScalarTimeSeries(self, after, before, bucket, op)

    def get_shape(self):
        return ()


class NonScalarSource(Source):
    def timeseries(self, after, before, bucket=None, op=None):
        return NonScalarTimeSeries(self, after, before, bucket, op)

    def get_shape(self):
        shape = self.description['shape']
        return tuple(shape)
