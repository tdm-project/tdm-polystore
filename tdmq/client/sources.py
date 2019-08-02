from tdmq.client.timeseries import ScalarTimeSeries
from tdmq.client.timeseries import NonScalarTimeSeries
import abc
from collections.abc import Iterable
from collections.abc import Mapping


class Source(abc.ABC):
    def __init__(self, client, tdmq_id, desc):
        self.client = client
        self.tdmq_id = tdmq_id
        self.id = desc['external_id']
        self.entity_category = desc['entity_category']
        self.entity_type = desc['entity_type']
        self.is_stationary = desc['stationary']
        self.default_footprint = desc['default_footprint']
        self.description = desc

    def default_footprint(self):
        return self.description['default_footprint']

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

    def get_shape(self):
        return ()


class NonScalarSource(Source):
    def timeseries(self, after, before, bucket=None, op=None):
        return NonScalarTimeSeries(self, after, before, bucket, op)

    def get_shape(self):
        shape = self.description['shape']
        return tuple(shape)
