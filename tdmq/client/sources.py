from tdmq.client.timeseries import ScalarTimeSeries
from tdmq.client.timeseries import NonScalarTimeSeries
from tdmq.errors import UnsupportedFunctionality
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
    def timeseries(self, after=None, before=None, bucket=None, op=None):
        return NonScalarTimeSeries(self, after, before, bucket, op)

    def ingest(self, t, data, slot=None):
        if slot is None:
            raise UnsupportedFunctionality(f'No auto-slot support yet.')
        for p in self.controlled_properties:
            if p not in data:
                raise ValueError(f'data is missing field {p}')
        self.client.save_tiledb_frame(self.tdmq_id, slot, data)
        self.add_record({'time': t.strftime(self.client.TDMQ_DT_FMT),
                         'data': {'tiledb_index': slot}})
