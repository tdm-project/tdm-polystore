from tdmq.client.timeseries import ScalarTimeSeries
from tdmq.client.timeseries import NonScalarTimeSeries
import abc


class Sensor(abc.ABC):
    def __init__(self, client, code, sensor_type_desc, description):
        self.client = client
        self.code = code
        self.sensor_type = sensor_type_desc
        self.description = description

    def geometry(self):
        return self.description['geometry']

    def get_timeseries(self, args):
        return self.client.get_timeseries(self.code, args)

    def fetch_data_block(self, block_of_refs, args):
        return self.client.fetch_data_block(block_of_refs, args)

    @abc.abstractmethod
    def timeseries(self, after, before, bucket=None, op=None):
        pass

    @abc.abstractmethod
    def get_shape(self):
        "Returns the shape of the sensor measure."
        pass

class ScalarSensor(Sensor):
    def timeseries(self, after, before, bucket=None, op=None):
        return ScalarTimeSeries(self, after, before, bucket, op)

    def get_shape(self):
        return ()

class NonScalarSensor(Sensor):
    def timeseries(self, after, before, bucket=None, op=None):
        return NonScalarTimeSeries(self, after, before, bucket, op)

    def get_shape(self):
        shape = self.description['shape']
        return tuple(shape)

