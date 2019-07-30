import os
from datetime import datetime, timedelta

import numpy as np
from clientplus import Client, ingest
from dpc import data_fetcher
# from dpc import dpc_mradar_mosaic_type
from dpc import dpc_temp_mosaic_type
from dpc import get_dpc_sensor

# FIXME
# tdmq constants
tdmq_base_url = 'http://web:8000/api/v0.0'


def main(tdmq_base_url, hdfs_url, api_version='0.0'):
    tdmq_url = os.path.join(tdmq_base_url, 'api', f'v{api_version}')
    client = Client(tdmq_url, hdfs_url)
    # --
    # if dpc_mradar_mosaic_type['name'] not in client.sensor_types:
    #     client.register_sensor_type(dpc_mradar_mosaic_type)
    # radar_sensor_name = 'dpc_meteoradar_mosaic'
    # radar = get_dpc_sensor(client, radar_sensor_name,
    #                        dpc_mradar_mosaic_type['name'],
    #                        ['VMI', 'SRI'], 300)
    # --
    if dpc_temp_mosaic_type['name'] not in client.sensor_types:
        client.register_sensor_type(dpc_temp_mosaic_type)
    temp_sensor_name = 'dpc_temperature_mosaic'
    temp = get_dpc_sensor(client, temp_sensor_name,
                          dpc_temp_mosaic_type['name'],
                          ['TEMP'], 3600)
    time_base = temp.description['timebase']
    now = datetime.now()
    ts = temp.timeseries(after=time_base, before=now)
    # FIXME ts.timebase should really return a datetime
    # FIXME also, we call way too many things timedelta....
    time_base = datetime.strptime(ts.timebase, '%Y-%m-%dT%H:%M:%SZ')
    time_delta = timedelta(seconds=temp.description['timedelta'])
    possible_tdeltas = np.arange(0, (now - time_base).total_seconds(),
                                 time_delta.total_seconds())
    # FIXME are we sure that ts.timedelta is sorted?
    idx = np.searchsorted(possible_tdeltas, ts.timedelta)
    for i in set(range(len(possible_tdeltas))) - set(idx):
        t = time_base + i * time_delta
        ingest(temp, t, data_fetcher)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--tdmq", dest='tdmq', help="tdmq web server address",
                        required=True)
    parser.add_argument("-V", dest='tdmq_version', help="tdmq version",
                        default='0.0')
    parser.add_argument("--hdfs", dest='hdfs', help="hdfs address",
                        required=True)

    args = parser.parse_args()
    main(args.tdmq, args.hdfs, args.tdmq_version)
