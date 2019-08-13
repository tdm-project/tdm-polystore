from tdmq.client.client import Client
from dpc import fetch_dpc_data

from datetime import datetime
from datetime import timedelta
import numpy as np
import argparse
import json
import sys

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info('Logging is active.')

from utils import load_desc, create_timebase


def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", dest='source',
                        choices=['radar', 'temperature'],
                        help="dpc data source", required=True)
    parser.add_argument("--tdmq", dest='tdmq', help="tdmq web server address",
                        required=False)
    parser.add_argument("-V", dest='tdmq_version', help="tdmq version",
                        default='0.0', required=False)
    parser.add_argument("--hdfs", dest='hdfs', help="hdfs address",
                        required=False)
    return parser


def main(args):
    parser = build_parser()
    opts = parser.parse_args(args)
    desc = load_desc(opts.source)

    now = datetime.now()
    dt = timedelta(seconds=desc['description']['acquisition_period'])

    c = Client()
    srcs = c.get_sources({'id': desc['id']})
    assert len(srcs) > 0
    s = srcs[0]
    logger.info(f"Using source {s.tdmq_id} for {s.id}.")
    # DPC keeps data only for a week, let's check if there are holes
    # in the timeseries that can be filled. We try to find at least one
    # old datapoint that we will use as temporal reference.
    window_start = now - timedelta(days=7)
    ts = s.timeseries(after=window_start)
    if len(ts) == 0:
        # In principle, we could load the whole timeseries, but this
        # is best handled manually...
        logger.error(f'No data acquired since {window_start}, aborting.')
        sys.exit(1)
    window_start = ts.time[0]
    # FIXME this is a dirty trick
    start_slot = ts.data['tiledb_index'][0]
    times = np.arange(window_start, now - dt, dt)
    filled = times[np.searchsorted(times, ts.time)]
    to_be_filled = set(times) - set(filled)
    for t in to_be_filled:
        t = t if isinstance(t, datetime) else t.tolist()
        data = {}
        for f in s.controlled_properties:
            data[f] = fetch_dpc_data(s, t, f)
        slot = start_slot + int((t - window_start).total_seconds() //
                                dt.total_seconds())
        logger.info(f"Ingesting data at time {t}, slot {slot}.")
        s.ingest(t, data, slot)
    logger.info(f"Done ingesting.")


if __name__ == "__main__":
    main(sys.argv[1:])
