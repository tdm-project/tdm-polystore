
import argparse
import logging
import numpy as np
import sys

from datetime import datetime
from datetime import timedelta

from tdmq.client.client import Client
from dpc import fetch_dpc_data
from utils import load_desc, create_timebase

logger = None


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
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"],
                        default="INFO", help="Logging level")

    return parser


def setup_logging(level_name):
    global logger
    level = getattr(logging, level_name)

    logging.basicConfig(level=level)
    logger = logging.getLogger('setup_source')
    logger.info('Logging is active (level %s).', level_name)


def main(args):
    parser = build_parser()
    opts = parser.parse_args(args)
    setup_logging(opts.log_level)

    desc = load_desc(opts.source)

    now = datetime.now()
    dt = timedelta(seconds=desc['description']['acquisition_period'])

    c = Client()
    srcs = c.get_sources({'id': desc['id']})
    if len(srcs) > 0:
        assert len(srcs) == 1
        s = srcs[0]
        logger.info(f"Using source {s.tdmq_id} for {s.id}.")
    else:
        s = c.register_source(desc)
        logger.info(f"Created source {s.tdmq_id} for {s.id}.")

    ts = s.timeseries()

    # The DPC source keeps data available for only one week
    time_base = ts.time[0] \
        if len(ts) > 0 else create_timebase(now, timedelta(seconds=6 * 24 * 3600))
    # It is unlikely that the last frame will be ready, so we stop the
    # request at now - dt.
    times = np.arange(time_base, now - dt, dt)
    filled = times[np.searchsorted(times, ts.time)]\
        if len(ts) > 0 else []
    to_be_filled = set(times) - set(filled)

    for t in to_be_filled:
        t = t if isinstance(t, datetime) else t.tolist()
        data = {}
        for f in s.controlled_properties:
            data[f] = fetch_dpc_data(s, t, f)
        slot = int((t - time_base).total_seconds() // dt.total_seconds())
        logger.info(f"Ingesting data at time {t}, slot {slot}.")
        s.ingest(t, data, slot)
    logger.info(f"Done ingesting.")


if __name__ == "__main__":
    main(sys.argv[1:])
