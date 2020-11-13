import logging
from datetime import datetime, timedelta
import dateparser
import numpy as np
from clize import parameters, run
from dpc import fetch_dpc_data
from utils import create_timebase, load_desc

from tdmq.client import Client

logger = None


def setup_logging(level_name):
    global logger
    level = getattr(logging, level_name)

    logging.basicConfig(level=level)
    logger = logging.getLogger('setup_source')
    logger.info('Logging is active (level %s).', level_name)


def main(source: parameters.one_of('radar', 'temperature'),
         *,
         tdmq_url: ('u'),
         start='6 days ago',
         end='now',
         log_level: ('v',
                     parameters.one_of("DEBUG", "INFO", "WARN", "ERROR",
                                       "CRITICAL")) = 'INFO'):
    setup_logging(log_level)

    desc = load_desc(source)

    dt = timedelta(seconds=desc['description']['acquisition_period'])

    c = Client(tdmq_url)
    srcs = c.find_sources({'id': desc['id']})
    if len(srcs) > 0:
        assert len(srcs) == 1
        s = srcs[0]
        logger.info(f"Using source {s.tdmq_id} for {s.id}.")
    else:
        s = c.register_source(desc)
        logger.info(f"Created source {s.tdmq_id} for {s.id}.")

    start = dateparser.parse(start)
    end = dateparser.parse(end)
    try:
        ts = s.timeseries(after=start, before=end)
    except Exception as ex:  # FIXME too general
        ts = []
    # The DPC source keeps data available for only one week
    time_base = ts.time[0] if len(ts) > 0 else create_timebase(
        end, end - start)
    times = np.arange(time_base, end, dt)
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
        try:
            s.ingest(t, data, slot)
        except Exception as e:
            logger.error(
                'an error occurred when ingesting time %s at slot %s. Exception: %s',
                t, slot, e)
    logger.info(f"Done ingesting.")


if __name__ == "__main__":
    run(main)
