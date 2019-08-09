import json
from datetime import datetime

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def load_desc(opt):
    if opt == 'radar':
        fname = 'dpc-meteoradar.json'
    elif opt == 'temperature':
        fname = 'dpc-temperature.json'
    else:
        assert False
    logger.debug(f'loading source description from {fname}.')
    with open(fname) as f:
        return json.load(f)


def create_timebase(when, dt_back):
    # We allign on the hourly edge
    return (datetime(when.year, when.month, when.day, when.hour) - dt_back)
