import json
from datetime import datetime

import logging
import os

logger = logging.getLogger(__name__)


def load_desc(opt):
    if opt == 'radar':
        fname = 'dpc-meteoradar.json'
    elif opt == 'temperature':
        fname = 'dpc-temperature.json'
    else:
        assert False
    full_path = os.path.join(os.path.dirname(__file__), fname)
    logger.debug('loading source description from %s.', full_path)
    with open(full_path) as f:
        return json.load(f)


def create_timebase(when, dt_back):
    # We allign on the hourly edge
    return (datetime(when.year, when.month, when.day, when.hour) - dt_back)
