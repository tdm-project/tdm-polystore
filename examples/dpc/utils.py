

import json
import logging
import os

from datetime import datetime


def load_desc(opt):
    if opt == 'radar':
        fname = 'dpc-meteoradar.json'
    elif opt == 'temperature':
        fname = 'dpc-temperature.json'
    else:
        assert False
    full_path = os.path.join(os.path.dirname(__file__), fname)
    logging.debug('loading source description from %s.', full_path)
    with open(full_path) as f:
        return json.load(f)


def create_timebase(when, dt_back):
    # We align on the hourly edge
    return (datetime(when.year, when.month, when.day, when.hour) - dt_back)
