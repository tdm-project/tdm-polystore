#!/usr/bin/env python3

import argparse
import logging
import sys

from tdmq.client.client import Client
from here import fetch_here_data
from here import get_description_of_src

logger = logging.getLogger(__name__)


def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", dest='source',
                        choices=['flow', 'incidents'],
                        help="here data source", required=True)
    parser.add_argument("--tdmq", dest='tdmq', help="tdmq web server address",
                        required=False)
    parser.add_argument("--tdmq-token", dest='tdmq_token', help="Authorization token for write access to service",
                        required=True)
    parser.add_argument("--app-id", dest='app_id', help="here app_id",
                        required=True)
    parser.add_argument("--app-code", dest='app_code', help="here app_code",
                        required=True)
    return parser


def main(args):
    parser = build_parser()
    opts = parser.parse_args(args)

    if opts.tdmq:
        c = Client(opts.tdmq, opts.tdmq_token)
    else:
        c = Client(auth_token=opts.tdmq_token)

    logger.info("pre-loading existing srcs.")
    srcs = dict((s.id, s)
                for s in c.find_sources({'entity_category': 'Station',
                                         'entity_type': 'TrafficObserver'}))
    logger.info("loaded %s sources", len(srcs))
    data = fetch_here_data(opts.app_id, opts.app_code, opts.source)
    logger.info("Started ingesting data.")
    for d in data:
        did = d['id']
        if did not in srcs:
            logger.info("created new src %s.", did)
            s = c.register_source(get_description_of_src(d))
            srcs[did] = s
        else:
            s = srcs[did]
        s.ingest(d['pbt'], d['CF'])
    logger.info("Done ingesting.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logger.setLevel(logging.INFO)
    logger.info('Logging is active.')
    main(sys.argv[1:])
