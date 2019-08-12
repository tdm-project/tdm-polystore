from tdmq.client.client import Client
from here import fetch_here_data
from here import get_description_of_src
import argparse
import sys

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.info('Logging is active.')


def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", dest='source',
                        choices=['flow', 'incidents'],
                        help="here data source", required=True)
    parser.add_argument("--tdmq", dest='tdmq', help="tdmq web server address",
                        required=False)
    parser.add_argument("-V", dest='tdmq_version', help="tdmq version",
                        default='0.0', required=False)
    parser.add_argument("--app-id", dest='app_id', help="here app_id",
                        required=True)
    parser.add_argument("--app-code", dest='app_code', help="here app_code",
                        required=True)
    return parser


def main(args):
    parser = build_parser()
    opts = parser.parse_args(args)

    c = Client()
    logger.info("pre-loading existing srcs.")
    srcs = dict((s.id, s)
                for s in c.get_sources({'entity_category': 'Station',
                                        'entity_type': 'TrafficObserver'}))
    logger.info(f"loaded {len(srcs)}.")
    data = fetch_here_data(opts.app_id, opts.app_code, opts.source)
    logger.info(f"Started ingesting data.")
    for d in data:
        did = d['id']
        if did not in srcs:
            logger.info(f"created new src {did}.")
            s = c.register_source(get_description_of_src(d))
        else:
            s = srcs[did]
        s.ingest(d['pbt'], d['CF'])
    logger.info(f"Done ingesting.")


if __name__ == "__main__":
    main(sys.argv[1:])
