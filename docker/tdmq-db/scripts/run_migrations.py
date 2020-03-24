#!/usr/bin/env python3

import logging
import os
import sys

import tdmq.db_manager as db_manager


def main(args=None):
    if args:
        if len(args) == 1:
            target = args[0]
        else:
            raise RuntimeError("Usage: {} [ target_version ]".format(os.path.basename(__file__)))
    else:
        target = "head"

    logging.info("Running migrations to target version %s", target)
    db_manager.alembic_run_migrations(target=target)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main(sys.argv[1:])
