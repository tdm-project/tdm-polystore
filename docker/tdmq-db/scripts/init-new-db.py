#!/usr/bin/env python3

import logging

import tdmq.db_manager as db_manager


def main():
    logging.info("Initializing new database")
    db_manager.init_db(None)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
