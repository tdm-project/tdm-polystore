
import logging
import os
import psycopg2 as psy
import psycopg2.sql as sql

from contextlib import contextmanager
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import tdmq.utils

logger = logging.getLogger(__name__)


def db_connect(conn_params=None, override_db_name=None):
    if conn_params is None:
        # Try to get the connection parameters from environment varibles
        conn_params = {
            'host': os.getenv("POSTGRES_HOST", ""),
            'port': os.getenv("POSTGRES_PORT", ""),
            'user': os.getenv("POSTGRES_USER", "tdm"),
            'password': os.getenv("POSTGRES_PASSWORD", ""),
            'dbname': os.getenv("POSTGRES_DB", "tdm")
        }
    actual_db_name = override_db_name if override_db_name else conn_params['dbname']
    con = psy.connect(
        host=conn_params.get('host'),
        port=conn_params.get('port'),
        user=conn_params['user'],
        password=conn_params['password'],
        dbname=actual_db_name)
    logger.debug("Connected to database '%s'", actual_db_name)
    return con


def create_db(conn_params, drop=False):
    logger.debug('drop_and_create_db:init')
    new_db_name = sql.Identifier(conn_params['dbname'])

    con = db_connect(conn_params, 'postgres')
    try:
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with con.cursor() as cur:
            if drop:
                cur.execute(
                    sql.SQL('DROP DATABASE IF EXISTS {}').format(new_db_name))
                cur.execute(sql.SQL('CREATE DATABASE {}').format(new_db_name))
            else:
                cur.execute(
                    'SELECT count(*) FROM pg_catalog.pg_database '
                    'WHERE datname = %s',
                    [conn_params['dbname']])
                if not cur.fetchone()[0]:
                    cur.execute(sql.SQL('CREATE DATABASE {}').format(new_db_name))
    finally:
        con.close()

    logger.debug('DB %s created.  Initializing', new_db_name.string)
    init_db(conn_params)
    logger.debug('DB %s ready for use.', new_db_name.string)


def get_schema_sql():
    SQL = """
      CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
      CREATE EXTENSION IF NOT EXISTS postgis;
      CREATE EXTENSION IF NOT EXISTS citext;

      CREATE TABLE entity_category (
          entity_category CITEXT PRIMARY KEY
      );

      CREATE TABLE entity_type (
          entity_category CITEXT REFERENCES entity_category(entity_category),
          entity_type CITEXT,
          schema JSONB,
          PRIMARY KEY (entity_category, entity_type)
      );

      CREATE TABLE source (
          tdmq_id UUID,
          external_id TEXT NOT NULL UNIQUE,
          default_footprint GEOMETRY NOT NULL,
          stationary BOOLEAN NOT NULL DEFAULT TRUE, -- source.stationary is true => record.geom is NULL
          entity_category CITEXT NOT NULL,
          entity_type CITEXT NOT NULL,
          description JSONB,
          registration_time TIMESTAMP NOT NULL DEFAULT NOW(),
          PRIMARY KEY (tdmq_id),
          FOREIGN KEY (entity_category, entity_type) REFERENCES entity_type(entity_category, entity_type)
      );

      CREATE TABLE record (
          time TIMESTAMP(6) NOT NULL,
          source_id UUID NOT NULL REFERENCES source(tdmq_id) ON DELETE CASCADE,
          footprint GEOMETRY, -- source.stationary is true => record.footprint is NULL
          data JSONB NOT NULL
      );

      -- create the hypertable on record. Rather than using the default index, we create an
      -- index on (source_id, time DESC) as suggested by TimescaleDB best practices:
      -- https://docs.timescale.com/v1.2/using-timescaledb/schema-management#indexing-best-practices
      SELECT create_hypertable('record', 'time', create_default_indexes => FALSE, if_not_exists => TRUE);
      CREATE INDEX ON record (source_id, time DESC);
      CREATE INDEX ON source USING GIST (default_footprint);

      INSERT INTO entity_category VALUES
          ('Radar'),
          ('Satellite'),
          ('Simulation'),
          ('Station');

      INSERT INTO entity_type VALUES
          ('Radar', 'MeteoRadarMosaic'),
          ('Station', 'WeatherObserver'),
          ('Station', 'PointWeatherObserver'),
          ('Station', 'TemperatureMosaic'),
          ('Station', 'EnergyConsumptionMonitor'),
          ('Station', 'TrafficObserver'),
          ('Station', 'DeviceStatusMonitor')
          ;
    """
    return SQL


def init_db(conn_params):
    """Create new tables in a fresh NEW database."""
    con = db_connect(conn_params)
    try:
        with con:  # transaction
            with con.cursor() as curs:
                curs.execute(get_schema_sql())
    finally:
        con.close()

    # alembic_stamp_new_db(conn_params)
    # alembic_run_migrations(conn_params)

    logger.debug('init_db: done')


def drop_db(conn_params):
    """Clear existing data and create new tables."""
    logger.debug('drop_db %s', conn_params)

    con = db_connect(conn_params, 'postgres')
    try:
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with con.cursor() as cur:
            cur.execute(
                sql.SQL('DROP DATABASE IF EXISTS {}').format(sql.Identifier(conn_params['dbname'])))
        logger.debug('database %s dropped', conn_params['dbname'])
    finally:
        con.close()


@contextmanager
def _alembic_context(conn_params=None):
    alembic_dir = os.path.dirname(os.path.abspath(__file__))
    logger.debug("Temporarily changing CWD to alembic directory %s", alembic_dir)
    with tdmq.utils.chdir_context(alembic_dir):
        config = _alembic_load_config(conn_params)
        yield config


def _alembic_load_config(conn_params=None):
    """
    Load the alembic configuration from the CWD.
    """
    from alembic.config import Config

    alembic_cfg = Config("alembic.ini")
    logger.debug("Alembic config loaded: %s", alembic_cfg)

    if conn_params:
        #  We can use the alembic config object to pass parameters to env.py in our alembic setup
        logger.debug("Setting DB connection parameters in alembic configuration.attributes")
        alembic_cfg.attributes['conn_params'] = conn_params

    return alembic_cfg


def alembic_stamp_new_db(conn_params=None):
    """
    Load the Alembic configuration and generate the
    version table, "stamping" it with the most recent rev:
    """
    from alembic import command

    with _alembic_context(conn_params) as alembic_cfg:
        logger.info("Stamping DB")
        command.stamp(alembic_cfg, "head")


def alembic_run_migrations(conn_params=None, target="head"):
    from alembic import command

    with _alembic_context(conn_params) as alembic_cfg:
        logger.info("Running migrations")
        command.upgrade(alembic_cfg, target)
