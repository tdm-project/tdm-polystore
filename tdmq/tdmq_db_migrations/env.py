from __future__ import with_statement

from alembic import context
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from logging.config import fileConfig

import tdmq.db_manager as db_manager

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = None

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def connect_to_db():
    if 'conn_params' in config.attributes:
        return db_manager.db_connect(config.attributes['conn_params'])
    else:
        # if db_connect parameters aren't provided, it'll try to get them
        # from the environment
        return db_manager.db_connect(None)


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    engine = create_engine('postgresql://', creator=connect_to_db, poolclass=NullPool)

    connection = engine.connect()
    try:
        context.configure(
            connection=connection, target_metadata=target_metadata, literal_binds=True)

        with context.begin_transaction():
            context.run_migrations()
    finally:
        connection.close()
        engine.dispose()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    engine = create_engine('postgresql://', creator=connect_to_db, poolclass=NullPool)

    connection = engine.connect()
    try:
        context.configure(
            connection=connection,
            target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()
    finally:
        connection.close()
        engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
