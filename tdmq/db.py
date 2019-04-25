import click
from flask import current_app, g
from flask.cli import with_appcontext
from flask.cli import AppGroup
# import psycopg2 as psy
from datetime import datetime, timedelta


def get_db():
    """Connect to the application's configured database. The connection
    is unique for each request and will be reused if this is called
    again.
    """
    if 'db' not in g:
        cstr = "dbname={} host={} user={} password={}".format(
            current_app.config['DB_NAME'],
            current_app.config['DB_HOST'],
            current_app.config['DB_USER'],
            current_app.config['DB_PASSWORD'])
        g.db = None # psy.connect(cstr)
    return g.db


def close_db(e=None):
    """If this request connected to the database, close the
    connection.
    """
    db = g.pop('db', None)

    if db is not None:
        db.close()


def init_db():
    """Clear existing data and create new tables."""
    db = get_db()

    with current_app.open_resource('schema.sql') as f:
        db.executescript(f.read().decode('utf8'))


def list_sensor_types():
    """List known sensor_types"""
    pass


def list_sensors(args):
    """List known sensor_types"""
    pass


def get_sensor(sid):
    """Provide sensor sid description """
    pass


def get_timeseries(sid, args):
    """Provide  timeseries for sensor sid"""
    pass


def add_db_cli(app):
    db_cli = AppGroup('db')

    @db_cli.command('init')
    def db_init():
        init_db()
        click.echo('Initialized the database.')
    app.cli.add_command(db_cli)


def init_app(app):
    app.teardown_appcontext(close_db)
    add_db_cli(app)
