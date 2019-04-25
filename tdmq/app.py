import os
from flask import Flask
from flask import request
from flask import url_for
from flask import jsonify

import tdmq.db as db


def add_routes(app):
    @app.route('/')
    def index():
        return 'The URL for this page is {}'.format(url_for('index'))

    @app.route('/sensor_types')
    def sensor_types():
        res = db.list_sensor_types()
        return jsonify(res)

    @app.route('/sensors')
    def sensors():
        rargs = request.args
        if not rargs:
            args = None
        else:
            args = dict((k, rargs.get(k, None))
                        for k in ['center', 'radius', 'after', 'before',
                                  'selector'])
            args['radius'] = int(args['radius'])
        res = db.list_sensors(args)
        return jsonify(res)

    @app.route('/sensors/<int:sid>')
    def sensor(sid):
        res = db.get_sensor(sid)
        return jsonify(res)

    @app.route('/sensors/<int:sid>/timeseries')
    def timeseries(sid):
        rargs = request.args
        args = dict((k, rargs.get(k, None))
                    for k in ['after', 'before', 'bucket', 'op'])
        res = db.get_timeseries(sid, args)
        return jsonify(res)


def create_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        # FIXME this is not working as expected
        APPLICATION_ROOT='/tdmq/v0.1',
        SECRET_KEY='dev',
        DB_HOST='localhost',
        DB_USER='foo',
        DB_PASSWD='bar'
    )
    if test_config is None:
        app.config.from_pyfile('config.py', silent=True)
    else:
        app.config.from_mapping(test_config)

    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    db.init_app(app)
    add_routes(app)

    return app
