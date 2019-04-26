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


