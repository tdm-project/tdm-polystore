
from prometheus_client.exposition import make_wsgi_app
from werkzeug.middleware.dispatcher import DispatcherMiddleware

from tdmq.app import create_app


class PrefixMiddleware(object):

    def __init__(self, app, prom_app, prefix='', prom_prefix='/metrics'):
        self.app = app
        self.prom_app = prom_app
        self.prefix = prefix
        self.prom_prefix = prom_prefix

    def __call__(self, environ, start_response):
        if environ['PATH_INFO'].startswith(self.prefix):
            environ['PATH_INFO'] = environ['PATH_INFO'][len(self.prefix):]
            environ['SCRIPT_NAME'] = self.prefix
            return self.app(environ, start_response)
        elif environ['PATH_INFO'].startswith(self.prom_prefix):
            return self.prom_app(environ, start_response)
        else:
            start_response('404', [('Content-Type', 'text/plain')])
            return ["This url does not belong to the app.".encode()]


main_app = create_app()
prom_app = make_wsgi_app()

main_app.wsgi_app = PrefixMiddleware(main_app.wsgi_app, prom_app, prefix='/api/v0.0')

if __name__ == "__main__":
    main_app.run()
