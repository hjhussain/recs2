import concurrent.futures
import json

import tornado.escape
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
from tornado.options import define, options

define("port", default=9090, help="run on the given port", type=int)
# A thread pool to be used for password hashing with bcrypt.
executor = concurrent.futures.ThreadPoolExecutor(2)


class WebApp(tornado.web.Application):
    def __init__(self, userHandelers):
        handlers = [
                       (r"/monitor", HealthCheckHandler)
                   ] + userHandelers
        super(WebApp, self).__init__(handlers)


class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        return self.application.db

    def data_received(self, chunk):
        pass


class HealthCheckHandler(BaseHandler):
    def get(self):
        self.write(json.dumps({"status": "OK", "version": "0.1"}))
        self.finish()


def start(handlers):
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(WebApp(handlers))
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()
