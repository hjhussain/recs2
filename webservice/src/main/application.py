import argparse
import json
import logging as log

import blob_utils as blob
import brand_recs.score as scorer
from WebApp import start, BaseHandler
from app_config import update_monitor, monitor
from app_monitor import Monitor
from utils import *
from tornado import gen
import concurrent.futures

# A thread pool to be used for password hashing with bcrypt.
executor = concurrent.futures.ThreadPoolExecutor(30)


def __config_log(log_path):
    if path.sep in log_path:
        mk_dir(path.dirname(log_path))
    log.basicConfig(filename=log_path,
                    format='time=%(asctime)s level=%(levelname)s msg=%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=log.INFO)


def __config_monitor(monitor_store, key):
    if monitor_store and key:
        update_monitor(Monitor("defaultMonitor", blob.get_connection_str(monitor_store), key))


def __parse_arguments():
    parser = argparse.ArgumentParser()
    default_logfile = "logs/recs_scorer_%s.log" % datetime_to_str(dt.date.today(), "%Y_%m_%d")

    parser.add_argument("-g", "--log_path", type=str, default=default_logfile, help='log filename name')
    parser.add_argument("-c", "--monitor_store", type=str, default="asosdsmonitoringprod",
                        help='Application Insights monitor storage name')
    parser.add_argument("-k", "--monitor_key", type=str, default="", help='Application Insights instrumentation key')
    return parser.parse_args()


class ScoreHandler(BaseHandler):
    @gen.coroutine
    def get(self, customer_id):
        score = yield executor.submit(scorer.score, customer_id)
        self.write(score)
        self.add_header("cache-control", "max-age=3600")
        self.finish()


class ScoreHandlerDotOnly(BaseHandler):
    @gen.coroutine
    def get(self, customer_id):
        score = yield executor.submit(scorer.score_dot_only, customer_id)
        # score = scorer.score_dot_only(customer_id)
        self.write(score)
        self.add_header("cache-control", "max-age=3600")
        self.finish()


class ScoreHandlerDbOnly(BaseHandler):
    @gen.coroutine
    def get(self, customer_id):
        score = yield executor.submit(scorer.score_db_only, customer_id)
        # score = scorer.score_db_only(customer_id)
        self.write(score)
        self.add_header("cache-control", "max-age=3600")
        self.finish()


class ScoreHandlerDbOnlyQ(BaseHandler):
    @gen.coroutine
    def get(self, customer_id):
        score = yield executor.submit(scorer.score_db_only, customer_id)
        # score = scorer.score_db_query(customer_id)
        self.write(score)
        self.add_header("cache-control", "max-age=3600")
        self.finish()


class ScoreHandlerAnn(BaseHandler):
    @gen.coroutine
    def get(self, customer_id):
        score = yield executor.submit(scorer.score_ann, customer_id)
        # score = scorer.score_db_only(customer_id)
        self.write(score)
        self.add_header("cache-control", "max-age=3600")
        self.finish()


def main(args):
    __config_log(args.log_path)
    __config_monitor(args.monitor_store, args.monitor_key)

    monitor().track_metric("ApplicationStarted", now())

    try:
        start([(r"/brands/([^/]+)", ScoreHandler)])
    except Exception:
        log.error("Application error", exc_info=True)
        monitor().track_metric("ApplicationFailed", now())
        raise


# Main method.
if __name__ == '__main__':
    # main(__parse_arguments())
    scorer.init()
    print("scorer initialised")
    start([
        (r"/brands/([^/]+)", ScoreHandler),
        (r"/dot/([^/]+)", ScoreHandlerDotOnly),
        (r"/db/([^/]+)", ScoreHandlerDbOnly),
        (r"/dbq/([^/]+)", ScoreHandlerDbOnlyQ),
        (r"/ann/([^/]+)", ScoreHandlerAnn)
    ])
