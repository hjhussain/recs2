import logging as log

from monitor import monitor_registry, monitor_api


def register_monitor(name, connection_string, instrumentation_key):
    mon_reg = monitor_registry.MonitorRegistry()
    mon_reg.register(name, monitor_api.MonitoringAPI(connection_string, instrumentation_key))
    return mon_reg.get_monitor(name)


class Monitor:
    def __init__(self, name="", connection=None, key=None):
        try:
            if name and connection and key:
                self.mon = register_monitor(name, connection, key)
            else:
                self.mon = None
        except ValueError:
            log.error("error creating monitor=%s, connection=%s,key=%s", name, connection, key)
            self.mon = None
            pass

    def track_metric(self, name, value):
        log.info("%s=%s", name, value)
        try:
            if self.mon:
                self.mon.track_metric(name, value)
        except:
            pass

    def track_metrics(self, kv):
        for k, v in kv.items():
            self.track_metric(k, v)
