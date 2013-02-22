import logging

from metrology.instruments import *  # noqa
from metrology.reporter.base import Reporter
import time

import socket


class OpenTSDBReporter(Reporter):
    """
    A reporter that writes metrics to an OpenTSDB tcollector instance ::

      reporter = TCollectorReporter(host='localhost', port=4242, interval=10)
      reporter.start()

    :param host: tsd host
    :param port: tsd port
    :param tags: tags to use for this metric (default: host=socket.getfqdn())
    :param interval: time between each reporting
    :param prefix: metrics name prefix

    #TODO: 
       use persistent connection 
       swallow exceptions when tsdb/network fails, log errors
       register metrics: tsdb mkmetric {metricname}
    """
    def __init__(self, host='localhost', port=4242, tags=None, **options):
        self.host = host
        self.port = port
        if tags is None:
            tags = {'host': socket.getfqdn(),}

        self.prefix = options.get('prefix', '')
        self.tags = " ".join(["%s=%s" % (key, value) for key, value in tags.iteritems()])
        super(OpenTSDBReporter, self).__init__(**options)

    def write(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))

        for name, metric in self.registry:
            if isinstance(metric, Meter):
                self.log_metric(name, 'meter', metric, [
                    'count', 'one_minute_rate', 'five_minute_rate',
                    'fifteen_minute_rate', 'mean_rate'
                ])
            if isinstance(metric, Gauge):
                self.log_metric(name, 'gauge', metric, [
                    'value'
                ])
            if isinstance(metric, UtilizationTimer):
                self.log_metric(name, 'timer', metric, [
                    'count', 'one_minute_rate', 'five_minute_rate',
                    'fifteen_minute_rate', 'mean_rate',
                    'min', 'max', 'mean', 'stddev',
                    'one_minute_utilization', 'five_minute_utilization',
                    'fifteen_minute_utilization', 'mean_utilization'
                ], [
                    'median', 'percentile_95th'
                ])
            if isinstance(metric, Timer):
                self.log_metric(name, 'timer', metric, [
                    'count', 'one_minute_rate', 'five_minute_rate',
                    'fifteen_minute_rate', 'mean_rate',
                    'min', 'max', 'mean', 'stddev'
                ], [
                    'median', 'percentile_95th'
                ])
            if isinstance(metric, Counter):
                self.log_metric(name, 'counter', metric, [
                    'count'
                ])
            if isinstance(metric, Histogram):
                self.log_metric(name, 'histogram', metric, [
                    'count', 'min', 'max', 'mean', 'stddev',
                ], [
                    'median', 'percentile_95th'
                ])
        self.socket.close()

    def log_metric(self, name, type, metric, keys, snapshot_keys=None):
        if snapshot_keys is None:
            snapshot_keys = []
        ts = int(time.time())
        messages = []
        if self.prefix:
            metric_name = "%(prefix)s.%(name)s" % locals()
        else:
            metric_name = "%(name)s" % locals()


        for name in keys:
            line = "put %s.%s %d %s %s" % (metric_name, name, ts, getattr(metric, name), self.tags)
            messages.append(line.strip())

        #if hasattr(metric, 'snapshot'):
        #    snapshot = metric.snapshot
        #    for name in snapshot_keys:
        #        messages.append("{0}={1}".format(name, getattr(snapshot, name)))

        print messages
        self.socket.send("\n".join(messages));

