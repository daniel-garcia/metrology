#!/usr/bin/env python

import time
from metrology import Metrology
from metrology.reporter import OpenTSDBReporter

import logging
logging.basicConfig(level=logging.INFO)

lr = OpenTSDBReporter(host='localhost', port=4242, interval=3)
lr.start()
log = logging.getLogger()

class Main(object):

    def __init__(self):
        self.timer = Metrology.timer('mytimer')

    def work(self):
        with self.timer:
            print "work"
            time.sleep(1)

    def run(self):
        while True:
           self.work()

if __name__=='__main__':

    log.info("Starting.")
    main = Main()
    main.run()

