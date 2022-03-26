# Contains the worker class that distributes the allocate work form the workqueue
# Reference: https://kubernetes.io/docs/tasks/job/fine-parallel-processing-work-queue/

__author__ = "Tom Mong"
__email__ = "u3556578@connect.hku.hk"


# Import dependancies
from email import generator
from logging import Logger
import os
import logging
from typing import Dict, Generator
from utils.database_utils import connect_to_mongodb
from utils.general_utils import get_configs
from workqueue.rediswq import RedisWQ

# Lurkers
from lurkers import *

# Debug
import time
import types

log_fmt = '%(asctime)s %(levelname)s %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)

class Worker():
    def __init__(self, subclass_config: dict, logger: Logger):
        """
        Initializes a Worker class.

        Args:
            logger (Logger): logger initialised by the subclass.
        """
        self.logger = logger

        self.logger.info("Connecting to microservices..")
        try:
            # Redis Params
            self.REDIS_HOST = os.getenv("REDIS_SERVICE_HOST")
            self.REDIS_WQS = subclass_config['redis_wqs']

        except Exception as e:
            self.logger.error(e)
            raise e

    def lurkerJob(self,lurker_type, payload):
        # Select Lurker job
        if lurker_type == "newsfilter":
            print(f"Worker: do {lurker_type}, payload: {payload}")
            lurker = Newsfilter(payload)
        elif lurker_type == "reddit":
            duration_hr, offset_hr = payload.split('-')
            duration_hr = int(duration_hr)
            offset_hr = int(offset_hr)
            print(f"Worker: do {lurker_type}, payload: {payload}")
            lurker = Reddit(duration_hr=duration_hr,offset_hr=offset_hr)
        elif lurker_type == "aastocks":
            print(f"Worker: do {lurker_type}, payload: {payload}")
            lurker = AAstocks(payload)
        elif lurker_type == "etnet":
            print(f"Worker: do {lurker_type}, payload: {payload}")
            lurker = Etnet(payload)
        elif lurker_type == "eastmoney":
            duration_hr, offset_hr = payload.split('-')
            duration_hr = int(duration_hr)
            offset_hr = int(offset_hr)
            print(f"Worker: do {lurker_type}, payload: {payload}")
            lurker = EastMoney(duration_hr=duration_hr,offset_hr=offset_hr)
        else:
            self.logger.error(f"Invalid Lurker Type: {lurker_type}")

        result = lurker.scrape()
        return result

    def doWork(self):
        try:
            source = self.__class__.__name__

            self.logger.info(f'{source} running...')

            wq = RedisWQ(name=self.REDIS_WQS, host=self.REDIS_HOST)
            print("Worker with sessionID: " +  wq.sessionID())
            print("Initial queue state: empty=" + str(wq.empty()))
            while not wq.empty():
                item = wq.lease(lease_secs=60, block=True, timeout=2) 
                if item is not None:
                    try:
                        # item is not string
                        self.logger.info(f"Item is not string: {item}")
                        itemstr = item.decode("utf-8")
                    except AttributeError:
                        # item is string
                        self.logger.info(f"Item is string: {item}")
                        itemstr = item

                    lurker_type, payload = itemstr.split(":")

                    # Pass params to lurker
                    print(f"[{lurker_type}] get payload:{payload}")
                    self.lurkerJob(lurker_type, payload)

                    wq.complete(item)
                else:
                    print("Waiting for work")

            print("Queue empty, exiting")

        except Exception as e:
            raise e

if __name__ == '__main__':
    config = get_configs('res/configs/setup-configs.yaml')

    worker = Worker(subclass_config=config,logger=logger)
    worker.doWork()