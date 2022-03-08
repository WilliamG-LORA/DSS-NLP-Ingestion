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

    def lurkerJob(self,lurker_type, ticker):
        # Select Lurker job
        if lurker_type == "newsfilter":
            lurker = Newsfilter()
        elif lurker_type == "reddit":
            lurker = Reddit()
        else:
            self.logger.error(f"Invalid Lurker Type: {lurker_type}")

        result = lurker.scrape(ticker)
        return result

    def doWork(self):
        try:
            source = self.__class__.__name__

            self.logger.info(f'{source} running...')

            print("hello from doWork!")

            wq = RedisWQ(name=self.REDIS_WQS, host=self.REDIS_HOST)
            print("Worker with sessionID: " +  wq.sessionID())
            print("Initial queue state: empty=" + str(wq.empty()))
            while not wq.empty():
                item = wq.lease(lease_secs=2, block=True, timeout=2) 
                if item is not None:
                    try:
                        # item is not string
                        self.logger.info(f"Item is not string: {item}")
                        itemstr = item.decode("utf-8")
                    except AttributeError:
                        # item is string
                        self.logger.info(f"Item is string: {item}")
                        itemstr = item


                    lurker_type, ticker = itemstr.split(":")

                    # Pass params to lurker
                    print(f"[{lurker_type}] on {ticker}")
                    # self.lurkerJob(lurker_type, ticker)

                    wq.complete(item)
                else:
                    print("Waiting for work")

            print("Queue empty, exiting")

        except Exception as e:
            raise e

if __name__ == '__main__':
    config = get_configs('res/configs/setup-configs.yaml')

    print(f"Hello World! {time.time()}")
    worker = Worker(subclass_config=config,logger=logger)
    worker.doWork()