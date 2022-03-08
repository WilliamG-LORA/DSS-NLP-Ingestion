# Contains the abstract class from which lurkers will inherit from
"""
    A Lurker will accept a ticker, and return the document regarding the given ticker
"""
__author__ = "Srijan Saxena"
__email__ = "srijan@loratechai.com"

# Import dependancies
import json
from logging import Logger
import os
import time
from functools import wraps
from typing import Dict, Generator
from dataclasses import asdict
from workqueue.rediswq import RedisWQ
from utils.database_utils import connect_to_mongodb, bulk_migrate_to_es
from res.models.datamodels import ESAction, ESDoc
from abc import ABC, abstractmethod
from datetime import datetime
from utils.general_utils import get_configs

# Lurkers
from lurkers import *

# String to Class
import sys

# Import grpc dependencies
# import grpc
# from grapevine_pb2_grpc import GPTKStub, StockHotnessCalculatorStub
# from grapevine_pb2 import GPTKIngressRequest, SHCIngressRequest
# from load_grpc_options import get_options as get_grpc_options
# from concurrent import futures

class Lurker(ABC):
    """
    Abstract lurker class
    """
    def __init__(self, subclass_config: dict, logger: Logger):
        """
        Initializes a Lurker Abstract Base class. Called only by subclasses.

        Args:
            logger (Logger): logger initialised by the subclass.
        """
        self.logger = logger

        self.logger.info("Connecting to microservices..")
        try:
            # Get Configs
            base_config = get_configs('res/configs/base-configs.yaml')
            # options = get_grpc_options()

            # # GPT-K Connection
            # gptk_host = os.getenv("GPTK_HOST")
            # gptk_channel = grpc.insecure_channel(f"{gptk_host}:50051", options=options)
            # self.gptk_client = GPTKStub(gptk_channel)

            # # Stock Hotness Calculator Connection
            # shc_host = os.getenv("SHC_HOST")
            # shc_channel = grpc.insecure_channel(f"{shc_host}:50051", options=options)
            # self.shc_client = StockHotnessCalculatorStub(shc_channel)

            # Connect to MongoDB
            self.universe_collection = connect_to_mongodb(base_config['universe_collection'])
            self.mongo_collection =  connect_to_mongodb(subclass_config['mongo_collection'])

            # Setup Config
            setup_configs = get_configs('res/configs/setup-configs.yaml')

            # ElasticSearch Param
            self.ES_INDEX = subclass_config['es_index']

            # Redis Params
            self.REDIS_HOST = os.getenv("REDIS_SERVICE_HOST")
            # Queue name
            self.REDIS_LIST_NAME = setup_configs['redis_wqs']

            # Subclass Params
            self.SOURCE_CLASS = subclass_config['class']

        except Exception as e:
            self.logger.error(e)
            raise e


    @abstractmethod
    def scraper_iterator(self,ticker) -> Generator[str, bool, None]:
        """
        Abstract Method. Generator Function that returns queries needed by scraper.

        NOTE: This method is to cater scraper that requires more than one query for each tickers (i.e. Pagnations)

        Args:
            ticker (str): The ticker

        Yields:
            Generator[str, None, None]: queries needed by get_document()
        """
        pass

    @abstractmethod
    def get_scraper_params(self) -> dict:
        """
        Abstract Method. Returns any additional parameters needed by scraper.

        Returns:
            dict: **kwargs needed by scraper function.
        """
        return {}

    @abstractmethod
    def get_document(self, query, **kwargs) -> bool:
        """
        Abstract Method. Scrapes and returns document scraped using query.

        Args:
            query (str): Query needed to scrape source.

        Returns:
            bool: Successful
        """
        pass

    @abstractmethod
    def get_text(self, doc: Dict) -> str:
        """
        Abstract Method. Extracts and combines text from various fields.

        Args:
            doc (Dict): scraped document.

        Returns:
            str: text
        """
        pass
    
    def generate_es_actions(self) -> Generator[dict, None, None]:
        """
        Generates Actions for ES Bulk Ingestion.

        Yields:
            Generator[dict, None, None]: Action for ES Bulk Ingestion.
        """
        cursor = self.mongo_collection.find({"just_insert": True})
        for item in cursor:
            id = item['_id']
            text = self.get_text(item)
            
            # for sector, some source will have 8 digits, some source will be 2,4,6,8 or none
            # here we set the sector as a Integer for elasticsearch mapping
            sector_code = item['sector_code']
            # TODO: sector_code = int(sector) if sector != None and math.isnan(sector) == False else None

            tickers = item['tickers'] # TODO: All subclasses have same ticker field
            source_link = item['source_link'] #TODO: Add to subclass scrapers

            if self.SOURCE_CLASS==1:
                sentiment = 0
                time = datetime.now()
            else:
                sentiment = item['sentiment']
                time = item['time']

            doc = ESDoc(
                text= text,
                tickers= tickers,
                sentiment= sentiment,
                sector_code= sector_code,
                source_link= source_link,
                time= time
            )

            action = ESAction(
                _id= id,
                _source= doc
            )

            yield asdict(action)        

    def scrape(self):
        try:
            source = self.__class__.__name__

            start_doc_count = self.mongo_collection.count_documents({})
            self.logger.info(f'{source} running...')

            # Get iterator for subclass scraper
            scraper_iter = self.scraper_iterator()

            # Get **kwargs for subclass scraper
            scraper_params = self.get_scraper_params()

            # Get documents
            for query in scraper_iter:
                success = self.get_document(query, **scraper_params)

            end_doc_count = self.mongo_collection.count_documents({})
            self.logger.info(f'{source} fininshes running successfully! {end_doc_count-start_doc_count} records inserted.')

            # Migrate to ES
            # self.logger.info("Migrating {num_docs} docs to ES...")

            # num_docs_to_migrate = self.mongo_collection.count_documents({"just_insert": True})
            # successes, failures, errors = bulk_migrate_to_es(
            #     mongo_collection= self.mongo_collection,
            #     es_index= self.ES_INDEX,
            #     actions= self.generate_es_actions(),
            # )

            # self.logger.info(
            #     f'''Successfully migrated {successes} ({successes/num_docs_to_migrate}) docs to ES. 
            #     {failures} ({failures/num_docs_to_migrate}) docs failed.
            #     '''
            # )
            # self.logger.debug(
            #     f'''Failed documents:
            #     {json.dumps(errors)}
            #     '''
            # )

        except Exception as e:
            raise e


"""
Deprecated Code 
"""

    # @staticmethod
    # def scraper(get_document_unbounded):
    #     """
    #     Decorator method for subclass document scraper.

    #     Args:
    #         get_document_unbounded (function): subclass document scraper method unbounded to its instance.

    #     Raises:
    #         e: Exception

    #     Returns:
    #         function: decorated function.
    #     """
    #     @wraps(get_document_unbounded)
    #     def scraper_wrapper(self):
    #         try:
    #             source = self.__class__.__name__

    #             start_doc_count = self.mongo_collection.count_documents({})
    #             self.logger.info(f'{source} running...')

    #             # Get iterator for subclass scraper
    #             scraper_iter = self.scraper_iterator()

    #             # Get **kwargs for subclass scraper
    #             scraper_params = self.get_scraper_params()

    #             # Bound get_document to instance
    #             get_document = get_document_unbounded.__get__(type(self), self)

    #             # Get documents
    #             for query in scraper_iter:
    #                 get_document(query, **scraper_params)

    #             end_doc_count = self.mongo_collection.count_documents({})
    #             self.logger.info(f'{source} fininshes running successfully! {end_doc_count-start_doc_count} records inserted.')

    #             # Migrate to ES
    #             self.logger.info("Migrating {num_docs} docs to ES...")

    #             num_docs_to_migrate = self.mongo_collection.count_documents({"just_insert": True})
    #             successes, failures, errors = bulk_migrate_to_es(
    #                 mongo_collection= self.mongo_collection,
    #                 es_index= self.ES_INDEX,
    #                 actions= self.generate_es_actions(),
    #             )

    #             self.logger.info(
    #                 f'''Successfully migrated {successes} ({successes/num_docs_to_migrate}) docs to ES. 
    #                 {failures} ({failures/num_docs_to_migrate}) docs failed.
    #                 '''
    #             )
    #             self.logger.debug(
    #                 f'''Failed documents:
    #                 {json.dumps(errors)}
    #                 '''
    #             )

    #         except Exception as e:
    #             raise e

    #     return scraper_wrapper

    # def __get_next_ticker(self) -> Generator[str, bool, None]:
    #     """
    #     Generator that fetches the next task from Redis Work Queue.

    #     Yields:
    #         Generator[str, None, None]: Items in the queue worked on by this pod.
    #     """
    #     wq = RedisWQ(name=self.REDIS_LIST_NAME, host=self.REDIS_HOST)

    #     self.logger.info(f"Worker with sessionID: {wq.sessionID()}")
    #     self.logger.info(f"Initial queue state: empty= {wq.empty()}")

    #     while not wq.empty():
    #         item = wq.lease(lease_secs=10, block=True, timeout=2) 
    #         if item is not None:
    #             itemstr = item.decode("utf-8")
    #             self.logger.info(f"Working on {itemstr}")
    #             # We yield itemstr and receive success message
    #             success = yield itemstr
    #             if success:
    #                 # we mark the message as success
    #                 wq.complete(item)
    #         else:
    #             self.logger.info("Waiting for work")
    #     self.logger.info("Queue empty, exiting")

    # def send_data_to_GPTK(self, data, source):
    #     """
    #     Sends scraped data to GPT-K
    #     """
    #     #TODO
    #     request = GPTKIngressRequest(data=data, source=source, timestamp=str(time.time()), clean=True)
    #     response = self.gptk_client.sendData(request)
    #     return(response.success)

    # def send_data_to_SHC(self, data):
    #     """
    #     Sends scraped data to Stock Hotness Calculator
    #     """
    #     #TODO
    #     request = SHCIngressRequest(data=data, timestamp=str(time.time()))
    #     response = self.shc_client.sendData(request)
    #     return(response.success)