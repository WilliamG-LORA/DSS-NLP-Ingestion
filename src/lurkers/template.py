"""
    Lurker Template Class

    This Lurker Template Class defines the format of the lurker class, please following the skeleton
    to code your lurker.

    A

"""

__author__ = "Tom Mong"
__email__ = "u3556578@connect.hku.hk"

import json
from typing import Dict, Generator, Iterator
from base import Lurker
from res.models.datamodels import MongoDocBase, MongoDocDefaultsBase
from dataclasses import dataclass, asdict
from bs4 import BeautifulSoup
import requests
from utils.general_utils import get_configs, get_sector_dict, get_sector_loose
import logging
from datetime import datetime

"""
LurkerTemplateMongoDoc(
    tickers = tickers,       # (list)
    sentiment=sentiment,     # (float)
    sector_code=sector_code, # (str)
    source_link=source_link, # (str)
    time=time,               # (str) ISO 8601
    source_id=source_id,     # article id
    text_hash=text_hash,     # hash (title + description + text)
    title=title,             # (str)
    description=description, # (str)
    text=text,               # (str)
    source=source            # (str)
)
"""

@dataclass
class __LurkerTemplateMongoDocBase(MongoDocBase):
    title: str
    description: str
    text: str
    source: str

@dataclass
class LurkerTemplateMongoDoc(MongoDocDefaultsBase, __LurkerTemplateMongoDocBase):
    pass


class LurkerTemplate(Lurker):
    """

    LurkerTemplate Lurker class

    Args:
        ticker (str): The ticker to scrape
        duration (int): Optional, the duration of the documents you want to scrape from.

    """
    def __init__(self, ticker, duration = 7):
        # Set logger
        log_fmt = '%(asctime)s %(levelname)s %(message)s'
        logging.basicConfig(level=logging.INFO, format=log_fmt)
        logger = logging.getLogger(__name__)

        # Base Class Parameters
        configs = get_configs('res/configs/lurkertemplate-configs.yaml')
        super().__init__(configs, logger)

        try:            
            # Subclass Params (Optional)
            api_configs = configs['api']

            self.DURATION = duration
            self.NUM_RETRIES = configs['num_retries']
            self.API_KEY = api_configs['key']
            self.QUERY_API = api_configs['query_api']
            self.RENDER_API = api_configs['render_api']

            self.sector_dict = get_sector_dict(self.universe_collection)

            self.ticker = ticker

        except Exception as e:
            self.logger.error(e)
            raise e

    def scraper_iterator(self) -> Generator[str, None, None]:
        """
        Implementation Generator Function that returns queries needed by scraper, given the ticker

        Yields:
            Generator[str, None, None]: queries needed by get_document()
        """

        for j in range(self.DURATION):
            queryString = f'symbols:{self.ticker} AND publishedAt:[now-{j}d/d TO *]  AND NOT title:\"4 Form\"'
            yield queryString

    def get_scraper_params(self) -> dict:
        """
        No special extra params. Superclass Abstract Method implementation used.
        """
        return super().get_scraper_params()

    def get_text(self, doc: dict) -> str:
        """
        Implementation of  Abstract Method. Extracts and combines text from various fields.

        Args:
            doc (Dict): scraped document.

        Returns:
            str: text
        """
        return doc['title'] + doc['description'] + doc['text']

    def get_document(self, query, **kwargs) -> bool:
        """
        Implementation of a function that scrape article(s) from a given query

        Returns:
            bool: True if all articles(s) are successfully scraped, False otherwise
        """

        total_articles = 0
        successful_payloads = 0

        for _ in range(self.NUM_RETRIES):
            # TODO: Do your scrapping work here
            pass

            record_list = []

            try:
                self.mongo_collection.insert_many(record_list, ordered=False)
                
            except Exception as e:
                failed_payloads += 1
                self.logger.info(f"Payload failed to migrate to mongo. {failed_payloads}; {query}")
                self.logger.debug(f"Failed Insertion into Mongo: {e}")
            else:
                successful_payloads+=1

        if total_articles and not successful_payloads:
            self.successful_queries.append(query)
            return True
        else:
            self.failed_queries.append(query)
            return False
