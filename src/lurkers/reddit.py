__author__ = "Srijan Saxena"
__email__ = "srijan@loratechai.com"

import json
from typing import Dict, Generator, Iterator
from base import Lurker
from res.models.datamodels import MongoDocBase, MongoDocDefaultsBase
from dataclasses import dataclass, asdict
from bs4 import BeautifulSoup
import requests
from utils.general_utils import get_configs, get_stock_list
import logging
from datetime import datetime

import re
import random
from time import sleep


@dataclass
class __RedditMongoDocBase(MongoDocBase):
    text: str
    source: str


@dataclass
class RedditMongoDoc(MongoDocDefaultsBase, __RedditMongoDocBase):
    pass


class Reddit(Lurker):
    """

    Reddit Lurker class, using pushshift API, the default duration_hr is 2 hrs 

    """

    def __init__(self, duration_hr: int = 12, offset_hr: int = 0, **kwargs):
        """[summary]

        Args:
            config (dict): the config is from config.yamlError
            sector_dict (dict): ticker symbol to icb code
            stock_list (list): list of ticker symbols, 556 stocks
            num_workers (int, optional): number of process running parellelly. Defaults to 2.
            duration_hr (int, optional): the scraping period in hrs. Defaults to 7.
            offset_hr (int, optional): the scraping period in hrs. Defaults to 0.
        """
        # Set logger
        log_fmt = '%(asctime)s %(levelname)s %(message)s'
        logging.basicConfig(level=logging.INFO, format=log_fmt)
        logger = logging.getLogger(__name__)

        # Base Class Parameters
        configs = get_configs('res/configs/reddit-configs.yaml')
        super().__init__(configs, logger, **kwargs)

        try:
            # Subclass Params
            self.DURATION_HR = duration_hr
            self.OFFSET_HR = offset_hr
            self.NUM_RETRIES = configs['num_retries']
            self.URL = configs['api']

            self.data_sources = configs['data_source']
            self.stock_list = get_stock_list(self.universe_collection)

        except Exception as e:
            self.logger.error(e)
            raise e

    def _get_ticker_from_text(self, text):
        """get the ticker symbol from reddit post using regex.

        Args:
            text (str): the submission text

        Returns:
            [list]: filtered tickers from the text
        """

        # use regex to find $AAPL, or BB, IT... and check if they are in the stock universe
        pattern1 = r'\$[A-Za-z]+'
        pattern2 = r'\b[A-Z][A-Z]+\b'
        ticker_names = re.findall(pattern1, text)
        for i in range(len(ticker_names)):
            ticker_names[i] = ticker_names[i][1:].upper()
        ticker_names.extend(re.findall(pattern2, text))
        ticker_name = set(ticker_names) & set(self.stock_list) - {'DD', 'ARE'}
        return list(ticker_name)

    def scraper_iterator(self) -> Generator[str, None, None]:
        """
        Implementation of Abstract Method. Generator Function that returns queries needed by scraper.
        * The format for the query string is at https://developers.reddit.io/docs/news-query-api-request-response-formats.html#request-format

        Yields:
            Generator[str, None, None]: queries needed by get_document()
        """
        for j in range(self.DURATION_HR):
            yield (f"{j+self.OFFSET_HR}h", f"{j+1+self.OFFSET_HR}h")

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

    def __get_text_from_id(self, article_id) -> str:
        """
        use article id to get the full text of that news

        Args:
            article_id (int): the id provided by the API

        Returns:
            str: the full text of news
        """
        url = self.RENDER_API.replace('{article_id}', str(article_id))
        response = requests.get(url, params={'token': self.API_KEY})
        if response.status_code == 200:
            content = BeautifulSoup((response.text), 'lxml').get_text()
        return content

    def get_document(self, query, **kwargs) -> bool:

        before, after = query
        base = self.URL

        # get resource from pushshift API
        for data_source in self.data_sources:
            while True:
                response = requests.get(
                    base, {"subreddit": data_source, 'size': 100, 'after': after, 'before': before})
                if response.status_code != 200:
                    # if request fail, request too frequently, sleep for a while and ask again
                    sleep(random.randint(1, 4))
                    continue
                data = response.json()['data']
                break

            # processing the body
            for item in data:

                # submissions with this key means the content is probabaly removed by the moderator
                if 'removed_by_category' in item:
                    continue

                ticker_timestamp = datetime.fromtimestamp(item['created_utc'])
                if 'selftext' not in item:
                    continue
                elif item['selftext'] in ['unknown', '[removed]']:
                    continue
                else:
                    body = item['selftext']

                text = item['title'] + body
                tickers = self._get_ticker_from_text(text)
                    
                # skip submissions without mentioning any ticker
                if len(tickers) == 0:
                    continue

                # Get UniqueIdentifier
                unique_identifier = self.tryAddArticleToHistory(item['id'])

                if unique_identifier:
                    doc = RedditMongoDoc(
                        unique_identifier=unique_identifier,
                        tickers=tickers,
                        sentiment=None,
                        sector_code=None,
                        time=ticker_timestamp,
                        text_hash=str(hash(text)),
                        text=text,
                        source=f"Reddit/{item['subreddit']}",
                        source_id=item['id'],
                        source_link=item['full_link'],
                    )

                    try:
                        self.successful_documents.append(asdict(doc))
                        self.successful_queries.append(item['id'])
                    except Exception as e:
                        self.failed_queries.append(item['id'])
                else:
                    self.skipped_queries.append(item['id'])