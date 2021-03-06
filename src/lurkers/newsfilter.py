__author__ = "Srijan Saxena"
__email__ = "srijan@loratechai.com"

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

@dataclass
class __NewsfilterMongoDocBase(MongoDocBase):
    title: str
    description: str
    text: str
    source: str

@dataclass
class NewsfilterMongoDoc(MongoDocDefaultsBase, __NewsfilterMongoDocBase):
    pass


class Newsfilter(Lurker):
    """

    Newsfilter Lurker class

    Args:
        ticker (list): The ticker(s) you want to scrape from.
        duration (int): Optional, the duration of the documents you want to scrape from.

    """
    def __init__(self, ticker, duration = 7, **kwargs):
        # Set logger
        log_fmt = '%(asctime)s %(levelname)s %(message)s'
        logging.basicConfig(level=logging.INFO, format=log_fmt)
        logger = logging.getLogger(__name__)

        # Base Class Parameters
        configs = get_configs('res/configs/newsfilter-configs.yaml')
        super().__init__(configs, logger, **kwargs)

        try:            
            # Subclass Params
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
        Implementation of Abstract Method. Generator Function that returns queries needed by scraper.
        * The format for the query string is at https://developers.newsfilter.io/docs/news-query-api-request-response-formats.html#request-format
        
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

        payload = {
            "type": "filterArticles",
            "queryString": query,
            "from": 0,
            "size": 50
        }

        # seems like we must transform the data into json data bytes, or the request will fail
        json_data = json.dumps(payload)
        json_data_bytes = json_data.encode('utf-8')

        # the 'authorization' is the API KEY from the dashboard
        headers = {
            'content-type': "application/json",
            'authorization': self.API_KEY,
        }

        for _ in range(self.NUM_RETRIES):
            try:
                response = requests.post(self.QUERY_API, data=json_data_bytes, headers=headers)
                if response.status_code == 200:
                    content = response.json()
                    break
            except requests.exceptions.RequestException as e:
                content = None
        
        if content is None:
            self.logger.warning(f"Failed to Connect. {query}")
            return
        elif 'message' in content:
            self.logger.warning(f"Failed to Connect. {content['message']}")
            return

        total_articles = content['total']['value']

        while payload['from'] < total_articles:
            payload['from'] += payload['size']

            articles = content['articles']
            for article in articles:
                source_id = article['id']
                
                # Get UniqueIdentifier
                unique_identifier = self.tryAddArticleToHistory(source_id)

                if unique_identifier:
                    source = article['source']['name']
                    tickers = article['symbols']
                    title= article['title']
                    description = article['description']
                    time = datetime.strptime(article['publishedAt'][0:10], "%Y-%m-%d")
                    source_link = article['url']
                    text = self.__get_text_from_id(source_id)
                    sector_code = get_sector_loose(tickers, self.sector_dict)
                    text_hash = str(hash(title+description+text))
                    sentiment = None

                    doc = NewsfilterMongoDoc(
                        unique_identifier = unique_identifier,
                        tickers = tickers,
                        sentiment=sentiment,
                        sector_code=sector_code,
                        source_link=source_link,
                        time=time,
                        source_id=source_id,
                        text_hash=text_hash,
                        title=title,
                        description=description,
                        text=text,
                        source=source
                    )

                    try:
                        self.successful_documents.append(asdict(doc))
                        self.successful_queries.append(query)
                    except Exception as e:
                        self.failed_queries.append(query)
                else:
                    self.skipped_queries.append(query)