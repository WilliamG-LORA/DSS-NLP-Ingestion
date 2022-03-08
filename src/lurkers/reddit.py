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

# TODO
@dataclass
class __RedditMongoDocBase(MongoDocBase):
    title: str
    description: str
    text: str
    source: str

@dataclass
class RedditMongoDoc(MongoDocDefaultsBase, __RedditMongoDocBase):
    pass


class Reddit(Lurker):
    """

    Reddit Lurker class

    Args:
        ticker (list): The ticker(s) you want to scrape from.
        duration (int): Optional, the duration of the documents you want to scrape from.

    """
    def __init__(self, tickers, duration = 7):
        # Set logger
        log_fmt = '%(asctime)s %(levelname)s %(message)s'
        logging.basicConfig(level=logging.INFO, format=log_fmt)
        logger = logging.getLogger(__name__)

        # Base Class Parameters
        configs = get_configs('res/configs/newsfilter-configs.yaml')
        super().__init__(configs, logger)

        try:            
            # Subclass Params
            api_configs = configs['api']

            self.DURATION = duration
            self.NUM_RETRIES = configs['num_retries']
            self.API_KEY = api_configs['key']
            self.QUERY_API = api_configs['query_api']
            self.RENDER_API = api_configs['render_api']

            self.sector_dict = get_sector_dict(self.universe_collection)

            self.successful_queries = [] #TODO
            self.failed_queries = []    #TODO

            self.tickers = tickers

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

        for ticker in self.tickers:
            for j in range(self.DURATION):
                
                queryString = f'symbols:{ticker} AND publishedAt:[now-{j}d/d TO *]  AND NOT title:\"4 Form\"'
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

    def get_document(self, query, **kwargs):

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
        elif content['message'].startswith("You tried to access the API without a valid API key."):
            self.logger.warning(f"Failed to Connect. {content['message']}")
            return
        
        total_articles = content['total']['value']
        successful_payloads = 0
        failed_payloads = 0

        while payload['from'] < total_articles:
            payload['from'] += payload['size']

            articles = content['articles']
            record_list = []
            for article in articles:
                source_id = article['id']
                if self.mongo_collection.find_one({'_id': source_id}) != None:
                    continue
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

                record_list.append(asdict(doc))

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
        else:
            self.failed_queries.append(query)
