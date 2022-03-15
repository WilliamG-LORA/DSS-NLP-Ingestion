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
import urllib
from utils.general_utils import get_configs, get_sector_dict, get_sector_loose
import logging
from datetime import datetime

@dataclass
class __AAstocksMongoDocBase(MongoDocBase):
    title: str
    text: str
    source: str

@dataclass
class AAstocksMongoDoc(MongoDocDefaultsBase, __AAstocksMongoDocBase):
    pass


class AAstocks(Lurker):
    """

    AAstocks Lurker class

    Args:
        ticker (str): The ticker to scrape
        duration (int): Optional, the duration of the documents you want to scrape from.

    """
    def __init__(self, ticker):
        # Set logger
        log_fmt = '%(asctime)s %(levelname)s %(message)s'
        logging.basicConfig(level=logging.INFO, format=log_fmt)
        logger = logging.getLogger(__name__)

        # Base Class Parameters
        configs = get_configs('res/configs/aastocks-configs.yaml')
        super().__init__(configs, logger)

        try:            
            # Subclass Params (Optional)
            self.NUM_RETRIES = configs['num_retries']
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
        ticker = self.ticker
        if type(ticker) == int:
            ticker = str(ticker)
        ticker = ticker.zfill(5)
        URL = f"http://aastocks.com/tc/stocks/analysis/stock-aafn/{ticker}/0/all/1"
        page = requests.get(URL,proxies=urllib.request.getproxies())
        soup = BeautifulSoup(page.content, "html.parser")
        news_list = soup.find_all('div', attrs={"ref" : lambda tag: tag and tag.startswith("NOW")})
        for news in news_list:
            url = 'http://aastocks.com' + news.find('a')['href']
            yield url

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

    def _getTickerCode(self,ticker_list):
        temp = list()
        for ticker in ticker_list:
            if len(ticker) == 6:
                suffix = ".SZ"
            else:
                suffix = ".HK"
            
            temp.append(ticker+suffix)
        return temp

    def getAllNewsLink(self,ticker):
        if type(ticker) == int:
            ticker = str(ticker)
        ticker = ticker.zfill(5)
        URL = f"http://aastocks.com/tc/stocks/analysis/stock-aafn/{ticker}/0/all/1"
        page = requests.get(URL,proxies=urllib.request.getproxies())
        soup = BeautifulSoup(page.content, "html.parser")
        news_list = soup.find_all('div', attrs={"ref" : lambda tag: tag and tag.startswith("NOW")})
        news_list = ['http://aastocks.com' + news.find('a')['href'] for news in news_list]
        return news_list

    def get_document(self, query, **kwargs) -> bool:
        """
        Implementation of a function that scrape a article from a given query

        Returns:
            bool: True if all articles(s) are successfully scraped, False otherwise
        """

        for _ in range(self.NUM_RETRIES):
            try:
                response = requests.get(query,proxies=urllib.request.getproxies())
                if response.status_code == 200:
                    page = response
                    break
            except requests.exceptions.RequestException as e:
                content = None
        
        if page is None:
            self.logger.warning(f"Failed to Connect. {query}")
            return

        soup = BeautifulSoup(page.content, "html.parser")
        
        # Get Title
        title = soup.find_all("div", class_="newshead5")
        title = title[0].text.strip()
        
        # Get Ticker List
        tickers = soup.find_all('a', class_="jsStock")
        ticker_list = list()
        
        # Get Sentiment
        like = soup.find("div", class_='divRecommend').find('div',class_='value').text
        pos = soup.find("div", class_="divBullish").find('div',class_='value').text
        neg = soup.find("div", class_="divBearish").find('div',class_='value').text

        sentiment = {"like": like, "pos": pos, "neg": neg}

        for ticker in tickers:
            ticker_list.append(ticker['sym'].strip())

        if len(ticker_list) == 0:
            ticker_list.append(self.ticker)

        ticker_list = self._getTickerCode(ticker_list)

        # Content
        text = soup.find(id="spanContent")
        
        try:
            unwanted = text.find(class_='quote-box2')
            unwanted.extract()
            unwanted = text.find(class_='quote-box2')
            unwanted.extract()
        except:
            pass
        
        text = "".join(text.p.text.split())

        # Get News Time
        timestamp = soup.find_all('div', class_="newstime5")
        timestamp = timestamp[0].text.strip()
        timestamp = datetime.strptime(timestamp,'%Y/%m/%d %H:%M')

        source = 'aastocks'
        source_id = str(hash(title))
        tickers = ticker_list
        title= title
        time = timestamp
        source_link = query
        text = text
        sector_code = None
        text_hash = str(hash(title+text))
        sentiment = None

        doc = AAstocksMongoDoc(
                    tickers = tickers,
                    sentiment=sentiment,
                    sector_code=sector_code,
                    source_link=source_link,
                    time=time,
                    source_id=source_id,
                    text_hash=text_hash,
                    title=title,
                    text=text,
                    source=source
                )

        try:
            self.successful_documents.append(asdict(doc))
            self.successful_queries.append(query)
            return True
        except Exception as e:
            self.logger.info(f"Payload failed to migrate to mongo. {query}")
            self.logger.debug(f"Failed Insertion into Mongo: {e}")
            self.failed_queries.append(query)
            return False