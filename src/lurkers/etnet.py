"""
    Lurker Template Class

    This Lurker Template Class defines the format of the lurker class, please following the skeleton
    to code your lurker.

    A

"""

__author__ = "Tom Mong"
__email__ = "u3556578@connect.hku.hk"

from typing import Dict, Generator
from base import Lurker
from res.models.datamodels import MongoDocBase, MongoDocDefaultsBase
from dataclasses import dataclass, asdict
from bs4 import BeautifulSoup
import requests
import urllib
from utils.general_utils import get_configs, get_sector_dict, get_sector_loose
import logging
from datetime import datetime
import re

from itertools import groupby 
from string import punctuation

@dataclass
class __EtnetMongoDocBase(MongoDocBase):
    title: str
    text: str
    source: str


@dataclass
class EtnetMongoDoc(MongoDocDefaultsBase, __EtnetMongoDocBase):
    pass


class Etnet(Lurker):
    """

    Etnet Lurker class

    Args:
        ticker (str): The ticker to scrape
        duration (int): Optional, the duration of the documents you want to scrape from.

    """
    def __init__(self, ticker, max_page=5):
        # Set logger
        log_fmt = '%(asctime)s %(levelname)s %(message)s'
        logging.basicConfig(level=logging.INFO, format=log_fmt)
        logger = logging.getLogger(__name__)

        # Base Class Parameters
        configs = get_configs('res/configs/etnet-configs.yaml')
        super().__init__(configs, logger)

        try:            
            # Subclass Params (Optional)
            self.NUM_RETRIES = configs['num_retries']
            self.sector_dict = get_sector_dict(self.universe_collection)
            self.ticker = ticker
            self.max_page = max_page

        except Exception as e:
            self.logger.error(e)
            raise e

    def scraper_iterator(self) :
        """
        Implementation Generator Function that returns queries needed by scraper, given the ticker

        Yields:
            list: queries needed by get_document()
        """
        ticker = self.ticker

        if type(ticker) == int:
            ticker = str(ticker)
        ticker = ticker.zfill(5)
        URL = f"http://www.etnet.com.hk/www/tc/stocks/realtime/quote_news_list.php?page=1&section=related&code={ticker}"
        page = requests.get(URL,proxies=urllib.request.getproxies())
        soup = BeautifulSoup(page.content, "html.parser")
        
        articles = soup.find(class_="DivArticlePagination")

        article_links = list()
        
        # Find Pagnation
        for idx, article in enumerate(articles.find_all('a')):
            if idx < self.max_page:
                domain = "http://www.etnet.com.hk"
                link = article['href']
                url = domain + link
                results = self.getNewsLink(url)
                article_links.extend(results)
            else:
                break
            
        return article_links

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

    # Start of Helper Func
    def strQ2B(self,ustring):
        """把字串全形轉半形"""
        ss = []
        for s in ustring:
            rstring = ""
            for uchar in s:
                inside_code = ord(uchar)
                if inside_code == 12288:  # 全形空格直接轉換
                    inside_code = 32
                elif (inside_code >= 65281 and inside_code <= 65374):  # 全形字元（除空格）根據關係轉化
                    inside_code -= 65248
                rstring += chr(inside_code)
            ss.append(rstring)
        return ''.join(ss)

    def removeConsecutive(self,s):
        punc = set(punctuation) - set('.')

        newtext = []
        for k, g in groupby(s):
            if k in punc:
                newtext.append(k)
            else:
                newtext.extend(g)

        return ''.join(newtext)

    # End of Helper Func

    def getHKTickers(self):
        URL = "http://www.etnet.com.hk/www/tc/stocks/cas_list.php"
        page = requests.get(URL)

        soup = BeautifulSoup(page.content, "html.parser")

        ticker_list = soup.find('table')

        tickers = list()

        for idx, row in enumerate(ticker_list.find_all('tr')):
            if idx != 0:
                for idx, col in enumerate(row.find_all('a')):
                    if (idx % 2) == 0:
                        # isTicker
                        tickers.append(col.text)              
        return tickers

    def getNewsLink(self,URL):
        page = requests.get(URL,proxies=urllib.request.getproxies())
        soup = BeautifulSoup(page.content, "html.parser")
        
        article_links = set()

        articles = soup.find(class_="DivArticleBox")

        for article in articles:
            try:

                link = article.find('a')['href']


                if not link.startswith('/www/tc/stocks/realtime/quote_news_list.php?page='):
                    domain = "http://www.etnet.com.hk/www/tc/stocks/realtime/"
                    url = domain + link
                    article_links.add(url)
            except:
                pass

        article_links = list(article_links)
        
        return article_links

    def getTickerCode(self,ticker_list):
        temp = list()
        for ticker in ticker_list:
            if len(ticker) == 6:
                suffix = ".SZ"
            else:
                suffix = ".HK"
            
            temp.append(ticker+suffix)
        return temp

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
        title = soup.find("p", class_="ArticleHdr")
        title = title.text.strip()
        title = self.strQ2B(title)

        # Content
        text = soup.find(id="NewsContent")
        text = "".join(text.p.text.split())

        # 全形 -> 半形
        text = self.strQ2B(text) 
        
        # Remove Consecutive Punctuation
        text = self.removeConsecutive(text)

        # Get Tickers
        ticker_list = re.findall(r"\((.*?)\)",text)
        for idx,ticker in enumerate(ticker_list):
            if not ticker.isnumeric():
                ticker_list = list(filter((ticker).__ne__, ticker_list))

        if len(ticker_list) == 0:
            ticker_list.append(self.ticker)

        ticker_list = self.getTickerCode(ticker_list)

        # Get News Time
        timestamp = soup.find_all('p', class_="date")
        timestamp = timestamp[0].text.strip()
        timestamp = datetime.strptime(timestamp,'%d/%m/%Y %H:%M')

        source = 'etnet'
        source_id = str(hash(title))
        tickers = ticker_list
        title= title
        time = timestamp
        source_link = query
        text = text
        sector_code = None
        text_hash = str(hash(title+text))
        sentiment = None

        doc = EtnetMongoDoc(
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