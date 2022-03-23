__author__ = "Guo Zhongyuan"

import json
from typing import Dict, Generator, Iterator
from base import Lurker
from res.models.datamodels import MongoDocBase, MongoDocDefaultsBase
from dataclasses import dataclass, asdict
from bs4 import BeautifulSoup
import requests
from utils.general_utils import get_configs, get_sector_dict, get_sector_loose
import logging
from datetime import datetime, timedelta

from utils.tencent_api import TecentNLU

@dataclass
class __EastMoneyMongoDocBase(MongoDocBase):
    link: str
    info: dict
    type: str
    content: str
    keywords: list

@dataclass
class EastMoneyMongoDoc(MongoDocDefaultsBase, __EastMoneyMongoDocBase):
    pass


class EastMoney(Lurker):
    """
        scraper for eastmoney stock researchs, "https://data.eastmoney.com/report/stock.jshtml"
    """
    def __init__(self, duration = 7):

        log_fmt = '%(asctime)s %(levelname)s %(message)s'
        logging.basicConfig(level=logging.INFO, format=log_fmt)
        logger = logging.getLogger(__name__)

        '''Base Class Parameters'''
        configs = get_configs('res/configs/eastmoney-configs.yaml')
        super().__init__(configs, logger)

        try:
            '''Subclass Params'''
            self.DURATION = duration

            api_configs = configs['api']
            self.nlu_tool = TecentNLU(api_configs)

            self.sector_dict = get_sector_dict(self.universe_collection)

            self.successful_queries = []
            self.failed_queries = []

        except Exception as e:
            self.logger.error(e)
            raise e

    def __get_query(self, time_0='2022-03-22', time_1='2022-03-23', pages=100):
        url_list = []
        for page in range(1, pages+1):
            url = f"https://reportapi.eastmoney.com/report/jg?cb=datatable6176985&pageSize=100&beginTime={time_0}&endTime={time_1}&pageNo={page}"
            url_list.append(url)
        query_list = []
        for i in range(len(url_list)):
            url = url_list[i]
            res = requests.get(url)
            res_text = res.text
            res_text = res_text[17:-1]
            res_js = json.loads(res_text)
            if len(res_js['data'])==0:
                break
            query_list.extend(res_js['data'])
        return query_list

    def scraper_iterator(self) -> Generator[str, None, None]:
        """ scrape by article date, from today to seven days ago """
        today = datetime.now()
        offset = timedelta(days=-self.DURATION)
        start_date = (today + offset).strftime('%Y-%m-%d')
        query_list = self.__get_query(start_date, today, pages=100)
        for query in query_list:
            encodeUrl = query['encodeUrl']
            content_url = f'https://data.eastmoney.com/report/zw_macresearch.jshtml?encodeUrl={encodeUrl}'
            query_dict = {
                'link': content_url,
                'info': query,
            }
            yield query_dict

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
        return doc['text_title'] + doc['text_content']

    def __get_content(self, text_link):
        html = requests.get(text_link).content
        soup = BeautifulSoup(html, "lxml")
        # class_name = {
        #     '个股研报': 'stockzw_content',
        #     '行业研报': 'zw-content',
        #     '宏观研究': 'ctx-content',
        #     '盈利预测': '',
        #     '新股研报': 'stockzw_content',
        #     '策略报告': 'ctx-content',
        #     '券商晨报': 'zw-content',
        # }
        res = {
            'stockzw_content': soup.find('div', class_='stockzw_content'),
            'ctx-content': soup.find('div', class_='ctx-content'),
            'zw-content': soup.find('div', class_='zw-content'),
        }
        for text_type in res.keys():
            if res[text_type] != None:
                content = [i.text for i in res[text_type].find_all('p')]
                content = ''.join(content)
                content = content.replace('\u3000\u30002', '')
                content = content.replace('\u3000\u3000', '')
                content = content.replace('\r', '')
                content = content.replace(' ', '')
                return text_type, content
        return None, None

    def get_document(self, query, **kwargs):

        text_link = query['link']
        text_info = query['info']
        text_type, text_content = self.__get_content(text_link)

        '''info from baidu-nlu-api'''
        if text_content!=None:
            keywords = self.__get_keywords_from_tencent_api(text_content)
        else:
            keywords = []

        doc = EastMoneyMongoDoc(
            unique_identifier = '',
            tickers = [],
            sentiment=[],
            sector_code=4,
            source_link=[],
            time=datetime.now(),
            source_id='',
            text_hash='',

            link=text_link,
            info=text_info,
            type=text_type,
            content=text_content,
            keywords=keywords,
        )

        try:
            self.successful_documents.append(asdict(doc))
            self.successful_queries.append(query)
        except Exception as e:
            failed_payloads = 1
            self.failed_queries.append(query)
            self.logger.info(f"Payload failed to migrate to mongo. {failed_payloads}; {query}")
            self.logger.debug(f"Failed Insertion into Mongo: {e}")
            return False

    def __get_keywords_from_tencent_api(self, content):
        """get text intent and keyword by tencent-nlu-api
        """
        try:
            resp = self.nlu_tool.get_keywords(content)
            return [{'word':i.Word, 'score':i.Score} for i in resp.Keywords]
        except Exception:
            pass

        return []