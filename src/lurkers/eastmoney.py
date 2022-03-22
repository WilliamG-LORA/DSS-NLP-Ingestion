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

import os
import time
import traceback
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from utils.baidu_api import BaiduNLU

@dataclass
class __EastMoneyMongoDocBase(MongoDocBase):
    ticker_id: str
    ticker_name: str
    ticker_class: str
    ticker_grade: list
    text_time: str
    text_title: str
    text_content: str
    text_keyword: dict
    text_topic: dict

@dataclass
class EastMoneyMongoDoc(MongoDocDefaultsBase, __EastMoneyMongoDocBase):
    pass


class EastMoney(Lurker):
    """
        scraper for eastmoney stock researchs, "https://data.eastmoney.com/report/stock.jshtml"

        Input: Number of days before
    """
    def __init__(self, duration = 7):

        log_fmt = '%(asctime)s %(levelname)s %(message)s'
        logging.basicConfig(level=logging.INFO, format=log_fmt)
        logger = logging.getLogger(__name__)

        '''Base Class Parameters'''
        configs = get_configs('res/configs/eastmoney_configs.yaml')
        super().__init__(configs, logger)

        try:
            '''Subclass Params'''
            self.DURATION = duration

            self.phantomjs_path = configs['phantomjs_path']
            self.url = configs['eastmoney_url']
            api_configs = configs['api']
            # self.nlu_tool = BaiduNLU(api_configs)

            self.sector_dict = get_sector_dict(self.universe_collection)

            self.successful_queries = []
            self.failed_queries = []

        except Exception as e:
            self.logger.error(e)
            raise e

    def __get_page_by_num(self, num):
        """simulate mouse to click the next page"""
        element_page = WebDriverWait(self.driver, 30).until(EC.presence_of_element_located((By.ID, "stock_table_pager")))
        tr_options = element_page.find_element_by_class_name("ipt")
        tr_options.clear()
        tr_options.send_keys('{}'.format(str(num)))
        element_page.find_element_by_class_name("btn").click()
        time.sleep(10)

    def scraper_iterator(self) -> Generator[str, None, None]:
        """ scrape by article date, from today to seven days ago """
        today = datetime.now()
        offset = timedelta(days=-self.DURATION)
        date_before = (today + offset).strftime('%Y-%m-%d')
        start_date = date_before

        # BUG: PhantomJS not found in the worker images
        '''init phantomjs driver'''
        self.driver = webdriver.PhantomJS(executable_path=self.phantomjs_path)
        self.driver.get(self.url)

        '''get page from the newest date (page 1)'''
        pageNum_init = 1
        FLAG = True

        while FLAG:
            self.__get_page_by_num(pageNum_init)
            try:
                element_table = WebDriverWait(self.driver, 30).until(EC.presence_of_element_located((By.ID, "stock_table")))
                tr_options = element_table.find_elements_by_tag_name("tr")
                for tr_option in tr_options:
                    '''get: idx,title,writer,writer's company,article num'''
                    td_options = tr_option.find_elements_by_tag_name("td")
                    re_sum_info = []
                    for td_option in td_options:
                        re_sum_info.append(td_option.text)
                    if not re_sum_info:
                        continue
                    '''judge text date'''
                    time_tmp = time.strptime(re_sum_info[-1], "%Y-%m-%d")
                    if time_tmp < start_date:
                        FLAG = False
                        break
                    elif len(td_options) >= 4:
                        '''get article content'''
                        url_element = td_options[4]
                        if not url_element:
                            continue
                        link = url_element.find_elements_by_xpath(".//*[@href]")[0]  # get the link
                        text_link = link.get_attribute('href')
                        queryInfoDict = {
                            'link': text_link,
                            'info': re_sum_info,
                        }
                        yield queryInfoDict

            except Exception as e:
                info = traceback.format_exc()
                print(info)
                pageNum_init += 1
            pageNum_init += 1
        self.driver.quit()

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

    def get_document(self, query, **kwargs):

        text_link = query['link']
        re_sum_info = query['info']

        orihtml = requests.get(text_link).content
        soup = BeautifulSoup(orihtml, "lxml")

        '''judge whether black page'''
        if soup.find('div', class_='stockzw_content') == None:
            return None
        page_con = []
        for a in soup.find('div', class_='stockzw_content').find_all('p'):
            page_con.append(str(a.text))
        text_content = "\n".join(page_con)

        '''info on website'''
        one = {}
        one['ticker_id'] = re_sum_info[1]
        one['ticker_name'] = re_sum_info[2]
        one['ticker_class'] = re_sum_info[13]
        one['ticker_grade'] = [re_sum_info[5], re_sum_info[6]]
        one['text_time'] = re_sum_info[14]
        one['text_title'] = re_sum_info[4]
        one['text_content'] = text_content.replace('\n', '').replace(' ', '').replace('\u3000\u3000', '')

        '''info from baidu-nlu-api'''
        # one = self.__send_data_to_baidu_api(one)

        doc = EastMoneyMongoDoc(
            ticker_id=one['ticker_id'],
            ticker_name=one['ticker_name'],
            ticker_class=one['ticker_class'],
            ticker_grade=one['ticker_grade'],
            text_time = one['text_time'],
            text_title=one['text_title'],
            text_content=one['text_content'],
            text_keyword=one['text_keyword'],
            text_topic=one['text_topic'],
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

    # def __send_data_to_baidu_api(self, one):
    #     """get text intent and keyword by baidu-nlu-api
    #     """
    #     title = one['text_title']
    #     content = one['text_content']
    #     try:
    #         res_keyword = self.nlu_tool.get_keyword(title, content)
    #     except Exception:
    #         res_keyword = {}
    #     try:
    #         res_topic = self.nlu_tool.get_topic(title, content)
    #     except Exception:
    #         res_topic = {}
    #     one['text_keyword'] = res_keyword["items"] if "items" in res_keyword.keys() else None
    #     one['text_topic'] = res_topic["item"] if "item" in res_topic.keys() else None
    #     return one