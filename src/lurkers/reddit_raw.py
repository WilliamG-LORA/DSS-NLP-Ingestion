from scrapers.scraper import Scraper
from utility.general_utils import connect_to_mongodb, get_sector_loose
import requests
from datetime import datetime
from multiprocessing import Pool
from functools import partial
import re
import time
import random


class Reddit(Scraper):
    """
        using pushshift API, the default duration is 7 days
    """
    def __init__(self, config, sector_dict, stock_list, num_workers=2, duration=7):
        """[summary]

        Args:
            config (dict): the config is from config.yamlError
            sector_dict (dict): ticker symbol to icb code
            stock_list (list): list of ticker symbols, 556 stocks
            num_workers (int, optional): number of process running parellelly. Defaults to 2.
            duration (int, optional): the scraping period. Defaults to 7.
        """
        super().__init__(config)
        self.mongo_collection = config['reddit']['collection']
        self.url = config['reddit']['API_endpoint']
        self.data_source = config['reddit']['data_source']
        self.num_workers = num_workers
        self.duration = duration
        self.sector_dict = sector_dict
        self.stock_list = stock_list

    def get_ticker_from_text(self, text, stock_list):
        """get the ticker symbol from reddit post using regex.

        Args:
            text (str): the submission text
            stock_list (list): the list of stocks we want to get from the text

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
        ticker_name = set(ticker_names) & set(stock_list) - {'DD', 'ARE'}
        return list(ticker_name)

    def get_reddit_post(self, i, stock_list, base, data_sources):
        """using pushshift to request the submissions accroding to time

        Args:
            i (int): the hour of request period
            stock_list (list): the target stock stock_list
            base (str): the API endpoint
            data_sources (list): a list of subreddit, can be modified in config.yaml
        """
        myCollection = connect_to_mongodb(
            self.mongo_database, self.mongo_collection, self.mongo_host, self.mongo_cert_path)

        # get resource from pushshift API
        before = f'{i}h'
        after = f'{i+1}h'

        for data_source in data_sources:
            while True:
                response = requests.get(
                    base, {"subreddit": data_source, 'size': 100, 'after': after, 'before': before})
                if response.status_code != 200:
                    # if request fail, request too frequently, sleep for a while and ask again
                    time.sleep(random.randint(1, 4))
                    continue
                data = response.json()['data']
                break

            # processing the body
            records = []
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
                text = body + item['title']
                ticker_name = self.get_ticker_from_text(text, stock_list)

                # skip submissions without mentioning any ticker
                if len(ticker_name) == 0:
                    continue
                sector = get_sector_loose(ticker_name, self.sector_dict)
                record = {
                    '_id': item['id'],
                    'ticker_symbols': ticker_name,
                    'time': ticker_timestamp,
                    'text': text,
                    'source': 'r/'+item['subreddit'],
                    'sector': sector,
                    'sentiment': None,
                    'just_insert': True
                }
                records.append(record)

            # upload data to the mongodb cloud
            try:
                myCollection.insert_many(records, ordered=False)
            except Exception as e:
                pass

    def run(self):
        """
        run reddit scraper
        """
        def wrapper():
            p = Pool(self.num_workers)
            # caution: here we use duration*24, because in reddit we are paging by the hour but not day
            p.map(partial(self.get_reddit_post, stock_list=self.stock_list, base=self.url,
                data_sources=self.data_source), range(self.duration*24))
        
        source = "reddit"
        super().run(source, wrapper)
