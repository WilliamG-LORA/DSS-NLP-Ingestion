from typing import List
from workqueue.rediswq import RedisWQ
from utils.database_api import DbConn
import logging
from utils.general_utils import get_configs
from utils.database_utils import connect_to_mongodb
import os

log_fmt = '%(asctime)s %(levelname)s %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)

def update_universe(universe_collection):
    """
    update the big_universe if any new stocks come to the stock_universe.

    Args:
        universe_collection (obj): the big_universe mongodb collection_name
        config (dict): the configuration for postgres, mongodb etc.
    """
    aws_universe = DbConn("res/configs/big-universe.yaml")
    
    try:
        res = aws_universe('''
                    SELECT ticker, ticker_symbol, ticker_name, icb_code, currency_code
                    FROM public.universe
                    WHERE is_active = true AND icb_code != 'NA'
                    ''')
    except Exception as e:
        logger.error('failed to connect to the postgres database.')
        raise e
    
    for i in range(len(res)):
        if universe_collection.find_one({'_id':res[i][0]}):
            continue
        try:
            universe_collection.insert_one({
                '_id': res[i][0],
                'ticker_symbol': res[i][1],
                'ticker_name': res[i][2],
                'icb_code': res[i][3],
                'currency_code': res[i][4]
            })
        except Exception as e:
                logging.error(e.args)

# TODO: Implement functions in RedisWQ module and test.
def populate_wq(tickers: List[str], name: str):
    wq = RedisWQ(name=name, host=os.getenv("REDIS_SERVICE_HOST"))
    # ? Reset wq
    for ticker in tickers:
        wq.add_task()


def main():
    config = get_configs('res/configs/base-configs.yaml')
    universe_collection = connect_to_mongodb(config['universe_collection'])
    redis_wqs = config['redis_wqs']
    #Update universe
    update_universe(universe_collection=universe_collection)

    # Populate RedisWQ
    tickers = universe_collection.distinct('ticker_symbol')
    for wq in redis_wqs:
        populate_wq(tickers, wq)