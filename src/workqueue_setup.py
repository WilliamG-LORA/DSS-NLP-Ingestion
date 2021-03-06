from typing import List
from workqueue.rediswq import RedisWQ
from utils.database_api import DbConn
import logging
from utils.general_utils import get_configs
from utils.database_utils import connect_to_mongodb
import os
import itertools

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
    aws_universe = DbConn("res/db-creds/big_universe.yaml")
    
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

# DONE: Implement functions in RedisWQ module and test.
def populate_wq(tickers: List[str], name: str):
    wq = RedisWQ(name=name, host=os.getenv("REDIS_SERVICE_HOST"))
    # Clean up the work queue.
    # wq.cleanup()

    # Add the item to the work queue
    for ticker in tickers:
        wq.add_task(ticker)


def main():
    config = get_configs('res/configs/setup_configs.yaml')
    universe_collection = connect_to_mongodb(config['universe_collection'])
    redis_wqs = config['redis_wqs'] # One Global Queue
    lurkers_collection = config['lurkers_collection']
    
    # Update universe
    update_universe(universe_collection=universe_collection)

    # Populate RedisWQ
    tickers = universe_collection.distinct('ticker_symbol')

    # tickers = itertools.islice(tickers,10)

    days_to_scrape = int(os.environ['DURATION_DAYS'])

    tasks = []
    # For give the work to a specific type of lurker
    for lurker in lurkers_collection:
        # Each lurker will try to scrape the universe
        print(f"Init for lurker type {lurker}")
        if lurker == 'reddit':
            duration = 24 * days_to_scrape
            payload = [ f"{lurker}:1-{offset}" for offset in range(duration) ]
        elif lurker == 'eastmoney':
            duration = 24 * days_to_scrape
            payload = [ f"{lurker}:1-{offset}" for offset in range(duration) ]
        else:
            payload = [ f"{lurker}:{ticker}" for ticker in tickers]
        
        tasks += payload

    print(len(tasks))
    populate_wq(tasks, redis_wqs)

if __name__ == "__main__":
    main()