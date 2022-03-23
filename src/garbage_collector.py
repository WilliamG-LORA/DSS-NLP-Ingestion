from typing import List
from workqueue.rediswq import RedisWQ
import logging
from utils.general_utils import get_configs
from utils.database_utils import connect_to_mongodb
import os
import itertools

log_fmt = '%(asctime)s %(levelname)s %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)

def main():
    config = get_configs('res/configs/setup_configs.yaml')
    redis_wqs = config['redis_wqs'] # One Global Queue

    wq = RedisWQ(name=redis_wqs, host=os.getenv("REDIS_SERVICE_HOST"))
    # Check if there are expired leases
    wq.check_expired_leases()

    logger.info("Checked Expired Leases")

if __name__ == "__main__":
    main()