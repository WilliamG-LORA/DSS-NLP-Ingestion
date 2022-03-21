# the RedisLease Class

__author__ = "Tom Mong"
__email__ = "u3556578@connect.hku.hk"


import hashlib
import logging
import uuid
from typing import Any

import redis

class RedisLease(object):
    """Simple Redis Lease Backend

    When this is live, the scraper will scrape the same article many times because the same search will yield the same results. 
    Fix this with a Redis list of the urls scraped the past week (don’t hard code this) so that we don’t have to search the whole db every time.

    The items in the work queue are assumed to have unique values.

    """

    def __init__(self, name: str, max_retries: int = 2, **redis_kwargs: Any) -> None:
        """Redis worker queue instance

        The default connection parameters are:
            * host='localhost'
            * port=6379
            * db=0

        Parameters
        ----------
        name : string
            A prefix that identified all of the objects associated with this
            worker queue. e.g., the main work queue is identified by `name`,
            and the processesing queue is identified by `name`:processing.
        max_retries : int, optional
            Number of times to retry a job before removing it from the queue.
            If you don't wish to retry jobs, set the limit to 0.
        """
        redis_kwargs.update(decode_responses=True)
        self._db = redis.StrictRedis(**redis_kwargs)
        # The session ID will uniquely identify this "worker".
        self._session = str(uuid.uuid4())
        # Work queue is implemented as two queues: main, and processing.
        # Work is initially in main, and moved to processing when a client
        # picks it up.
        self._lease_key_prefix = name + ":leased_by_session:"
        self._logger = logging.getLogger("redislease")

    def _itemkey(self, item: str) -> str:
        """Returns a string that uniquely identifies an item (bytes)."""
        return hashlib.sha224(item.encode("utf-8")).hexdigest()

    def isExist(self, item: str) -> bool:
        """True if a lease on 'item' exists."""
        return bool(self._db.exists(self._lease_key_prefix + self._itemkey(item)))

    def tryAdd(self, item, lease_secs: int = 604800) -> bytes:
        """Add a item to the lease atomically."""
        with self._db.lock('mylock'):
            # Check if item exist in the lease db
            if item:
                if not self.isExist(item):
                    p = self._db.pipeline()
                    # Record that we (this session id) are working on a key. Expire
                    # that note after the lease timeout.
                    # Note: if we crash at this line of the program, then GC will see
                    # no lease for this item a later return it to the main queue.
                    itemkey = self._itemkey(item)
                    p.setex(self._lease_key_prefix + itemkey, lease_secs, self._session)
                    p.execute()

                    return item
                else:
                    return False
            else:
                return False