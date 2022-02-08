import os
from typing import Iterable
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
import pymongo
import yaml
from res.models.datamodels import ESAction

def connect_to_elasticsearch():
    """
    Builds and returns a connection to ElasticSearch.

    Raises:
        Exception: *need to be more descriptive*

    Returns:
        obj: ES Connection Object 
    """
    try:
        es = Elasticsearch( hosts=[os.getenv("ES_HOST").strip()],
                            http_auth=(os.getenv("ES_USERNAME").strip(), 
                                       os.getenv("ES_PASSWORD").strip()),
                            use_ssl=True)
    except Exception as e:
        raise e


    return es

def connect_to_mongodb(collection: str) -> pymongo.collection.Collection:
    """
    Builds a connection to a MongoDB collection.

    Args:
        collection (str): name of MongoDB collection.

    Raises:
        Exception: Errors with reading scraper_storage.yaml
        TypeError: Error connecting to mongo collection.

    Returns:
        pymongo.collection.Collection: self-explanatory.
    """
    # Load Creds
    creds_file_path = "res/configs/scraper_storage.yaml"
    
    try:
        with open(creds_file_path, 'r') as f:
            creds = yaml.safe_load(f)
    except Exception as e:
        raise Exception(e)

    service_type = creds['service_type']
    # Connect to DB
    if service_type == 'mongo':
        myclient = pymongo.MongoClient(
            host=creds['host'], ssl=True, ssl_ca_certs=creds['ssl_ca_certs_path'])
        mydb = myclient[creds['database']]
        mycollection = mydb[collection]
        return mycollection
    else:
        raise TypeError(__name__, "No such type")

# TODO: Implement a way to reset just_insert in mongodb for successful documents only. Perhaps by storing their Object IDs and then update_many()
def reset_just_insert(mongo_collection: pymongo.collection.Collection):
    """
    Reset the just_insert field for all documents in the mongodb collection to False.

    Args:
        mongo_collection (obj): self-explanatory.

    Raises:
        e: Exception
    """
    try:
        query = {"just_insert": True}
        new_value = {"$set": {"just_insert": False}}
        mongo_collection.update_many(query, new_value)
    except Exception as e:
        raise e

def bulk_migrate_to_es(mongo_collection: pymongo.collection.Collection, es_index: str, actions: Iterable[ESAction]) -> tuple:
    """
    Bulk Migrates Documents to ES

    Args:
        index (str): Destination Index.
        actions (Generator[str, None, None]): Actions to be performed.
    """
    es_conn = connect_to_elasticsearch()
    successes, failures = 0, 0
    errors = []

    try:
        for ok, item in streaming_bulk(
            client=es_conn, 
            index=es_index, 
            actions=actions,
            max_retries=5
        ):
            if ok:
                successes+=1
            else:
                failures+=1
                errors.append(item)

        reset_just_insert(mongo_collection)

    except Exception as e:
        raise e    

    return successes, failures, errors