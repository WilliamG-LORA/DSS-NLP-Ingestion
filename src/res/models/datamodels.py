from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Union

@dataclass
class BaseDoc:
    """
    Base Dataclass to represent documents stored in the ElasticSearch/Mongo Database. 
    """
    unique_identifier: str
    tickers: List[str]
    sentiment: Union[float,None]
    sector_code: int
    source_link: Union[str, None]
    time: datetime


@dataclass
class ESDoc(BaseDoc):
    """
    Dataclass to represent documents stored in the ElasticSearch Database. 
    """
    text: str

@dataclass
class ESAction:
    """
    Dataclass to represent Action package for ES Bulk Ingestion.
    """
    _id: str
    _source: ESDoc
    _op_type: str = 'create'
    pipeline: str = 'add-timestamp'

@dataclass
class MongoDocBase(BaseDoc):
    """
    Dataclass to represent documents stored in MongoDB.
    """
    source_id: str
    text_hash: str

@dataclass
class MongoDocDefaultsBase:
    retrieval_time: datetime = field(default_factory=datetime.now)
    just_insert: bool = True