All the settings are done on Kibana, Dev Tools or Stack Management

1. set index lifecycle policy

   ```elm
   PUT _ilm/policy/stock_news
   {
     "policy": {
       "phases": {
         "hot": {
           "min_age": "0ms",
           "actions": {
             "rollover": {
               "max_age": "7d",
               "max_size": "5gb"
             },
             "forcemerge": {
               "max_num_segments": 1
             },
             "set_priority": {
               "priority": 100
             }
           }
         },
         "delete": {
           "min_age": "1088d",
           "actions": {
             "delete": {}
           }
         }
       }
     }
   }
   
   PUT _ilm/policy/stock_tweets
   {
     "policy": {
       "phases": {
         "hot": {
           "min_age": "0ms",
           "actions": {
             "rollover": {
               "max_age": "7d",
               "max_size": "5gb"
             },
             "forcemerge": {
               "max_num_segments": 1
             },
             "set_priority": {
               "priority": 100
             }
           }
         },
         "delete": {
           "min_age": "85d",
           "actions": {
             "delete": {}
           }
         }
       }
     }
   }
   ```

2. create index template

   ```elm
   PUT _index_template/stock_news_doc
   {
     "index_patterns": ["stock_news-*"],
     "data_stream": {},
     "priority": 200,
     "template": {
       "settings": {
         "number_of_shards": 1,
         "index.lifecycle.name": "stock_news"
       },
       "mappings":{
         "properties": {
           "sentiment": {
             "type": "keyword",
             "fields": {
               "double": {
                 "type": "double"
               }
             }
           },
           "sector_description": {
             "type": "text",
             "fields": {
               "keyword": {
                 "type": "keyword"
               }
             }
           },
           "text": {
             "type": "text",
             "fields": {
               "keyword": {
                 "type": "keyword"
               }
             }
           },
           "time": {
             "type": "date"
           },
           "sector_code": {
             "type": "keyword"
           },
           "tickers": {
             "type": "keyword"
           }
         }
       }
     }
   }
   
   PUT _index_template/stock_tweets_doc
   {
     "index_patterns": ["stock_tweets-*"],
     "data_stream": {},
     "priority": 200,
     "template": {
       "settings": {
         "number_of_shards": 1,
         "index.lifecycle.name": "stock_tweets"
       },
       "mappings":{
         "properties": {
           "sentiment": {
             "type": "keyword",
             "fields": {
               "double": {
                 "type": "double"
               }
             }
           },
           "sector_description": {
             "type": "text",
             "fields": {
               "keyword": {
                 "type": "keyword"
               }
             }
           },
           "text": {
             "type": "text",
             "fields": {
               "keyword": {
                 "type": "keyword"
               }
             }
           },
           "time": {
             "type": "date"
           },
           "sector_code": {
             "type": "keyword"
           },
           "tickers": {
             "type": "keyword"
           }
         }
       }
     }
   }
   ```

3. create the data stream

   ```elm
   PUT /_data_stream/stock_news-ds/
   PUT /_data_stream/stock_news-google-ds/
   PUT /_data_stream/stock_tweets-reddit-ds/
   PUT /_data_stream/stock_tweets-stocktwits-ds/
   ```

4. check the status

   ```elm
   GET _cat/shards/stock_news-ds?v
   
   GET _data_stream/stock_news-ds
   
   GET .ds-stock_news-ds-000001/_settings
   
   GET stock_news-ds/_ilm/explain
   ```

5. put a ingest pipeline for insert the @timestamp, this is necessary field for data stream

   ```elm
   PUT _ingest/pipeline/add-timestamp
   {
     "processors": [
       {
         "set": {
           "field": "@timestamp",
           "value": "{{_source.time}}"
         }
       }
     ]
   }
   ```

6. you can check if documents can be inserted successfully by:

   ```elm
   POST stock_news-ds/_doc?pipeline=add-timestamp
   {
     "text":"haha",
     "sentiment":1.0,
     "sector_description":"hehe",
     "time":"2021-08-22T12:10:30Z",
     "sector_code":"20102010",
     "tickers":["AMZN.O", "AAPL"]
   }
   ```

7.  plus, we have one index template for static corpus

   ```elm
   PUT _index_template/stock_static
   {
     "settings": {
       "index": {
         "number_of_replicas": "1"
       }
     },
     "mappings": {
       "properties": {
         "sector_code": {
           "type": "keyword"
         },
         "sentiment": {
           "type": "keyword",
           "fields": {
             "double": {
               "type": "double"
             }
           }
         },
         "text": {
           "type": "text",
           "fields": {
             "keyword": {
               "type": "keyword"
             }
           }
         },
         "tickers": {
           "type": "keyword"
         }
       }
     },
     "aliases": {}
   }
   ```

   

8. add three indices for static corpus

   ```elm
   PUT stock_static-eikon/
   PUT stock_static-wikipedia/
   PUT stock_static-sector/
   ```

   
