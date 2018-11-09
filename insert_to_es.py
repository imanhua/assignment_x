from elasticsearch import Elasticsearch, helpers
import json
from datetime import datetime

# JSON_FILE = "sample.json"
# JSON_FILE = "new_sample.json"
JSON_FILE = "data_eng_dataset.json"
INDEX_NAME = 'assignment-index-full'
DOC_TYPE = 'assignment'
int_list = ["id", "duration", "playbackPercentage", "channelId", "deviceSessionId", "videoId"]

# by default we connect to localhost:9200
# es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])

# 'This is used to connect to the cloud

API_ENDPOINTS = 'https://544c1289c9ec450889b30cc9ce45524c.us-east-1.aws.found.io:9243'
USER_NAME = 'elastic'
PASSWORD = 'WjTBf9bONfqBPXHAuLPwDXko'

es = Elasticsearch([API_ENDPOINTS],
                   http_auth=[USER_NAME, PASSWORD],
                   scheme="https"
                   )


def create_index():

    # doc = '''{
    #     "mappings": {
    #         "_doc": {
    #             "properties": {
    #                 "id": {"type": "long"},
    #                 "duration": {"type": "integer"},
    #                 "playbackPercentage": {"type": "integer"},
    #                 "channelId": {"type": "long"},
    #                 "deviceSessionId": {"type": "long"},
    #                 "videoId": {"type": "integer"}
    #             }
    #         }
    #     }
    # }
    # '''

    doc = {
        "_timestamp": {
            "enabled": "true"
        },
        "properties": {"id": {"type": "long"},
                       "duration": {"type": "integer"},
                       "playbackPercentage": {"type": "integer"},
                       "channelId": {"type": "long"},
                       "deviceSessionId": {"type": "long"},
                       "videoId": {"type": "integer"}
        }
    }

    # es.indices.create(index=INDEX_NAME, body=doc)
    es.indices.put_mapping(
        index=INDEX_NAME,
        doc_type=DOC_TYPE,
        body= doc
    )
    # es.indices.put_mapping(index='assignment-index-2', doc_type='assignment', )


def es_insert():

    # count = 0
    print('%s es_insert starts ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))
    with open(JSON_FILE) as output_json_read:
        json_reader = json.load(output_json_read)

    output_json_read.close()
    print('%s Json file finishes reading ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))

    # test usage
    for records in json_reader:
        # print(records)
        for field_convert in int_list:
            # convert string to int
            if records[field_convert] != "":
                records[field_convert] = int(records[field_convert])
            else:
                pass
    print('%s Json file finishes converting ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))
    # test end

    helpers.bulk(es, json_reader, index=INDEX_NAME, doc_type=DOC_TYPE)
    print('%s es_insert ends ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))
    # helpers.bulk()
'''
    for json_records in json_reader:
            count += 1
            # print(json_records)
            es.index(index='assignment-index-full', doc_type='assignment', id=count, body=json_records)
        # print("%s There are %s records in %s." % (datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'),
        #                                           len(json_reader), JSON_FILE))
            
            if count % 10000 == 0:
                print(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))
'''

create_index
es_insert()

# drop index
# es.indices.delete(index=INDEX_NAME, ignore=[400, 404])
