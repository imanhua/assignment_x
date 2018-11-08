
from datetime import datetime
from elasticsearch import Elasticsearch, helpers
import requests
import json
from datetime import datetime

# JSON_FILE = "sample1.json"
# JSON_FILE = "new_sample.json"
JSON_FILE = "data_eng_dataset.json"

# res = requests.get('http://localhost:9200')
# print(res.content)
#
# by default we connect to localhost:9200
es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])

# datetimes will be serialized
# es.index(index='test-index', doc_type='assignment', id=1, body={'test': 'test'})
# delete test data and try with something more interesting
# es.delete(index='test-index', doc_type='assignment', id=1)

def es_insert():

    count = 0
    print('%s es_insert starts ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))
    with open(JSON_FILE) as output_json_read:
        json_reader = json.load(output_json_read)

    output_json_read.close()

    # es.bulk()
    helpers.bulk(es, json_reader, index='assignment-index-full-2', doc_type='assignment')
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

    # print('%s es_insert ends ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))


es_insert()
print('%s es_insert ends ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))
# a = es.search(index='test-index', doc_type='test' )

# a = es.search(index='test-index', doc_type='test')
# a = es.
# b = es.
# new_data = a['hits']['hits']
# print(new_data)
# for records in new_data:
    # print(records['_source'])

#
# # but not deserialized
# es.get(index="my-index", doc_type="test-type", id=42)['_source']
