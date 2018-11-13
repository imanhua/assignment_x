from elasticsearch import Elasticsearch, helpers
import json
from datetime import datetime
import readconfig

cfg = readconfig.read_config('info')
json_file = cfg['json_file']         # the json file contains data to be inserted
es_config = readconfig.read_config('es_config')
index_name = es_config['index_name']
doc_type = es_config['doc_type']
int_list = ["duration", "playbackPercentage"]

# by default we connect to localhost:9200
es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])


''' Enable this if we want to connect to the cloud 

# 'This is used to connect to the cloud
es_config = readconfig.read_config("es_config")
api_endpoints = es_config["api_endpoints"]
user_name = es_config["user_name"]
password = es_config["password"]


es = Elasticsearch([api_endpoints],
                   http_auth=[user_name, password],
                   scheme="https"
                   )
'''


def drop_index():
    es.indices.delete(index=index_name, ignore=[400, 404])
    print('%s Index [%s] dropped ' % (datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), index_name))


def create_index():

    doc = {
        "_timestamp": {"enabled": "true"},
        "properties": {"duration": {"type": "integer"},
                       "playbackPercentage": {"type": "integer"}
                       }
    }

    es.indices.put_mapping(
        index=index_name,
        doc_type=doc_type,
        ignore=[400, 404],
        body=doc
    )
    print('%s Index [%s] created ' % (datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), index_name))


def es_insert():

    # count = 0
    print('%s es_insert starts ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))
    with open(json_file) as output_json_read:
        json_reader = json.load(output_json_read)

    output_json_read.close()
    print('%s Json file [%s] finishes reading ' % (datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), json_file))

    # convert certain fields to int type
    for records in json_reader:
        # print(records)
        for field_convert in int_list:
            # convert string to int
            if records[field_convert] != "":
                records[field_convert] = int(records[field_convert])
            else:
                pass
    print('%s Json file [%s] finishes converting ' % (datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), json_file))

    count = 0
    records_list = []
    for records in json_reader:
        count += 1
        records_list.append(records)

        # insert every 100000 records
        if count % 100000 == 0:
            # bulk insert
            helpers.bulk(es, records_list, index=index_name, doc_type=doc_type)
            print('%s %d records inserted, in total %d records '
                  % (datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), len(records_list), count))
            records_list = []

    # insert the rest records
    helpers.bulk(es, records_list, index=index_name, doc_type=doc_type)
    print('%s %d records inserted, in total %d records '
          % (datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), len(records_list), count))
    print('%s es_insert ends ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))


def main():

    drop_index()
    create_index()
    es_insert()


if __name__ == '__main__':
    main()
