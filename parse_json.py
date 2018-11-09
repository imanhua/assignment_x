from datetime import datetime
import json

JSON_FILE = "sample.json"
# JSON_FILE = "new_sample.json"
# JSON_FILE = "data_eng_dataset.json"
INDEX_NAME = 'assignment-index-full'
DOC_TYPE = 'assignment'

int_list = ["id", "duration", "playbackPercentage", "channelId", "deviceSessionId", "videoId"]

print('%s es_insert starts ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))
with open(JSON_FILE) as output_json_read:
    json_reader = json.load(output_json_read)
output_json_read.close()
print('%s Json file finishes reading ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))

# print(type(json_reader))

for records in json_reader:
    print(records)
    for field_convert in int_list:
        # convert string to int
        if records[field_convert] != "":
            records[field_convert] = int(records[field_convert])
        else:
            pass


        # print(records[field_convert], type(records[field_convert]))
