import elasticsearch
import csv
import json
from datetime import datetime

# SOURCE_FILE_NAME = "data_eng_dataset.csv"
SOURCE_FILE_NAME = "new_sample.csv"
# JSON_FILE = "data_eng_dataset.json"
JSON_FILE = "new_sample.json"


def read_csv(input_csv_file):

    print('%s read_csv starts ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))
    with open(input_csv_file) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')
        title = csv_reader.fieldnames
        rows = list(csv_reader)
    csv_file.close()
    print('%s read_csv ends ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))
    return rows


def write_json(output_json_file):

    print('%s write_json starts ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))
    with open(JSON_FILE, 'w') as output_json_write:

        json.dump(output_json_file, output_json_write, sort_keys=False, indent=4, separators=(',', ': '),
                  ensure_ascii=False)

    output_json_write.close()
    print('%s write_json ends ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))


def check_json(input_json_file):

    print('%s check_json starts ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))
    with open(input_json_file) as output_json_read:
        json_reader = json.load(output_json_read)
        print("%s There are %s records in %s." % (datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'),
                                                  len(json_reader), input_json_file))

    output_json_read.close()
    print('%s check_json ends ' % datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'))


def main():

    csv_input = read_csv(SOURCE_FILE_NAME)
    write_json(csv_input)
    check_json(JSON_FILE)


if __name__ == '__main__':
    main()
