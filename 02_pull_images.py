#!/usr/bin/env python3

import json
import pymongo
import requests

import lib.mongo_utils as mongo_utils

FMT_FILE_MYSQL_CONFIG = "/home/{}/.my.cnf"
MONGO_CONFIG_FILE = "config.yml"

FMT_IDIGBIO_SEARCH_IMAGE = 'http://search.idigbio.org/v2/search/media?mq={}&fields=["accessuri"]'
FMT_IDIGBIO_IMG = "https://api.idigbio.org/v2/media/{}"

MAX_THREADS = 256


def main():
    mongo_url = mongo_utils.get_mongo_url(MONGO_CONFIG_FILE)
    db_connection_mongo = pymongo.MongoClient(mongo_url)
    mongo_db_scan = db_connection_mongo["scan"]
    mongo_collection_occurrences = mongo_db_scan["occurrences"]

    for occurrence in mongo_collection_occurrences.find():
        imgs = occurrence["images"]
        for img in imgs:
            print(img)


if __name__ == "__main__":
    main()
