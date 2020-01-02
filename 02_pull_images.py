#!/usr/bin/env python3

import os
import pymongo
import requests
import urllib.parse

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
    mongo_collection_images = mongo_db_scan["images"]

    if not os.path.exists("images"):
        os.mkdir("images")

    for image in mongo_collection_images.find():
        orig_url = image["original_url"]
        if "nau.edu" in orig_url:
            file_name = orig_url.split("/")[-1]
            file_content = requests.get(
                FMT_IDIGBIO_IMG.format(image["idigbio_uuid"])
            ).content
            with open(os.path.join("images", file_name), 'wb') as f:
                f.write(file_content)

    db_connection_mongo.close()

if __name__ == "__main__":
    main()
