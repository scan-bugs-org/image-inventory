#!/usr/bin/env python3

import os
import pymongo
import requests

import lib.mongo_utils as mongo_utils
from lib.thread_pool import thread_pool

FMT_FILE_MYSQL_CONFIG = "/home/{}/.my.cnf"
MONGO_CONFIG_FILE = "config.yml"

FMT_IDIGBIO_SEARCH_IMAGE = 'http://search.idigbio.org/v2/search/media?mq={}&fields=["accessuri"]'
FMT_IDIGBIO_IMG = "https://api.idigbio.org/v2/media/{}"

OUTPUT_DIR = os.path.join("/scratch", "scan-images")


def download_img(img_doc):
    orig_url = img_doc["original_url"]
    if "nau.edu" in orig_url:
        file_name = orig_url.split("/")[-1]
        file_content = requests.get(
            FMT_IDIGBIO_IMG.format(img_doc["idigbio_uuid"])
        ).content
        file_path = os.path.join(OUTPUT_DIR, file_name)
        with open(file_path, 'wb') as f:
            f.write(file_content)
        print("Wrote {}".format(file_path))


def main():
    mongo_url = mongo_utils.get_mongo_url(MONGO_CONFIG_FILE)
    db_connection_mongo = pymongo.MongoClient(mongo_url)
    mongo_db_scan = db_connection_mongo["scan"]
    mongo_collection_images = mongo_db_scan["images"]

    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)

    images = mongo_collection_images.find()
    thread_pool(download_img, images)
    db_connection_mongo.close()


if __name__ == "__main__":
    main()
