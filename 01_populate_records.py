#!/usr/bin/env python3

import json
import pymongo
import os
import requests
import sys

import lib.mongo_utils as mongo_utils
from lib.thread_pool import thread_pool, MAX_THREADS

FMT_FILE_MYSQL_CONFIG = "/home/{}/.my.cnf"
MONGO_CONFIG_FILE = "config.yml"

FMT_IDIGBIO_SEARCH_RECORDS = 'http://search.idigbio.org/v2/search/records?rq={}&fields=["uuid", "data.id", "mediarecords"]&limit=10000000'
FMT_IDIGBIO_SEARCH_IMAGE = 'http://search.idigbio.org/v2/search/media?mq={}&fields=["accessuri", "data.ac:bestQualityAccessURI", "data.ac:goodQualityAccessURI"]'


def get_img_obj(idigbio_uuid, occid):
    mongo_img_obj = {
        "occid": occid,
        "idigbio_uuid": idigbio_uuid,
        "original_url": None
    }

    img_rq = json.dumps({"uuid": idigbio_uuid})
    img_search_url = FMT_IDIGBIO_SEARCH_IMAGE.format(img_rq)
    img_res = requests.get(img_search_url)

    if img_res.ok:
        img_obj = img_res.json()["items"][0]
        if "accessuri" in img_obj["indexTerms"].keys():
            mongo_img_obj["original_url"] = img_obj["indexTerms"]["accessuri"]
        elif "ac:bestQualityAccessURI" in img_obj["data"].keys():
            mongo_img_obj["original_url"] = img_obj["data"]["ac:bestQualityAccessURI"]
        elif "ac:goodQualityAccessURI" in img_obj["data"].keys():
            mongo_img_obj["original_url"] = img_obj["data"]["ac:goodQualityAccessURI"]
        else:
            print("iDigBio doesn't have an original url for {}".format(idigbio_uuid), file=sys.stderr)
    else:
        print("{} returned {}".format(img_search_url, img_res.status_code), file=sys.stderr)
    return mongo_img_obj


def create_occurrences(collection_doc, db_connection_mongo):
    mongo_db_scan = db_connection_mongo["scan"]
    mongo_collection_occurrences = mongo_db_scan["occurrences"]
    mongo_collection_images = mongo_db_scan["images"]

    idigbio_rq = json.dumps({
        "recordset": collection_doc["idigbio_uuid"],
        "hasImage": True
    })
    idigbio_url = FMT_IDIGBIO_SEARCH_RECORDS.format(idigbio_rq)
    records_req = requests.get(idigbio_url)
    if records_req.ok:
        records_obj = records_req.json()
        for record in records_obj["items"]:
            mongo_occurrence_obj = {
                "_id": int(record["data"]["id"]),
                "collid": collection_doc["_id"],
                "idigbio_uuid": record["uuid"],
            }
            images = [get_img_obj(img, mongo_occurrence_obj["_id"]) for img in record["indexTerms"]["mediarecords"]]
            mongo_collection_occurrences.insert_one(mongo_occurrence_obj)
            mongo_collection_images.insert_many(images)
        print("Added {} occurrences to {}".format(records_obj["itemCount"], collection_doc["name"]), flush=True)
    else:
        print("Error pulling records for {} from iDigBio".format(collection_doc["name"]), file=sys.stderr, flush=True)


def main():
    mongo_url = mongo_utils.get_mongo_url(MONGO_CONFIG_FILE)
    db_connection_mongo = pymongo.MongoClient(mongo_url, maxPoolSize=MAX_THREADS + 1)
    mongo_db_scan = db_connection_mongo["scan"]
    mongo_collection_collections = mongo_db_scan["collections"]

    # Populate occurrences for each collection
    collections = mongo_collection_collections.find()
    thread_pool(create_occurrences, collections, db_connection_mongo)

    db_connection_mongo.close()


if __name__ == '__main__':
    main()