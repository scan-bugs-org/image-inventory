import getpass
import json
import multiprocessing as mp
import pymongo
import pymysql
import requests
import sys

import lib.mongo_utils as mongo_utils
import lib.mysql_utils as sql_utils

FMT_FILE_MYSQL_CONFIG = "/home/{}/.my.cnf"
MONGO_CONFIG_FILE = "config.yml"

FMT_IDIGBIO_SEARCH_MEDIA = 'http://search.idigbio.org/v2/search/media?rq={}&fields=["uuid", "accessuri"]'
SQL_LOOKUP_IMAGE = "SELECT imgid FROM images where originalurl = %s"


def url_lookup(collid, img):
    # Get sql db connection
    mysql_config_file = FMT_FILE_MYSQL_CONFIG.format(getpass.getuser())
    mysql_config = sql_utils.get_sql_client_cnf(mysql_config_file)
    db_connection_sql = pymysql.connect(
        host=mysql_config["host"],
        user=mysql_config["user"],
        password=mysql_config["password"],
        db=mysql_config["database"]
    )

    # Get mongo db connection
    mongo_url = mongo_utils.get_mongo_url(MONGO_CONFIG_FILE)
    db_connection_mongo = pymongo.MongoClient(mongo_url)

    with db_connection_sql.cursor() as cursor:
        idigbio_uuid = img["uuid"]
        access_uri = img["indexTerms"]["accessuri"]

        cursor.execute(SQL_LOOKUP_IMAGE, access_uri)
        sql_img = cursor.fetchone()

        if sql_img is not None:
            img_id = int(sql_img[0])
            img_obj = {
                "_id": img_id,
                "collid": collid,
                "original_url": access_uri,
                "idigbio_uuid": idigbio_uuid
            }
            db_connection_mongo["scan"]["images"].insert_one(img_obj)
        else:
            print("Could not find imgid for {}".format(access_uri), file=sys.stderr)


def main():
    # Get sql db connection
    mysql_config_file = FMT_FILE_MYSQL_CONFIG.format(getpass.getuser())
    mysql_config = sql_utils.get_sql_client_cnf(mysql_config_file)
    db_connection_sql = pymysql.connect(
        host=mysql_config["host"],
        user=mysql_config["user"],
        password=mysql_config["password"],
        db=mysql_config["database"]
    )

    # Get mongo db connection
    mongo_url = mongo_utils.get_mongo_url(MONGO_CONFIG_FILE)
    db_connection_mongo = pymongo.MongoClient(mongo_url)
    mongo_db_scan = db_connection_mongo["scan"]
    mongo_collection_collections = mongo_db_scan["collections"]
    mongo_collection_images = mongo_db_scan["images"]

    with db_connection_sql.cursor() as cursor:
        for doc in mongo_collection_collections.find():
            collid = doc["_id"]
            collection_code = doc["collection_code"]

            idigbio_rq = json.dumps({
                "collectioncode": collection_code,
            })

            imgs_url = FMT_IDIGBIO_SEARCH_MEDIA.format(idigbio_rq)
            img_res = requests.get(imgs_url)
            if img_res.ok:
                img_res_obj = img_res.json()

                # SQL lookup of up to 10 imgid's at once
                with mp.Pool(10) as p:
                    p.starmap(url_lookup, [(collid, img) for img in img_res_obj["items"]])
                    p.join()
            else:
                print("{} returned {}".format(imgs_url, img_res.status_code), file=sys.stderr)

            break

    db_connection_sql.close()
    db_connection_mongo.close()


if __name__ == "__main__":
    main()