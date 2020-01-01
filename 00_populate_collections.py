import getpass
import json
import pymongo
import pymysql
import sys

import lib.mongo_utils as mongo_utils
import lib.mysql_utils as sql_utils

FILE_COLLECTION_CODE_LIST = "collections.json"

FMT_FILE_MYSQL_CONFIG = "/home/{}/.my.cnf"
FMT_IDIGBIO_COLLECTION_QUERY = 'http://search.idigbio.org/v2/search/recordsets?rsq={}&fields=["name"]'

SQL_GET_COLLECTION_COLLID = "SELECT collid, collectioncode from omcollections where collectionName = %s;"

MONGO_ROOT_USER = "root"
MONGO_ROOT_PASSWORD = "password"
MONGO_CONFIG_FILE = "config.yml"


def main():
    # Get source db config & connect
    mysql_config_file = FMT_FILE_MYSQL_CONFIG.format(getpass.getuser())
    mysql_config = sql_utils.get_sql_client_cnf(mysql_config_file)

    db_connection_sql = pymysql.connect(
        host=mysql_config["host"],
        user=mysql_config["user"],
        password=mysql_config["password"],
        db=mysql_config["database"]
    )

    # Get list of collection codes
    with open(FILE_COLLECTION_CODE_LIST) as f:
        collection_names = json.load(f)

    # Connect to mongodb & drop root privileges
    mongo_utils.create_mongo_user(MONGO_CONFIG_FILE, MONGO_ROOT_USER, MONGO_ROOT_PASSWORD)
    mongo_url = mongo_utils.get_mongo_url(MONGO_CONFIG_FILE)
    db_connection_mongo = pymongo.MongoClient(mongo_url)
    mongo_db_scan = db_connection_mongo["scan"]
    mongo_collection_collections = mongo_db_scan["collections"]

    with db_connection_sql.cursor() as cursor:
        for cn in collection_names:
            try:
                cursor.execute(SQL_GET_COLLECTION_COLLID, cn)
                collection = cursor.fetchone()
                if collection is None:
                    print("Could not find collid for {}".format(cn))
                else:
                    collid, collection_code = collection
                    collid = int(collid)
                    collection_obj = {
                        "_id": collid,
                        "name": cn,
                        "collection_code": collection_code
                    }

                    # Add to mongo
                    mongo_collection_collections.insert_one(collection_obj)
            except Exception as e:
                print(e, file=sys.stderr)

        # Create index on idigbio uuid
        mongo_collection_collections.create_index("idigbio_uuid")

    db_connection_mongo.close()
    db_connection_sql.close()


if __name__ == "__main__":
    main()
