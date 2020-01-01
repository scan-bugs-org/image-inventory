import pymongo
import yaml

try:
    import yaml.CLoader as YamlLoader
except ImportError:
    from yaml import Loader as YamlLoader


def get_mongo_config(config_file):
    with open(config_file) as f:
        mongo_config = yaml.load(f, Loader=YamlLoader)["mongo"]
    return mongo_config


def create_mongo_user(config_file, root_username, root_password):
    mongo_config = get_mongo_config(config_file)
    mongo_url = "mongodb://{}:{}@{}:{}".format(
        root_username,
        root_password,
        mongo_config["host"],
        mongo_config["port"]
    )

    # Create a non-root db user
    db_connection_mongo = pymongo.MongoClient(mongo_url)
    db_connection_mongo[mongo_config["dbName"]].command(
        "createUser",
        mongo_config["user"],
        pwd=mongo_config["password"],
        roles=["readWrite"]
    )
    db_connection_mongo.close()


def get_mongo_url(config_file):
    mongo_config = get_mongo_config(config_file)
    return "mongodb://{}:{}@{}:{}/{}".format(
        mongo_config["user"],
        mongo_config["password"],
        mongo_config["host"],
        mongo_config["port"],
        mongo_config["dbName"]
    )