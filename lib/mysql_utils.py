import configparser


def get_sql_client_cnf(cnf_path):
    mysql_config_parser = configparser.ConfigParser()
    mysql_config_parser.read(cnf_path)
    return mysql_config_parser["client"]

