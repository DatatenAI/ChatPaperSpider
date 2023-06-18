import os
from datetime import datetime
from peewee import *

import logging

logger = logging.getLogger('peewee')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

# 开发的时候取消注释
# from dotenv import load_dotenv
# load_dotenv()

mysql_db = MySQLDatabase(os.getenv("MYSQL_DATABASE"), host=os.getenv("MYSQL_HOST"), user=os.getenv("MYSQL_USER"),
                         password=os.getenv("MYSQL_PASSWORD"))
mysql_db_new = MySQLDatabase(os.getenv("MYSQL_DATABASE_NEW"), host=os.getenv("MYSQL_HOST"),
                             user=os.getenv("MYSQL_USER"), password=os.getenv("MYSQL_PASSWORD"))


class BaseModel(Model):
    class Meta:
        database = mysql_db


class BaseModelNew(Model):
    class Meta:
        database = mysql_db_new


class ApiKey(BaseModel):
    id = AutoField()
    apikey = CharField(unique=True)
    is_alive = BooleanField()
    total_amount = DoubleField()
    consumption = DoubleField()

    class Meta:
        table_name = 'apikeys'


class SearchKeys(BaseModelNew):
    id = AutoField()
    keyword_short = CharField()
    search_keywords = CharField()

    class Meta:
        table_name = 'keyword_subscribe_table'



def test():

    query_api_keys = ApiKey.select().order_by(ApiKey.consumption.asc()).where(
        ApiKey.is_alive == True)
    keys = [apikey.apikey for apikey in query_api_keys]
    print(keys)

    query_search_keywords = SearchKeys.select()
    search_keywords = [keywords.search_keywords for keywords in query_search_keywords]
    keyword_short = [keywords.keyword_short for keywords in query_search_keywords]
    print(search_keywords)

if __name__ == "__main__":

    test()

