import os
from peewee import *
import datetime
import logging

logger = logging.getLogger('peewee')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

# 开发的时候取消注释
from dotenv import load_dotenv
load_dotenv()

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

# 简写的关键词搜索长关键词
class SearchKeys(BaseModelNew):
    id = AutoField()
    keyword_short = CharField()
    search_keywords = CharField()

    class Meta:
        table_name = 'keyword_subscribe_table'

# 从关键词搜索pdf_url
class SearchKeyPdf(BaseModelNew):
    id = AutoField()
    search_keywords = CharField()
    search_from = CharField()
    pdf_url = CharField()

    class Meta:
        table_name = 'search_keywords_pdf'


class SubscribePaperInfo(BaseModelNew):
    id = AutoField()
    url = CharField()
    pdf_url = CharField()
    pdf_hash = CharField()
    year = IntegerField()
    title = CharField()
    code = CharField()
    doi = CharField()
    related_doi = CharField()
    cited_by_url = CharField()
    authors = CharField()
    abstract = TextField()
    img_url = CharField()
    pub_time = TimeField()
    paper_keywords = CharField()
    create_time = DateTimeField(default=datetime.datetime.now)

    class Meta:
        table_name = 'subscribe_paper_info'

# 任务表
class Tasks(BaseModelNew):
    id = AutoField()
    user_id = CharField()
    pdf_hash = CharField()
    language = CharField()
    type = CharField()
    consume_points = IntegerField()
    state = CharField(choices=('RUNNING', 'SUCCESS', 'FAIL'))
    created_at = DateTimeField(default=datetime.datetime.now)
    finished_at = TimeField()

    class Meta:
        table_name = 'tasks'

# 总结表
class Summaries(BaseModelNew):
    basic_info = CharField()
    briefIntroduction = CharField()
    content = CharField()
    create_time = TimeField()
    first_page_conclusion = CharField()
    id = AutoField()
    language = CharField()
    medium_content = CharField()
    pdf_hash = CharField()
    short_content = CharField()
    short_title = CharField()
    title = CharField()
    title_zh = CharField()
    update_time = TimeField()

    class Meta:
        table_name = 'summaries'


def test():
    # query_api_keys = ApiKey.select().order_by(ApiKey.consumption.asc()).where(
    #     ApiKey.is_alive == True)
    # keys = [apikey.apikey for apikey in query_api_keys]
    # print(keys)
    #
    # query_search_keywords = SearchKeys.select()
    # search_keywords = [keywords.search_keywords for keywords in query_search_keywords]
    # keyword_short = [keywords.keyword_short for keywords in query_search_keywords]
    # print(search_keywords)

    # test tasks
    # 创建任务对象
    task = Tasks(
        user_id='chat-paper',
        pdf_hash='1234567890',
        language='中文',
        type='summary',
        consume_points=0,
        state='RUNNING',
        finished_at=''
    )

    # 保存任务到数据库
    task.save()
    print(task.id)


if __name__ == "__main__":
    test()
