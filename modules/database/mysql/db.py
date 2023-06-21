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
    id = CharField()
    search_keywords = CharField()
    search_from = CharField()
    pdf_url = CharField()

    class Meta:
        table_name = 'search_keywords_pdf'


class PaperInfo(BaseModelNew):
    id = CharField()    # uuid
    url = CharField()   # 文章网页连接
    pdf_url = CharField()   # pdf url
    eprint_url = CharField()   # 预印版pdf url
    pdf_hash = CharField()  # pdf hash
    year = IntegerField()   # 年份
    title = CharField()
    venue = CharField()
    conference = CharField()
    url_add_scib = CharField()
    bibtex = TextField()
    url_scholarbib = CharField()
    code = CharField()
    eprint_url = CharField()
    num_citations = IntegerField()
    cited_by_url = CharField()
    url_related_articles = CharField()  # 相关文章链接
    authors = CharField()
    abstract = TextField()
    img_url = CharField()
    pub_time = DateTimeField()
    keywords = CharField()
    create_time = DateTimeField(default=datetime.datetime.now)
    doi = CharField()

    class Meta:
        table_name = 'paper_info'

# 任务表
class SubscribeTasks(BaseModelNew):
    id = CharField(primary_key=True)
    type = CharField(choices=('SUMMARY', 'TRANSLATE'))
    tokens = IntegerField()
    pages = IntegerField()
    pdf_hash = CharField()
    language = CharField()
    state = CharField(choices=('WAIT', 'RUNNING', 'SUCCESS', 'FAIL'))
    created_at = DateTimeField(default=datetime.datetime.now)
    finished_at = DateTimeField()
    class Meta:
        table_name = 'subscribe_tasks'

# 总结表
class Summaries(BaseModelNew):
    id = CharField()
    basic_info = CharField()
    briefIntroduction = CharField()
    content = CharField()
    create_time = DateTimeField()
    first_page_conclusion = CharField()
    language = CharField()
    medium_content = CharField()
    pdf_hash = CharField()
    short_content = CharField()
    short_title = CharField()
    title = CharField()
    title_zh = CharField()
    update_time = DateTimeField()

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
    data_tasks = {
        'id': '1234',
        'type': 'SUMMARY',
        'tokens': 0,
        'state': 'RUNNING',
        'pdf_hash': '123445',
        'pages': 10,
        'language': '中文',
        'created_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    try:
        obj, created_task = SubscribeTasks.get_or_create(pdf_hash=data_tasks['pdf_hash'],
                                                         type=data_tasks['type'],
                                                         language=data_tasks['language'],
                                                         defaults=data_tasks)
        if created_task:  # 创建了任务
            logger.info(f"create task {data_tasks['pdf_hash']}, type={data_tasks['type']}, "
                        f"language={data_tasks['language']}")
    except Exception as e:
        logger.error(f"{e}")

if __name__ == "__main__":
    test()
