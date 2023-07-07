import asyncio
import datetime
import hashlib
import json
import traceback
from datetime import timedelta
from pydantic import BaseModel, Field, validator
import fc2
import httpx
from loguru import logger
import itertools
from dotenv import load_dotenv
import os

from modules.download.donwload_pdf import download_pdf_from_url
from modules.scripts.bio_wraper import biomedrxivsearch
from modules.scripts.get_arxiv_web import get_all_titles
from modules.utils import ScriptModel, split_list, get_uuid

load_dotenv()

from modules.database.mysql import db

previous_day = 5  # 每次爬取前的2天的

if os.getenv('ENV') == 'DEV':
    load_dotenv()
    is_dev = True
else:
    is_dev = False


async def search_keywords_data(keydata):
    """
    一个线程处理关键词搜索任务
    :param keydata:
    :return:
    """
    current_date = datetime.datetime.now()
    # 减去 n 天
    current_date -= timedelta(days=previous_day)
    current_date = current_date.strftime('%Y-%m-%d %H:%M:%S')
    # year, mon, day = current_date.year, current_date.month, current_date.day

    all_paper = []

    for keyword_short, search_keyword in keydata:
        # 从bioxiv上进行爬取
        try:
            data = await biomedrxivsearch(
                start_date=current_date,
                # start_date=datetime.date(year, 5, 1),
                end_date=datetime.date.today(),
                subjects=[],
                kwd=['domain'] + search_keyword.split(' '),
                # kwd=['domain', 'Single-Cell'],
                kwd_type='all',
                athr=[],
                max_records=50,
                max_time=300)
            if data:
                for onedata in data:
                    data1 = ScriptModel(
                        keyword_short=keyword_short,
                        search_keywords=search_keyword,
                        search_from='Bioxiv',
                        url=onedata['url'],
                        pdf_url=onedata['pdf_url'],
                        pdf_hash='',
                        title=onedata['title'],
                        abstract=onedata['abstract'],
                        authors=[name.strip() for name in onedata['authors'].split(',')],
                        pub_time=onedata['pub_time'],
                        year=onedata['year'],
                        doi=onedata['doi'],
                        related_doi=onedata['related_doi'],
                        cited_by_url=onedata['cited_by_url'],
                        code=onedata['code'],
                        paper_keywords=onedata['paper_keywords'])

                    all_paper.append(data1)
                    logger.info(f"add bioxiv paper search:{search_keyword},url={onedata['url']}")
            else:
                logger.info(f'keywords:{search_keyword} has no new paper in bioxiv')
        except Exception as e:
            logger.error(f'search bioxiv error:{e}')

        # 从arxiv上进行爬取
        try:

            data = await get_all_titles(search_keyword, days=previous_day, max_results=200)
            logger.info(f"end get all titles")
            if data:
                for onedata in data:
                    data1 = ScriptModel(
                        keyword_short=keyword_short,
                        search_keywords=search_keyword,
                        search_from='Arxiv',
                        url=onedata['url'],
                        pdf_url=onedata['pdf_url'],
                        pdf_hash='',
                        title=onedata['title'],
                        abstract=onedata['abstract'],
                        authors=onedata['authors'],
                        pub_time=onedata['submitted_date'],
                        year=onedata['year'],
                        doi=onedata['doi'],
                        related_doi='',
                        cited_by_url='',
                        code='',
                        paper_keywords=onedata['subjects'])
                    all_paper.append(data1)
                    logger.info(f"add arxiv paper search:{search_keyword},url={onedata['url']}")
            else:
                logger.info(f'keywords:{search_keyword} has no new paper in arxiv')
        except Exception as e:
            logger.error(f'search arxiv error:{e}')
    return all_paper


class RequestParams(BaseModel):
    task_id: str = Field(..., description='任务表的task id')
    user_type: str = Field(..., description="用户类型，可选值：user|spider")

    @validator('task_id', 'user_type')
    def validate_required_fields(cls, value):
        if not value:
            raise ValueError("该字段为必填字段")
        return value


async def insert_download_pdf(flat_list):
    for res in flat_list:
        created_task = None
        try:
            # 数据要更新/追加的值
            data = {
                'search_keywords': res.search_keywords,
                'search_from': res.search_from,
                'pdf_url': res.pdf_url
            }

            # 使用get_or_create()方法更新/追加数据

            # 向subscribe_paper_info表写入paper基础信息

            try:
                with db.mysql_db_new.atomic():
                    res_down = await download_pdf_from_url(res.pdf_url, os.getenv('FILE_PATH'), os.environ.get('IMAGE_PATH'))
                    if res_down:  # 如果保存了pdf文件了
                        image_list, file_hash, pages = res_down
                        logger.info(
                            f'search_keywords:{res.search_keywords}, pdf_url: {res.pdf_url}, filename:{file_hash}')
                        data_info = {
                            'url': res.url,
                            'pdf_url': res.pdf_url,
                            'pdf_hash': file_hash,  # 之后需要更改
                            'search_From': res.search_from,
                            'year': res.year,
                            'title': res.title,
                            'code': res.code,
                            'doi': res.doi,
                            'related_doi': res.related_doi,
                            'cited_by_url': res.cited_by_url,
                            'authors': res.authors,
                            'abstract': res.abstract,
                            'img_url': json.dumps(image_list),
                            'eprint_url': res.pdf_url,
                            'pub_time': res.pub_time.strftime('%Y-%m-%d %H:%M:%S'),
                            'paper_keywords': res.paper_keywords,
                            'create_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                        # 添加任务表并传参数
                        data_tasks = {
                            'id': get_uuid(),
                            'type': 'SUMMARY',
                            'tokens': 0,
                            'state': 'RUNNING',
                            'pdf_hash': file_hash,
                            'pages': pages,
                            'language': '中文',
                            'created_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }

                        obj, created_pdf = db.SearchKeyPdf.get_or_create(search_keywords=data['search_keywords'],
                                                                         pdf_url=data['pdf_url'],
                                                                         defaults=data)

                        if created_pdf:  # 如果是新创建的

                            try:
                                obj, created_info = db.PaperInfo.get_or_create(pdf_url=data_info['pdf_url'],
                                                                               defaults=data_info)
                                if created_info:  # 新创建了paper信息
                                    logger.info(f'paper info: {res.pdf_url} 数据已添加')

                                    try:
                                        obj_id, created_task = db.SubscribeTasks.get_or_create(
                                            pdf_hash=data_tasks['pdf_hash'],
                                            type=data_tasks['type'],
                                            language=data_tasks['language'],
                                            defaults=data_tasks)

                                    except Exception as e:
                                        logger.error(f"Failed to create paper task: {repr(e)}")
                                else:
                                    logger.info(f'paper info: {res.pdf_url} 数据已存在，进行更新')
                            except Exception as e:
                                logger.error(f"Failed to create or update PaperInfo: {repr(e)}")
                        else:

                            logger.error(f"save file {res.pdf_url} fail")

            except Exception as e:
                logger.error(f"Error in database transaction: {repr(e)}")
                traceback.print_exc()  # 打印完整的异常堆栈跟踪
        except Exception as e:
            logger.error(f"Error in processing loop: {repr(e)}")
            traceback.print_exc()  # 打印完整的异常堆栈跟踪
        if created_task:  # 创建了任务
            logger.info(
                f"create task {data_tasks['pdf_hash']}, type={data_tasks['type']}, "
                f"language={data_tasks['language']}")
            # 触发任务
            try:
                data_params = RequestParams(
                    task_id=obj_id.id,
                    user_type="spider",
                )

                if is_dev is False:
                    fc_client = fc2.Client(
                        endpoint=os.getenv("FUNCTION_ENDPOINT"),
                        accessKeyID=os.getenv("FUNCTION_ACCESS_KEY_ID"),
                        accessKeySecret=os.getenv("FUNCTION_ACCESS_KEY_SECRET")
                    )
                    task_res = fc_client.invoke_function(
                        os.getenv("FUNCTION_SERVICE_NAME"),
                        os.getenv("FUNCTION_SUMMARY_TASK_NAME"),
                        qualifier='production',
                        payload=data_params.dict(),
                        headers={
                            'x-fc-invocation-type': 'Async',
                            'x-fc-stateful-async-invocation-id': obj_id.id
                        })
                    logger.info(
                        f"invoke subscribe summary task function, id:{obj_id.id},res {task_res.data}")
                else:
                    try:
                        response = httpx.get(os.getenv("FUNCTION_ENDPOINT"),
                                             params=data_params.dict())
                        if response.status_code == 200:
                            logger.info(
                                f"invoke subscribe summary task dev ,res {response.text}")
                        else:
                            # 更改 sub task 状态为 Fail
                            task_obj = db.SubscribeTasks.update(state='FAIL',
                                                                tokens=0,
                                                                finished_at=datetime.datetime.now().strftime(
                                                                    '%Y-%m-%d %H:%M:%S')
                                                                ).where(
                                db.SubscribeTasks.id == obj_id.id).execute()
                            logger.error(
                                f"Fail Subscribe tasks {task_obj}, pdf_hash={file_hash}")
                    except Exception as e:
                        raise Exception(f'summary task start up error:{repr(e)}')

            except Exception as e:
                logger.error(f"{repr(e)}")


async def get_paper_info():
    logger.info("begin search paper")
    query_search_keywords = db.KeywordsTable.select()
    all_keywords = [[keywords.keyword_short, keywords.search_keywords] for keywords in query_search_keywords]

    if len(all_keywords) < 20:
        num_chunks = len(all_keywords)
    else:
        num_chunks = int(len(all_keywords) / 20)

    chunk_keywords = split_list(all_keywords, num_chunks)
    # 创建协程列表，对每个数据进行处理
    tasks = [search_keywords_data(data) for data in chunk_keywords]
    # 使用 asyncio.gather() 并行运行所有协程，并收集结果
    results = await asyncio.gather(*tasks)

    flat_results = list(itertools.chain(*results))

    logger.info(f"end search paper, num {len(flat_results)} new papers find")


    if is_dev:
        flat_results = flat_results[-20:-10]
    # 爬取PDF然后写入数据库
    if flat_results:  # 非空
        if len(flat_results) < 20:
            num_chunks = len(flat_results)
        else:
            num_chunks = int(len(flat_results) / 20)
        chunk_pdf_task = split_list(flat_results, num_chunks)
        pdf_tasks = [insert_download_pdf(data) for data in chunk_pdf_task]
        results = await asyncio.gather(*pdf_tasks)

        logger.info(f"end insert pdf tasks")
    logger.info("finish spider tasks")


if __name__ == "__main__":
    asyncio.run(get_paper_info())
