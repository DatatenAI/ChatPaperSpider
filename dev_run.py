import asyncio
import datetime
import hashlib
import json
from datetime import timedelta

import fc2
import httpx
from loguru import logger
import itertools
from dotenv import load_dotenv
import os

import redis_manager
from modules.database.mysql.db import SearchKeyPdf, SubscribePaperInfo, Tasks
from modules.scripts.bio_wraper import biomedrxivsearch
from modules.scripts.get_arxiv_web import get_all_titles
from modules.utils import ScriptModel, split_list, get_uuid

load_dotenv()

from modules.database.mysql import db

previous_day = 5  # 每次爬取前的2天的
is_dev = os.getenv("ENV") == 'DEV'

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
                kwd=['domain']+search_keyword.split(' '),
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
                        search_from='arxiv',
                        url=onedata['url'],
                        pdf_url=onedata['pdf_url'],
                        pdf_hash='',
                        title=onedata['title'],
                        abstract=onedata['abstract'],
                        authors=onedata['authors'],
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
            if data:
                for onedata in data:
                    data1 = ScriptModel(
                        keyword_short=keyword_short,
                        search_keywords=search_keyword,
                        search_from='bioxiv',
                        url=onedata['url'],
                        pdf_url=onedata['pdf_url'],
                        pdf_hash='',
                        title=onedata['title'],
                        abstract=onedata['abstract'],
                        authors=','.join(onedata['authors']),
                        pub_time=onedata['submitted_date'],
                        year=onedata['year'],
                        doi=onedata['doi'],
                        related_doi='',
                        cited_by_url='',
                        code='',
                        paper_keywords=','.join(onedata['subjects']))
                    all_paper.append(data1)
                    logger.info(f"add arxiv paper search:{search_keyword},url={onedata['url']}")
            else:
                logger.info(f'keywords:{search_keyword} has no new paper in bioxiv')
        except Exception as e:
            logger.error(f'search bioxiv error:{e}')

    return all_paper





async def get_paper_info():
    logger.info("begin search paper")
    query_search_keywords = db.SearchKeys.select()
    all_keywords = [[keywords.keyword_short, keywords.search_keywords] for keywords in query_search_keywords]

    if len(all_keywords) < 20:
        num_chunks = len(all_keywords)
    else:
        num_chunks = int(len(all_keywords)/20)

    chunk_keywords = split_list(all_keywords, num_chunks)
    # 创建协程列表，对每个数据进行处理
    tasks = [search_keywords_data(data) for data in chunk_keywords]
    # 使用 asyncio.gather() 并行运行所有协程，并收集结果
    results = await asyncio.gather(*tasks)

    logger.info(f"end search paper, num {len(results)} new papers find")

    flat_results = list(itertools.chain(*results))
    if flat_results:     # 非空
        # 将数据存到数据库中，并添加到任务表中
        # 向search_keywords_pdf 中写入查询数据
        for res in flat_results:
            try:
                # 数据要更新/追加的值
                data = {
                    'id': get_uuid(),
                    'search_keywords': res.search_keywords,
                    'search_from': res.search_from,
                    'pdf_url': res.pdf_url
                }

                # 使用get_or_create()方法更新/追加数据
                obj, created = SearchKeyPdf.get_or_create(search_keywords=data['search_keywords'], pdf_url=data['pdf_url'],
                                                          defaults=data)

                if created:
                    logger.info(f'search_keywords:{res.search_keywords}, pdf_url: {res.pdf_url} 数据已追加')

                    # 向subscribe_paper_info表写入paper基础信息
                    # 添加进任务表
                    # 数据要更新/添加的值
                    summary_id = hashlib.md5(res.pdf_url.encode('utf-8')).hexdigest()  # 将pdf url 转为 hash md5 16进制

                    data_info = {
                        'id': get_uuid(),
                        'url': res.url,
                        'pdf_url': res.pdf_url,
                        'pdf_hash': summary_id,     # 之后需要更改
                        'year': res.year,
                        'title': res.title,
                        'code': res.code,
                        'doi': res.doi,
                        'related_doi': res.related_doi,
                        'cited_by_url': res.cited_by_url,
                        'authors': res.authors,
                        'abstract': res.abstract,
                        'img_url': '',
                        'pub_time': res.pub_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'paper_keywords': res.paper_keywords,
                        'create_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }

                    try:
                        # 使用get_or_create()方法更新/添加数据
                        try:
                            obj, created = SubscribePaperInfo.get_or_create(pdf_url=data_info['pdf_url'], defaults=data_info)
                        except Exception as e:
                            logger.error(f"SubscribePaperInfo error: {e}")

                        if created:
                            logger.info(f'paper info: {res.pdf_url} 数据已添加')
                        else:
                            logger.info(f'paper info: {res.pdf_url} 数据已存在，进行更新')

                        # 触发任务
                        try:

                            summary_key = f"subscribe_summary:{summary_id}"
                            await redis_manager.set(summary_key, res.pdf_url)  # summary_id => user_id
                            if is_dev is False:
                                task_id = get_uuid()
                                fc_client = fc2.Client(
                                    endpoint=os.getenv("FUNCTION_ENDPOINT"),
                                    accessKeyID=os.getenv("FUNCTION_ACCESS_KEY_ID"),
                                    accessKeySecret=os.getenv("FUNCTION_ACCESS_KEY_SECRET")
                                )
                                task_res = fc_client.invoke_function(os.getenv("FUNCTION_SERVICE_NAME"),
                                                                     os.getenv("FUNCTION_SUMMARY_TASK_NAME"),
                                                                     qualifier='production',
                                                                     payload=json.dumps({'summary_id': summary_id},
                                                                                        ensure_ascii=False).encode(
                                                                         'utf-8'),
                                                                     headers={
                                                                         'x-fc-invocation-type': 'Async',
                                                                         'x-fc-stateful-async-invocation-id': task_id
                                                                     })
                                logger.info(
                                    f"invoke subscribe summary task function,summary_id:{summary_id} id:{task_id},res {task_res.data}")
                            else:
                                response = httpx.get(os.getenv("FUNCTION_ENDPOINT"), params={'summary_id': summary_id})
                                logger.info(f"invoke subscribe summary task dev ,summary_id:{summary_id},res {response.text}")
                        except Exception as e:
                            logger.error(f"{e}")

                    except Exception as e:
                        logger.error(f"paper {res.pdf_url} add paper info fail,{e}")


                else:
                    logger.info(f'search_keywords:{res.search_keywords}, pdf_url: {res.pdf_url} 数据已存在，进行更新')

            except Exception as e:
                logger.error(f"paper {res.pdf_url} add sql search fail,{e}")

    # 向数据库中写数据


if __name__ == "__main__":
    asyncio.run(get_paper_info())
