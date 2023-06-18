import asyncio
import datetime
from datetime import timedelta
from loguru import logger
import itertools
from dotenv import load_dotenv

from modules.scripts.bio_wraper import biomedrxivsearch
from modules.scripts.get_arxiv_web import get_all_titles
from modules.utils import ScriptModel, split_list

load_dotenv()

from modules.database.mysql import db

previous_day = 5  # 每次爬取前的2天的


async def search_keywords_data(keydata):
    """
    一个线程处理关键词搜索任务
    :param keydata:
    :return:
    """
    current_date = datetime.datetime.now()
    # 减去 n 天
    current_date -= timedelta(days=previous_day)
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
    if not flat_results:     # 非空
        # 将数据存到数据库中，并添加到任务表中
        # 向search_keywords_pdf 中写入查询数据



        # 向subscribe_paper_info表写入paper基础信息



        # 向subscribe_paper_summary_task表写入任务信息，并触发任务


                
        pass

    # 向数据库中写数据


    pass


if __name__ == "__main__":
    asyncio.run(get_paper_info())
