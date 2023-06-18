import asyncio
import datetime
from datetime import timedelta
from loguru import logger

from dotenv import load_dotenv

from modules.scripts.bio_wraper import biomedrxivsearch
from modules.utils import ScriptModel

load_dotenv()

from modules.database.mysql import db

previous_day = 2  # 每次爬取前的2天的


async def get_paper_info():
    query_search_keywords = db.SearchKeys.select()
    search_keywords = [keywords.search_keywords for keywords in query_search_keywords]
    keywords_short = [keywords.keyword_short for keywords in query_search_keywords]

    current_date = datetime.datetime.now()
    # 减去 n 天
    current_date -= timedelta(days=previous_day)

    year, mon, day = current_date.year, current_date.month, current_date.day

    all_bioxiv_paper = []
    for keyword_short, search_keyword in zip(keywords_short, search_keywords):
        try:
            data = await biomedrxivsearch(
                # start_date=datetime.date(year, mon, day),
                start_date=datetime.date(year, 5, 1),
                end_date=datetime.date.today(),
                subjects=[],
                # kwd=['domain', search_keyword],
                kwd=['domain', 'Single-Cell'],
                kwd_type='all',
                athr=[],
                max_records=50,
                max_time=300)
        except Exception as e:
            logger.error(f'search bioxiv error:{e}')
        if data:
            for onedata in data:
                data1 = ScriptModel(
                    keyword_short=keyword_short,
                    search_keywords=search_keyword,
                    url=onedata['url'],
                    pdf_url=onedata['pdf_url'],
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
                all_bioxiv_paper.append(data1)
        else:
            logger.info(f'keywords:{search_keyword} has no new paper in bioxiv')




    pass


if __name__ == "__main__":
    asyncio.run(get_paper_info())
