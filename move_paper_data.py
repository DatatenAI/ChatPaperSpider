import os
import asyncio
from peewee import *
import datetime
import logging

logger = logging.getLogger('peewee')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

# 开发的时候取消注释
from dotenv import load_dotenv
load_dotenv()

from modules.database.mysql.db import Paper, PaperInfo, Summaries


def move_paper_data():
    papers = Paper.select()

    for paper in papers:
        paper_info_data = {
            'url': paper.pub_url,
            'pdf_url': paper.pub_url,
            'pdf_hash': paper.pdf_hash,
            'year': paper.year,
            'title': paper.title,
            'venue': paper.venue,
            'conference': paper.conference,
            'url_add_scib': paper.url_add_sclib,
            'bibtex': paper.bibtex,
            'url_scholarbib': paper.url_scholarbib,
            'code': paper.code,
            'eprint_url': paper.eprint_url,
            'num_citations': paper.num_citations,
            'cited_by_url': paper.cited_by_url,
            'url_related_articles': paper.url_related_articles,
            'authors': paper.authors,
            'abstract': paper.abstract,
            'pub_time': paper.pub_date,
            'keywords': paper.keywords,
            'create_time': datetime.datetime.now()
        }
        try:
            _, created_paper_info = PaperInfo.get_or_create(**paper_info_data)
            if created_paper_info:
                logger.info(f'move paper info, title: {paper.title}, pdf_hash: {paper.pdf_hash}')
        except Exception as e:
                logger.error(f"{e}")

        summary_data = {
            'content': paper,
            'create_time': datetime.datetime.now(),
            'pdf_hash': paper.pdf_hash,
            'title': paper.title,
        }
        try:
            _, created_summary = Summaries.get_or_create(**summary_data)
            if created_summary:
                logger.info(f'move summary, title: {paper.title}, pdf_hash: {paper.pdf_hash}')
        except Exception as e:
                logger.error(f"{e}")

if __name__ == "__main__":
    move_paper_data()
