import asyncio
import math
import pandas as pd
import datetime
import time
import sys
import string
import gc
import requests
from bs4 import BeautifulSoup as bs
from loguru import logger

async def biomedrxivsearch(start_date=datetime.date.today().replace(day=2),
                     end_date=datetime.date.today(),
                     subjects=[],
                     kwd=[],
                     kwd_type='all',
                     athr: list = [],
                     max_records=75,
                     max_time=300,
                     cols: list = ['abstract', 'title', 'authors', 'pub_time', 'year', 'url', 'pdf_url',
                                   'code', 'doi', 'related_doi', 'cited_by_url', 'paper_keywords']
                     ):
    """
    Input:
    - start_date, end_date: datetime.date, the start and end time for paper to search
    - subjects: the subjects of paper to be searched, if None, use []
    - kwd: the key words to be searched, if None, use []
    - kwd_type: all or any
    - athr: the authors of paper to be searched, if None, use []
    - max_records, max_time
    - cols: ouput column name

    Output: The needed search results. [{col1: content}]

    How to use:

    records_df1 = biomedrxivsearch(
        start_date = datetime.date(2023,5,1), 
        end_date = datetime.date.today(), 
        subjects = [], 
        kwd = ['domain', 'Single-Cell'], 
        kwd_type = 'all',
        athr = [], 
        max_records = 50,
        max_time = 300)
    """
    logger.info(f"begin search bioxiv:{kwd}")

    overall_time = time.time()

    BASE = 'http://{:s}.org/search/'.format('biorxiv')
    url = BASE

    start_date = str(start_date)
    end_date = str(end_date)
    kwd_type = kwd_type.lower()

    journal_str = 'jcode%3A' + 'biorxiv'
    url += journal_str

    if len(subjects) > 0:
        first_subject = ('%20').join(subjects[0].split())
        subject_str = 'subject_collection_code%3A' + first_subject
        for subject in subjects[1:]:
            subject_str = subject_str + '%2C' + ('%20').join(subject.split())
        url += '%20' + subject_str

    if len(kwd) > 0:
        kwd_str = 'abstract_title%3A' + \
                  ('%252C%2B').join(
                      [kwd[0]] + [('%2B').join(keyword.split()) for keyword in kwd[1:]])
        kwd_str = kwd_str + '%20abstract_title_flags%3Amatch-' + kwd_type
        url += '%20' + kwd_str

    if len(athr) == 1:
        athr_str = 'author1%3A' + ('%2B').join(athr[0].split())
        url += '%20' + athr_str
    if len(athr) == 2:
        athr_str = 'author1%3A' + \
                   ('%2B').join(athr[0].split()) + \
                   '%20author2%3A' + ('%2B').join(athr[1].split())
        url += '%20' + athr_str

    date_str = 'limit_from%3A' + start_date + '%20limit_to%3A' + end_date
    url += '%20' + date_str

    num_page_results = 75
    url += '%20numresults%3A' + \
           str(num_page_results) + '%20format_result%3Acondensed' + \
           '%20sort%3Arelevance-rank'

    titles = []
    author_lists = []
    urls = []
    dates = []
    years = []
    codes = []
    pdf_urls = []
    dois = []

    page = 0

    while True:
        if page == 0:
            url_response = requests.post(url)
            html = bs(url_response.text, features='html.parser')
            num_results_text = html.find(
                'div', attrs={'class': 'highwire-search-summary'}).text.strip().split()[0]
            if num_results_text == 'No':
                return ()

            num_results = int(num_results_text)
            num_fetch_results = min(max_records, num_results)

        else:
            page_url = url + '?page=' + str(page)
            url_response = requests.post(page_url)
            html = bs(url_response.text, features='html.parser')

        articles = html.find_all(attrs={'class': 'search-result'})
        dois += [article.find_all('span', attrs={'class': 'highwire-cite-metadata-doi highwire-cite-metadata'})[
                     0].text.strip()[5:] for article in articles]
        titles += [article.find('span', attrs={'class': 'highwire-cite-title'}).text.strip() if article.find(
            'span', attrs={'class': 'highwire-cite-title'}) is not None else None for article in articles]
        author_lists += [",".join([author.text for author in article.find_all(
            'span', attrs={'class': 'highwire-citation-author'})]) for article in articles]

        urls += ['http://www.{:s}.org'.format('biorxiv') + article.find(
            'a', href=True)['href'] for article in articles]
        pdf_urls += [url + '.full.pdf' for url in urls]
        dates += [('-').join(article.find('div', attrs={'class': 'hw-make-citation'}).get(
            'data-encoded-apath').strip().split(';')[-1].split('.')[0:3]) for article in articles]
        years += [tim[:4] for tim in dates]
        codes += ["" for tim in dates]
        if time.time() - overall_time > max_time or (page + 1) * num_page_results >= num_fetch_results:
            break

        page += 1

    records_data = list(zip(*list(map(lambda dummy_list: dummy_list[0:num_fetch_results], [
        titles, author_lists, urls, dates, years, pdf_urls, codes, dois, dois, urls]))))
    full_records_df = pd.DataFrame(records_data, columns=[
        'title', 'authors', 'url', 'pub_time', 'year', 'pdf_url', 'code', 'doi', 'related_doi', 'cited_by_url'])

    absts = []
    keys = []
    for paper_url in full_records_df.url:
        bsoup = bs(requests.post(paper_url).text, features='html.parser')
        absts.append(bsoup.find('div', attrs={'class': 'section abstract'}).text.replace(
            'Abstract', '').replace('\n', ''))
        keys.append(bsoup.find('span', attrs={
            'class': "highwire-article-collection-term"}).text[:-1])
    full_records_df['abstract'] = absts
    full_records_df['paper_keywords'] = keys

    records_df = full_records_df[cols]
    logger.info(f"end search bioxiv:{kwd}")
    return records_df.to_dict('records')


async def test():
    data = await biomedrxivsearch(
        start_date=datetime.date(2023, 5, 14),
        end_date=datetime.date.today(),
        subjects=[],
        kwd=['domain', 'C.elegans'],
        kwd_type='all',
        athr=[],
        max_records=50,
        max_time=300)
    print(data)


if __name__ == "__main__":
    asyncio.run(test())

