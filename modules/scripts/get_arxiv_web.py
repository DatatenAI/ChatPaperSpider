# 导入所需的库
import requests
from bs4 import BeautifulSoup
import datetime
from loguru import logger


# 定义一个函数，根据关键词和页码生成arxiv搜索链接
async def get_url(keyword, page):
    base_url = "https://arxiv.org/search/?"
    params = {
        "query": keyword,
        "searchtype": "all",  # 搜索所有字段
        "abstracts": "show",  # 显示摘要
        "order": "-announced_date_first",  # 按日期降序排序
        "size": 50  # 每页显示50条结果
    }
    if page > 0:
        params["start"] = page * 50  # 设置起始位置
    return base_url + requests.compat.urlencode(params)


# 定义一个函数，根据链接获取网页内容，并解析出论文标题
async def get_titles(url, days, max_results=100, timeout_seconds: float = 10):
    """
        Input:
        - days: 从今天开始，往前推days天，作为搜索的时间范围
        - max_results: 最多返回多少条结果

        Output:
        paper_list: 返回包含论文标题的列表
        paper_dict: 返回包含论文信息的字典
        paper_dict keys:
            title: str, paper title;
            url: str, paper url;
            pdf_url: str, pdf url;
            authors: list, authors of the paper;
            abstract: str, abstract of the paper;
            submitted_date: datetime.datetime, submitted date of the paper;
            year: int, year of the paper;
            subjects: list, subjects of the paper; such as ['Robotics']
            sub_subjects: list, sub_subjects of the paper; such as ['cs.RO']
            doi: str, doi of the paper;
    """

    paper_list = []
    try:
        response = requests.get(url, timeout=timeout_seconds)
        soup = BeautifulSoup(response.text, "html.parser")
        articles = soup.find_all("li", class_="arxiv-result")  # 找到所有包含论文信息的li标签
        today = datetime.date.today()
        last_days = datetime.timedelta(days=days)
        # Rest of your code to process the articles and add them to the paper_list
    except requests.Timeout:
        logger.error("The request timed out.")
        raise requests.Timeout
    except requests.RequestException as e:
        logger.error(f"An error occurred:{repr(e)}")
        raise Exception(f"arxiv get error:{repr(e)}")
    flag = 1
    for article in articles:
        try:
            logger.info(f"soup {flag}/{len(articles)}")
            flag += 1
            paper_dict = {}
            title = article.find("p", class_="title").text  # 找到每篇论文的标题，并去掉多余的空格和换行符
            title = title.strip()
            link = article.find("span").find_all("a")[0].get('href')
            paper_dict['title'] = title
            paper_dict['url'] = link.replace("pdf", "abs")
            authors = article.find("p", class_="authors").text.strip().replace("Authors:\n", "").split(
                ", \n      \n      ")
            paper_dict['authors'] = authors

            abstract_elem = article.find("span", class_="abstract-full")
            if abstract_elem is not None:
                paper_dict['abstract'] = abstract_elem.text.replace("\n        △ Less", '').strip()
            else:
                paper_dict['abstract'] = ""

            date_format = '%d %B, %Y'
            date_str = article.find("p", class_="is-size-7").text.split(";")[0].replace("Submitted ", '')
            date_obj = datetime.datetime.strptime(date_str, date_format)
            paper_dict['submitted_date'] = date_obj
            paper_dict['year'] = date_obj.year

            subjects = article.find_all("span", class_="tag")
            subjects_list = []
            sub_list = []
            for sub in subjects:
                if 'data-tooltip' in sub.attrs:
                    subjects_list.append(sub.attrs['data-tooltip'])
                    sub_list.append(sub.text)
            paper_dict['subjects'] = subjects_list
            paper_dict['sub_subjects'] = sub_list

            paper_dict['pdf_url'] = link + '.pdf'
            doi_links = []
            for link in article.find_all('a', href=True):
                href = link['href']
                if href.startswith('https://doi.org/'):
                    doi_links.append(href)
            if len(doi_links) > 0:
                paper_dict['doi'] = doi_links[0]
            else:
                paper_dict['doi'] = ''
            if today - date_obj.date() <= last_days:
                paper_list.append(paper_dict)
        except Exception as e:
            logger.error(f"arxiv extract html error,url:{article},{repr(e)}")
    return paper_list


# 定义一个函数，根据关键词获取所有可用的论文标题，并打印出来
async def get_all_titles(keyword, days=3, pages=3, max_results=1000):
    logger.info(f"begin get all arxiv titles:{keyword}")
    paper_list = []
    for page in range(pages):
        logger.info(f"begin page:{page + 1}/{pages}")
        url = await get_url(keyword, page)  # 根据关键词和页码生成链接
        temp_paper_list = await get_titles(url, days=days, max_results=max_results)  # 根据链接获取论文标题
        logger.info(f"get {len(temp_paper_list)} papers")
        if not temp_paper_list:  # 如果没有获取到任何标题，说明已经到达最后一页，退出循环
            break
        paper_list.extend(temp_paper_list)  # 将获取到的标题添加到paper_list中
    logger.info(f"end get all arxiv titles:{keyword}")
    return paper_list


async def main():
    # 调用函数，输入你想要搜索的关键词（例如"artificial intelligence"）
    paper_list = await get_all_titles("Graphics", days=2, max_results=200)
    for paper_index, paper in enumerate(paper_list):  # 遍历每个标题，并打印出来
        print('-' * 30)
        print(paper_index, paper['title'])
        for key, value in paper.items():
            print(key, ":\n", value)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
