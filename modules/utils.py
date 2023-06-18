from datetime import datetime, timedelta

import dataclasses

@dataclasses.dataclass
class ScriptModel:
    keyword_short: str
    search_keywords: str
    search_from: str    # 从哪里搜索的
    url: str    # 文章页面链接
    pdf_url: str    # pdf 链接
    pdf_hash: str   # 计算的pdf hash
    year: int   # 文章年份
    title: str  # 文章title
    abstract: str   # 摘要
    authors: str    # 作者，用','分隔
    doi: str    #
    pub_time: str
    related_doi: str
    paper_keywords: str
    code: str
    cited_by_url: str


def split_list(lst:list, chunk_size:int):
    """
    将list进行按照chunk_size大小拆分
    :param lst:
    :param chunk_size:
    :return:
    """
    assert len(lst)>0
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def get_previous_dates(n: int = 1):
    current_date = datetime.now()

    # 减去 n 天
    current_date -= timedelta(days=n)

    return current_date


# 测试函数
n = 20
previous_dates = get_previous_dates(n)
print(previous_dates)
