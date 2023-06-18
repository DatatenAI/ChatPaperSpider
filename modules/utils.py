from datetime import datetime, timedelta

import dataclasses

@dataclasses.dataclass
class ScriptModel:
    keyword_short: str
    search_keywords: str
    url: str
    pdf_url: str
    pdf_hash: str
    year: int
    title: str
    abstract: str
    authors: str
    doi: str
    pub_time: str
    related_doi: str
    paper_keywords: str
    code: str
    cited_by_url: str




def get_previous_dates(n: int = 1):
    current_date = datetime.now()

    # 减去 n 天
    current_date -= timedelta(days=n)

    return current_date


# 测试函数
n = 20
previous_dates = get_previous_dates(n)
print(previous_dates)
