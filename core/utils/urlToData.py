from typing import Dict

import chardet
import requests
from bs4 import BeautifulSoup

analysis_method = [
    {"domain": "http://caifuhao.eastmoney.com/news/",
     "value": {
         "element": "div",
         "attr": {"class": "article-body"},
     },
     "replace": ["\n\n", "  "],
     "temp": "http://caifuhao.eastmoney.com/news/20230828194502076311480"
     },

    {"domain": "http://blog.eastmoney.com/",
     "value": {
         "element": "div",
         "attr": {"class": "stockcodec"},
     },
     "replace": ["\n\n", "  ", '</em>', '<em>'],
     "temp": "http://blog.eastmoney.com/k7529386105985542/blog_1345979348.html"
     },
    {"domain": "https://data.eastmoney.com/report/info/",
     "value": {
         "element": "div",
         "attr": {"class": "newsContent"},
     },
     "replace": ["\n\n", "  "],
     "temp": "https://data.eastmoney.com/report/info/AP000000000334343.html"
     },
    {"domain": "https://data.eastmoney.com/report/zw_industry.jshtml?infocode",
     "value": {
         "element": "div",
         "attr": {"class": "ctx-content"},
     },
     "replace": ["\n\n", "  "],
     "temp": "https://data.eastmoney.com/report/zw_industry.jshtml?infocode=AN122334534556"
     },
]


def get_analysis_method(url: str) -> Dict:
    for item in analysis_method:
        if url.find(item['domain']) >= 0:
            return item


def get_text(url):
    try:
        item = get_analysis_method(url)
        if item:
            soup = BeautifulSoup(download_page(url))
            text = None
            # 获取内容
            if "value" in item:
                all_comments = soup.find_all(item['value']['element'], item['value']['attr'])
                if all_comments and len(all_comments) > 0:
                    text = all_comments[0].get_text()
            else:
                text = soup.get_text()
            # 替换
            if text and "replace" in item:
                for t in item['replace']:
                    text = text.replace(t, "")
            return text
    except Exception as e:
        print(f"解析网页内容异常:{e}")


def download_page(url: str):
    if not url or len(url.strip()) == 0:
        return ""

    response = requests.get(url)
    print(f"response:{response}")
    if response.status_code == 200:
        # 以下为乱码异常处理
        try:
            code1 = chardet.detect(response.content)['encoding']
            text = response.content.decode(code1)
        except:
            code = response.encoding
            try:
                text = response.text.encode(code).decode('utf-8')
            except:
                try:
                    text = response.text.encode(code).decode('gbk')
                except:
                    text = response.text
        return text
    else:
        print("failed to download the page:")


if __name__ == '__main__':
    text = get_text("http://caifuhao.eastmoney.com/news/20230829201224487514490")
    print(text)
