import datetime
import json
import re
import sys
import time
import urllib
from typing import Dict

import chardet
import requests
from bs4 import BeautifulSoup, Comment

sys.path.append("..")
from config.common_config import crowBaseUrl

analysis_method = [
    {"domain": "http://caifuhao.eastmoney.com/news/",
     "value": [{
         "element": "div",
         "attr": {"class": "article-body"},
     }],
     "extract": ["table"],
     "replace": ["\n\n", "  "],
     "temp": "http://caifuhao.eastmoney.com/news/20230828194502076311480"
     },

    {"domain": "http://blog.eastmoney.com/",
     "value": [{
         "element": "div",
         "attr": {"class": "stockcodec"},
     }],
     "extract": ["table"],
     "replace": ["\n\n", "  ", '</em>', '<em>'],
     "temp": "http://blog.eastmoney.com/k7529386105985542/blog_1345979348.html"
     },
    {"domain": "https://data.eastmoney.com/report/info/",
     "value": [{
         "element": "div",
         "attr": {"class": "newsContent"},
     }],
     "extract": ["table"],
     "replace": ["\n\n", "  "],
     "temp": "https://data.eastmoney.com/report/info/AP000000000334343.html"
     },
    {"domain": "https://data.eastmoney.com/report/zw_industry.jshtml?infocode",
     "value": [{
         "element": "div",
         "attr": {"class": "ctx-content"},
     }],
     "replace": ["\n\n", "  "],
     "temp": "https://data.eastmoney.com/report/zw_industry.jshtml?infocode=AN122334534556"
     },
    {"domain": "http://finance.eastmoney.com/a/",
     "value": [{
         "element": "div",
         "attr": {"class": "txtinfos"},
     }],
     "replace": ["\n\n", "  ", "<em>", "</em>"],
     "temp": "http://finance.eastmoney.com/a/202309052838047556.html"
     },
    {"domain": "https://finance.eastmoney.com/a/",
     "value": [{
         "element": "div",
         "attr": {"class": "txtinfos"},
     }],
     "extract": ["table"],
     "replace": ["\n\n", "  "],
     "temp": "https://finance.eastmoney.com/a/202309052838047556.html"
     },
    {"domain": "http://www.pbc.gov.cn/goutongjiaoliu/113456/113469/index.html",
     "value": [{
         "element": "div",
         "attr": {"class": "txtinfos"},
     }],
     "replace": ["\n\n", "  "],
     "temp": "http://www.pbc.gov.cn/goutongjiaoliu/113456/113469/index.html"
     },
    {"domain": "http://www.stats.gov.cn/sj/zxfb/",
     "value": [{
         "element": "div",
         "attr": {"class": "txt-content"},
     }],
     "extract": ["table"],
     "replace": ["\n\n", "  "],
     "temp": "http://www.stats.gov.cn/sj/zxfb/202309/t20230909_1942695.html"
     },
    {"domain": "http://www.stats.gov.cn/sj/sjjd/",
     "value": [{
         "element": "div",
         "attr": {"class": "txt-content"},
     }],
     "extract": ["table"],
     "replace": ["\n\n", "  "],
     "temp": "http://www.stats.gov.cn/sj/sjjd/202309/t20230909_1942694.html"
     },
    {"domain": "http://stock.eastmoney.com/a/",
     "value": [{
         "element": "div",
         "attr": {"class": "txtinfos"},
     }],
     "extract": ["table"],
     "replace": ["\n\n", "  "],
     "temp": "http://stock.eastmoney.com/a/202309092842368482.html"
     },
    {"domain": "https://www.cls.cn/detail/",
     "value": [
         {
             "element": "div",
             "attr": {"class": "detail-content"},
         }, {
             "element": "div",
             "attr": {"class": "detail-telegraph-content"},
         }
     ],
     "replace": ["\n\n", "  "],
     "temp": "https://www.cls.cn/detail/1459566"
     },
]


def get_analysis_method(url: str) -> Dict:
    for item in analysis_method:
        if url.find(item['domain']) >= 0:
            return item


def get_text(url, useproxy: bool = True, **kwargs):
    text = None
    try:
        item = get_analysis_method(url)
        if item:
            soup = BeautifulSoup(download_page(url, useproxy, **kwargs))

            # （1）过滤不想要的标签,如果有多个标签也可以逗号分割：
            if "extract" in item:
                [s.extract() for s in soup(item['extract'])]

            # （2）去除注释
            comments = soup.findAll(text=lambda text: isinstance(text, Comment))
            [comment.extract() for comment in comments]

            # （3）获取内容
            if "value" in item:
                textlist = []
                for pre_value in item['value']:
                    elementLab = soup.find_all(pre_value['element'], pre_value['attr'])
                    for pre_elementLab in elementLab:
                        textlist.append(pre_elementLab.get_text())
                text = "".join(textlist)
            else:
                text = soup.get_text()

            # （4）替换
            if text and "replace" in item:
                for t in item['replace']:
                    text = text.replace(t, "")

            # （5）删除两个换行符之间的任意数量的空白字符
            text = re.sub(r'\n\s*\n', r'\n\n', text.strip(), flags=re.M)
            return text, None
        else:
            errlog = f"该url【{url}】没有配置内容解析方式"
            print(errlog)
            return text, errlog

    except Exception as e:
        print(f"解析网页内容异常:{e}")
        return text, f"解析网页内容异常:{e}"


def download_page(url: str, useproxy: bool = True, **kwargs):
    if not url or len(url.strip()) == 0:
        return ""
    print(f"url:{url}")
    crawUrl = f"{crowBaseUrl}&url={urllib.parse.quote(url)}" if useproxy else url
    print(f"crawUrl:{crawUrl}")
    starttime = int(time.time() * 1000)
    response = requestUtil(crawUrl, **kwargs)
    print(f"response:{response}，耗时：{int(time.time() * 1000) - starttime}")
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
        print(f"text:{text}")
        return text
    else:
        print("failed to download the page")
        raise Exception(f"获取页面数据异常：{crawUrl}")
        # try:
        #     if response:
        #         print(json.dumps(response))
        #     else:
        #         print("response is None")
        # except:
        #     print("解析response异常")


def requestUtil(link, **kwargs):
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3 import Retry
    # 创建重试策略
    retries_http = Retry(total=10, backoff_factor=5, status_forcelist=[500, 502, 503, 504, 520])
    retries_https = Retry(total=10, backoff_factor=5, status_forcelist=[500, 502, 503, 504, 520])

    # 创建一个带有重试策略的 Session 对象
    session = requests.Session()
    session.proxies

    # 使用不同的重试策略挂载 http:// 和 https://
    session.mount('http://', HTTPAdapter(max_retries=retries_http))
    session.mount('https://', HTTPAdapter(max_retries=retries_https))

    # 发起请求（自动根据协议应用对应的重试策略）
    response = session.get(link, **kwargs)

    return response


if __name__ == '__main__':
    test = download_page(
        "https://np-cnotice-stock.eastmoney.com/api/content/ann?art_code=AN202309071597979547&client_source=web&page_index=1&_=1694897075027",
        False)
    # test, err = get_text("https://www.cls.cn/detail/1459408", False, headers={'user-agent': 'Mozilla/5.0'})
    print(test)
