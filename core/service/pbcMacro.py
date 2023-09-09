#============  中国人民银行新闻===========
# eastmoney个股研报

import datetime
import json
import sys
import time
import urllib.parse

import requests
from bs4 import BeautifulSoup

from core.utils.urlToData import download_page

sys.path.append("..")
from config.common_config import crowBaseUrl
from storage import MilvusStore
from storage.MySqlStore import batchStockInfo
from storage import MongoDbStore
from utils.urlToData import get_text


def pbcMacro(beginTime: str, endTime: str, bStore: bool = True):  # 两个参数分别表示开始读取与结束读取的页码

    # 遍历每一个URL
    type = "pbc_macro"
    total = 0
    pageIndex = 1
    count = 0
    errorList: list = []
    # Yesterday = datetime.date.today() - datetime.timedelta(days=1)
    beginTime = (datetime.date.today() - datetime.timedelta(days=1)).strftime(
        "%Y-%m-%d") if not beginTime else beginTime
    endTime = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d") if not endTime else endTime
    while count < 5:
        print(f"开始获取第{pageIndex}页数据")
        link = f"http://www.pbc.gov.cn/goutongjiaoliu/113456/113469/11040/index{pageIndex}.html"

        print(f"link:{link}")  # 用于检查
        crawUrl = f"{crowBaseUrl}&url={urllib.parse.quote(link)}"
        try:
            soup = BeautifulSoup(download_page(link),"html.parser")
            news_list = soup.find_all("li", class_="clearfix")


        except:
            count += 1
            continue
        # 读取的是json文件。因此就用json打开啦
        data = []

        print(f"获取第{pageIndex}页的数据，大小为{len(data)}")
        if len(data) == 0:
            break
        storageList: list = []
        for i in range(0, len(data)):
            print("\n---------------------")
            total += 1
            print(f"开始处理第{total}条数据：{data[i]}")
            url = f"https://data.eastmoney.com/report/info/{data[i]['infoCode']}.html"

            text, err = get_text(url)
            if text:
                abstract = ""
                if text and len(text) > 0:
                    abstract = text[0:100]

            # 数据处理
            metadata = {"source": "Web",
                        "uniqueId": "" if "infoCode" not in data[i] else data[i]['infoCode'],
                        "url": url,
                        "date": "" if "publishDate" not in data[i] else data[i]['publishDate'],
                        "type": type,
                        "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "abstract": abstract,
                        "title": "" if "title" not in data[i] else data[i]['title'],
                        "mediaName": "" if "orgSName" not in data[i] else data[i]['orgSName'],
                        "text": text}
            if text:
                storageList.append(metadata)
            else:
                errdata = {"err": err}
                errdata.update(metadata)
                errorList.append(errdata)

            print(f"第{total}条数据处理完成,数据内容：{json.dumps(metadata, ensure_ascii=False)}")
            print("\n")

        if bStore and len(storageList) > 0:
            # 存入矢量库
            status = 0
            try:
                MilvusStore.storeData(storageList, "aifin_macro")
            except:
                print(f"第{pageIndex}页的数据，大小为{len(data)} 存入矢量库异常")
                status = -1
            # 存入mongoDB库
            MongoDbStore.storeData(storageList, f"aifin_macro", status)

        print(f"第{pageIndex}页数据处理完成")
        print("\n")
        pageIndex += 1
        count = 0

    # 异常数据处理
    if bStore:
        if len(errorList) > 0:
        	MongoDbStore.storeData(errorList, f"aifin_stock_error", 3)

        # 日志入库
        content = f"完成了从{beginTime}到{endTime}内的数据，一共处理{total}条数据,异常数据{len(errorList)}条"
        logdata = [{"type": type,
                    "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "content": content}]
        MongoDbStore.storeData(logdata, f"aifin_logs", 0)
        print(content)


if __name__ == "__main__":
    pbcMacro(None, None)
