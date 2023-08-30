# eastmoney个股研报

import datetime
import json
import sys
import time
import urllib.parse

import requests
sys.path.append("..")
from config.common_config import crowBaseUrl
from storage import MilvusStore
from storage.MySqlStore import batchStockInfo
from storage import MongoDbStore
from utils.urlToData import get_text


def eastmoney(code: str, stockName: str, beginTime: str, endTime: str,bStore:bool=True):  # 两个参数分别表示开始读取与结束读取的页码

    # 遍历每一个URL
    total = 0
    pageIndex = 1
    pageSize = 10
    count = 0
    errorList: list = []
    #Yesterday = datetime.date.today() - datetime.timedelta(days=1)
    beginTime = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d") if not beginTime else beginTime
    endTime = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d") if not endTime else endTime
    while count < 5:
        print(f"开始获取第{pageIndex}页数据")
        st = int(round(time.time() * 1000))
        link = f"https://reportapi.eastmoney.com/report/list?industryCode=*&pageSize={pageSize}&industry=*&rating=*&ratingChange=*&beginTime={beginTime}&endTime={endTime}&pageNo={pageIndex}&fields=&qType=0&orgCode=&code={code}&_={st}"

        print(f"link:{link}")  # 用于检查
        crawUrl = f"{crowBaseUrl}&url={urllib.parse.quote(link)}"
        try:
            response = requests.get(crawUrl, verify=False, timeout=30)  # 禁止重定向
            print(response.text)
        except:
            count += 1
            continue
        content = response.text
        # 读取的是json文件。因此就用json打开啦
        data = []
        if not content:
            jsonContent = json.loads(content)
            if "data" in jsonContent:
                data = jsonContent['data']

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
                        "code": code,
                        "name": stockName,
                        "url": url,
                        "date": "" if "publishDate" not in data[i] else data[i]['publishDate'],
                        "type": "eastmoney-stock-report",
                        "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "abstract": abstract,
                        "title": "" if "title" not in data[i] else data[i]['title'],
                        "mediaName": "" if "orgSName" not in data[i] else data[i]['orgSName'],
                        "text": text}
            if text:
                storageList.append(metadata)
            else:
                errorList.append(metadata)

            print(f"第{total}条数据处理完成,数据内容：{json.dumps(metadata, ensure_ascii=False)}")
            print("\n")

        if bStore and len(storageList) > 0:
            # 存入矢量库
            milvusFlag = True
            try:
                MilvusStore.storeData(storageList, f"aifin_stock_{code}")
            except:
                print(f"第{pageIndex}页的数据，大小为{len(data)} 存入矢量库异常")
                milvusFlag = False
            # 存入mongoDB库
            MongoDbStore.storeData(storageList, f"aifin_stock", milvusFlag,err)

        print(f"第{pageIndex}页数据处理完成")
        print("\n")
        pageIndex += 1
        count = 0

    # 异常数据处理
    if bStore and len(errorList) > 0:
        MongoDbStore.storeData(errorList, f"aifin_stock_error", False,err)

    # 日志入库
    content = f"{stockName}-{code}完成了从{beginTime}到{endTime}内的数据，一共处理{total}条数据,异常数据{len(errorList)}条"
    logdata = [{"type": "eastmoney-stock-report",
                "code": code,
                "name": stockName,
                "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "content": content}]
    MongoDbStore.storeData(logdata, f"aifin_logs", False)
    print(content)


if __name__ == "__main__":
    start = sys.argv[1]  # 起始
    offset = sys.argv[2]  # 偏移量
    beginTime = None
    endTime = None
    if len(sys.argv)>3:
    	beginTime = sys.argv[3]  # 开始时间
    	endTime = sys.argv[4]  # 结束时间"2023-08-28"
    # startPage = sys.argv[4]  # 从第几页
    # print(f"参数列表，domain:{domain},code:{code},type:{type},startPage:{startPage}")
    # eastmoney(code, type, int(startPage))
    print(f"参数列表，start:{start}，offset：{offset},beginTime:{beginTime},endTime:{endTime}")
    stockList: list = batchStockInfo(1000, int(start),int(offset))
    if stockList and len(stockList) > 0:
        num = 0
        for stock in stockList:
            num+=1
            print(f"一共获取到了{len(stockList)}支股票，现在处理第{num}个：{stock}")
            eastmoney(stock['stock_code'], stock['securities_name'], beginTime, endTime)
