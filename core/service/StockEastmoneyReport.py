# eastmoney个股研报

import datetime
import json
import sys
import time
import urllib.parse

import requests



sys.path.append("..")
from config.common_config import crowBaseUrl
from storage.MilvusStore import storeMilvusTool
from storage.MongoDbStore import MongoDbStore
from utils.urlToData import get_text
from config.Logger import logger

def eastmoney(bMilvus:bool,code: str, stockName: str, beginTime: str, endTime: str, bStore: bool = True):  # 两个参数分别表示开始读取与结束读取的页码
    type = "eastmoney-stock-report"
    # 遍历每一个URL
    total = 0
    pageIndex = 1
    pageSize = 10
    count = 0
    errorList: list = []
    # Yesterday = datetime.date.today() - datetime.timedelta(days=1)
    beginTime = (datetime.date.today() - datetime.timedelta(days=1)).strftime(
        "%Y-%m-%d") if not beginTime else beginTime
    endTime = (datetime.date.today()).strftime("%Y-%m-%d") if not endTime else endTime
    while count < 5:
        logger.info(f"开始获取第{pageIndex}页数据")
        st = int(round(time.time() * 1000))
        link = f"https://reportapi.eastmoney.com/report/list?industryCode=*&pageSize={pageSize}&industry=*&rating=*&ratingChange=*&beginTime={beginTime}&endTime={endTime}&pageNo={pageIndex}&fields=&qType=0&orgCode=&code={code}&_={st}"

        logger.info(f"link:{link}")  # 用于检查
        crawUrl = f"{crowBaseUrl}&url={urllib.parse.quote(link)}"
        try:
            response = requests.get(crawUrl, verify=False, timeout=30)  # 禁止重定向
            logger.info(response.text)
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

        logger.info(f"获取第{pageIndex}页的数据，大小为{len(data)}")
        if len(data) == 0:
            break
        storageList: list = []
        for i in range(0, len(data)):
            logger.info("\n---------------------")
            total += 1
            logger.info(f"开始处理第{total}条数据：{data[i]}")
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

            new_dict = {key: metadata[key] if key != 'text' else metadata[key][0:1000] for key in metadata.keys()}
            logger.info(f"第{total}条数据处理完成,数据内容：{json.dumps(new_dict, ensure_ascii=False)}")
            logger.info("\n")

        if bStore and len(storageList) > 0:
            # 存入矢量库
            result_total_list = storeMilvusTool(bMilvus, storageList, f"aifin_stock_{code}")
            # 存入mongoDB库
            MongoDbStore("aifin_stock").storeData(result_total_list).close()

        logger.info(f"第{pageIndex}页数据处理完成")
        logger.info("\n")
        pageIndex += 1
        count = 0

    # 异常数据处理
    if bStore and len(errorList) > 0:
        MongoDbStore("aifin_stock_error").storeData(errorList, -1).close()

    # 日志入库
    content = f"{stockName}-{code}完成了从{beginTime}到{endTime}内的数据，一共处理{total}条数据,异常数据{len(errorList)}条"
    logdata = [{"type": "eastmoney-stock-report",
                "code": code,
                "name": stockName,
                "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "content": content}]
    MongoDbStore("aifin_logs").storeData(logdata, 0).close()
    logger.info(content)


if __name__ == "__main__":

    eastmoney(False,"300375", "宁德时代", None, None,False)
