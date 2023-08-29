###################### 获取数据 ###############################################

import datetime
import json
import re
import sys
import time
import urllib.parse

import requests

sys.path.append("..")
from config.common_config import crowBaseUrl
from storage.MySqlStore import batchStockInfo
from utils.urlToData import get_text
from storage import MongoDbStore, MilvusStore

htmlcontent = {
    "eastmoney-stock-carticle": {
        "domainurl": "https://search-api-web.eastmoney.com/search/jsonp?cb=jQuery35107761762966427765_1687662386467&_=$st",
        "parse_param": {
            "key": "param",
            "value": '{"uid": "4529014368817886", "keyword": "$code", "type": ["article"], "client": "web", "clientType": "web", "clientVersion": "curr", "param": {"article": {"searchScope": "ALL", "sort": "DATE", "pageIndex": $pageIndex, "pageSize": $pageSize, "preTag": "", "postTag": ""}}}',
        },
        "result_re": 'jQuery35107761762966427765_1687662386467\((.*)\)',
        "data": ['result', 'article'],

    }
}


def eastmoney(code: str, stockName: str, beginTime: str, endTime: str):  # 两个参数分别表示开始读取与结束读取的页码
    domain = "eastmoney-stock-carticle"
    param_content = htmlcontent[domain]
    if not param_content:
        print(f"该域名数据无法获取，domain:{domain}")
        return

    # 遍历每一个URL
    total = 0
    pageIndex = 1
    pageSize = 10
    count = 0
    flag = True
    errorList: list = []
    beginTime = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d") if not beginTime else beginTime
    endTime = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d") if not endTime else endTime
    while flag and count < 5:
        print(f"开始获取第{pageIndex}页数据")
        domainurl: str = param_content['domainurl']
        st = int(round(time.time() * 1000))
        domainurl = domainurl.replace("$code", code).replace("$pageIndex", str(pageIndex)).replace("$pageSize",
                                                                                                   str(pageSize)).replace(
            "$st", str(st))
        parse_param = param_content['parse_param']
        link = f"{domainurl}"
        if parse_param:
            key = parse_param['key']
            value: str = parse_param['value']
            value = value.replace("$code", code).replace("$pageIndex", str(pageIndex)).replace("$pageSize",
                                                                                               str(pageSize))
            link = link + "&" + key + "=" + urllib.parse.quote(value)

        print(f"link:{link}")  # 用于检查
        crawUrl = f"{crowBaseUrl}&url={urllib.parse.quote(link)}"
        try:
            response = requests.get(crawUrl, verify=False, timeout=30)  # 禁止重定向
            print(response.text)
        except:
            count += 1
            continue
        content = response.text
        if 'result_re' in param_content:
            content = re.findall(param_content['result_re'], response.text)[0]
        # 读取的是json文件。因此就用json打开啦
        data = json.loads(content)
        # 找到原始页面中数据所在地
        for pre in param_content['data']:
            data = data[pre]

        print(f"获取第{pageIndex}页的数据，大小为{len(data)}")
        if len(data) == 0:
            break
        storageList: list = []
        for i in range(0, len(data)):
            print("\n---------------------")

            date = data[i]['date']
            s_date = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S').date()
            beginTimeObj = datetime.datetime.strptime(beginTime, "%Y-%m-%d").date()
            if beginTimeObj > s_date:
                print("比开始时间还小，直接结束本次任务")
                flag = False
                break
            endTimeObj = datetime.datetime.strptime(endTime, "%Y-%m-%d").date()
            if endTimeObj < s_date:
                print(f"比结束时间还大，终止并继续下个循环")
                continue
            total += 1

            print(f"开始处理第{total}条数据：{data[i]}")
            url = data[i]['url']
            text = get_text(url)
            abstract = data[i]['content'].replace('</em>', '').replace('<em>', '').strip()
            if not abstract or len(abstract) == 0:
                if text and len(text) > 0:
                    abstract = text[0:100]

            metadata = {"source": "Web",
                        "uniqueId": "" if "code" not in data[i] else data[i]['code'],
                        "code": code,
                        "name": stockName,
                        "url": url,
                        "date": date,
                        "type": domain,
                        "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "abstract": abstract,
                        "title": "" if "title" not in data[i] else data[i]['title'].replace('</em>', '').replace('<em>',
                                                                                                                 '').strip(),
                        "mediaName": "" if "nickname" not in data[i] else data[i]['nickname'],
                        "text": text}
            if text:
                storageList.append(metadata)
            else:
                errorList.append(metadata)

            print(f"第{total}条数据处理完成,数据内容：{json.dumps(metadata, ensure_ascii=False)}")
            print("\n")

        if len(storageList) > 0:
            # 存入矢量库
            milvusFlag = True
            try:
                MilvusStore.storeData(storageList, f"aifin_stock_{code}")
            except:
                print(f"第{pageIndex}页的数据，大小为{len(data)} 存入矢量库异常")
                milvusFlag = False
            # 存入mongoDB库
            MongoDbStore.storeData(storageList, f"aifin_stock", milvusFlag)

        print(f"第{pageIndex}页数据处理完成")
        print("\n")
        pageIndex += 1
        count = 0

        # 异常数据处理
    if len(errorList) > 0:
        MongoDbStore.storeData(errorList, f"aifin_stock_error", False)

        # 日志入库
    content = f"{stockName}-{code}完成了从{beginTime}到{endTime}内的数据，一共处理{total}条数据,异常数据{len(errorList)}条"
    logdata = [{"type": domain,
                "code": code,
                "name": stockName,
                "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "content": content}]
    MongoDbStore.storeData(logdata, f"aifin_logs", False)
    print(content)


if __name__ == "__main__":
    current = sys.argv[1]  # 第几页
    beginTime = None
    endTime = None
    if len(sys.argv) > 2:
        beginTime = sys.argv[2]  # 开始时间
        endTime = sys.argv[3]  # 结束时间"2023-08-28"
    # startPage = sys.argv[4]  # 从第几页
    # print(f"参数列表，domain:{domain},code:{code},type:{type},startPage:{startPage}")
    # eastmoney(code, type, int(startPage))
    print(f"参数列表，current:{current},beginTime:{beginTime},endTime:{endTime}")
    stockList: list = batchStockInfo(1000, int(current))
    if stockList and len(stockList) > 0:
        num = 0
        for stock in stockList:
            num += 1
            print(f"一共获取到了{len(stockList)}支股票，现在处理第{num}个：{stock}")
            eastmoney(stock['stock_code'], stock['securities_name'], beginTime, endTime)
