###################### 获取数据 ###############################################

import datetime
import json
import re
import sys
import time
import urllib.parse

import requests



sys.path.append("..")
from utils.urlToData import download_page
from config.common_config import crowBaseUrl
from config.Logger import logger
from utils.urlToData import get_text
from storage import MilvusStore
from storage.MongoDbStore import MongoDbStore

htmlcontent = {
    "eastmoney-stock-news": {
        "domainurl": "https://search-api-web.eastmoney.com/search/jsonp?cb=jQuery35101521898020840038_1694190946726&_=$st",
        "parse_param": {
            "key": "param",
            "value": '{"uid": "4529014368817886", "keyword": "$code", "type": ["cmsArticleWebOld"], "client": "web", "clientType": "web", "clientVersion": "curr", "param": {"cmsArticleWebOld": {"searchScope": "default", "sort": "time", "pageIndex": $pageIndex, "pageSize": $pageSize, "preTag": "<em>", "postTag": "</em>"}}}',
        },
        "result_re": 'jQuery35101521898020840038_1694190946726\((.*)\)',
        "data": ['result', 'cmsArticleWebOld'],

    }
}


def eastmoney(code: str, stockName: str, beginTime: str, endTime: str,beStore:bool=True):  # 两个参数分别表示开始读取与结束读取的页码
    domain = "eastmoney-stock-news"
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
    beginTime = (datetime.date.today() - datetime.timedelta(days=1)).strftime(
        "%Y-%m-%d") if not beginTime else beginTime
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
        content = download_page(link, beStore)
        if 'result_re' in param_content:
            content = re.findall(param_content['result_re'], content)[0]
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
            text, err = get_text(url,beStore)
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
                errdata = {"err": err}
                errdata.update(metadata)
                errorList.append(errdata)

            print(f"第{total}条数据处理完成,数据内容：{json.dumps(metadata, ensure_ascii=False)}")
            print("\n")

        if len(storageList) > 0:
            # 存入矢量库
            status = 0
            try:
                MilvusStore.storeData(storageList, f"aifin_stock_{code}")
            except Exception as e:
                print(f"第{pageIndex}页的数据，大小为{len(data)} 存入矢量库异常,{e}")
                status = -1
            # 存入mongoDB库
            MongoDbStore("aifin_stock").storeData(storageList, status).close()

        print(f"第{pageIndex}页数据处理完成")
        print("\n")
        pageIndex += 1
        count = 0

        # 异常数据处理
    if len(errorList) > 0:
        MongoDbStore("aifin_stock_error").storeData(errorList, -1).close()

        # 日志入库
    content = f"{stockName}-{code}完成了从{beginTime}到{endTime}内的数据，一共处理{total}条数据,异常数据{len(errorList)}条"
    logdata = [{"type": domain,
                "code": code,
                "name": stockName,
                "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "content": content}]
    MongoDbStore("aifin_logs").storeData(logdata, 0).close()
    print(content)


if __name__ == "__main__":
    eastmoney("300375", "鹏翎股份", "2023-09-01", "2023-09-09",False)
