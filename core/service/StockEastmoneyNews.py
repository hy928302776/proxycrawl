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
from utils.urlToData import get_text
from storage.MilvusStore import storeMilvusTool
from storage.MongoDbStore import MongoDbStore
from config.Logger import logger

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


def eastmoney_news(bMilvus: bool, code: str, stockName: str,num:int, beginTime: str, endTime: str,
              beStore: bool = True):  # 两个参数分别表示开始读取与结束读取的页码
    domain = "eastmoney-stock-news"
    param_content = htmlcontent[domain]
    if not param_content:
        logger.info(f"该域名数据无法获取，domain:{domain}")
        return

    # 遍历每一个URL
    total = 0
    valid_data_total = 0  # 统计有效数据
    pageIndex = 1
    pageSize = 10
    err_count = 0
    flag = True
    errorList: list = []
    beginTime = (datetime.date.today() - datetime.timedelta(days=1)).strftime(
        "%Y-%m-%d") if not beginTime else beginTime
    endTime = (datetime.date.today()).strftime("%Y-%m-%d") if not endTime else endTime
    while flag and err_count < 5:
        logger.info(f"开始获取第{num}个股票{code}的第{pageIndex}页数据")
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

        logger.info(f"link:{link}")  # 用于检查
        content = download_page(link, beStore)
        #判断查询列异常，重试机制
        if not content or len(content)==0:
            err_count+=1
            continue

        if 'result_re' in param_content:
            content = re.findall(param_content['result_re'], content)[0]
        # 读取的是json文件。因此就用json打开啦
        data = json.loads(content)
        # 找到原始页面中数据所在地
        for pre in param_content['data']:
            data = data[pre]

        logger.info(f"获取第{pageIndex}页的数据，大小为{len(data)}")
        if len(data) == 0:
            break
        storageList: list = []
        for i in range(0, len(data)):
            logger.info("\n---------------------")
            total += 1
            logger.info(f"开始处理第{num}个股票{code}的第{total}条数据：{data[i]}")
            date = data[i]['date']
            s_date = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S').date()
            beginTimeObj = datetime.datetime.strptime(beginTime, "%Y-%m-%d").date()
            if beginTimeObj > s_date:
                logger.info("比开始时间还小，直接结束本次任务")
                flag = False
                break
            endTimeObj = datetime.datetime.strptime(endTime, "%Y-%m-%d").date()
            if endTimeObj < s_date:
                logger.info(f"比结束时间还大，终止并继续下个循环")
                continue


            url = data[i]['url']
            text, err = get_text(url, beStore)
            if err is not None:
                url = f"http://finance.eastmoney.com/a/{data[i]['code']}.html"
                text, err = get_text(url, beStore)

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
            valid_data_total+=1
            if text:
                storageList.append(metadata)
            else:
                errdata = {"err": err}
                errdata.update(metadata)
                errorList.append(errdata)

            new_dict = {key: metadata[key] if key != 'text' else metadata[key][0:1000] for key in metadata.keys()}
            logger.info(f"第{total}条数据处理完成,数据内容：{json.dumps(new_dict, ensure_ascii=False)}")
            logger.info("\n")

        if beStore and len(storageList) > 0:
            # 存入矢量库
            result_total_list = storeMilvusTool(bMilvus, storageList, f"aifin_stock_{code}")
            # 存入mongoDB库
            MongoDbStore("aifin_stock").storeData(result_total_list).close()

        logger.info(f"第{pageIndex}页数据处理完成")
        logger.info("\n")
        pageIndex += 1
        err_count = 0

    content = f"{stockName}-{code}完成了从{beginTime}到{endTime}内的数据，一共处理{total}条数据，有效数据{valid_data_total}条,异常数据{len(errorList)}条"
    logger.info(content)
    if beStore:
        # 异常数据处理
        if len(errorList) > 0:
            MongoDbStore("aifin_stock_error").storeData(errorList, -1).close()

            # 日志入库
        logdata = [{"type": domain,
                    "code": code,
                    "name": stockName,
                    "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "content": content}]
        MongoDbStore("aifin_logs").storeData(logdata, 0).close()


    return total,valid_data_total


if __name__ == "__main__":
    eastmoney_news(False,"300375", "鹏翎股份",1, "2023-09-01", "2023-09-09", False)
