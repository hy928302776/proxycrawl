###################### 个股公告-东方财富 ###############################################

import datetime
import json
import re
import sys
import time
import urllib.parse


sys.path.append("..")
from utils.urlToData import download_page
from storage import MilvusStore
from storage.MongoDbStore import MongoDbStore

htmlcontent = {
    "eastmoney-stock-notice": {
        "domainurl": "https://search-api-web.eastmoney.com/search/jsonp?cb=jQuery3510756880723959142_1694537051337&_=$st",
        "parse_param": {
            "key": "param",
            "value": '{"uid": "4529014368817886", "keyword": "$code", "type": ["noticeWeb"], "client": "web", "clientType": "web", "clientVersion": "curr", "param": {"noticeWeb": {"pageIndex": $pageIndex, "pageSize": $pageSize, "preTag": "<em>", "postTag": "</em>"}}}',
        },
        "result_re": 'jQuery3510756880723959142_1694537051337\((.*)\)',
        "data": ['result', 'noticeWeb'],

    }
}


def east_notice(bMilvus:bool,code: str, stockName: str, beginTime: str, endTime: str, beStore: bool = True):  # 两个参数分别表示开始读取与结束读取的页码
    type = "eastmoney-stock-notice"
    param_content = htmlcontent[type]
    if not param_content:
        print(f"该域名数据无法获取，type:{type}")
        return

    # 遍历每一个URL
    total = 0
    valid_data_total = 0  # 统计有效数据
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
            value = value.replace("$code", stockName).replace("$pageIndex", str(pageIndex)).replace("$pageSize",
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
            link = f"https://np-cnotice-stock.eastmoney.com/api/content/ann?art_code={data[i]['code']}&client_source=web&page_index=1&_={st}"
            text_data = download_page(link, beStore)
            text = ""
            if text_data:
                pre_data = json.loads(text_data)
                if 'data' in pre_data and 'notice_content' in pre_data['data']:
                    text = pre_data['data']['notice_content']
            # （5）删除两个换行符之间的任意数量的空白字符
            text = re.sub(r'\n\s*\n', r'\n\n', text.strip(), flags=re.M)
            text = text.replace("\n\n", "").replace("  ", "")
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
                        "type": type,
                        "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "abstract": abstract,
                        "title": "" if "title" not in data[i] else data[i]['title'].replace('</em>', '').replace('<em>',
                                                                                                                 '').strip(),
                        "mediaName": "东方财富" if "nickname" not in data[i] else data[i]['nickname'],
                        "text": text}
            if text:
                storageList.append(metadata)
            else:
                errdata = {"err": "文本解析为空"}
                errdata.update(metadata)
                errorList.append(errdata)
            valid_data_total+=1
            print(f"第{total}条数据处理完成,数据内容：{json.dumps(metadata, ensure_ascii=False)}")
            print("\n")

        if beStore and len(storageList) > 0:
            status = -1
            if bMilvus:
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

    content = f"{stockName}-{code}完成了从{beginTime}到{endTime}内的数据，一共处理{total}条数据,异常数据{len(errorList)}条"
    print(content)
    if beStore:
        # 异常数据处理
        if len(errorList) > 0:
            MongoDbStore("aifin_stock_error").storeData(errorList, -1).close()

            # 日志入库

        logdata = [{"type": type,
                    "code": code,
                    "name": stockName,
                    "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "content": content}]
        MongoDbStore("aifin_logs").storeData(logdata, 0).close()


if __name__ == "__main__":
    east_notice("300375", "鹏翎股份", "2023-09-1", "2023-09-13", False)
