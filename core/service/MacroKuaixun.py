# ==============全球快讯==================
if __name__ == '__main__':
    "https://kuaixun.eastmoney.com/yw.html"

    "https://newsapi.eastmoney.com/kuaixun/v1/getlist_101_ajaxResult_50_1_.html?r=0.030520175844247932&_=1694354042840"
    "https://newsapi.eastmoney.com/kuaixun/v1/getlist_101_ajaxResult_50_2_.html?r=0.7542036687962226&_=1694354249503"

import sys
import uuid
sys.path.append("..")
import datetime
import json
from utils.urlToData import download_page, get_text
from storage import MilvusStore
from storage.MongoDbStore import MongoDbStore


def kuaixun_macro(beginTime: str, endTime: str, bStore: bool = True):  # 两个参数分别表示开始读取与结束读取的页码

    # 遍历每一个URL
    type = "kuaixun"  # 此次查询类型
    total = 0  # 统计总数量
    pageIndex = 1  # 起始业
    err_count = 0  # 统计异常次数
    errorList: list = []  # 统计异常信息

    beginTime = (datetime.date.today() - datetime.timedelta(days=1)).strftime(
        "%Y-%m-%d") if not beginTime else beginTime
    endTime = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d") if not endTime else endTime
    flag = True
    while flag and err_count < 5:

        print(f"开始获取第{pageIndex}页数据")
        ## （1）组装请求路径
        link = f"https://newsapi.eastmoney.com/kuaixun/v1/getlist_101_ajaxResult_10_{pageIndex}_.html"
        print(f"请求路径:{link}")

        # （2）请求列表数据
        list_data = []
        try:
            dataContent_str = download_page(link).replace("var ajaxResult=", "")
            data_Content = json.loads(dataContent_str)
            list_data = data_Content['LivesList']
        except:
            err_count += 1
            print(f"获取第{pageIndex}页数据异常")
            continue
        print(f"获取第{pageIndex}页的数据，大小为{len(list_data)}")
        if len(list_data) == 0:
            break

        # （3）解析单条数据
        storageList: list = []
        for i in range(0, len(list_data)):
            print("\n---------------------")
            total += 1
            element_data = list_data[i]
            print(f"开始处理第{total}条数据：{element_data}")

            # （4）通过日期判断是否符合条件
            showtime = element_data['showtime']
            s_date = datetime.datetime.strptime(showtime, "%Y-%m-%d %H:%M:%S").date()
            beginTimeObj = datetime.datetime.strptime(beginTime, "%Y-%m-%d").date()
            if beginTimeObj > s_date:
                print("比开始时间还小，直接结束本次任务")
                flag = False
                break
            endTimeObj = datetime.datetime.strptime(endTime, "%Y-%m-%d").date()
            if endTimeObj < s_date:
                print(f"比结束时间还大，终止并继续下个循环")
                continue

            # （5）获取摘要内容
            url = element_data['url_unique']
            if not url:
                continue
            newsid = element_data['newsid']
            if not newsid or len(newsid) == 0:
                newsid = str(uuid.uuid4())
            title = element_data['title']
            abstract = element_data['digest']
            text, err = get_text(url)
            if not abstract:
                if text and len(text) > 0:
                    abstract = text[0:100]

            # 数据处理
            metadata = {"source": "Web",
                        "uniqueId": str(newsid),
                        "url": url,
                        "date": showtime,
                        "type": type,
                        "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "abstract": abstract,
                        "title": title,
                        "mediaName": "东方财富网",
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
            except Exception as e:
                print(f"第{pageIndex}页的数据，大小为{len(list_data)} 存入矢量库异常:{e}")
                status = -1
            # 存入mongoDB库
            MongoDbStore("aifin_macro").storeData(storageList, status).close()

        print(f"第{pageIndex}页数据处理完成")
        print("\n")
        pageIndex += 1
        err_count = 0

    content = f"完成了从{beginTime}到{endTime}内的数据，一共处理{total}条数据,异常数据{len(errorList)}条"
    print(content)
    # 异常数据处理
    if bStore:
        if len(errorList) > 0:
            MongoDbStore("aifin_macro_error").storeData(errorList, -1).close()

        # 日志入库

        logdata = [{"type": type,
                    "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "content": content}]
        MongoDbStore("aifin_logs").storeData(logdata, 0).close()


if __name__ == "__main__":
    kuaixun_macro('2023-09-09', '2023-09-09', False)
