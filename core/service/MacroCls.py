# =============财联社宏观数据==============

import datetime
import json
import sys

sys.path.append("..")
from storage import MilvusStore
from storage.MongoDbStore import MongoDbStore
from utils.urlToData import download_page
from utils.urlToData import get_text


def cls_macro_data(industryCode: str, industryName: str, beginTime: str = None, endTime: str = None,
                   bStore: bool = True):  # 两个参数分别表示开始读取与结束读取的页码

    # 遍历每一个URL
    type = "cls_macro"  # 此次查询类型
    total = 0  # 统计总数量
    valid_data_total = 0  # 统计有效数据
    err_count = 0  # 统计异常次数
    errorList: list = []  # 统计异常信息
    # （1）初始化开始时间和结束时间
    beginTimeStamp = int(datetime.datetime.combine((datetime.date.today() - datetime.timedelta(days=1)),
                                                   datetime.time.min).timestamp()) if not beginTime else int(
        datetime.datetime.strptime(beginTime, '%Y-%m-%d %H:%M:%S').timestamp())

    endTimeStamp = int(
        datetime.datetime.combine(datetime.datetime.today(), datetime.time.min).timestamp()) if not endTime else int(
        datetime.datetime.strptime(endTime, '%Y-%m-%d %H:%M:%S').timestamp())
    # （2）循环获取数据列表
    flag = True
    while flag and err_count < 5:
        endTime_str = datetime.datetime.fromtimestamp(endTimeStamp).strftime('%Y-%m-%d %H:%M:%S')
        print(f"开始获取{endTime_str}以来的数据")
        # （3）组装请求路径
        link = f"https://www.cls.cn/api/subject/{industryCode}/article?app=CailianpressWeb&last_article_time={endTimeStamp}&os=web&Subject_Id={industryCode}&sv=7.7.5"
        print(f"link:{link}")  # 用于检查
        # crawUrl = f"{crowBaseUrl}&url={urllib.parse.quote(link)}"
        # （4）获取请求列表数据
        try:
            content = download_page(link, bStore, headers={'user-agent': 'Mozilla/5.0'})
            # response = requests.get(crawUrl, verify=False, timeout=30)  # 禁止重定向
        except:
            err_count += 1
            continue
        # 读取的是json文件。因此就用json打开啦
        jsonContent = json.loads(content)
        data = []
        if "data" in jsonContent:
            data = jsonContent['data']

        print(f"获取了{endTime_str}以来的{len(data)}条数据")
        # （5）解析单条数据
        storageList: list = []
        for i in range(0, len(data)):
            print("\n---------------------")
            total += 1
            element_data = data[i]
            print(f"开始处理第{total}条数据：{element_data}")
            # （6）通过日期判断是否符合条件
            s_datetime = element_data['article_time']

            if beginTimeStamp > s_datetime:
                print("比开始时间还小，直接结束本次任务")
                flag = False
                break
            if endTimeStamp < s_datetime:
                print(f"比结束时间还大，终止并继续下个循环")
                continue

            # （7）获取字段数据数据
            url = f"https://www.cls.cn/detail/{element_data['article_id']}"
            text, err = get_text(url, bStore, headers={'user-agent': 'Mozilla/5.0'})
            abstract = element_data['article_brief']
            if (abstract is None or len(abstract) == 0) and text is not None and len(text) > 0:
                abstract = text[0:100]

            # 数据处理
            metadata = {"source": "Web",
                        "uniqueId": str(element_data['article_id']),
                        "url": url,
                        "date": datetime.datetime.fromtimestamp(s_datetime).strftime('%Y-%m-%d %H:%M:%S'),
                        "type": type,
                        "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "abstract": abstract,
                        "title": element_data['article_title'],
                        "mediaName": "财联社",
                        "text": text}

            if text:
                storageList.append(metadata)
            else:
                errdata = {"err": err}
                errdata.update(metadata)
                errorList.append(errdata)

            # 重置endTimeStamp时间，以循环获取后续的数据
            endTimeStamp = s_datetime
            valid_data_total += 1  # 统计有效数据

            print(f"第{total}条数据处理完成,数据内容：{json.dumps(metadata, ensure_ascii=False)}")
            print("\n")

        if bStore and len(storageList) > 0:
            # 存入矢量库
            status = 0
            try:
                MilvusStore.storeData(storageList, f"aifin_macro")
            except Exception as e:
                print(f"{endTime_str}以来的{len(data)}条数据， 存入矢量库异常:{e}")
                status = -1
            # 存入mongoDB库
            MongoDbStore("aifin_macro").storeData(storageList, status).close()

        print(f"获取{endTime_str}以来的{len(data)}条数据处理完成")
        print("\n")
        # 重置err_count判断条件
        err_count = 0

    beginTime_str = datetime.datetime.fromtimestamp(beginTimeStamp).strftime('%Y-%m-%d %H:%M:%S')
    content = f"完成了从{beginTime_str}到{endTime_str}内的数据，一共处理{total}条数据,有效数据{valid_data_total}条,异常数据{len(errorList)}条"
    print(content)
    # 异常数据处理
    if bStore:
        if len(errorList) > 0:
            MongoDbStore("aifin_stock_error").storeData(errorList, -1).close()

        # 日志入库

        logdata = [{"type": type,
                    "createTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "content": content}]
        MongoDbStore("aifin_logs").storeData(logdata, 0).close()


if __name__ == "__main__":
    datalist = [
        {"industryCode": "1103", "industryName": "A股盘面直播"},
        {"industryCode": "1486", "industryName": "回购股票"},
        {"industryCode": "1399", "industryName": "次新股"},
        {"industryCode": "1443", "industryName": "业绩预增"},
        {"industryCode": "1381", "industryName": "停复牌动态"},
        {"industryCode": "1452", "industryName": "股权转让"},
        {"industryCode": "1760", "industryName": "壳资源"},
    ]
    beginTime = '2023-09-10 00:00:00'
    endTime = '2023-09-11 00:00:00'
    for data in datalist:
        cls_macro_data(data['industryCode'], data['industryName'], beginTime, endTime, False)
