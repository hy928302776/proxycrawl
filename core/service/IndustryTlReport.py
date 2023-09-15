#===================行业通联研报==============================
import datetime
import sys

sys.path.append("..")
from storage import MilvusStore
from storage.MongoDbStore import MongoDbStore
from storage.MySqlStore import TlDb


def industry_tl_report(industry_code:str,industry_name:str, beginTime: str, endTime: str, bStore: bool = True):
    tldb = TlDb()
    # （1）获取符合条件的数据总数
    count_result = tldb.select(
        f"select count(*) as `count` from rr_main where REPORT_TYPE='行业研究' AND INDUSTRY_L1='{industry_name}' and WRITE_DATE between '{beginTime}' and '{endTime}'")
    print(f"符合条件的数据有{count_result[0]['count']}条")
    # （2）
    total = 0
    startIndex = 0
    offset = 10
    while True:
        currenttime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # （1）根据REPORT_TYPE+SEC_CODE获取
        querysql = "SELECT 'DB' as source, REPORT_ID as uniqueId,WRITE_DATE as date," \
                   f"'tl-industry-report' as type,'{currenttime}' as createTime, ABSTRACT as abstract,TITLE as title,'通联' as mediaName," \
                   "(SELECT ABSTRACT_TEXT from rr_abstract  ra WHERE ra.REPORT_ID= rm.REPORT_ID ) as `text`" \
                   f" from rr_main  rm where REPORT_TYPE='行业研究' AND INDUSTRY_L1='{industry_name}' and WRITE_DATE between '{beginTime}' and '{endTime}'" \
                   f" LIMIT {startIndex},{offset}"

        print(f"开始执行sql:{querysql}")
        query_result = tldb.select(querysql)

        if len(query_result) == 0:
            print(f"本次【{industry_code}】未找到符合的数据")
            break
        # （2）入库处理
        print(f"本次【{industry_code}】获取到{len(query_result)}条数据")
        if bStore:
            status = 0
            try:
                MilvusStore.storeData(query_result, f"aifin_industry_{industry_code}")
            except Exception as e:
                print(f"{industry_code}的数据，大小为{len(query_result)} 存入矢量库异常,{e}")
                status = -1
            # 存入mongoDB库
            MongoDbStore("aifin_industry").storeData(query_result, status).close()
            print("本次入库完成")

        total += len(query_result)
        # （3）后续处理
        startIndex += offset

    # （4）关闭矢量库
    tldb.close()
    print(f"【{industry_code}】一共处理了{total}条数据")


# （2）


if __name__ == '__main__':
    beginTime: str = '2023-09-10'
    endTime: str = '2023-09-10'
    bStore: bool = False
    industry_code = '002466'
    industry_name = '行业名称'
    industry_tl_report(industry_code,industry_name, beginTime, endTime, bStore)
