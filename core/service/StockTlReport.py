#===================个股通联研报==============================
import datetime
import sys

sys.path.append("..")
from storage import MilvusStore
from storage.MongoDbStore import MongoDbStore
from storage.MySqlStore import TlDb


def stock_tl_report(bMilvus:bool,sec_code:str, beginTime: str, endTime: str, bStore: bool = True):
    tldb = TlDb()
    # （1）获取符合条件的数据总数
    count_result = tldb.select(
        f"select count(*) as `count` from rr_main where REPORT_TYPE='公司研究' AND SEC_CODE='{sec_code}' and WRITE_DATE between '{beginTime}' and '{endTime}'")
    print(f"符合条件的数据有{count_result[0]['count']}条")
    # （2）
    total = 0
    startIndex = 0
    offset = 10
    while True:
        currenttime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # （1）根据REPORT_TYPE+SEC_CODE获取
        querysql = "SELECT 'DB' as source, REPORT_ID as uniqueId,SEC_CODE as code,SEC_NAME as name,WRITE_DATE as date," \
                   f"'tl-stock-report' as type,'{currenttime}' as createTime, ABSTRACT as abstract,TITLE as title,'通联' as mediaName," \
                   "(SELECT ABSTRACT_TEXT from rr_abstract  ra WHERE ra.REPORT_ID= rm.REPORT_ID ) as `text`" \
                   f" from rr_main  rm where REPORT_TYPE='公司研究' AND SEC_CODE='{sec_code}' and WRITE_DATE between '{beginTime}' and '{endTime}'" \
                   f" LIMIT {startIndex},{offset}"

        print(f"开始执行sql:{querysql}")
        query_result = tldb.select(querysql)

        if len(query_result) == 0:
            print(f"本次【{sec_code}】未找到符合的数据")
            break
        # （2）入库处理
        print(f"本次【{sec_code}】获取到{len(query_result)}条数据")
        if bStore:
            status = -1
            if bMilvus:
                status = 0
                try:
                    MilvusStore.storeData(query_result, f"aifin_stock_{sec_code}")
                except Exception as e:
                    print(f"{sec_code}的数据，大小为{len(query_result)} 存入矢量库异常,{e}")
                    status = -1
            # 存入mongoDB库
            MongoDbStore("aifin_stock").storeData(query_result, status).close()
            print("本次入库完成")

        total += len(query_result)
        # （3）后续处理
        startIndex += offset

    # （4）关闭矢量库
    tldb.close()
    print(f"【{sec_code}】一共处理了{total}条数据")


# （2）


if __name__ == '__main__':
    beginTime: str = '2023-09-01'
    endTime: str = '2023-09-10'
    bStore: bool = False
    sec_code = '002466'
    stock_tl_report(sec_code, beginTime, endTime, bStore)
