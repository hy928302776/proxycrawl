# ===================个股公司调研==============================
import datetime
import sys

sys.path.append("..")
from storage import MilvusStore
from storage.MongoDbStore import MongoDbStore
from storage.MySqlStore import TlDb


def stock_tl_survey(bMilvus:bool,sec_code, beginDateStr: str, endDateStr: str,bStore:bool=True):
    beginDateStr = (datetime.date.today() - datetime.timedelta(days=1)).strftime(
        "%Y-%m-%d") if not beginDateStr else beginDateStr
    endDateStr = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d") if not endDateStr else endDateStr
    tldb = TlDb()
    # （1）获取符合条件的数据总数
    count_result = tldb.select(
        f"select count(*) as `count` from equ_is_activity where TICKER_SYMBOL='{sec_code}' and PUBLISH_DATE between CONVERT('{beginDateStr}',DATE) and CONVERT('{endDateStr}',DATE)")
    print(f"符合条件的数据有{count_result[0]['count']}条")
    # （2）
    total = 0
    startIndex = 0
    offset = 5
    while True:
        currenttime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # （1）根据REPORT_TYPE+SEC_CODE获取
        querysql = "SELECT DISTINCT 'DB' AS source,IFNULL(CAST(ea.EVENT_ID AS CHAR),'')  AS uniqueId,IFNULL(ea.TICKER_SYMBOL,'') AS code,IFNULL(ea.SEC_SHORT_NAME,'') AS name,IFNULL(CAST(ea.PUBLISH_DATE AS CHAR),'') AS `date`,'tl-stock-survey' AS type,'' as url,"\
        f" '{currenttime}' AS createTime,SUBSTR(IFNULL(ep.CONTENT,''),0,50) AS abstract,CONCAT(IFNULL(ea.SEC_SHORT_NAME,''),'于',IFNULL(ea.SURVEY_DATE,''),'在', IFNULL(ep.LOCATION,''),'的', IFNULL(ep.ACTIVITY_TYPE,''),'事件') AS title,"\
        " '通联' AS mediaName,ep.CONTENT AS `text`"\
        " FROM equ_is_activity ea INNER JOIN equ_is_participant_qa ep ON ea.EVENT_ID = ep.EVENT_ID"\
        f" WHERE ea.TICKER_SYMBOL = '{sec_code}' AND ep.CONTENT IS NOT NULL AND ea.PUBLISH_DATE BETWEEN CONVERT('{beginDateStr}',DATE) and CONVERT('{endDateStr}',DATE) LIMIT {startIndex},{offset}"\

        print(f"开始执行sql:{querysql}")
        query_result = tldb.select(querysql)

        if len(query_result) == 0:
            print(f"本次【{sec_code}】未找到符合的数据")
            break
        # （2）入库处理
        print(f"本次【{sec_code}】获取到{len(query_result)}条数据")
        if bStore:
            status=-1
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
    beginDateStr: str = '2023-09-01'
    endDateStr: str = '2023-09-16'
    bStore: bool = False
    sec_code = '000069'
    stock_tl_survey(bStore,sec_code, beginDateStr, endDateStr, False)
