# ===================个股通联研报==============================
import datetime
import sys



sys.path.append("..")
from storage.MilvusStore import storeMilvusTool
from storage.MongoDbStore import MongoDbStore
from storage.MySqlStore import TlDb
from config.Logger import logger

def stock_tl_report(bMilvus: bool, sec_code: str, beginDateStr: str, endDateStr: str, bStore: bool = True):
    beginDateStr = (datetime.date.today() - datetime.timedelta(days=1)).strftime(
        "%Y-%m-%d") if not beginDateStr else beginDateStr
    endDateStr = (datetime.date.today()).strftime(
        "%Y-%m-%d") if not endDateStr else endDateStr

    tldb = TlDb()
    # （1）获取符合条件的数据总数
    count_sql = f"select count(*) as `count` from rr_main where REPORT_TYPE='公司研究' AND SEC_CODE='{sec_code}' and UPDATE_TIME between CONVERT('{beginDateStr}',DATE) and CONVERT('{endDateStr}',DATE)"
    logger.info(f"count_sql:{count_sql}")
    count_result = tldb.select(count_sql)
    result_count = count_result[0]['count']
    logger.info(f"符合条件的数据有{result_count}条")
    if result_count==0:
        return 0
    # （2）
    total = 0
    startIndex = 0
    offset = 10
    while True:
        currenttime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # （1）根据REPORT_TYPE+SEC_CODE获取

        querysql = "SELECT 'DB' AS source, IFNULL(CAST(rm.REPORT_ID AS CHAR),'') AS uniqueId,IFNULL(rm.SEC_CODE,'') AS code,IFNULL(rm.SEC_NAME,'') AS name ,IFNULL(CAST( rm.WRITE_DATE AS CHAR ),'') AS `date`,'' as url," \
                   f" 'tl-stock-report' as type,'{currenttime}' as createTime, IFNULL(rm.ABSTRACT,'') as abstract,IFNULL(rm.TITLE,'') as title," \
                   "'通联' AS mediaName,ra.ABSTRACT_TEXT AS `text`" \
                   " FROM rr_main rm INNER JOIN rr_abstract ra ON ra.REPORT_ID = rm.REPORT_ID" \
                   f" WHERE rm.REPORT_TYPE = '公司研究' AND rm.SEC_CODE = '{sec_code}' AND ra.ABSTRACT_TEXT IS NOT NULL" \
                   f" AND rm.UPDATE_TIME between CONVERT('{beginDateStr}',DATE) and CONVERT('{endDateStr}',DATE)" \
                   f" LIMIT {startIndex},{offset}"

        logger.info(f"开始执行sql:{querysql}")
        query_result = tldb.select(querysql)

        if len(query_result) == 0:
            logger.info(f"本次【{sec_code}】未找到符合的数据")
            break
        # （2）入库处理
        logger.info(f"本次【{sec_code}】获取到{len(query_result)}条数据")
        if bStore:
            # 存入矢量库
            result_total_list = storeMilvusTool(bMilvus, query_result, f"aifin_stock_{sec_code}")
            # 存入mongoDB库
            MongoDbStore("aifin_stock").storeData(result_total_list).close()

        total += len(query_result)
        # （3）后续处理
        startIndex += offset

    # （4）关闭矢量库
    tldb.close()
    logger.info(f"【{sec_code}】一共处理了{total}条数据")

    return total

# （2）


if __name__ == '__main__':
    beginTime: str = '2022-09-01'
    endTime: str = '2023-09-10'
    bStore: bool = False
    bMilvus: bool = False
    sec_code = '300750'
    stock_tl_report(bMilvus, sec_code, beginTime, endTime, bStore)
