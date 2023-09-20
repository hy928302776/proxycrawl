# ===================行业通联研报==============================
import datetime
import sys

sys.path.append("..")
from storage.MilvusStore import storeMilvusTool
from storage.MongoDbStore import MongoDbStore
from storage.MySqlStore import TlDb
from config.Logger import logger


def industry_tl_report(bMilvus: bool, industry_code: str, industry_name: str, num: int, beginDateStr: str,
                       endDateStr: str, bStore: bool = True):
    beginDateStr = (datetime.date.today() - datetime.timedelta(days=1)).strftime(
        "%Y-%m-%d") if not beginDateStr else beginDateStr
    endDateStr = (datetime.date.today()).strftime(
        "%Y-%m-%d") if not endDateStr else endDateStr

    tldb = TlDb()
    # （1）获取符合条件的数据总数
    count_sql = f"select count(*) as `count` from rr_main where REPORT_TYPE='行业研究' AND INDUSTRY_L1='{industry_name}' and UPDATE_TIME between CONVERT('{beginDateStr}',DATE) and CONVERT('{endDateStr}',DATE)"
    logger.info(f"count_sql:{count_sql}")
    count_result = tldb.select(count_sql)
    result_count = count_result[0]['count']
    logger.info(f"第{num}个行业符合条件的数据有{result_count}条")
    if result_count == 0:
        return 0
    # （2）
    total = 0
    startIndex = 0
    offset = 1000
    while True:
        currenttime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # （1）根据REPORT_TYPE+SEC_CODE获取

        querysql = f"SELECT 'DB' AS source, IFNULL(CAST(rm.REPORT_ID AS CHAR),'') AS uniqueId,'{industry_code}' AS code,'{industry_name}' AS name ,IFNULL(CAST( rm.WRITE_DATE AS CHAR ),'') AS `date`,'' as url," \
                   f" 'tl-industry-report' as type,'{currenttime}' as createTime, IFNULL(rm.ABSTRACT,'') as abstract,IFNULL(rm.TITLE,'') as title," \
                   "'通联' AS mediaName,ra.ABSTRACT_TEXT AS `text`" \
                   " FROM rr_main rm INNER JOIN rr_abstract ra ON ra.REPORT_ID = rm.REPORT_ID" \
                   f" WHERE rm.REPORT_TYPE = '行业研究' AND rm.INDUSTRY_L1 = '{industry_name}' AND ra.ABSTRACT_TEXT IS NOT NULL" \
                   f" AND rm.UPDATE_TIME between CONVERT('{beginDateStr}',DATE) and CONVERT('{endDateStr}',DATE)" \
                   f" LIMIT {startIndex},{offset}"

        logger.info(f"开始执行sql:{querysql}")
        query_result = tldb.select(querysql)

        if len(query_result) == 0:
            logger.info(f"本次【{industry_code}】未找到符合的数据")
            break
        # （2）入库处理
        logger.info(f"本次【{industry_code}】获取到{len(query_result)}条数据")
        if bStore:
            # 存入矢量库
            result_total_list = storeMilvusTool(bMilvus, query_result, f"aifin_industry_{industry_code}")
            # 存入mongoDB库
            MongoDbStore("aifin_industry").storeData(result_total_list).close()

            logger.info("本次入库完成")

        total += len(query_result)
        # （3）后续处理
        startIndex += offset

    # （4）关闭矢量库
    tldb.close()
    logger.info(f"第{num}个行业【{industry_code}】一共处理了{total}条数据")

    return total


# （2）


if __name__ == '__main__':
    beginTime: str = '2022-09-01'
    endTime: str = '2023-09-10'
    bStore: bool = False
    bMilvus: bool = False
    industry_code = '01032116'
    industry_name = '轻工制造'
    industry_tl_report(bMilvus, industry_code, industry_name, 1, beginTime, endTime, bStore)
