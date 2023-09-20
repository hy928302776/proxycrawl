import sys

from service.IndustryTlReport import industry_tl_report
from storage.MySqlStore import MainDb
from config.Logger import logger

if __name__ == "__main__":

    bMilvus = True if len(sys.argv) < 2 else sys.argv[1] == 'True'
    start = None if len(sys.argv) < 3 else int(sys.argv[2])
    offset = None if len(sys.argv) < 4 else int(sys.argv[3])
    beginTime = None if len(sys.argv) < 5 else sys.argv[4]  # 开始时间 "2023-08-27"
    endTime = None if len(sys.argv) < 6 else sys.argv[5]  # 结束时间"2023-08-28"

    logger.info(f"参数列表，bMilvus：{bMilvus},start:{start},offset:{offset}，beginTime:{beginTime},endTime:{endTime}")

    industryList: list = MainDb().batchIndustryInfo("申万行业分类", 1, start, offset, )
    if industryList and len(industryList) > 0:
        num = 0
        result_totle = 0
        result_map = {}
        for industry in industryList:
            num += 1
            logger.info(f"一共获取到了{len(industryList)}个行业，现在处理第{num}个：{industry}")
            total = industry_tl_report(bMilvus, industry['industry_code'], industry['industry_name'], num, beginTime,
                                       endTime)
            result_totle += total
            result_map[industry['industry_code']] = f"处理了{total}条数据"
    logger.info(f"一共{len(industryList)}个行业，处理了{result_totle}条数据，其中：{result_map}")
