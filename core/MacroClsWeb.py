import sys

from service.MacroCls import cls_macro_data
from config.Logger import logger

datalist = [
    {"industryCode": "1103", "industryName": "A股盘面直播"},
    {"industryCode": "1486", "industryName": "回购股票"},
    {"industryCode": "1399", "industryName": "次新股"},
    {"industryCode": "1443", "industryName": "业绩预增"},
    {"industryCode": "1381", "industryName": "停复牌动态"},
    {"industryCode": "1452", "industryName": "股权转让"},
    {"industryCode": "1760", "industryName": "壳资源"},
]

if __name__ == "__main__":
    bMilvus = True if len(sys.argv) < 2 else sys.argv[1] == 'True'
    beginTime = None if len(sys.argv) < 5 else sys.argv[4]  # 开始时间 "2023-08-27 00:00:00"
    endTime = None if len(sys.argv) < 6 else sys.argv[5]  # 结束时间"2023-08-28 00:00:00"
    result_totle = 0
    result_valid_data_total = 0
    result_map = {}

    for data in datalist:
        logger.info(f"开始获取{data['industryCode']}的数据，bMilvus：{bMilvus},beginTime:{beginTime},endTime:{endTime}")
        logger.info(f"一共{len(datalist)}组数据")
        total, valid_data_total = cls_macro_data(bMilvus, data['industryCode'], data['industryName'], beginTime,
                                                 endTime)
        result_totle += total
        result_valid_data_total += valid_data_total
        result_map[data['industryCode']] = f"一共处理了{total}条，有效{valid_data_total}条"

    logger.info(f"本次脚本一共处理了{result_totle}条，有效{result_valid_data_total}条，其中：{result_map}")
