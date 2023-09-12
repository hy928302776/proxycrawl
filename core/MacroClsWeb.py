import sys

from core.service.MacroCls import cls_macro_data

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
    beginTime = None
    endTime = None
    if len(sys.argv) > 1:
        beginTime = sys.argv[1]  # 开始时间 "2023-08-27 00:00:00"
        endTime = sys.argv[2]  # 结束时间"2023-08-27 00:00:00"

    for data in datalist:
        cls_macro_data(data['industryCode'], data['industryName'], beginTime, endTime)
