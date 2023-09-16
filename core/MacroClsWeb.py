import sys

from service.MacroCls import cls_macro_data

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
    beginTime = None if len(sys.argv) < 5 else sys.argv[4]  # 开始时间 "2023-08-27"
    endTime = None if len(sys.argv) < 6 else sys.argv[5]  # 结束时间"2023-08-28"

    for data in datalist:
        cls_macro_data(bMilvus,data['industryCode'], data['industryName'], beginTime, endTime)
