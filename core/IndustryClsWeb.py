import sys

from service.IndustryCls import cls_industry_data
from storage.MySqlStore import MainDb

if __name__ == "__main__":
    beginTime = None
    endTime = None
    start: str = None
    offset: str = None
    if len(sys.argv) > 1:
        start = sys.argv[1],
        offset = sys.argv[2],
        beginTime = sys.argv[3]  # 开始时间 "2023-08-27 00:00:00"
        endTime = sys.argv[4]  # 结束时间"2023-08-28 00:00:00"

    # startPage = sys.argv[4]  # 从第几页
    # print(f"参数列表，domain:{domain},code:{code},type:{type},startPage:{startPage}")
    # eastmoney(code, type, int(startPage))
    if start:
        start = int(start[0])
    if offset:
        offset = int(offset[0])
    print(f"参数列表,start:{start},offset:{offset}，beginTime:{beginTime},endTime:{endTime}")

    industryList: list = MainDb().batchIndustryInfo("cls", start, offset)
    if industryList and len(industryList) > 0:
        num = 0
        for industry in industryList:
            num += 1
            print(f"一共获取到了{len(industryList)}个行业，现在处理第{num}个：{industry}")
            cls_industry_data(industry['industry_code'], industry['industry_name'], beginTime, endTime)
