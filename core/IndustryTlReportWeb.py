import sys

from service.IndustryTlReport import industry_tl_report
from storage.MySqlStore import MainDb

if __name__ == "__main__":

    bMilvus = True if len(sys.argv) < 2 else sys.argv[1] == 'True'
    start = None if len(sys.argv) < 3 else int(sys.argv[2])
    offset = None if len(sys.argv) < 4 else int(sys.argv[3])
    beginTime = None if len(sys.argv) < 5 else sys.argv[4]  # 开始时间 "2023-08-27"
    endTime = None if len(sys.argv) < 6 else sys.argv[5]  # 结束时间"2023-08-28"

    print(f"参数列表，bMilvus：{bMilvus},start:{start},offset:{offset}，beginTime:{beginTime},endTime:{endTime}")

    industryList: list = MainDb().batchIndustryInfo("申万行业分类", start, offset,)
    if industryList and len(industryList) > 0:
        num = 0
        for industry in industryList:
            num += 1
            print(f"一共获取到了{len(industryList)}个行业，现在处理第{num}个：{industry}")
            industry_tl_report(bMilvus,industry['industry_code'], industry['industry_name'], beginTime, endTime)
