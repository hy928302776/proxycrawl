import sys

from service.IndustryCls import cls_industry_data
from storage.MySqlStore import MainDb

if __name__ == "__main__":

    bMilvus = True if len(sys.argv) < 2 else sys.argv[1] == 'True'
    start = None if len(sys.argv) < 3 else int(sys.argv[2])
    offset = None if len(sys.argv) < 4 else int(sys.argv[3])
    beginTime = None if len(sys.argv) < 5 else sys.argv[4]  # 开始时间 "2023-08-27 00:00:00"
    endTime = None if len(sys.argv) < 6 else sys.argv[5]  # 结束时间"2023-08-28 00:00:00"

    print(f"参数列表,start:{start},offset:{offset}，beginTime:{beginTime},endTime:{endTime}")

    industryList: list = MainDb().batchIndustryInfo("cls",1, start, offset)

    result_totle = 0
    result_valid_data_total = 0
    result_map = {}

    if industryList and len(industryList) > 0:
        num = 0
        for industry in industryList:
            num += 1
            print(f"一共获取到了{len(industryList)}个行业，现在处理第{num}个：{industry}")
            total, valid_data_total = cls_industry_data(bMilvus, industry['industry_code'], industry['industry_name'],
                                                        num, beginTime, endTime)

            result_totle += total
            result_valid_data_total += valid_data_total

            result_map[industry['industry_code']] = f"一共处理了{total}条，有效{valid_data_total}条"
    print(f"本次脚本一共处理了{result_totle}条，有效{result_valid_data_total}条，其中：{result_map}")
