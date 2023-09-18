import sys

from service.MacroStatsSjjd import stats_sjjd

if __name__ == '__main__':
    bMilvus = True if len(sys.argv) < 2 else sys.argv[1] == 'True'
    beginTime = None if len(sys.argv) < 5 else sys.argv[4]  # 开始时间 "2023-08-27"
    endTime = None if len(sys.argv) < 6 else sys.argv[5]  # 结束时间"2023-08-28"

    print(f"参数列表，bMilvus:{bMilvus},beginTime:{beginTime},endTime:{endTime}")
    total, valid_data_total = stats_sjjd(bMilvus, beginTime, endTime, True)
    print(f"一共处理了{total}条数据，有效数据{valid_data_total}条")
