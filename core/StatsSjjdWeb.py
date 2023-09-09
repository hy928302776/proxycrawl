import sys

from service.StatsSjjd import stats_sjjd

if __name__ == '__main__':
    beginTime = sys.argv[1]  # 开始时间
    endTime = sys.argv[2]  # 结束时间"2023-08-28"
    stats_sjjd(beginTime, endTime, True)
