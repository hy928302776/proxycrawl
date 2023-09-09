import sys

from service.StatsZxfb import stats_zxfb

if __name__ == '__main__':
    beginTime = sys.argv[1]  # 开始时间
    endTime = sys.argv[2]  # 结束时间"2023-08-28"
    stats_zxfb(beginTime, endTime, True)
