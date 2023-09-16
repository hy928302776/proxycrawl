import sys

from service.MacroStatsZxfb import stats_zxfb

if __name__ == '__main__':
    bMilvus = True if len(sys.argv) < 2 else sys.argv[1] == 'True'
    beginTime = None if len(sys.argv) < 5 else sys.argv[4]  # 开始时间 "2023-08-27"
    endTime = None if len(sys.argv) < 6 else sys.argv[5]  # 结束时间"2023-08-28"

    stats_zxfb(bMilvus,beginTime, endTime, True)
