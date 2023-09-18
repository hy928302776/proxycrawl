# ===================个股通联研报==============================
import sys

from service.StockTlReport import stock_tl_report
from storage.MySqlStore import MainDb

if __name__ == "__main__":

    bMilvus = True if len(sys.argv) < 2 else sys.argv[1] == 'True'
    start = None if len(sys.argv) < 3 else int(sys.argv[2])
    offset = None if len(sys.argv) < 4 else int(sys.argv[3])
    beginTime = None if len(sys.argv) < 5 else sys.argv[4]  # 开始时间 "2023-08-27"
    endTime = None if len(sys.argv) < 6 else sys.argv[5]  # 结束时间"2023-08-28"

    print(f"参数列表，start:{start}，offset：{offset},beginTime:{beginTime},endTime:{endTime}")
    stockList: list = MainDb().batchStockInfo(int(start), int(offset))
    countTotal = 0
    countmap = {}
    if stockList and len(stockList) > 0:
        num = 0
        for stock in stockList:
            num += 1
            print(f"一共获取到了{len(stockList)}支股票，现在处理第{num}个：{stock}")
            total = stock_tl_report(bMilvus, stock['stock_code'], beginTime, endTime)
            countmap[stock['stock_code']] = total
            countTotal += total

    print(f"本次脚本一共处理了{countTotal}条数据，其中：{countmap}")