import sys

from service.StockEastmoneyNotice import east_notice
from storage.MySqlStore import  MainDb

if __name__ == "__main__":
    start = sys.argv[1]  # 起始
    offset = sys.argv[2]  # 偏移量
    beginTime = None
    endTime = None
    if len(sys.argv) > 3:
        beginTime = sys.argv[3]  # 开始时间
        endTime = sys.argv[4]  # 结束时间"2023-08-28"
    print(f"参数列表，start:{start},offset:{offset},beginTime:{beginTime},endTime:{endTime}")
    stockList: list = MainDb().batchStockInfo(int(start), int(offset))
    if stockList and len(stockList) > 0:
        num = 0
        for stock in stockList:
            num += 1
            print(f"一共获取到了{len(stockList)}支股票，现在处理第{num}个：{stock}")
            east_notice(stock['stock_code'], stock['securities_name'], beginTime, endTime)
