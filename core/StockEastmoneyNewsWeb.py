import sys

from storage.MySqlStore import  MainDb
from service.StockEastmoneyNews import eastmoney

if __name__ == "__main__":

    bMilvus = True if len(sys.argv) < 2 else sys.argv[1] == 'True'
    start = None if len(sys.argv) < 3 else int(sys.argv[2])
    offset = None if len(sys.argv) < 4 else int(sys.argv[3])
    beginTime = None if len(sys.argv) < 5 else sys.argv[4]  # 开始时间 "2023-08-27"
    endTime = None if len(sys.argv) < 6 else sys.argv[5]  # 结束时间"2023-08-28"


    print(f"参数列表，start:{start},offset:{offset},beginTime:{beginTime},endTime:{endTime}")
    stockList: list = MainDb().batchStockInfo(int(start), int(offset))
    if stockList and len(stockList) > 0:
        num = 0
        for stock in stockList:
            num += 1
            print(f"一共获取到了{len(stockList)}支股票，现在处理第{num}个：{stock}")
            eastmoney(bMilvus,stock['stock_code'], stock['securities_name'], beginTime, endTime)
