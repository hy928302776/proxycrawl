# ===================个股异常数据同步==============================
import sys

from storage.MongoDbStore import MongoDbStore
from service.DataSynAifin import data_sys_aifin

if __name__ == '__main__':
    status = 2 if len(sys.argv) < 2 else int(sys.argv[1])
    # 同步数据
    data_sys_aifin("aifin_stock_error", "aifin_stock", True,status)
    # 删除数据
    delete_count = MongoDbStore("aifin_stock_error").deleteBystatus(0)
    print(f"删除了{delete_count}条数据")
