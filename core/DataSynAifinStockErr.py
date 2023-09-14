from storage.MongoDbStore import MongoDbStore
from service.DataSynAifin import data_sys_aifin

if __name__ == '__main__':
    # 同步数据
    data_sys_aifin("aifin_stock_error", "aifin_stock", True)
    # 删除数据
    delete_count = MongoDbStore("aifin_stock_error").deleteBystatus(0)
    print(f"删除了{delete_count}条数据")
