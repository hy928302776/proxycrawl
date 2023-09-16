# ===================宏观异常数据同步==============================
import sys

from storage.MongoDbStore import MongoDbStore
from service.DataSynAifin import data_sys_aifin

if __name__ == '__main__':
    status = 1 if len(sys.argv) < 2 else int(sys.argv[1])
    # 同步数据
    data_sys_aifin("aifin_macro_error", "aifin_macro", False, status)
    # 删除数据
    delete_count = MongoDbStore("aifin_macro_error").deleteBystatus(0)
    print(f"删除了{delete_count}条数据")
