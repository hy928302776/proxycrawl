from storage.MongoDbStore import MongoDbStore
from service.DataSynAifin import data_sys_aifin

if __name__ == '__main__':
    # 同步数据
    data_sys_aifin("aifin_industry", "aifin_industry", True)
