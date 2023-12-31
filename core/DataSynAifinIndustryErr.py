import sys

# ===================行业异常数据同步==============================
from storage.MongoDbStore import MongoDbStore
from service.DataSynAifin import data_sys_aifin
from config.Logger import logger

if __name__ == '__main__':
    status = 0 if len(sys.argv) < 2 else int(sys.argv[1])
    # 同步数据
    data_sys_aifin("aifin_industry_error", "aifin_industry_error", True, status)
    # 删除数据
    delete_count = MongoDbStore("aifin_industry_error").deleteBystatus(0)
    logger.info(f"删除了{delete_count}条数据")
