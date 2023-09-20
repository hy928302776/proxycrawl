# ===========个股数据数据同步==================
import sys

sys.path.append("..")
from pymongo import UpdateOne
from storage import MilvusStore
from storage.MongoDbStore import MongoDbStore
from config.Logger import logger

def data_sys_aifin(monggo_db: str, milvus_db: str, fenku: bool, status: int = 1):
    """

    :param monggo_db: mongodb库
    :param milvus_db: milvus库
    :param fenku: 是否分库
    :return:
    """

    # （1）创建mongodb连接
    dbStore = MongoDbStore(monggo_db)

    # （2）统计有多少数据,以及每个item的数量
    count = dbStore.countData({"status": {"$lt": status}})
    item_list = dbStore.listgroupCount({"status": {"$lt": status}},"code")
    searchList = list(item_list)
    logger.info(f"一共有{count}条数据需要处理,各item数量{searchList}")

    total = 0  # 统计处理数据总数
    # （3）循环处理每个item
    for item_info in searchList:
        item = item_info['_id']
        pre_count = item_info['count']  # 每只item需处理的总个数
        logger.info(f"开始处理item{item}，数量{pre_count}")

        collects = [
            {"status": {"$lt": status}},
            {"code": item}
        ]
        query = {"$and": collects}

        pre_totle = 0  # 统计每只item已处理的数据
        while pre_totle < pre_count:
            # （2）查询第一批数据
            results = dbStore.searchData(query, 10)
            if not results:
                break

            # （3）构建批量处理数据
            milvus_datalist = []
            # 指定要删除的键
            keys_to_remove = ["_id", "status"]

            bulk_operations = []
            # 定义更新操作
            update = {"$set": {"status": status}}  # 替换为实际的更新操作
            for document in results:
                bulk_operations.append(UpdateOne({"_id": document["_id"]}, update))
                # 使用字典推导式生成新字典
                milvus_datalist.append({key: value for key, value in document.items() if key not in keys_to_remove})

            logger.info(f"milvus_datalist:{milvus_datalist}")
            # （4）入矢量库
            milvus_db_str = f"{milvus_db}_{item}" if fenku else milvus_db

            MilvusStore.storeData(milvus_datalist, milvus_db_str)
            # （5）执行批量更新
            result = dbStore.collection.bulk_write(bulk_operations)
            update_count = result.modified_count
            # 输出更新结果
            logger.info("更新的文档数量:", update_count)
            pre_totle += update_count
            logger.info(f"{item}一共有{pre_count}条数据需要处理,现在处理了{pre_totle}条数据")

        logger.info(f"同步{item}数据表完成，一共处理{pre_totle}条数据")
        total += pre_totle
    logger.info(f"所有数据处理完成一共{count}条数据，处理了{total}条数据")
    dbStore.close()


if __name__ == '__main__':
    data_sys_aifin("aifin_stock", "aifin_stock", True,2)
