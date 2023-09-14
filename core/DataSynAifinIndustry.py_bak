# ===========行业数据数据同步==================
import sys

from pymongo import UpdateOne

from storage.MySqlStore import MainDb
from storage import MilvusStore
from storage.MongoDbStore import MongoDbStore


def pre(industry: str):
    status = 0
    if len(sys.argv) > 1:
        status = sys.argv[1]

    # （1）创建mongodb连接
    dbStore = MongoDbStore("aifin_industry")

    # （1）统计有多少数据
    collects = [
        {"status": {"$lt": status}},
        {"code": industry}
    ]
    query = {"$and": collects}
    count = dbStore.countData(query)
    print(f"一共有{count}条数据需要处理")

    total = 0  # 统计处理数据总数
    err_count = 0
    while True and err_count < 3 and total < count:
        # （2）查询第一批数据
        results = dbStore.searchData(query, 10)
        if not results:
            print(f"同步{industry}数据表完成，一共处理{total}条数据")
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

        print(f"milvus_datalist:{milvus_datalist}")
        # （4）入矢量库
        MilvusStore.storeData(milvus_datalist, f"aifin_industry_{industry}")
        # （5）执行批量更新
        result = dbStore.collection.bulk_write(bulk_operations)
        pre_count = result.modified_count
        # 输出更新结果
        print("更新的文档数量:", pre_count)
        total += pre_count
        print(f"{industry}一共有{count}条数据需要处理,现在处理了{total}条数据")

    dbStore.close()


if __name__ == '__main__':
    industryList: list = MainDb().batchIndustryInfo()
    if industryList and len(industryList) > 0:
        num = 0
        for industry in industryList:
            num += 1
            print("============================")
            print(f"一共获取到了{len(industryList)}个行业，现在处理第{num}个：{industry}")
            pre(industry['industry_code'])
