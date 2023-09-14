# -*- coding: UTF-8 -*-
import pymongo


class MongoDbStore:
    conn = None
    collection = None
    collection_name = None

    def __init__(self, collection_name: str):
        self.collection_name = collection_name
        self.conn = pymongo.MongoClient('mongodb://root:QAZwsx123@36.138.93.247:31966')
        database = self.conn['milvus_data']
        self.collection = database[self.collection_name]

    def storeData(self, docList: list, status: int = 0):
        """
        :param docList:
        :param collection_name:
        :param status:
        :return:
        """

        for doc in docList:
            doc.update({'status': status})

        self.collection.insert_many(docList)
        print(f"写入mongodb【{self.collection_name}】库over")
        return self

    def searchData(self, query: dict = {}, size: int = 100):
        """

        :param condation: 条件
        :param size: 大小
        :return: 返回结果集
        """
        return self.collection.find(query).limit(size)

    # db.test.find({xxx...xxx}).sort({"amount":1}).skip(10).limit(10)/

    def countData(self, query: dict = {}):
        # 构建聚合管道
        pipeline = [
            {"$match": query},
            {"$count": "total"}
        ]
        # 执行聚合查询
        result = self.collection.aggregate(pipeline)
        # 获取符合条件的文档总数
        nextObj = result.try_next()
        total_count = 0
        if nextObj:
            total_count = nextObj['total']
        return total_count

    def listgroupCount(self, category: str):
        result = self.collection.aggregate([
            {"$group": {"_id": f"${category}", "count": {"$sum": 1}}}
        ])
        return result

    def deleteBystatus(self,status):
        result = self.collection.delete_many({"status":status})
        return result.deleted_count

    def close(self):
        # 关闭连接
        self.conn.close()


if __name__ == '__main__':
    metadata = [{"source": "Web",
                 "uniqueId": 'A111',
                 "code": '123',
                 "name": 'stockName',
                 "url": 'url',
                 "date": '2023-09-09',
                 "type": "eastmoney-stock-report",
                 "createTime": '2022-09-08',
                 "abstract": 'abstract',
                 "title": '你好',
                 "mediaName": '中国',
                 "text": 'text'}]
    MongoDbStore("aifin_stock").storeData(metadata, 0).close()
