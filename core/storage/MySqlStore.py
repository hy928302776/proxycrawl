# -*- coding: UTF-8 -*-
import json

import pymysql

dbinfo = {
    "host": "36.138.93.247",
    "user": "root",
    "password": "QAZwsx123",
    "port": 31652}


class DbConnect():
    def __init__(self, db_cof, database=""):
        self.db_cof = db_cof
        # 打开数据库连接
        self.db = pymysql.connect(database=database,
                                  cursorclass=pymysql.cursors.DictCursor,
                                  **db_cof)

        # 使用cursor()方法获取操作游标
        self.cursor = self.db.cursor()

    def select(self, sql):
        # SQL 查询语句
        # sql = "SELECT * FROM EMPLOYEE \
        #        WHERE INCOME > %s" % (1000)
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        return results

    def execute(self, sql):
        # SQL 删除、提交、修改语句
        # sql = "DELETE FROM EMPLOYEE WHERE AGE > %s" % (20)
        try:
            # 执行SQL语句
            self.cursor.execute(sql)
            self.cursor.executemany()
            # 提交修改
            self.db.commit()
        except:
            # 发生错误时回滚
            self.db.rollback()

    def close(self):
        # 关闭连接
        self.db.close()

def batchStockInfo(start: int=None, offset: int=None) -> list:
    """
    获取所有的表数据
    """

    sqlList = [f"select securities_code,securities_name,stock_code from stock_info where is_deleted = 0"]

    sqlList.append(" ORDER BY id")
    if start is not None and offset is not None:
        sqlList.append(f" limit {start},{offset}")
    sql = " ".join(sqlList)
    print(f"sql:{sql}")
    db = DbConnect(dbinfo, database="milvus_data")
    result = db.select(sql)
    db.close()
    return result




if __name__ == '__main__':
    print(batchStockInfo())
