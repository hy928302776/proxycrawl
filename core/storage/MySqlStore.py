# -*- coding: UTF-8 -*-
import sys

import pymysql
sys.path.append("..")
from config.Logger import logger

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


class MainDb(DbConnect):
    dbinfo = {
        "host": "36.138.93.247",
        # "host": "192.168.0.3",
        "user": "root",
        "password": "QAZwsx123",
        "port": 31652}

    def __init__(self):
        super().__init__(self.dbinfo, "milvus_data")

    def batchStockInfo(self, start: int = None, offset: int = None) -> list:
        """
        获取所有的表数据
        """

        sqlList = [f"select securities_code,securities_name,stock_code from stock_info where is_deleted = 0"]

        sqlList.append(" ORDER BY id")
        if start is not None and offset is not None:
            sqlList.append(f" limit {start},{offset}")
        sql = " ".join(sqlList)
        logger.info(f"sql:{sql}")
        ##db = self(dbinfo, database="milvus_data")
        result = self.select(sql)
        self.close()
        return result

    def batchIndustryInfo(self, belong: str, industry_level: int = 1, start: int = None,
                          offset: int = None) -> list:
        """
        获取所有的表数据
        """

        sqlList = [
            f"select industry_code,industry_name,belong from industry_info where is_deleted = 0 and industry_level={industry_level}"]

        if belong is not None:
            sqlList.append(f" and belong='{belong}'")

        sqlList.append(" ORDER BY id")
        if start is not None and offset is not None:
            sqlList.append(f" limit {start},{offset}")
        sql = " ".join(sqlList)
        logger.info(f"sql:{sql}")
        result = self.select(sql)
        self.close()
        return result


class TlDb(DbConnect):
    dbinfo = {
        # "host": "192.168.6.5",
        "host": "36.138.94.158",
        "user": "root",
        "password": "juw&2155FR345&$",
        "port": 23456}

    def __init__(self):
        super().__init__(self.dbinfo, "sdsg")

    def listSecurityIdsByStock(self):
        sql = f"SELECT SECURITY_ID,TICKER_SYMBOL,SEC_SHORT_NAME FROM md_security limit 10"
        logger.info(f"sql:{sql}")
        result = self.select(sql)
        self.close()
        return result


if __name__ == '__main__':
    logger.info("====")
    logger.info(MainDb().batchIndustryInfo('cls',2))
