#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time     : 2023/4/7 11:09
# @Author   : buding
# @Project  : pythonProject
# @File     : getdbconnect.py
# @Software : PyCharm

class DatabaseConnect():
    def __init__(self,db_connect):
        self.db_connect = db_connect

    def connectMysql(self):
        from sqlalchemy import create_engine
        from sqlalchemy.pool import NullPool
        from urllib.parse import quote_plus  # 密码含有@特殊字符,需要进行编码处理
        connect_info = f'mysql+pymysql://{self.db_connect["username"]}:{quote_plus(self.db_connect["password"])}@{self.db_connect["host"]}:{self.db_connect["port"]}/{self.db_connect["database"]}?charset=utf8'
        # pymysql和mysqlclient的语法比较相似，处理成DataFrame过程相对复杂一些，SQLAlchemy使用pandas的read_sql()方法更加便捷处理MySQL数据
        engine = create_engine(connect_info, poolclass=NullPool)  # 不用连接池,poolclass=NullPool
        return engine.connect()

    def connectDb2(self):
        import ibm_db_dbi
        connect_info = f'DATABASE={self.db_connect["database"]};HOSTNAME={self.db_connect["host"]};PORT={self.db_connect["port"]};PROTOCOL=TCPIP;UID={self.db_connect["username"]};PWD={self.db_connect["password"]};'
        conn = ibm_db_dbi.connect(connect_info, "", "")
        return conn

    def connectGaussDb(self):
        import psycopg2
        connect_info = f'dbname={self.db_connect["database"]}, user={self.db_connect["username"]}, password={self.db_connect["password"]}, host={self.db_connect["host"]}, port={self.db_connect["port"]}'
        conn = psycopg2.connect(connect_info)
        return conn




