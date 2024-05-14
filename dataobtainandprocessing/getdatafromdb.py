#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time     : 2023/5/22 16:23
# @Author   : buding
# @Project  : pythonProject
# @File     : getdatafromdb.py
# @Software : PyCharm
import io
import sys
import pandas as pd
from sqlalchemy import text
from loguru import logger

from DataCompare2.util.getconfigration import GetConfig
from DataCompare2.util.getdbconnect import DatabaseConnect


class DatabaseData():#获取全部数据，然后排序，排序后分批取切片数据
    def __init__(self,conn,db,table):
        self.conn = conn
        self.db = db
        self.table = table

    def getTableStructure(self,get_table_structure_sql):
        get_table_structure = get_table_structure_sql.format(db=self.db,table=self.table)
        table_structure = pd.read_sql(text(get_table_structure), self.conn)
        return table_structure

    def getPrimaryKey(self,get_primary_key_sql):
        get_primary_key = get_primary_key_sql.format(db=self.db,table=self.table)
        primary_key = pd.read_sql(text(get_primary_key), self.conn)
        logger.info(f'获取主键sql：{get_primary_key}\n')
        return primary_key

    def getIndexes(self,get_indexes_sql):
        get_indexes = get_indexes_sql.format(db=self.db, table=self.table)
        indexes = pd.read_sql(text(get_indexes), self.conn)
        logger.info(f'获取索引sql：{get_indexes}\n')
        return indexes

    def getColumnsNumber(self,get_columns_num_sql):
        get_columns_num = get_columns_num_sql.format(db=self.db,table=self.table)
        columns_num = pd.read_sql(text(get_columns_num), self.conn)
        logger.info(f'获取列数sql：{get_columns_num}\n')
        return columns_num.iloc[0,0]

    def getColumnsNames(self,get_columns_names_sql):
        get_columns_names = get_columns_names_sql.format(db=self.db,table=self.table)
        columns_names = pd.read_sql(text(get_columns_names), self.conn)
        logger.info(f'获取列名sql：{get_columns_names}\n')
        return columns_names

    def getRowsNumber(self,get_rows_num_sql):
        get_rows_num = get_rows_num_sql.format(db=self.db,table=self.table)
        rows_num = pd.read_sql(text(get_rows_num), self.conn)
        logger.info(f'获取行数sql：{get_rows_num}\n')
        return rows_num.iloc[0,0]

    def getFullData(self,get_data_sql,get_primary_key_sql,get_indexes_sql,get_columns_names_sql,sort_values,incremental_field='',incremental_date=''):
        # 优先获取配置文件指定字段进行排序，若为空，即获取主键||索引||全字段进行排序,默认升序排序
        logger.info('开始获取排序后的数据...')
        if eval(sort_values):
            self.sort_values = eval(sort_values)
            sql = get_data_sql.format(db=self.db, table=self.table, cols=self.sort_values)
            logger.info(f'获取全表排序后的数据sql:{sql}\n')
            # 设置DataFrame的输出格式
            pd.set_option('display.max_columns', None)  # 所有列
            pd.set_option('display.max_rows', None)  # 所有行
            df = pd.read_sql(text(sql), self.conn)
            return df
        primary_key = self.getPrimaryKey(get_primary_key_sql)
        primary_key_bool = ~primary_key.isnull()['COLUMN_NAME'][0]
        if primary_key_bool:
            self.sort_values = primary_key.loc[0, 'COLUMN_NAME']
            sql = get_data_sql.format(db=self.db, table=self.table, cols=self.sort_values)
            logger.info(f'获取全表排序后的数据sql:{sql}\n')
            # 设置DataFrame的输出格式
            pd.set_option('display.max_columns', None)  # 所有列
            pd.set_option('display.max_rows', None)  # 所有行
            df = pd.read_sql(text(sql), self.conn)
            return df
        indexes = self.getIndexes(get_indexes_sql)
        indexes_bool = ~indexes.isnull()['COLUMN_NAME'][0]
        if indexes_bool:
            self.sort_values = indexes.loc[0, 'COLUMN_NAME']
            sql = get_data_sql.format(db=self.db, table=self.table, cols=self.sort_values)
            logger.info(f'获取全表排序后的数据sql:{sql}\n')
            # 设置DataFrame的输出格式
            pd.set_option('display.max_columns', None)  # 所有列
            pd.set_option('display.max_rows', None)  # 所有行
            df = pd.read_sql(text(sql), self.conn)
            return df
        self.sort_values = self.getColumnsNames(get_columns_names_sql)
        # str = ','
        # sort_values = str.join(self.sort_values)
        sql = get_data_sql.format(db=self.db, table=self.table, cols=self.sort_values)
        logger.info(f'获取全表排序后的数据sql:{sql}\n')
        # 设置DataFrame的输出格式
        pd.set_option('display.max_columns', None)  # 所有列
        pd.set_option('display.max_rows', None)  # 所有行
        df = pd.read_sql(text(sql), self.conn)
        return df

    def getSlicesData(self,get_slice_data_sql,get_primary_key_sql,get_indexes_sql,get_columns_names_sql,sort_values,once_get_rows,initial_row = 0,incremental_field='',initial=''):
        # 优先获取配置文件指定字段进行排序，若为空，即获取主键||索引||全字段进行排序,默认升序排序
        logger.info('开始获取切片数据...\n')
        if eval(sort_values):
            sort_values = eval(sort_values)
            sql = get_slice_data_sql.format(db=self.db, table=self.table, cols=sort_values,
                                            initial_row=initial_row, once_get_rows=once_get_rows)
            # 设置DataFrame的输出格式
            pd.set_option('display.max_columns', None)  # 显示完整的列（否则会以省略号的形式省略）
            pd.set_option('display.max_rows', None)  # 显示完整的行（否则会以省略号的形式省略）
            df = pd.read_sql(text(sql), self.conn)
            logger.info(f'获取切片数据的sql:{sql}\n')
            return df
        primary_key = self.getPrimaryKey(get_primary_key_sql)
        primary_key_bool = ~primary_key.isnull()['COLUMN_NAME'][0]
        if primary_key_bool:
            sort_values = primary_key.loc[0, 'COLUMN_NAME']
            sql = get_slice_data_sql.format(db=self.db, table=self.table, cols=sort_values,
                                            initial_row=initial_row, once_get_rows=once_get_rows)
            # 设置DataFrame的输出格式
            pd.set_option('display.max_columns', None)  # 显示完整的列（否则会以省略号的形式省略）
            pd.set_option('display.max_rows', None)  # 显示完整的行（否则会以省略号的形式省略）
            logger.info(f'获取切片数据的sql:{sql}\n')
            df = pd.read_sql(text(sql), self.conn)
            return df
        indexes = self.getIndexes(get_indexes_sql)
        indexes_bool = ~indexes.isnull()['COLUMN_NAME'][0]
        if indexes_bool:
            sort_values = indexes.loc[0, 'COLUMN_NAME']
            sql = get_slice_data_sql.format(db=self.db, table=self.table, cols=sort_values,
                                            initial_row=initial_row, once_get_rows=once_get_rows)
            # 设置DataFrame的输出格式
            pd.set_option('display.max_columns', None)  # 显示完整的列（否则会以省略号的形式省略）
            pd.set_option('display.max_rows', None)  # 显示完整的行（否则会以省略号的形式省略）
            logger.info(f'获取切片数据的sql:{sql}\n')
            df = pd.read_sql(text(sql), self.conn)
            return df
        sort_values = self.getColumnsNames(get_columns_names_sql)
        # str = ','
        # sort_values = str.join(self.sort_values)
        sql = get_slice_data_sql.format(db=self.db, table=self.table, cols=sort_values,
                                        initial_row=initial_row, once_get_rows=once_get_rows)
        # 设置DataFrame的输出格式
        pd.set_option('display.max_columns', None)  # 显示完整的列（否则会以省略号的形式省略）
        pd.set_option('display.max_rows', None)  # 显示完整的行（否则会以省略号的形式省略）
        df = pd.read_sql(text(sql), self.conn)
        logger.info(f'获取切片数据的sql:{sql}\n')
        return df

    # def getDb2SectionData(self):
    #     temp = ibm_db.exec_immediate(self.conn,self.sql)
    #     result = ibm_db.fetch_both(temp)
    #     alldata = []
    #     while result:
    #         alldata.append(result)
    #         result = ibm_db.fetch_both(temp)
    #     df = pd.DataFrame(alldata,columns=cols)




if __name__ == '__main__':
    conf_user = r'D:\pythonProject\DataCompare\configuration\userprofile.ini'
    conf_db = r'D:\pythonProject\DataCompare\configuration\databaseinformation.ini'
    conf_sql = r'D:\pythonProject\DataCompare\configuration\sql.ini'
    # 连接数据库
    db_info = GetConfig(conf_user, 'PARAMETERS').getItem('source_db_conect')
    db_connect = GetConfig(conf_db, eval(db_info)[0]).getItems()
    conn = DatabaseConnect(db_connect).connectMysql()
    cols = GetConfig(conf_user, 'PARAMETERS').getItem('sort_values')

    db = GetConfig(conf_db, eval(db_info)[0]).getItem('database')
    table = GetConfig(conf_user, 'PARAMETERS').getItem('source_table')
    get_data_sql = GetConfig(conf_sql, eval(db_info)[1]).getItem('get_data')
    get_primary_key_sql = GetConfig(conf_sql, eval(db_info)[1]).getItem('get_primary_key')
    get_indexes_sql = GetConfig(conf_sql, eval(db_info)[1]).getItem('get_indexes')
    get_columns_names_sql = GetConfig(conf_sql, eval(db_info)[1]).getItem('get_columns_names')
    once_get_rows = GetConfig(conf_user, 'PARAMETERS').getInt('once_get_rows')
    incremental_field = GetConfig(conf_user, 'PARAMETERS').getItem('incremental_field')
    initial = GetConfig(conf_user, 'PARAMETERS').getItem('initial')

    df = DatabaseData(conn, db, table).getSectionData(get_data_sql, get_primary_key_sql, get_indexes_sql,
                                                      get_columns_names_sql, cols, once_get_rows,
                                                      incremental_field=incremental_field, initial=initial)
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='gb18030')
    print(df)

    #
    # primary_key = DatabaseData(conn,db,table).getPrimaryKey(get_primary_key)
    # print(primary_key['COLUMN_NAME'].tolist())
    #
    # indexes = DatabaseData(conn,db,table).getIndexes(get_indexes)
    # print(indexes['Column_name'].tolist())

    # initial_row = 0
    # while True:
    #     print(initial_row,once_get_rows)
    #     df = DatabaseData(conn,db,table,sql,once_get_rows,initial_row).getSectionData()
    #     if df.empty:
    #         break
    #     initial_row += once_get_rows
    #     print(df)