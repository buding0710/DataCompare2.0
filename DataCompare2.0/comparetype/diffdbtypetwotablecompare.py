#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time     : 2023/5/28 6:05
# @Author   : buding
# @Project  : pythonProject
# @File     : diffdbtypetwotablecompare.py
# @Software : PyCharm
from loguru import logger

from DataCompare.datacheck.databasedatacompare import CompareDatabaseData
from DataCompare.util.getconfigration import GetConfig
from DataCompare.util.getdbconnect import DatabaseConnect


class DiffDbTypeTwoTableCompare():
    def __init__(self,conf_user,conf_db,conf_sql):
        #连接数据库
        self.user_profile= GetConfig(conf_user,'PARAMETERS').getItems()
        self.conf_sql = conf_sql
        self.source_db_connect = self.user_profile['source_db_connect']
        self.source_db_connect_info = GetConfig(conf_db,eval(self.source_db_connect)[0]).getItems()
        self.source_db_type = eval(self.source_db_connect)[1]
        match self.source_db_type:
            case 'MYSQL':
                logger.info(f'连接源数据库MYSQL：{self.source_db_connect_info}')
                self.source_conn = DatabaseConnect(self.source_db_connect_info).connectMysql()
            case 'DB2':
                logger.info(f'连接源数据库DB2：{self.source_db_connect_info}')
                self.source_conn = DatabaseConnect(self.source_db_connect_info).connectDb2()
            case 'GaussDB':
                logger.info(f'连接源数据库GaussDB：{self.source_db_connect_info}')
                self.source_conn = DatabaseConnect(self.source_db_connect_info).connectGaussDb()
        self.source_db = GetConfig(conf_db, eval(self.source_db_connect)[0]).getItem('database')
        self.source_table = self.user_profile['source_table']
        self.target_db_connect = self.user_profile['target_db_connect']
        self.target_db_connect_info = GetConfig(conf_db, eval(self.target_db_connect)[0]).getItems()
        self.target_db_type = eval(self.target_db_connect)[1]
        match self.target_db_type:
            case 'MYSQL':
                logger.info(f'连接目标数据库MYSQL：{self.target_db_connect_info}')
                self.target_conn = DatabaseConnect(self.target_db_connect_info).connectMysql()
            case 'DB2':
                logger.info(f'连接目标数据库DB2：{self.target_db_connect_info}')
                self.target_conn = DatabaseConnect(self.target_db_connect_info).connectDb2()
            case 'GaussDB':
                logger.info(f'连接目标数据库GaussDB：{self.target_db_connect_info}')
                self.target_conn = DatabaseConnect(self.target_db_connect_info).connectGaussDb()
        self.target_db = GetConfig(conf_db, eval(self.target_db_connect)[0]).getItem('database')
        self.target_table = self.user_profile['target_table']

    def columnsCompare(self):
        logger.info(
            "************************************************列数对比****************************************************")
        # 列数对比
        source_db_columns_number_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_columns_num')
        target_db_columns_number_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_columns_num')
        compare_columns_num_result = CompareDatabaseData(self.source_conn,self.source_db,self.source_table,self.target_conn,self.target_db,self.target_table).columnsNumCompare(source_db_columns_number_sql,target_db_columns_number_sql)
        logger.info(f'列数对比结果：{compare_columns_num_result}')

        logger.info(
            "************************************************列名对比****************************************************")
        # 列名对比，包含列名、排序、默认值、是否为空、数据类型、长度、精度
        source_db_columns_name_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_columns_name')
        target_db_columns_name_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_columns_name')
        compare_columns_name_result = CompareDatabaseData(self.source_conn,self.source_db,self.source_table,self.target_conn,self.target_db,self.target_table).columnsNameCompare(source_db_columns_name_sql,target_db_columns_name_sql)
        logger.info(f'列名对比结果：{compare_columns_name_result}')

    def primaryKeyCompare(self):
        logger.info(
            "************************************************主键对比****************************************************")
        # 主键对比
        source_db_primary_key_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_primary_key')
        target_db_primary_key_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_primary_key')
        compare_primary_key_result = CompareDatabaseData(self.source_conn, self.source_db, self.source_table, self.target_conn, self.target_db,
                                                          self.target_table).columnsNameCompare(source_db_primary_key_sql,
                                                                                           target_db_primary_key_sql)
        logger.info(f'主键对比结果：{compare_primary_key_result}')

    def indexesCompare(self):
        logger.info(
            "************************************************索引对比****************************************************")
        # 索引对比
        source_db_indexes_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_indexes')
        target_db_indexes_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_indexes')
        compare_indexes_result = CompareDatabaseData(self.source_conn, self.source_db, self.source_table, self.target_conn, self.target_db,
                                                          self.target_table).columnsNameCompare(source_db_indexes_sql,
                                                                                           target_db_indexes_sql)
        logger.info(f'索引对比结果：{compare_indexes_result}')

    def rowsNumCompare(self):
        logger.info(
            "************************************************行数对比****************************************************")
        # 行数对比
        source_db_rows_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_rows_num')
        target_db_rows_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_rows_num')
        compare_rows_result = CompareDatabaseData(self.source_conn, self.source_db, self.source_table, self.target_conn, self.target_db,
                                                          self.target_table).rowsNumCompare(source_db_rows_sql,
                                                                                           target_db_rows_sql)
        logger.info(f'行数对比结果：{compare_rows_result}')
        # 如果其中一个的数据为空，另一个有数据，即最终对比结果为不通过，终止程序执行;如果两个数据均为空，即行数对比通过，终止程序执行
        if compare_rows_result['table1_rows_num'] == 0 or compare_rows_result['table2_rows_num'] == 0:
            logger.info('数据为空，终止执行！')
            compare_rows_content_result = {"result": False}
            logger.info(f'行内容对比结果：{compare_rows_content_result}')
            return compare_rows_content_result

    def fullDataCompare(self):  # 全量对比
        logger.info("************************************************全量行内容对比****************************************************")
        source_db_get_data_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_data')
        target_db_get_data_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_data')
        source_db_primary_key_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_primary_key')
        target_db_primary_key_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_primary_key')
        source_db_indexes_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_indexes')
        target_db_indexes_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_indexes')
        source_db_columns_name_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem(
            'get_columns_name')
        target_db_columns_name_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem(
            'get_columns_name')
        sort_values = self.user_profile['sort_values']
        compare_full_table_result = CompareDatabaseData(self.source_conn, self.source_db, self.source_table,
                                                         self.target_conn,
                                                         self.target_db,
                                                         self.target_table).fullTableMD5compare(source_db_get_data_sql, target_db_get_data_sql,
                                                                             source_db_primary_key_sql,
                                                                             target_db_primary_key_sql,
                                                                             source_db_indexes_sql,
                                                                             target_db_indexes_sql,
                                                                             source_db_columns_name_sql,
                                                                             target_db_columns_name_sql,
                                                                             sort_values)
        # 如果全表数据MD5对比结果通过，即不管哪种对比方式，最终对比结果为通过，终止程序执行
        logger.info(f'全表数据MD5对比结果：{compare_full_table_result}')
        if compare_full_table_result['result']:
            logger.info('全表数据MD5比对通过，终止执行！')
            return compare_full_table_result
        source_db_get_slice_data_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_slice_data')
        target_db_get_slice_data_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_slice_data')
        join_columns = self.user_profile['join_columns']
        once_get_rows = int(self.user_profile['once_get_rows'])
        compare_rows_content_result = CompareDatabaseData(self.source_conn, self.source_db, self.source_table, self.target_conn, self.target_db,
                            self. target_table).fullRowsContentCompare(source_db_get_slice_data_sql,target_db_get_slice_data_sql,source_db_primary_key_sql,target_db_primary_key_sql, source_db_indexes_sql, target_db_indexes_sql, source_db_columns_name_sql,target_db_columns_name_sql, sort_values,once_get_rows,join_columns)

        logger.info(f'行内容对比结果：{compare_rows_content_result}')
        return compare_rows_content_result

    def sampleDataCompare(self):  # 抽样对比
        logger.info("************************************************抽样行内容对比****************************************************")
        source_db_get_data_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_data')
        target_db_get_data_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_data')
        source_db_primary_key_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_primary_key')
        target_db_primary_key_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_primary_key')
        source_db_indexes_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_indexes')
        target_db_indexes_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_indexes')
        source_db_columns_name_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem(
            'get_columns_name')
        target_db_columns_name_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem(
            'get_columns_name')
        sort_values = self.user_profile['sort_values']
        compare_full_table_result = CompareDatabaseData(self.source_conn, self.source_db, self.source_table,
                                                         self.target_conn,
                                                         self.target_db,
                                                         self.target_table).fullTableMD5compare(source_db_get_data_sql, target_db_get_data_sql,
                                                                             source_db_primary_key_sql,
                                                                             target_db_primary_key_sql,
                                                                             source_db_indexes_sql,
                                                                             target_db_indexes_sql,
                                                                             source_db_columns_name_sql,
                                                                             target_db_columns_name_sql,
                                                                             sort_values)
        # 如果全表数据MD5对比结果通过，即不管哪种对比方式，最终对比结果为通过，终止程序执行
        logger.info(f'全表数据MD5对比结果：{compare_full_table_result}')
        if compare_full_table_result['result']:
            logger.info('全表数据MD5比对通过，终止执行！')
            return compare_full_table_result
        ratio = self.user_profile['sample_ratio']
        source_db_get_slice_data_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem(
            'get_slice_data')
        target_db_get_slice_data_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem(
            'get_slice_data')
        join_columns = self.user_profile['join_columns']
        once_get_rows = int(self.user_profile['once_get_rows'])
        compare_rows_content_result = CompareDatabaseData(self.source_conn, self.source_db, self.source_table, self.target_conn, self.target_db,
                                                          self.target_table).sampleRowsContentCompare(source_db_get_slice_data_sql,target_db_get_slice_data_sql,source_db_primary_key_sql,target_db_primary_key_sql, source_db_indexes_sql, target_db_indexes_sql, source_db_columns_name_sql,target_db_columns_name_sql, sort_values,once_get_rows,join_columns,ratio)
        logger.info(f'行内容对比结果：{compare_rows_content_result}')
        return compare_rows_content_result

    def incrementalDataCompare(self):  # 增量对比
        logger.info("************************************************增量行内容对比****************************************************")
        date_format = self.user_profile['date_format']
        source_db_get_data_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_data')
        target_db_get_data_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_data')
        source_db_primary_key_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_primary_key')
        target_db_primary_key_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_primary_key')
        source_db_indexes_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_indexes')
        target_db_indexes_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_indexes')
        source_db_columns_name_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_columns_name')
        target_db_columns_name_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_columns_name')
        sort_values = self.user_profile['sort_values']
        # 增量比对
        incremental_field = self.user_profile['incremental_field']
        incremental_date = self.user_profile['incremental_date']
        comparison = "{field} >= '{value}'".format(field=incremental_field, value=incremental_date)
        incremental_data_sql1 = source_db_get_data_sql.replace("1 = 1", comparison)
        incremental_data_sql2 = target_db_get_data_sql.replace("1 = 1", comparison)
        compare_full_table_result = CompareDatabaseData(self.source_conn, self.source_db, self.source_table,
                                                         self.target_conn,
                                                         self.target_db,
                                                         self.target_table).fullTableMD5compare(incremental_data_sql1,
                                                                                                incremental_data_sql2,
                                                                                                source_db_primary_key_sql,
                                                                                                target_db_primary_key_sql,
                                                                                                source_db_indexes_sql,
                                                                                                target_db_indexes_sql,
                                                                                                source_db_columns_name_sql,
                                                                                                target_db_columns_name_sql,
                                                                                                sort_values)
        # 如果全表数据MD5对比结果通过，即不管哪种对比方式，最终对比结果为通过，终止程序执行
        logger.info(f'全表数据MD5对比结果：{compare_full_table_result}')
        if compare_full_table_result['result']:
            logger.info('全表数据MD5比对通过，终止执行！')
            return compare_full_table_result
        source_db_get_slice_data_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem(
            'get_slice_data')
        target_db_get_slice_data_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem(
            'get_slice_data')
        join_columns = self.user_profile['join_columns']
        once_get_rows = int(self.user_profile['once_get_rows'])
        compare_rows_content_result = CompareDatabaseData(self.source_conn, self.source_db, self.source_table, self.target_conn, self.target_db,
                            self.target_table).incrementalRowsContentCompare(source_db_get_slice_data_sql, target_db_get_slice_data_sql,
                                                                   source_db_primary_key_sql, target_db_primary_key_sql,
                                                                   source_db_indexes_sql, target_db_indexes_sql,
                                                                   source_db_columns_name_sql, target_db_columns_name_sql,
                                                                   sort_values, once_get_rows, join_columns, incremental_field,incremental_date,date_format)

        logger.info(f'行内容对比结果：{compare_rows_content_result}')
        return compare_rows_content_result


if __name__ == '__main__':
    conf_user = r'D:\pythonProject\DataCompare\configuration\userprofile.ini'
    conf_db = r'D:\pythonProject\DataCompare\configuration\databaseinformation.ini'
    conf_sql = r'D:\pythonProject\DataCompare\configuration\sql.ini'
    compare_type = GetConfig(conf_user, 'PARAMETERS').getItem('compare_type')
    match compare_type:
        case 'full_compare':
            td = DiffDbTypeTwoTableCompare(conf_user,conf_db,conf_sql)
            td.columnsCompare()
            td.primaryKeyCompare()
            td.indexesCompare()
            td.rowsNumCompare()
            td.fullDataCompare()
        case 'sample_compare':
            td = DiffDbTypeTwoTableCompare(conf_user, conf_db, conf_sql)
            td.columnsCompare()
            td.primaryKeyCompare()
            td.indexesCompare()
            td.rowsNumCompare()
            td.sampleDataCompare()
        case 'incremental_compare':
            td = DiffDbTypeTwoTableCompare(conf_user, conf_db, conf_sql)
            td.columnsCompare()
            td.primaryKeyCompare()
            td.indexesCompare()
            td.rowsNumCompare()
            td.incrementalDataCompare()

