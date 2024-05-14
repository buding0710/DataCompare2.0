#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time     : 2023/5/28 6:07
# @Author   : buding
# @Project  : pythonProject
# @File     : samedbtypebatchtablecompare.py
# @Software : PyCharm
import datetime
import time

from loguru import logger

from DataCompare2.datacheck.databasedatacompare import CompareDatabaseData
from DataCompare2.util.getconfigration import GetConfig
from DataCompare2.util.getdbconnect import DatabaseConnect


class SameDbTypeBatchTableCompare():
    def __init__(self,conf_user,conf_db,conf_sql,batch_file):
        #连接数据库
        self.user_profile= GetConfig(conf_user,'PARAMETERS').getItems()
        self.conf_sql = conf_sql
        self.batch_file = batch_file
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
        self.today = str(datetime.date.today())
        self.current_time = time.strftime('%H%M%S', time.localtime())


    def fullDataCompare(self):  # 全量对比
        for _, line in enumerate(open(self.batch_file, 'r')):
            log_path = './log/{to}/{li}_{ct}.log'.format(to=self.today, ct=self.current_time, li=line.rstrip('\n'))
            logger.add(log_path, level='INFO')

            f_path = './report/{to}/{li}_{ct}.txt'.format(to=self.today, ct=self.current_time,
                                                          li=line.rstrip('\n'))
            f = open(f_path, 'a')
            self.source_table = line.split(',')[0]
            self.target_table = line.split(',')[1].rstrip('\n')
            comp = CompareDatabaseData(self.source_conn, self.source_db, self.source_table, self.target_conn, self.target_db,
                                self.target_table)

            logger.info(
                "************************************************表结构对比****************************************************")
            # 表结构对比
            table_structure_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem(
                'get_table_structure')
            compare_table_structure_result = comp.tableStructureCompare(table_structure_sql)
            f.write(f'表结构对比结果：{compare_table_structure_result[0]}\n')
            f.write(f'表结构对比报告：{compare_table_structure_result[1]}\n')
            logger.info(f'表结构对比结果：{compare_table_structure_result[0]}\n')
            logger.info(f'表结构对比报告：{compare_table_structure_result[1]}\n')

            source_db_columns_name_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_columns_name')
            target_db_columns_name_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_columns_name')

            source_db_primary_key_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_primary_key')
            target_db_primary_key_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_primary_key')

            source_db_indexes_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_indexes')
            target_db_indexes_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_indexes')

            logger.info(
                "************************************************行数对比****************************************************")
            # 行数对比
            source_db_rows_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_rows_num')
            target_db_rows_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_rows_num')
            compare_rows_result = comp.rowsNumCompare(source_db_rows_sql,
                                                                                               target_db_rows_sql)
            f.write(f'行数对比结果：{compare_rows_result}\n')
            logger.info(f'行数对比结果：{compare_rows_result}\n')
            # 如果其中一个的数据为空，另一个有数据，即最终对比结果为不通过，终止程序执行;如果两个数据均为空，即行数对比通过，终止程序执行
            if compare_rows_result['table1_rows_num'] == 0 and compare_rows_result['table2_rows_num'] == 0:
                logger.info('数据为空，终止执行！\n')
                compare_rows_content_result = {"result": True}
                f.write(f'行内容对比结果：{compare_rows_content_result}\n')
                logger.info(f'行内容对比结果：{compare_rows_content_result}\n')
                continue
            if compare_rows_result['table1_rows_num'] == 0 or compare_rows_result['table2_rows_num'] == 0:
                logger.info('数据为空，终止执行！\n')
                compare_rows_content_result = {"result": False}
                f.write(f'行内容对比结果：{compare_rows_content_result}\n')
                logger.info(f'行内容对比结果：{compare_rows_content_result}\n')
                continue

            logger.info(
                "************************************************全量行内容对比****************************************************")
            # 行内容对比
            sort_values = self.user_profile['sort_values']
            source_db_data_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_data')
            target_db_data_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_data')
            compare_full_table_result = comp.fullTableMD5compare(source_db_data_sql, target_db_data_sql,source_db_primary_key_sql, target_db_primary_key_sql ,source_db_indexes_sql, target_db_indexes_sql ,source_db_columns_name_sql, target_db_columns_name_sql,sort_values)
            # 如果全表数据MD5对比结果通过，即不管哪种对比方式，最终对比结果为通过，终止程序执行
            f.write(f'全表数据MD5对比结果：{compare_full_table_result}\n')
            logger.info(f'全表数据MD5对比结果：{compare_full_table_result}\n')
            if compare_full_table_result['result']:
                f.write(f'全表数据MD5比对通过，终止执行！\n')
                logger.info('全表数据MD5比对通过，终止执行！\n')
                continue
            source_db_get_slice_data_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_slice_data')
            target_db_get_slice_data_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_slice_data')
            join_columns = self.user_profile['join_columns']
            once_get_rows = int(self.user_profile['once_get_rows'])
            compare_rows_content_result = comp.fullRowsContentCompare(source_db_get_slice_data_sql,target_db_get_slice_data_sql,source_db_primary_key_sql,target_db_primary_key_sql, source_db_indexes_sql, target_db_indexes_sql, source_db_columns_name_sql,target_db_columns_name_sql, sort_values,once_get_rows,join_columns)
            f.write(f'行内容对比结果：{compare_rows_content_result[0]}\n')
            f.write(f'行内容对比报告：{compare_rows_content_result[1]}\n')
            f.close()
            logger.info(f'行内容对比结果：{compare_rows_content_result}\n')

    def sampleDataCompare(self):  # 抽样对比
        for _, line in enumerate(open(self.batch_file, 'r')):
            log_path = './log/{to}/{li}_{ct}.log'.format(to=self.today, ct=self.current_time, li=line.rstrip('\n'))
            logger.add(log_path, level='INFO')

            f_path = './report/{to}/{li}_{ct}.txt'.format(to=self.today, ct=self.current_time,
                                                          li=line.rstrip('\n'))
            f = open(f_path, 'a')
            self.source_table = line.split(',')[0]
            self.target_table = line.split(',')[1].rstrip('\n')
            comp = CompareDatabaseData(self.source_conn, self.source_db, self.source_table, self.target_conn,
                                       self.target_db,
                                       self.target_table)

            logger.info(
                "************************************************表结构对比****************************************************")
            # 表结构对比
            table_structure_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem(
                'get_table_structure')
            compare_table_structure_result = comp.tableStructureCompare(table_structure_sql)
            f.write(f'表结构对比结果：{compare_table_structure_result[0]}\n')
            f.write(f'表结构对比报告：{compare_table_structure_result[1]}\n')
            logger.info(f'表结构对比结果：{compare_table_structure_result[0]}\n')
            logger.info(f'表结构对比报告：{compare_table_structure_result[1]}\n')

            source_db_columns_name_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_columns_name')
            target_db_columns_name_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_columns_name')

            source_db_primary_key_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_primary_key')
            target_db_primary_key_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_primary_key')

            source_db_indexes_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_indexes')
            target_db_indexes_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_indexes')

            logger.info(
                "************************************************行数对比****************************************************")
            # 行数对比
            source_db_rows_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_rows_num')
            target_db_rows_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_rows_num')
            compare_rows_result = comp.rowsNumCompare(source_db_rows_sql,
                                                                                               target_db_rows_sql)
            f.write(f'行数对比结果：{compare_rows_result}\n')
            logger.info(f'行数对比结果：{compare_rows_result}\n')

            # 如果其中一个的数据为空，另一个有数据，即最终对比结果为不通过，终止程序执行;如果两个数据均为空，即行数对比通过，终止程序执行
            if compare_rows_result['table1_rows_num'] == 0 and compare_rows_result['table2_rows_num'] == 0:
                logger.info('数据为空，终止执行！\n')
                compare_rows_content_result = {"result": True}
                f.write(f'行内容对比结果：{compare_rows_content_result}\n')
                logger.info(f'行内容对比结果：{compare_rows_content_result}\n')
                continue
            if compare_rows_result['table1_rows_num'] == 0 or compare_rows_result['table2_rows_num'] == 0:
                logger.info('数据为空，结束比对！')
                compare_rows_content_result = {"result": False}
                f.write(f'行内容对比结果：{compare_rows_content_result}\n')
                logger.info(f'行内容对比结果：{compare_rows_content_result}\n')
                continue

            logger.info(
                "************************************************抽样行内容对比****************************************************")
            # 行内容对比
            sort_values = self.user_profile['sort_values']
            source_db_data_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_data')
            target_db_data_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_data')
            compare_full_table_result = comp.fullTableMD5compare(source_db_data_sql, target_db_data_sql,source_db_primary_key_sql, target_db_primary_key_sql ,source_db_indexes_sql, target_db_indexes_sql ,source_db_columns_name_sql, target_db_columns_name_sql,sort_values)
            # 如果全表数据MD5对比结果通过，即不管哪种对比方式，最终对比结果为通过，终止程序执行
            f.write(f'全表数据MD5对比结果：{compare_full_table_result}\n')
            logger.info(f'全表数据MD5对比结果：{compare_full_table_result}\n')

            if compare_full_table_result['result']:
                f.write(f'全表数据MD5比对通过，终止执行！\n')
                logger.info('全表数据MD5比对通过，终止执行！\n')
                continue
            ratio = self.user_profile['sample_ratio']
            source_db_get_slice_data_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_slice_data')
            target_db_get_slice_data_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_slice_data')
            join_columns = self.user_profile['join_columns']
            once_get_rows = int(self.user_profile['once_get_rows'])
            compare_rows_content_result = comp.sampleRowsContentCompare(source_db_get_slice_data_sql,target_db_get_slice_data_sql,source_db_primary_key_sql,target_db_primary_key_sql, source_db_indexes_sql, target_db_indexes_sql, source_db_columns_name_sql,target_db_columns_name_sql, sort_values,once_get_rows,join_columns,ratio)

            f.write(f'行内容对比结果：{compare_rows_content_result[0]}\n')
            f.write(f'行内容对比报告：{compare_rows_content_result[1]}\n')
            f.close()
            logger.info(f'行内容对比结果：{compare_rows_content_result}\n')

    def incrementalDataCompare(self):  # 增量对比
        for _, line in enumerate(open(self.batch_file, 'r')):
            log_path = './log/{to}/{li}_{ct}.log'.format(to=self.today, ct=self.current_time, li=line.rstrip('\n'))
            logger.add(log_path, level='INFO')

            f_path = './report/{to}/{li}_{ct}.txt'.format(to=self.today, ct=self.current_time,
                                                          li=line.rstrip('\n'))
            f = open(f_path, 'a')
            self.source_table = line.split(',')[0]
            self.target_table = line.split(',')[1].rstrip('\n')
            comp = CompareDatabaseData(self.source_conn, self.source_db, self.source_table, self.target_conn,
                                       self.target_db,
                                       self.target_table)

            logger.info(
                "************************************************表结构对比****************************************************")
            # 表结构对比
            table_structure_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem(
                'get_table_structure')
            compare_table_structure_result = comp.tableStructureCompare(table_structure_sql)
            f.write(f'表结构对比结果：{compare_table_structure_result[0]}\n')
            f.write(f'表结构对比报告：{compare_table_structure_result[1]}\n')
            logger.info(f'表结构对比结果：{compare_table_structure_result[0]}\n')
            logger.info(f'表结构对比报告：{compare_table_structure_result[1]}\n')

            source_db_columns_name_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_columns_name')
            target_db_columns_name_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_columns_name')

            source_db_primary_key_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_primary_key')
            target_db_primary_key_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_primary_key')

            source_db_indexes_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_indexes')
            target_db_indexes_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_indexes')

            logger.info(
                "************************************************行数对比****************************************************")
            # 行数对比
            source_db_rows_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_rows_num')
            target_db_rows_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_rows_num')
            compare_rows_result = comp.rowsNumCompare(source_db_rows_sql,
                                                                                               target_db_rows_sql)
            f.write(f'行数对比结果：{compare_rows_result}\n')
            logger.info(f'行数对比结果：{compare_rows_result}\n')
            # 如果其中一个的数据为空，另一个有数据，即最终对比结果为不通过，终止程序执行;如果两个数据均为空，即行数对比通过，终止程序执行
            if compare_rows_result['table1_rows_num'] == 0 and compare_rows_result['table2_rows_num'] == 0:
                logger.info('数据为空，终止执行！\n')
                compare_rows_content_result = {"result": True}
                f.write(f'行内容对比结果：{compare_rows_content_result}\n')
                logger.info(f'行内容对比结果：{compare_rows_content_result}\n')
                continue
            if compare_rows_result['table1_rows_num'] == 0 or compare_rows_result['table2_rows_num'] == 0:
                logger.info('数据为空，结束比对！\n')
                compare_rows_content_result = {'result':False}
                f.write(f'行内容对比结果：{compare_rows_content_result}\n')
                logger.info(f'行内容对比结果：{compare_rows_content_result}\n')
                continue
            logger.info(
                "************************************************增量行内容对比****************************************************")
            # 行内容对比
            sort_values = self.user_profile['sort_values']
            source_db_data_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_data')
            target_db_data_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_data')
            # 增量比对
            incremental_field = self.user_profile['incremental_field']
            incremental_date = self.user_profile['incremental_date']
            comparison = "{field} >= '{value}'".format(field=incremental_field, value=incremental_date)
            incremental_data_sql1 = source_db_data_sql.replace("1 = 1", comparison)
            incremental_data_sql2 = target_db_data_sql.replace("1 = 1", comparison)
            compare_full_table_result = comp.fullTableMD5compare(incremental_data_sql1, incremental_data_sql2,source_db_primary_key_sql, target_db_primary_key_sql ,source_db_indexes_sql, target_db_indexes_sql ,source_db_columns_name_sql, target_db_columns_name_sql,sort_values)
            # 如果全表数据MD5对比结果通过，即不管哪种对比方式，最终对比结果为通过，终止程序执行
            f.write(f'全表数据MD5对比结果：{compare_full_table_result}\n')
            logger.info(f'全表数据MD5对比结果：{compare_full_table_result}\n')
            if compare_full_table_result['result']:
                f.write(f'全表数据MD5比对通过，终止执行！\n')
                logger.info('全表数据MD5比对通过，终止执行！\n')
                continue
            date_format = self.user_profile['date_format']
            source_db_get_slice_data_sql = GetConfig(self.conf_sql, eval(self.source_db_connect)[1]).getItem('get_slice_data')
            target_db_get_slice_data_sql = GetConfig(self.conf_sql, eval(self.target_db_connect)[1]).getItem('get_slice_data')
            incremental_data_sql1 = source_db_get_slice_data_sql.replace("1 = 1", comparison)
            incremental_data_sql2 = target_db_get_slice_data_sql.replace("1 = 1", comparison)
            join_columns = self.user_profile['join_columns']
            once_get_rows = int(self.user_profile['once_get_rows'])
            compare_rows_content_result = comp.incrementalRowsContentCompare(incremental_data_sql1, incremental_data_sql2,
                                                                       source_db_primary_key_sql, target_db_primary_key_sql,
                                                                       source_db_indexes_sql, target_db_indexes_sql,
                                                                       source_db_columns_name_sql, target_db_columns_name_sql,
                                                                       sort_values, once_get_rows, join_columns, incremental_field,incremental_date,date_format)
            f.write(f'行内容对比结果：{compare_rows_content_result[0]}\n')
            f.write(f'行内容对比报告：{compare_rows_content_result[1]}\n')
            f.close()
            logger.info(f'行内容对比结果：{compare_rows_content_result}\n')


if __name__ == '__main__':
    conf_user = r'D:\pythonProject\DataCompare\configuration\userprofile.ini'
    conf_db = r'D:\pythonProject\DataCompare\configuration\databaseinformation.ini'
    conf_sql = r'D:\pythonProject\DataCompare\configuration\sql.ini'
    batch_file = r'D:\pythonProject\DataCompare\configuration\batchtablenames.txt'
    compare_type = GetConfig(conf_user, 'PARAMETERS').getItem('compare_type')
    match compare_type:
        case 'full_compare':
            td = SameDbTypeBatchTableCompare(conf_user,conf_db,conf_sql,batch_file)
            td.fullDataCompare()
        case 'sample_compare':
            td = SameDbTypeBatchTableCompare(conf_user, conf_db, conf_sql,batch_file)
            td.sampleDataCompare()
        case 'incremental_compare':
            td = SameDbTypeBatchTableCompare(conf_user, conf_db, conf_sql,batch_file)
            td.incrementalDataCompare()
