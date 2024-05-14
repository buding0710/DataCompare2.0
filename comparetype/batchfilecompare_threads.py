#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time     : 2023/5/28 13:54
# @Author   : buding
# @Project  : pythonProject
# @File     : batchfilecompare.py
# @Software : PyCharm
import datetime
import math
import random
import threading
import time
import gevent as gevent
from loguru import logger

from DataCompare2.datacheck.filedatacolumnscompare import CompareColumns
from DataCompare2.datacheck.filedatarowscontentcompare_threads import CompareRowsContent
from DataCompare2.datacheck.filedatarowscompare import CompareRows
from DataCompare2.dataobtainandprocessing.dataprocessing import DataProcessing
from DataCompare2.dataobtainandprocessing.getdatafromfile import GetData
from DataCompare2.util.getconfigration import GetConfig
from DataCompare2.util.getdfbylist import getdataframebylist
#userprofile.ini里配置两个文件对比
lock = threading.Lock()


class BatchFileCompare():
    def __init__(self,user_conf,line):
        self.user_profile = GetConfig(user_conf, 'PARAMETERS').getItems()
        self.sort_values = self.user_profile['sort_values']
        self.join_columns = self.user_profile['join_columns']
        self.once_get_rows = int(self.user_profile['once_get_rows'])
        self.line = line
        self.today = str(datetime.date.today())
        self.current_time = time.strftime('%H%M%S', time.localtime())

    def fullDataCompare(self):#全量对比
        with lock:
            source_file_path = self.line.split(',')[0]
            target_file_path = self.line.split(',')[1].rstrip('\n')
            if source_file_path.split('.')[1] == 'xls' or source_file_path.split('.')[1] == 'xlsx':
                df1 = GetData(source_file_path).getXlsOrXlsxFullData()
            else:
                df1 = GetData(source_file_path).getCsvOrTxtFullData()
            if target_file_path.split('.')[1] == 'xls' or target_file_path.split('.')[1] == 'xlsx':
                df2 = GetData(target_file_path).getXlsOrXlsxFullData()
            else:
                df2 = GetData(target_file_path).getCsvOrTxtFullData()
            f_path = './report/{to}/{sf}&{tf}_{ct}.txt'.format(to=self.today, ct=self.current_time,
                                                          sf=source_file_path.split('\\')[-1],tf=target_file_path.split('\\')[-1].rstrip('\n'))
            f = open(f_path, 'a')

            logger.info("************************************************列数对比****************************************************")
            # 列数对比
            compare_columns = CompareColumns(df1, df2)
            compare_columns_num_result = compare_columns.columnsNumCompare()
            f.write(f'列数对比结果：{compare_columns_num_result}\n')
            logger.info(f'列数对比结果：{compare_columns_num_result}\n')

            logger.info("************************************************列名对比****************************************************")
            #列名对比
            compare_columns_name_result = compare_columns.columnsNameCompare()
            f.write(f'列名对比结果：{compare_columns_name_result[0]}\n')
            f.write(f'列名对比报告：{compare_columns_name_result[1]}\n')
            logger.info(f'列名对比结果：{compare_columns_name_result[0]}\n')
            logger.info(f'列名对比报告：{compare_columns_name_result[1]}\n')

            # 行对比前先排序
            Sort1 = DataProcessing(df1).getSortFullData(self.sort_values)
            Sort2 = DataProcessing(df2).getSortFullData(self.sort_values)

            logger.info("************************************************行数对比****************************************************")
            # 行数对比
            compare_rows = CompareRows(Sort1,Sort2)
            compare_rows_result = compare_rows.rowsNumCompare()
            f.write(f'行数对比结果：{compare_rows_result}\n')
            logger.info(f'行数对比结果：{compare_rows_result}\n')

            #如果其中一个的数据为空，另一个有数据，即最终对比结果为不通过，终止程序执行;如果两个数据均为空，即行数对比通过，终止程序执行
            if Sort1.empty and Sort2.empty:
                logger.info('数据为空，终止执行！\n')
                compare_rows_content_result = {"result": True}
                f.write(f'行内容对比结果：{compare_rows_content_result}\n')
                logger.info(f'行内容对比结果：{compare_rows_content_result}\n')
                return compare_rows_content_result
            if Sort1.empty or Sort2.empty:
                logger.info('数据为空，终止执行！\n')
                compare_rows_content_result = {"result": False}
                f.write(f'行内容对比结果：{compare_rows_content_result}\n')
                logger.info(f'行内容对比结果：{compare_rows_content_result}\n')
                return compare_rows_content_result

            logger.info("************************************************全量行内容对比****************************************************")
            # 行内容对比
            compare_full_table_result = compare_rows.fullTableMD5compare()
            # 如果全表数据MD5对比结果通过，即不管哪种对比方式，最终对比结果为通过，终止程序执行
            f.write(f'全表数据MD5对比结果：{compare_full_table_result}\n')
            logger.info(f'全表数据MD5对比结果：{compare_full_table_result}\n')

            if compare_full_table_result['result']:
                f.write(f'全表数据MD5比对通过，终止执行！\n')
                logger.info('全表数据MD5比对通过，终止执行！\n')
                return compare_full_table_result
            slices = []
            count = 0
            skip_rows = 0
            end = self.once_get_rows
            while True:
                count += 1
                df1_slice = DataProcessing(Sort1).getSlicesData(skip_rows, end)
                df2_slice = DataProcessing(Sort2).getSlicesData(skip_rows, end)
                skip_rows += self.once_get_rows
                end += self.once_get_rows
                if df1_slice.empty or df2_slice.empty:
                    logger.info(f'第{count}个切片数据为空，终止循环！\n')
                    break
                # 如果关联列的类型不一致，需先转换为一致的类型
                for _, element in enumerate(eval(self.join_columns)):
                    if df1_slice[element].dtype != df2_slice[element].dtype:
                        if isinstance(df1_slice[element].dtype, object):
                            convert_type = {element: str}
                        else:
                            convert_type = {element: df1_slice[element].dtype}
                        df2_slice = df2_slice.astype(convert_type)
                slices.append([count,df1_slice, df2_slice])
                logger.info(f'当前为第{count}个切片数据\n')

            logger.info('Coroutine begin...\n')
            begin = time.time()
            # 在线程中运行协程,创建协程任务列表
            comp = CompareRowsContent()
            tasks = [gevent.spawn(comp.slice_parse,slice,self.join_columns) for slice in slices]
            # 等待所有任务完成
            gevent.joinall(tasks)
            # 获取每个任务返回值
            results = [task.value[0] for task in tasks]
            reports = [task.value[1] for task in tasks]
            end = time.time()
            logger.info('Coroutine end,time cost:', end - begin, "seconds\n")
            logger.info(
                '************************************************行内容对比完成****************************************************\n')
            f.write(f'行内容对比结果：{results}\n')
            f.write(f'行内容对比报告：{reports}\n')
            f.close()
            logger.info(f'行内容对比结果：{results}\n')
            logger.info(f'行内容对比报告：{reports}\n')
            logger.info(
                f"///////////////////////当前线程为：{threading.current_thread().name}///////////////////////\n")
            return results,reports
        
    def sampleDataCompare(self):  # 抽样对比
        with lock:
            source_file_path = self.line.split(',')[0]
            target_file_path = self.line.split(',')[1].rstrip('\n')
            if source_file_path.split('.')[1] == 'xls' or source_file_path.split('.')[1] == 'xlsx':
                df1 = GetData(source_file_path).getXlsOrXlsxFullData()
            else:
                df1 = GetData(source_file_path).getCsvOrTxtFullData()
            if target_file_path.split('.')[1] == 'xls' or target_file_path.split('.')[1] == 'xlsx':
                df2 = GetData(target_file_path).getXlsOrXlsxFullData()
            else:
                df2 = GetData(target_file_path).getCsvOrTxtFullData()
            f_path = './report/{to}/{sf}&{tf}_{ct}.txt'.format(to=self.today, ct=self.current_time,
                                                               sf=source_file_path.split('\\')[-1],
                                                               tf=target_file_path.split('\\')[-1].rstrip(
                                                                   '\n'))
            f = open(f_path, 'a')

            logger.info(
                "************************************************列数对比****************************************************")
            # 列数对比
            compare_columns = CompareColumns(df1, df2)
            compare_columns_num_result = compare_columns.columnsNumCompare()
            f.write(f'列数对比结果：{compare_columns_num_result}\n')
            logger.info(f'列数对比结果：{compare_columns_num_result}\n')

            logger.info(
                "************************************************列名对比****************************************************")
            # 列名对比
            compare_columns_name_result = compare_columns.columnsNameCompare()
            f.write(f'列名对比结果：{compare_columns_name_result[0]}\n')
            f.write(f'列名对比报告：{compare_columns_name_result[1]}\n')
            logger.info(f'列名对比结果：{compare_columns_name_result[0]}\n')
            logger.info(f'列名对比报告：{compare_columns_name_result[1]}\n')

            # 行对比前先排序
            Sort1 = DataProcessing(df1).getSortFullData(self.sort_values)
            Sort2 = DataProcessing(df2).getSortFullData(self.sort_values)

            logger.info("************************************************行数对比****************************************************")
            #行数对比
            compare_rows = CompareRows(Sort1, Sort2)
            compare_rows_result = compare_rows.rowsNumCompare()
            f.write(f'行数对比结果：{compare_rows_result}\n')
            logger.info(f'行数对比结果：{compare_rows_result}\n')

            # 如果其中一个的数据为空，另一个有数据，即最终对比结果为不通过，终止程序执行;如果两个数据均为空，即行数对比通过，终止程序执行
            if Sort1.empty and Sort2.empty:
                logger.info('数据为空，终止执行！\n')
                compare_rows_content_result = {"result": True}
                f.write(f'行内容对比结果：{compare_rows_content_result}\n')
                logger.info(f'行内容对比结果：{compare_rows_content_result}\n')
                return compare_rows_content_result
            if Sort1.empty or Sort2.empty:
                logger.info('数据为空，结束比对！\n')
                compare_rows_content_result = {"result":False}
                f.write(f'行内容对比结果：{compare_rows_content_result}\n')
                logger.info(f'行内容对比结果：{compare_rows_content_result}\n')
                return compare_rows_content_result

            logger.info(
                "************************************************抽样行内容对比****************************************************")
            # 行内容对比
            compare_full_table_result = compare_rows.fullTableMD5compare()
            # 如果全表数据MD5对比结果通过，即不管哪种对比方式，最终对比结果为通过，终止程序执行
            f.write(f'全表数据MD5对比结果：{compare_full_table_result}\n')
            logger.info(f'全表数据MD5对比结果：{compare_full_table_result}\n')
            if compare_full_table_result['result']:
                f.write(f'全表数据MD5比对通过，终止执行！\n')
                logger.info('全表数据MD5比对通过，终止执行！\n')
                return compare_full_table_result
            slices = []
            count = 0
            skip_rows = 0
            end = self.once_get_rows
            while True:
                count += 1
                df1_slice = DataProcessing(Sort1).getSlicesData(skip_rows, end)
                df2_slice = DataProcessing(Sort2).getSlicesData(skip_rows, end)
                skip_rows += self.once_get_rows
                end += self.once_get_rows
                if df1_slice.empty or df2_slice.empty:
                    logger.info(f'第{count}个切片数据为空，终止循环！\n')
                    break

                ratio = self.user_profile['sample_ratio']
                # 获取一个切片的抽样数据,random.sample返回的数据会乱序，需重新排序
                l = [i for i in range(len(df1_slice))]
                index_list = random.sample(l, math.floor(len(df1_slice) * float(ratio)))#抽样
                index_list.sort()
                df1_sample = df1_slice.iloc[index_list]
                #解决IndexError: positional indexers are out-of-bounds
                df2_sample = getdataframebylist(df2_slice,index_list)

                # 如果关联列的类型不一致，需先转换为一致的类型
                for _, element in enumerate(eval(self.join_columns)):
                    if df1_sample[element].dtype != df2_sample[element].dtype:
                        if isinstance(df1_sample[element].dtype, object):
                            convert_type = {element: str}
                        else:
                            convert_type = {element: df1_sample[element].dtype}
                        df2_sample = df2_sample.astype(convert_type)
                slices.append([count,df1_sample, df2_sample])
                logger.info(f'当前为第{count}个切片数据\n')


            logger.info('Coroutine begin...\n')
            begin = time.time()
            # 在线程中运行协程,创建协程任务列表
            comp = CompareRowsContent()
            tasks = [gevent.spawn(comp.slice_parse, slice, self.join_columns) for slice in slices]
            # 等待所有任务完成
            gevent.joinall(tasks)
            # 获取每个任务返回值
            results = [task.value[0] for task in tasks]
            reports = [task.value[1] for task in tasks]
            end = time.time()
            logger.info('Coroutine end,time cost:', end - begin, "seconds\n")
            logger.info(
                '************************************************行内容对比完成****************************************************\n')
            f.write(f'行内容对比结果：{results}\n')
            f.write(f'行内容对比报告：{reports}\n')
            f.close()
            logger.info(f'行内容对比结果：{results}\n')
            logger.info(f'行内容对比报告：{reports}\n')
            logger.info(
                f"///////////////////////当前线程为：{threading.current_thread().name}///////////////////////\n")
            return results, reports

    def incrementalDataCompare(self):  # 增量对比
        with lock:
            source_file_path = self.line.split(',')[0]
            target_file_path = self.line.split(',')[1].rstrip('\n')
            if source_file_path.split('.')[1] == 'xls' or source_file_path.split('.')[1] == 'xlsx':
                df1 = GetData(source_file_path).getXlsOrXlsxFullData()
            else:
                df1 = GetData(source_file_path).getCsvOrTxtFullData()
            if target_file_path.split('.')[1] == 'xls' or target_file_path.split('.')[1] == 'xlsx':
                df2 = GetData(target_file_path).getXlsOrXlsxFullData()
            else:
                df2 = GetData(target_file_path).getCsvOrTxtFullData()
            f_path = './report/{to}/{sf}&{tf}_{ct}.txt'.format(to=self.today, ct=self.current_time,
                                                               sf=source_file_path.split('\\')[-1],
                                                               tf=target_file_path.split('\\')[-1].rstrip(
                                                                   '\n'))
            f = open(f_path, 'a')

            logger.info("************************************************列数对比****************************************************")
            # 列数对比
            compare_columns = CompareColumns(df1, df2)
            compare_columns_num_result = compare_columns.columnsNumCompare()
            f.write(f'列数对比结果：{compare_columns_num_result}\n')
            logger.info(f'列数对比结果：{compare_columns_num_result}\n')

            logger.info("************************************************列名对比****************************************************")
            # 列名对比
            compare_columns_name_result = compare_columns.columnsNameCompare()
            f.write(f'列名对比结果：{compare_columns_name_result[0]}\n')
            f.write(f'列名对比报告：{compare_columns_name_result[1]}\n')
            logger.info(f'列名对比结果：{compare_columns_name_result[0]}\n')
            logger.info(f'列名对比报告：{compare_columns_name_result[1]}\n')

            # 行对比前获取增量数据并排序
            incremental_field = self.user_profile['incremental_field']
            incremental_date = self.user_profile['incremental_date']
            date_format = self.user_profile['date_format']
            Incremental1 = DataProcessing(df1).getIncrementalData(incremental_field, incremental_date,
                                                                          date_format)
            Incremental2 = DataProcessing(df2).getIncrementalData(incremental_field, incremental_date,
                                                                          date_format)
            Sort1 = DataProcessing(Incremental1).getSortFullData(self.sort_values)
            Sort2 = DataProcessing(Incremental2).getSortFullData(self.sort_values)

            logger.info("************************************************行数对比****************************************************")
            #行数对比
            compare_rows = CompareRows(Sort1, Sort2)
            compare_rows_result = compare_rows.rowsNumCompare()
            f.write(f'行数对比结果：{compare_rows_result}\n')
            logger.info(f'行数对比结果：{compare_rows_result}\n')

            # 如果其中一个的数据为空，另一个有数据，即最终对比结果为不通过，终止程序执行;如果两个数据均为空，即行数对比通过，终止程序执行
            if Sort1.empty and Sort2.empty:
                logger.info('数据为空，终止执行！\n')
                compare_rows_content_result = {"result": True}
                f.write(f'行内容对比结果：{compare_rows_content_result}\n')
                logger.info(f'行内容对比结果：{compare_rows_content_result}\n')
                return compare_rows_content_result
            if Sort1.empty or Sort2.empty:
                logger.info('数据为空，结束比对！\n')
                compare_rows_content_result = {'result':False}
                f.write(f'行内容对比结果：{compare_rows_content_result}\n')
                logger.info(f'行内容对比结果：{compare_rows_content_result}\n')
                return compare_rows_content_result

            logger.info(
                "************************************************增量行内容对比****************************************************")
            # 行内容对比
            compare_full_table_result = compare_rows.fullTableMD5compare()
            # 如果全表数据MD5对比结果通过，即不管哪种对比方式，最终对比结果为通过，终止程序执行
            f.write(f'全表数据MD5对比结果：{compare_full_table_result}\n')
            logger.info(f'全表数据MD5对比结果：{compare_full_table_result}\n')

            if compare_full_table_result['result']:
                f.write(f'全表数据MD5比对通过，终止执行！\n')
                logger.info('全表数据MD5比对通过，终止执行！\n')
                return compare_full_table_result
            slices = []
            count = 0
            skip_rows = 0
            end = self.once_get_rows
            while True:
                count += 1
                df1_slice = DataProcessing(Sort1).getSlicesData(skip_rows, end)
                df2_slice = DataProcessing(Sort2).getSlicesData(skip_rows, end)
                skip_rows += self.once_get_rows
                end += self.once_get_rows
                if df1_slice.empty or df2_slice.empty:
                    logger.info(f'第{count}个切片数据为空，终止循环！\n')
                    break
                # 如果关联列的类型不一致，需先转换为一致的类型
                for _, element in enumerate(eval(self.join_columns)):
                    if df1_slice[element].dtype != df2_slice[element].dtype:
                        if isinstance(df1_slice[element].dtype, object):
                            convert_type = {element: str}
                        else:
                            convert_type = {element: df1_slice[element].dtype}
                        df2_slice = df2_slice.astype(convert_type)
                slices.append([count,df1_slice, df2_slice])
                logger.info(f'当前为第{count}个切片数据\n')


            logger.info('Coroutine begin...\n')
            begin = time.time()
            # 在线程中运行协程,创建协程任务列表
            comp = CompareRowsContent()
            tasks = [gevent.spawn(comp.slice_parse, slice, self.join_columns) for slice in slices]
            # 等待所有任务完成
            gevent.joinall(tasks)
            # 获取每个任务返回值
            results = [task.value[0] for task in tasks]
            reports = [task.value[1] for task in tasks]
            end = time.time()
            logger.info('Coroutine end,time cost:', end - begin, "seconds\n")
            logger.info(
                '************************************************行内容对比完成****************************************************\n')
            f.write(f'行内容对比结果：{results}\n')
            f.write(f'行内容对比报告：{reports}\n')
            f.close()
            logger.info(f'行内容对比结果：{results}\n')
            logger.info(f'行内容对比报告：{reports}\n')
            logger.info(
                f"///////////////////////当前线程为：{threading.current_thread().name}///////////////////////\n")
            return results, reports


if __name__ == '__main__':
    user_conf = r'D:\pythonProject\DataCompare\configuration\userprofile.ini'
    batch_file = r'D:\pythonProject\DataCompare\configuration\batchfilepaths.txt'




