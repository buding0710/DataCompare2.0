#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time     : 2023/5/8 17:04
# @Author   : buding
# @Project  : pythonProject
# @File     : twofilecompare.py
# @Software : PyCharm
import hashlib
import math
import random
import time
import datacompy
import gevent
from loguru import logger

from DataCompare.datacheck.filedatacolumnscompare import CompareColumns
from DataCompare.datacheck.filedatarowscompare import CompareRows
from DataCompare.dataobtainandprocessing.dataprocessing import DataProcessing
from DataCompare.util.getconfigration import GetConfig
from DataCompare.dataobtainandprocessing.getdatafromfile import GetData

# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='gb18030')
#userprofile.ini里配置两个文件对比

#协程函数
def slice_parse(slice,join_columns):#每行数据先转换为MD5对比，MD5对比不通过再根据行号获取dataframe数据，使用datacompy对比
    df1_list = slice[1].values.tolist()
    df2_list = slice[2].values.tolist()
    md5_list1 = [hashlib.md5(str(i).encode('utf8')).hexdigest() for i in df1_list]# 逐行数据MD5
    md5_list2 = [hashlib.md5(str(i).encode('utf8')).hexdigest() for i in df2_list]# 逐行数据MD5
    l = [i for i in md5_list1 if i not in md5_list2]#MD5对比
    r = [i for i in md5_list2 if i not in md5_list1]#MD5对比
    if not l and not r:
        result = {slice[0]: True}
        return result
    l_index = [md5_list1.index(i) for i in l]
    r_index = [md5_list2.index(i) for i in r]
    df1_diffrent = slice[1].iloc[l_index].copy()
    df2_diffrent = slice[2].iloc[r_index].copy()
    if eval(join_columns):
        # join_columns: 指定索引的列名，默认“None”，可以传入数组，比如：['ID', 'Name']
        # 如果对比的两份数据差异较大，可能导致关联列对比匹配不上
        res = datacompy.Compare(df1_diffrent, df2_diffrent, join_columns=eval(join_columns), ignore_spaces=True,
                                   ignore_case=False)
        result = {slice[0]: res.report()}
        return result
    # on_index: 是否要开启索引，开启之后不需要指定 join_columns，默认“False”;ignore_spaces: 是否忽略空格，默认“False”;ignore_case: 是否忽略大小写，默认“False”
    res = datacompy.Compare(df1_diffrent,df2_diffrent,on_index=True,ignore_spaces=True,ignore_case=False)
    result = {slice[0]: res.report()}
    return result


class TwoFileCompare():
    def __init__(self,user_conf):
        self.user_profile = GetConfig(user_conf, 'PARAMETERS').getItems()
        self.source_file_path = self.user_profile['source_file_path']
        self.target_file_path = self.user_profile['target_file_path']
        self.sort_values = self.user_profile['sort_values']
        self.join_columns = self.user_profile['join_columns']
        self.once_get_rows = int(self.user_profile['once_get_rows'])
        #getdata
        if self.source_file_path.split('.')[1] == 'xls' or self.source_file_path.split('.')[1] == 'xlsx':
            self.df1 = GetData(self.source_file_path).getXlsOrXlsxFullData()
        else:
            self.df1 = GetData(self.source_file_path).getCsvOrTxtFullData()
        if self.target_file_path.split('.')[1] == 'xls' or self.target_file_path.split('.')[1] == 'xlsx':
            self.df2 = GetData(self.target_file_path).getXlsOrXlsxFullData()
        else:
            self.df2 = GetData(self.target_file_path).getCsvOrTxtFullData()

    def columnsCompare(self):
        logger.info("************************************************列数对比****************************************************")
        # 列数对比
        compare_columns = CompareColumns(self.df1, self.df2)
        compare_columns_num_result = compare_columns.columnsNumCompare()
        logger.info(f'列数对比结果：{compare_columns_num_result}')
        
        logger.info("************************************************列名对比****************************************************")
        #列名对比
        compare_columns_name_result = compare_columns.columnsNameCompare()
        logger.info(f'列名对比结果：{compare_columns_name_result}')

    def rowsNumCompare(self):
        # 行对比前先排序
        Sort1 = DataProcessing(self.df1).getSortFullData(self.sort_values)
        Sort2 = DataProcessing(self.df2).getSortFullData(self.sort_values)

        logger.info(
            "************************************************行数对比****************************************************")
        # 行数对比
        compare_rows_num_result = CompareRows(Sort1, Sort2).rowsNumCompare()
        logger.info(f'行数对比结果：{compare_rows_num_result}')

        # 如果其中一个的数据为空，另一个有数据，即最终对比结果为不通过，终止程序执行;如果两个数据均为空，即行数对比通过，终止程序执行
        if Sort1.empty or Sort2.empty:
            logger.info('数据为空，终止执行！')
            compare_rows_content_result = {"result": False}
            logger.info(f'行内容对比结果：{compare_rows_content_result}')
            return compare_rows_content_result

    def fullDataCompare(self):#全量对比
        logger.info("************************************************全量行内容对比****************************************************")
        # 行内容对比
        Sort1 = DataProcessing(self.df1).getSortFullData(self.sort_values)
        Sort2 = DataProcessing(self.df2).getSortFullData(self.sort_values)
        compare_full_table_result = CompareRows(Sort1, Sort2).fullTableMD5compare()
        # 如果全表数据MD5对比结果通过，即不管哪种对比方式，最终对比结果为通过，终止程序执行
        logger.info(f'全表数据MD5对比结果：{compare_full_table_result}')
        if compare_full_table_result['result']:
            logger.info('全表数据MD5对比结果通过，终止执行！')
            return compare_full_table_result
        slices = []
        count = 1
        skip_rows = 0
        end = self.once_get_rows
        while True:
            df1_slice = DataProcessing(Sort1).getSlicesData(skip_rows, end)
            df2_slice = DataProcessing(Sort2).getSlicesData(skip_rows, end)
            skip_rows += self.once_get_rows
            end += self.once_get_rows
            if df1_slice.empty or df2_slice.empty:
                logger.info(f'第{count}个切片数据为空，终止循环！')
                print('数据为空，终止循环！')
                break
            #如果关联列的类型不一致，需先转换为一致的类型
            for _,element in enumerate(eval(self.join_columns)):
                if df1_slice[element].dtype != df2_slice[element].dtype:
                    if isinstance(df1_slice[element].dtype,object):
                        convert_type = {element: str}
                    else:
                        convert_type = {element: df1_slice[element].dtype}
                    df2_slice = df2_slice.astype(convert_type)
            slices.append([count,df1_slice, df2_slice])
            logger.info(f'当前为第{count}个切片数据')
            count += 1

        logger.info('Coroutine begin...')
        begin = time.time()
        # 在线程中运行协程,创建协程任务列表
        tasks = [gevent.spawn(slice_parse,slice,self.join_columns) for slice in slices]
        # 等待所有任务完成
        gevent.joinall(tasks)
        # 获取每个任务返回值
        results = [task.value for task in tasks]
        end = time.time()
        logger.info('Coroutine end,time cost:', end - begin, "seconds")
        logger.info(
    '************************************************行内容对比完成****************************************************')
        logger.info(f'行内容对比结果：{results}')
        return results

    def sampleDataCompare(self):  # 抽样对比
        logger.info(
                "************************************************抽样行内容对比****************************************************")
        # 行内容对比
        Sort1 = DataProcessing(self.df1).getSortFullData(self.sort_values)
        Sort2 = DataProcessing(self.df2).getSortFullData(self.sort_values)
        compare_full_table_result = CompareRows(Sort1, Sort2).fullTableMD5compare()
        # 如果全表数据MD5对比结果通过，即不管哪种对比方式，最终对比结果为通过，终止程序执行
        logger.info(f'全表数据MD5对比结果：{compare_full_table_result}')
        if compare_full_table_result['result']:
            logger.info('全表数据MD5对比结果通过，终止执行！')
            return compare_full_table_result
        slices = []
        count = 1
        skip_rows = 0
        end = self.once_get_rows
        while True:
            df1_slice = DataProcessing(Sort1).getSlicesData(skip_rows, end)
            df2_slice = DataProcessing(Sort2).getSlicesData(skip_rows, end)
            skip_rows += self.once_get_rows
            end += self.once_get_rows
            if df1_slice.empty or df2_slice.empty:
                logger.info(f'第{count}个切片数据为空，终止循环！')
                print('数据为空，终止循环！')
                break

            ratio = self.user_profile['sample_ratio']
            # 获取一个切片的抽样数据,random.sample返回的数据会乱序，需重新排序
            l = [i for i in range(len(df1_slice))]
            index_list = random.sample(l, math.floor(len(df1_slice) * float(ratio)))
            index_list.sort()
            df1_sample = df1_slice.iloc[index_list]
            df2_sample = df2_slice.iloc[index_list]

            # 如果关联列的类型不一致，需先转换为一致的类型
            for _, element in enumerate(eval(self.join_columns)):
                if df1_sample[element].dtype != df2_sample[element].dtype:
                    if isinstance(df1_sample[element].dtype, object):
                        convert_type = {element: str}
                    else:
                        convert_type = {element: df1_sample[element].dtype}
                    df2_sample = df2_sample.astype(convert_type)
            slices.append([count,df1_sample, df2_sample])
            logger.info(f'当前为第{count}个切片数据')
            count += 1

        logger.info('Coroutine begin...')
        begin = time.time()
        # 在线程中运行协程,创建协程任务列表
        tasks = [gevent.spawn(slice_parse, slice, self.join_columns) for slice in slices]
        # 等待所有任务完成
        gevent.joinall(tasks)
        # 获取每个任务返回值
        results = [task.value for task in tasks]
        end = time.time()
        logger.info('Coroutine end,time cost:', end - begin, "seconds")
        logger.info(
            '************************************************行内容对比完成****************************************************')
        logger.info(f'行内容对比结果：{results}')
        return results

    def getIncrementalData(self):
        # 行对比前获取增量数据
        incremental_field = self.user_profile['incremental_field']
        incremental_date = self.user_profile['incremental_date']
        date_format = self.user_profile['date_format']
        Incremental1 = DataProcessing(self.df1).getIncrementalData(incremental_field, incremental_date,
                                                                      date_format)
        Incremental2 = DataProcessing(self.df2).getIncrementalData(incremental_field, incremental_date,
                                                                      date_format)
        return Incremental1,Incremental2

    def incrementalDataCompare(self):  # 增量对比
        logger.info(
                "************************************************增量行内容对比****************************************************")
        # 行内容对比
        Incremental1, Incremental2 = self.getIncrementalData()
        Sort1 = DataProcessing(Incremental1).getSortFullData(self.sort_values)
        Sort2 = DataProcessing(Incremental2).getSortFullData(self.sort_values)
        compare_full_table_result = CompareRows(Sort1, Sort2).fullTableMD5compare()
        logger.info(f'全表数据MD5对比结果：{compare_full_table_result}')
        # 如果全表数据MD5对比结果通过，即不管哪种对比方式，最终对比结果为通过，终止程序执行
        if compare_full_table_result['result']:
            logger.info('全表数据MD5对比结果通过，终止执行！')
            return compare_full_table_result
        slices = []
        count = 1
        skip_rows = 0
        end = self.once_get_rows
        while True:
            df1_slice = DataProcessing(Sort1).getSlicesData(skip_rows, end)
            df2_slice = DataProcessing(Sort2).getSlicesData(skip_rows, end)
            skip_rows += self.once_get_rows
            end += self.once_get_rows
            if df1_slice.empty or df2_slice.empty:
                logger.info(f'第{count}个切片数据为空，终止循环！')
                print('数据为空，终止循环！')
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
            logger.info(f'当前为第{count}个切片数据')
            count += 1

        logger.info('Coroutine begin...')
        begin = time.time()
        # 在线程中运行协程,创建协程任务列表
        tasks = [gevent.spawn(slice_parse, slice, self.join_columns) for slice in slices]
        # 等待所有任务完成
        gevent.joinall(tasks)
        # 获取每个任务返回值
        results = [task.value for task in tasks]
        end = time.time()
        logger.info('Coroutine end,time cost:', end - begin, "seconds")
        logger.info(
            '************************************************行内容对比完成****************************************************')
        logger.info(f'行内容对比结果：{results}')
        return results

if __name__ == '__main__':
    user_conf = r'D:\pythonProject\DataCompare\configuration\userprofile.ini'
    compare_type = GetConfig(user_conf, 'PARAMETERS').getItem('compare_type')
    match compare_type:
        case 'full_compare':
            tf = TwoFileCompare(user_conf)
            tf.columnsCompare()
            tf.rowsNumCompare()
            tf.fullDataCompare()  # 全量对比
        case 'sample_compare':
            tf = TwoFileCompare(user_conf)
            tf.columnsCompare()
            tf.rowsNumCompare()
            tf.sampleDataCompare()  # 抽样对比
        case 'incremental_compare':
            tf = TwoFileCompare(user_conf)
            tf.columnsCompare()
            tf.rowsNumCompare()
            tf.incrementalDataCompare()#增量对比
