#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time     : 2023/5/28 13:54
# @Author   : buding
# @Project  : pythonProject
# @File     : batchfilecompare.py
# @Software : PyCharm
import math
import random
from loguru import logger

from DataCompare.datacheck.filedatacolumnscompare import CompareColumns
from DataCompare.datacheck.filedatarowscompare import CompareRows
from DataCompare.dataobtainandprocessing.dataprocessing import DataProcessing
from DataCompare.dataobtainandprocessing.getdatafromfile import GetData
from DataCompare.util.getconfigration import GetConfig

#userprofile.ini里配置两个文件对比
from DataCompare.util.getdfbylist import getdataframebylist


class BatchFileCompare():
    def __init__(self,user_conf,batch_file):
        self.user_profile = GetConfig(user_conf, 'PARAMETERS').getItems()
        self.sort_values = self.user_profile['sort_values']
        self.join_columns = self.user_profile['join_columns']
        self.once_get_rows = int(self.user_profile['once_get_rows'])
        self.batch_file = batch_file

    def fullDataCompare(self):#全量对比
        for _, line in enumerate(open(self.batch_file, 'r')):
            source_file_path = line.split(',')[0]
            target_file_path = line.split(',')[1].rstrip('\n')
            if source_file_path.split('.')[1] == 'xls' or source_file_path.split('.')[1] == 'xlsx':
                df1 = GetData(source_file_path).getXlsOrXlsxFullData()
            else:
                df1 = GetData(source_file_path).getCsvOrTxtFullData()
            if target_file_path.split('.')[1] == 'xls' or target_file_path.split('.')[1] == 'xlsx':
                df2 = GetData(target_file_path).getXlsOrXlsxFullData()
            else:
                df2 = GetData(target_file_path).getCsvOrTxtFullData()
            logger.info("************************************************列数对比****************************************************")
            # 列数对比
            compare_columns = CompareColumns(df1, df2)
            compare_columns_num_result = compare_columns.columnsNumCompare()
            logger.info(f'列数对比结果：{compare_columns_num_result}')
			
            logger.info("************************************************列名对比****************************************************")
            #列名对比
            compare_columns_name_result = compare_columns.columnsNameCompare()
            logger.info(f'列名对比结果：{compare_columns_name_result}')

            # 行对比前先排序
            Sort1 = DataProcessing(df1).getSortFullData(self.sort_values)
            Sort2 = DataProcessing(df2).getSortFullData(self.sort_values)
			
            logger.info("************************************************行数对比****************************************************")
            # 行数对比
            compare_rows = CompareRows(Sort1,Sort2)
            compare_rows_num_result = compare_rows.rowsNumCompare()
            logger.info(f'行数对比结果：{compare_rows_num_result}')
            #如果其中一个的数据为空，另一个有数据，即最终对比结果为不通过，终止程序执行;如果两个数据均为空，即行数对比通过，终止程序执行
            if Sort1.empty or Sort2.empty:
                logger.info('数据为空，终止执行！')
                compare_rows_content_result = {"result": False}
                logger.info(f'行内容对比结果：{compare_rows_content_result}')
                continue

            logger.info("************************************************全量行内容对比****************************************************")
            # 行内容对比
            compare_full_table_result = compare_rows.fullTableMD5compare()
            # 如果全表数据MD5对比结果通过，即不管哪种对比方式，最终对比结果为通过，终止程序执行
            logger.info(f'全表数据MD5对比结果：{compare_full_table_result}')
            if compare_full_table_result['result']:
                logger.info('全表数据MD5对比结果通过，终止执行！')
                continue
            results = []
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
                compare_rows_content_result = CompareRows(df1_slice,df2_slice).rowsContentCompare(count,self.join_columns)
                results.append(compare_rows_content_result)
                logger.info(f'当前为第{count}个切片数据')
                count += 1
            logger.info(
                '************************************************行内容对比完成****************************************************')
            logger.info(f'行内容对比结果：{results}')
            return results


    def sampleDataCompare(self):  # 抽样对比
        for _, line in enumerate(open(self.batch_file, 'r')):
            source_file_path = line.split(',')[0]
            target_file_path = line.split(',')[1].rstrip('\n')
            if source_file_path.split('.')[1] == 'xls' or source_file_path.split('.')[1] == 'xlsx':
                df1 = GetData(source_file_path).getXlsOrXlsxFullData()
            else:
                df1 = GetData(source_file_path).getCsvOrTxtFullData()
            if target_file_path.split('.')[1] == 'xls' or target_file_path.split('.')[1] == 'xlsx':
                df2 = GetData(target_file_path).getXlsOrXlsxFullData()
            else:
                df2 = GetData(target_file_path).getCsvOrTxtFullData()

            logger.info(
                "************************************************列数对比****************************************************")
            # 列数对比
            compare_columns = CompareColumns(df1, df2)
            compare_columns_num_result = compare_columns.columnsNumCompare()
            logger.info(f'列数对比结果：{compare_columns_num_result}')

            logger.info(
                "************************************************列名对比****************************************************")
            # 列名对比
            compare_columns_name_result = compare_columns.columnsNameCompare()
            logger.info(f'列名对比结果：{compare_columns_name_result}')

            # 行对比前先排序
            Sort1 = DataProcessing(df1).getSortFullData(self.sort_values)
            Sort2 = DataProcessing(df2).getSortFullData(self.sort_values)

            logger.info(
                "************************************************行数对比****************************************************")
            # 行数对比
            compare_rows = CompareRows(Sort1, Sort2)
            compare_rows_num_result = compare_rows.rowsNumCompare()
            logger.info(f'行数对比结果：{compare_rows_num_result}')

            # 如果其中一个的数据为空，另一个有数据，即最终对比结果为不通过，终止程序执行;如果两个数据均为空，即行数对比通过，终止程序执行
            if Sort1.empty or Sort2.empty:
                logger.info('数据为空，结束比对！')
                compare_rows_content_result = {"result": False}
                logger.info(f'行内容对比结果：{compare_rows_content_result}')
                continue

            compare_full_table_result = compare_rows.fullTableMD5compare()
            logger.info(f'全表数据MD5对比结果：{compare_full_table_result}')
            # 如果全表数据MD5对比结果通过，即不管哪种对比方式，最终对比结果为通过，终止程序执行
            if compare_full_table_result['result']:
                logger.info('全表数据MD5对比结果通过，终止执行！')
                continue
            results = []
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
                compare_rows_content_result = CompareRows(df1_sample,df2_sample).rowsContentCompare(count,self.join_columns)
                results.append(compare_rows_content_result)
                logger.info(f'当前为第{count}个切片数据')
                count += 1

            logger.info(
                '************************************************行内容对比完成****************************************************')
            logger.info(f'行内容对比结果：{results}')
            return results

    def incrementalDataCompare(self):  # 增量对比
        for _, line in enumerate(open(self.batch_file, 'r')):
            source_file_path = line.split(',')[0]
            target_file_path = line.split(',')[1].rstrip('\n')
            if source_file_path.split('.')[1] == 'xls' or source_file_path.split('.')[1] == 'xlsx':
                df1 = GetData(source_file_path).getXlsOrXlsxFullData()
            else:
                df1 = GetData(source_file_path).getCsvOrTxtFullData()
            if target_file_path.split('.')[1] == 'xls' or target_file_path.split('.')[1] == 'xlsx':
                df2 = GetData(target_file_path).getXlsOrXlsxFullData()
            else:
                df2 = GetData(target_file_path).getCsvOrTxtFullData()

            logger.info(
                "************************************************列数对比****************************************************")
            # 列数对比
            compare_columns = CompareColumns(df1, df2)
            compare_columns_num_result = compare_columns.columnsNumCompare()
            logger.info(f'列数对比结果：{compare_columns_num_result}')

            logger.info(
                "************************************************列名对比****************************************************")
            # 列名对比
            compare_columns_name_result = compare_columns.columnsNameCompare()
            logger.info(f'列名对比结果：{compare_columns_name_result}')

            # 行对比前获取增量数据并排序
            incremental_field = self.user_profile['incremental_field']
            incremental_date = self.user_profile['incremental_date']
            date_format = self.user_profile['date_format']
            incremental1 = DataProcessing(df1).getIncrementalData(incremental_field, incremental_date,
                                                                          date_format)
            incremental2 = DataProcessing(df2).getIncrementalData(incremental_field, incremental_date,
                                                                          date_format)
            Sort1 = DataProcessing(incremental1).getSortFullData(self.sort_values)
            Sort2 = DataProcessing(incremental2).getSortFullData(self.sort_values)

            logger.info(
                "************************************************行数对比****************************************************")
            # 行数对比
            compare_rows = CompareRows(Sort1, Sort2)
            compare_rows_num_result = compare_rows.rowsNumCompare()
            logger.info(f'行数对比结果：{compare_rows_num_result}')

            # 如果其中一个的数据为空，另一个有数据，即最终对比结果为不通过，终止程序执行;如果两个数据均为空，即行数对比通过，终止程序执行
            if Sort1.empty or Sort2.empty:
                logger.info('数据为空，结束比对！')
                compare_rows_content_result = {'result': False}
                logger.info(f'行内容对比结果：{compare_rows_content_result}')
                continue

            logger.info(
                "************************************************增量行内容对比****************************************************")
            # 行内容对比
            compare_full_table_result = compare_rows.fullTableMD5compare()
            logger.info(f'全表数据MD5对比结果：{compare_full_table_result}')
            # 如果全表数据MD5对比结果通过，即不管哪种对比方式，最终对比结果为通过，终止程序执行
            if compare_full_table_result['result']:
                logger.info('全表数据MD5对比结果通过，终止执行！')
                continue
            count = 1
            results = []
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
                compare_rows_content_result = CompareRows(df1_slice, df2_slice).rowsContentCompare(count,
                                                                                                     self.join_columns)
                results.append(compare_rows_content_result)
                logger.info(f'当前为第{count}个切片数据')
                count += 1
            logger.info(
                '************************************************行内容对比完成****************************************************')
            logger.info(f'行内容对比结果：{results}')
            return results

if __name__ == '__main__':
    user_conf = r'D:\pythonProject\DataCompare\configuration\userprofile.ini'
    batch_file = r'D:\pythonProject\DataCompare\configuration\batchfilepaths.txt'
    compare_type = GetConfig(user_conf, 'PARAMETERS').getItem('compare_type')
    match compare_type:
        case 'full_compare':
            BatchFileCompare(user_conf,batch_file).fullDataCompare()  # 全量对比
        case 'sample_compare':
            BatchFileCompare(user_conf,batch_file).sampleDataCompare()  # 抽样对比
        case 'incremental_compare':
            BatchFileCompare(user_conf,batch_file).incrementalDataCompare()  # 增量对比