#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time     : 2023/4/13 14:50
# @Author   : buding
# @Project  : pythonProject
# @File     : dataprocessing.py
# @Software : PyCharm
import datetime
import math
import random
import pandas as pd

from DataCompare.dataobtainandprocessing.getdatafromfile import GetData


class DataProcessing(object):
    def __init__(self,df):
        self.df = df

    def getRows(self):#获取行数
        rows = self.df.shape[0]
        return rows

    # def getFileRows(path):  # 获取文件行数,仅支持txt/csv，经比较为效率最高方法，但不支持xls/xlsx
    #     with open(path, 'rb') as f:
    #         count = 0
    #         while True:
    #             data = f.read(0x400000)
    #             if not data:
    #                 break
    #             count += data.count(b'\n')
    #     return count

    def getColumns(self):#获取列数
        columns = self.df.shape[1]
        return columns

    def getColumnsName(self):#获取列名
        columns_name = list(self.df)
        return columns_name

    def getSortFullData(self,sort_values):#获取所有数据并排序
        if eval(sort_values):
            sort_df = self.df.sort_values(by=eval(sort_values), ascending=True)
            return sort_df
        sort_values = self.getColumnsName()
        sort_df = self.df.sort_values(by=sort_values,ascending=True)  # sort_index索引排序；sort_values值排序
        return sort_df

    def getSlicesData(self,initial,end):#获取切片数据
        section_df = self.df.iloc[initial:end]
        return section_df

    def getSampleData(self,ratio):#获取一个切片的抽样数据,random.sample返回的数据会乱序，需重新排序
        l = [i for i in range(len(self.df))]
        index_list = random.sample(l, math.floor(len(self.df) * float(ratio)))
        index_list.sort()
        df1_sample = self.df.iloc[index_list]
        return df1_sample

    def getIncrementalData(self,incremental_field,incremental_date,date_format):#获取增量数据，使用数据日期判断
        t = datetime.datetime.strptime(incremental_date, date_format)
        incremental_df = self.df[pd.to_datetime(self.df[incremental_field], format=date_format)>=t]
        return incremental_df

if __name__ == '__main__':
    xls_file = r'/DataCompare/mapping1.xls'
    csv_file = r'/DataCompare/mapping1.csv'
    xls_d = GetData(xls_file)
    xls_df = xls_d.getXlsOrXlsxFullData()
    csv_d = GetData(csv_file)
    csv_df = csv_d.getCsvOrTxtFullData()
    d = DataProcessing(xls_df).getSlicesData(4,4)
    print(d)