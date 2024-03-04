#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time     : 2023/4/7 11:08
# @Author   : buding
# @Project  : pythonProject
# @File     : getdatafromfile.py
# @Software : PyCharm
import pandas as pd


'''
从测试角度，要对比的两份数据，在设定例外条件下的数据比对，结果的反馈不能只说明行数不一致/列数不一致/列名不一致/内容不一致等笼统描述和出现一个不一致就不往下进行了；如果存在不一致，需要确定具体的不一致的行、不一致的列和不一致的内容等。
支持的数据量为亿级，并发执行

1、读取csv/txt/xls/xlsx/数据并转换成dataframe格式，然后再获取行数/列数/列名和内容等
2、假定事先不知道每个数据源的行数，通过用户设定的once_get_rows参数读取固定量的数据，通过skiprows（过滤前几行，从0开始）和nrows（一次读取行数）两个参数分批读取数据
3、后续行数对比方案：为如果rows == once_get_rows即不对比；如果下一次数据获取为空，即表示源与目标行数一致；如果rows < once_get_rows 即对比源和目标的行数。
'''


class GetData(object):
    def __init__(self,path):
        self.path = path

    def getCsvOrTxtFullData(self):# read_csv支持csv和txt的格式，with open()也支持csv和txt的格式，read_csv性能更好
        df = pd.read_csv(self.path,header=0)
        return df

    def getXlsOrXlsxFullData(self):# read_excel支持xls和xlsx的格式
        df = pd.read_excel(self.path,header=0)
        return df





if __name__ == '__main__':
    xls_file = r'/DataCompare/mapping1.xls'
    csv_file = r'/DataCompare/mapping1.csv'
    xls_d = GetData(xls_file)
    xls_df = xls_d.getXlsOrXlsxFullData()
    csv_d = GetData(csv_file)
    csv_df = csv_d.getCsvOrTxtFullData()
    # print(xls_df)
    # print(csv_df)
