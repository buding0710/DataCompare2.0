#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time     : 2023/5/22 17:03
# @Author   : buding
# @Project  : pythonProject
# @File     : filedatacolumnscompare.py
# @Software : PyCharm
from DataCompare.dataobtainandprocessing.dataprocessing import DataProcessing
from DataCompare.util.getconfigration import GetConfig
from DataCompare.dataobtainandprocessing.getdatafromfile import GetData
from DataCompare.util.strtomd5 import strToMD5


class CompareColumns:
    def __init__(self,df1,df2):
        self.df1 = df1
        self.df2 = df2

    def columnsNum1(self):
        columns_num = DataProcessing(self.df1).getColumns()
        return columns_num

    def columnsNum2(self):
        columns_num = DataProcessing(self.df2).getColumns()
        return columns_num

    def columnsNumCompare(self):
        cn1 = self.columnsNum1()
        cn2 = self.columnsNum2()
        if cn1 == cn2:
            d = {"result": True}
            return d
        d = {"result": False, "cn1": cn1, "cn2": cn2}
        return d

    @strToMD5
    def columnsName1(self):
        columns_name = DataProcessing(self.df1).getColumnsName()
        return columns_name

    @strToMD5
    def columnsName2(self):
        columns_name = DataProcessing(self.df2).getColumnsName()
        return columns_name

    def columnsNameCompare(self):
        cn1,s1 = self.columnsName1()
        cn2,s2 = self.columnsName2()
        if cn1 == cn2:
            d = {"result":True}
            return d
        l = [i for i in s1 if i not in s2]
        r = [i for i in s2 if i not in s1]
        d = {"result":False,"cn1":cn1,"l":l,"cn2":cn2,"r":r}
        return d


if __name__ == '__main__':
    file_path1 = r'D:\pythonProject\DataCompare\mapping1.csv'
    conf_user = r'D:\pythonProject\DataCompare\configuration\userprofile.ini'
    file_path2 = r'D:\pythonProject\DataCompare\mapping1.xls'
    cols = GetConfig(conf_user, 'PARAMETERS').getItem('sort_values')
    df1 = GetData(file_path1).getCsvOrTxtFullData()
    df2 = GetData(file_path2).getXlsOrXlsxFullData()
    result = CompareColumns(df1,df2).columnsNameCompare()
    if result['result']:
        print(f'{file_path1}与{file_path2}的列字段名称比较结果为：一致')
    else:
        cn1 = result['cn1']
        cn2 = result['cn2']
        s1 = result['l']
        s2 = result['r']
        print(f'{file_path1}的列字段MD5编码为：{cn1}\n,独有字段为：{s1}')
        print(f'{file_path2}的列字段MD5编码为：{cn2}\n,独有字段为：{s2}')

    # result = CompareColumns(df1,df2).columnsNameCompare()
    # if result['result']:
    #     print(f'{file_path1}与{file_path2}的列字段数量比较结果为：一致')
    # else:
    #     cn1 = result['cn1']
    #     cn2 = result['cn2']
    #     print(f'{file_path1}的列字段数量为：{cn1}')
    #     print(f'{file_path2}的列字段数量为：{cn2}')
