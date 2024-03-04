#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time     : 2023/4/7 11:16
# @Author   : buding
# @Project  : pythonProject
# @File     : filedatarowscompare.py
# @Software : PyCharm
import hashlib
import datacompy

from DataCompare.dataobtainandprocessing.dataprocessing import DataProcessing
from DataCompare.util.getconfigration import GetConfig
from DataCompare.dataobtainandprocessing.getdatafromfile import GetData


class CompareRows:
    def __init__(self,df1,df2):
        self.df1 = df1
        self.df2 = df2

    def rowsNum1(self):
        rows_num = DataProcessing(self.df1).getRows()
        return rows_num

    def rowsNum2(self):
        rows_num = DataProcessing(self.df2).getRows()
        return rows_num

    def rowsNumCompare(self):
        cn1 = self.rowsNum1()
        cn2 = self.rowsNum2()
        if cn1 == cn2:
            d = {"result":True}
            return d
        d = {"result":False, "cn1":cn1, "cn2":cn2}
        return d

    def fullTableMD5compare(self):
        val1 = hashlib.md5(str(self.df1).encode('utf8')).hexdigest()
        val2 = hashlib.md5(str(self.df2).encode('utf8')).hexdigest()
        if val1 == val2:
            d = {"result":True}
            return d
        d = {"result":False,"val1":val1,"val2":val2}
        return d

    def rowsContentCompare(self,count,join_columns):#每行数据先转换为MD5对比，MD5对比不通过再根据行号获取dataframe数据，使用datacompy对比
        df1_list = self.df1.values.tolist()
        df2_list = self.df2.values.tolist()
        md5_list1 = [hashlib.md5(str(i).encode('utf8')).hexdigest() for i in df1_list]# 逐行数据MD5
        md5_list2 = [hashlib.md5(str(i).encode('utf8')).hexdigest() for i in df2_list]# 逐行数据MD5
        l = [i for i in md5_list1 if i not in md5_list2]#MD5对比
        r = [i for i in md5_list2 if i not in md5_list1]#MD5对比
        if not l and not r:
            result = {count: True}
            return result
        l_index = [md5_list1.index(i) for i in l]
        r_index = [md5_list2.index(i) for i in r]
        df1_diffrent = self.df1.iloc[l_index].copy()
        df2_diffrent = self.df2.iloc[r_index].copy()
        if eval(join_columns):
            # join_columns: 指定索引的列名，默认“None”，可以传入数组，比如：['ID', 'Name']
            # 如果对比的两份数据差异较大，可能导致关联列对比匹配不上
            res = datacompy.Compare(df1_diffrent, df2_diffrent, join_columns=eval(join_columns), ignore_spaces=True,
                                    ignore_case=False)
            result = {count: res.report()}
            return result
        # on_index: 是否要开启索引，开启之后不需要指定 join_columns，默认“False”;ignore_spaces: 是否忽略空格，默认“False”;ignore_case: 是否忽略大小写，默认“False”
        res = datacompy.Compare(df1_diffrent,df2_diffrent,on_index=True,ignore_spaces=True,ignore_case=False)
        result = {count: res.report()}
        return result


if __name__ == '__main__':
    file_path1 = r'D:\pythonProject\DataCompare\mapping1.xls'
    conf_user = r'D:\pythonProject\DataCompare\configuration\userprofile.ini'
    file_path2 = r'D:\pythonProject\DataCompare\mapping3.xls'
    once_get_rows = int(GetConfig(conf_user, 'PARAMETERS').getItem('once_get_rows'))
    cols = GetConfig(conf_user, 'PARAMETERS').getItem('sort_values')
    initial = 0
    end = once_get_rows
    gd1 = GetData(file_path1)
    gd2 = GetData(file_path2)
    df1 = gd1.getXlsOrXlsxFullData()
    df2 = gd2.getXlsOrXlsxFullData()
    sort_df1 = DataProcessing(df1).getSortFullData(cols)
    sort_df2 = DataProcessing(df2).getSortFullData(cols)
    section_df1 = DataProcessing(sort_df1).getSectionData(initial,end)
    section_df2 = DataProcessing(sort_df2).getSectionData(initial, end)

    # result = CompareRows(df1,df2).rowsNumCompare()
    # if result['result']:
    #     print(f'{file_path1}与{file_path2}的行数比较结果为：一致')
    # else:
    #     cn1 = result["cn1"]
    #     cn2 = result["cn2"]
    #     print(f'{file_path1}的行数为：{cn1}')
    #     print(f'{file_path2}的行数为：{cn2}')

    # result = CompareRows(df1,df2).fullTableMD5compare()
    # print(result)

    result = CompareRows(section_df1,section_df2).rowsContentCompare(cols)
    print(result)