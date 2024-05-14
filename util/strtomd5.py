#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time     : 2023/5/22 16:25
# @Author   : buding
# @Project  : pythonProject
# @File     : strtomd5.py
# @Software : PyCharm
import hashlib


def md5(x):
    md5_val = hashlib.md5(x.encode('utf8')).hexdigest()
    return md5_val


def strToMD5(func):
    def warp(*args,**kwargs):
        s = func(*args,**kwargs)
        md5_val = hashlib.md5(str(s).encode('utf8')).hexdigest()
        return md5_val,s
    return warp

if __name__ == '__main__':
    #md5
    file1 = r'D:\pythonProject\DataCompare\mapping1.xls'
    file2 = r'D:\pythonProject\DataCompare\mapping2.xls'
    df1 = XlsOrXlsxData(file1).getFullData()
    df2 = XlsOrXlsxData(file2).getFullData()
    cols1 = GetData(df1).getColumnNames()
    # t1 = md5(str(df1))#整表MD5
    # t2 = md5(str(df2))
    # print(t1,t2)
    r1 = [md5(str(i)) for i in df1.values.tolist()]#逐行数据MD5
    r2 = [md5(str(i)) for i in df2.values.tolist()]
    print(r1)
    print(r2)
    # c1 = df1.astype(str).applymap(md5)#每个数据md5
    # c2 = df2.astype(str).applymap(md5)#每个数据md5
    # print(c1)
    # print(c2)
