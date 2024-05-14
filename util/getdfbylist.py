#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time     : 2023/5/31 21:05
# @Author   : buding
# @Project  : pythonProject
# @File     : getdfbylist.py
# @Software : PyCharm
import pandas as pd

from DataCompare2.dataobtainandprocessing.getdatafromfile import GetData


#解决IndexError: positional indexers are out-of-bounds
def getdataframebylist(df,li):
    df2 = pd.DataFrame()
    for i in li:
        if i < len(df):
            r = df.iloc[[i],:]
            df2 = pd.concat([r, df2], axis=0)
    return df2





if __name__ == '__main__':
    path = 'D:\pythonProject\DataCompare\mapping1.txt'
    li = [0, 3, 4, 6, 9, 10, 13]
    df = GetData(path).getCsvOrTxtFullData()
    # print(df)
    getdataframebylist(df,li)
