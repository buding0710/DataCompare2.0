#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time     : 2023/4/7 11:14
# @Author   : buding
# @Project  : pythonProject
# @File     : databasedatacompare.py
# @Software : PyCharm
import hashlib
import math
import random
import datacompy
from loguru import logger

from DataCompare.util.getconfigration import GetConfig
from DataCompare.dataobtainandprocessing.getdatafromdb import DatabaseData
from DataCompare.util.getdbconnect import DatabaseConnect


class CompareDatabaseData:
    def __init__(self,conn1, db1, table1, conn2, db2, table2):
        self.conn1 = conn1
        self.db1 = db1
        self.table1 = table1
        self.conn2 = conn2
        self.db2 = db2
        self.table2 = table2

    def tableStructureCompare(self,get_table_structure_sql):#表结构对比，仅适用于同类型的数据库
        table1_structure = DatabaseData(self.conn1, self.db1, self.table1).getTableStructure(get_table_structure_sql)
        table2_structure = DatabaseData(self.conn2, self.db2, self.table2).getTableStructure(get_table_structure_sql)
        result = datacompy.Compare(table1_structure, table2_structure, join_columns=['Field'], ignore_spaces=True, ignore_case=True)
        return result.report()

    def columnsNumCompare(self,get_columns_num_sql1,get_columns_num_sql2):
        table1_columns_num = DatabaseData(self.conn1, self.db1, self.table1).getColumnsNumber(get_columns_num_sql1)
        table2_columns_num = DatabaseData(self.conn2, self.db2, self.table2).getColumnsNumber(get_columns_num_sql2)
        if table1_columns_num == table2_columns_num:
            d = {"result": True}
            return d
        d = {"result": False, "table1_columns_num": table1_columns_num, "table2_columns_num": table2_columns_num}
        return d

    # 获取列名、排序、默认值、是否为空、数据类型、长度、精度
    def columnsNameCompare(self,get_columns_names_sql1,get_columns_names_sql2):
        table1_columns_names = DatabaseData(self.conn1, self.db1, self.table1).getColumnsNames(get_columns_names_sql1)
        table2_columns_names = DatabaseData(self.conn2, self.db2, self.table2).getColumnsNames(get_columns_names_sql2)
        df1_list = table1_columns_names.values.tolist()
        df2_list = table2_columns_names.values.tolist()
        md5_list1 = [hashlib.md5(str(i).encode('utf8')).hexdigest() for i in df1_list]# 逐行数据MD5
        md5_list2 = [hashlib.md5(str(i).encode('utf8')).hexdigest() for i in df2_list]  # 逐行数据MD5
        l = [i for i in md5_list1 if i not in md5_list2]#MD5对比
        r = [i for i in md5_list2 if i not in md5_list1]#MD5对比
        if not l and not r:
            d = {"result": True}
            return d
        l_index = [md5_list1.index(i) for i in l]
        r_index = [md5_list2.index(i) for i in r]
        df1 = table1_columns_names.iloc[l_index].copy()
        df2 = table2_columns_names.iloc[r_index].copy()
        # ignore_spaces: 是否忽略空格，默认“False”;ignore_case: 是否忽略大小写，默认“False”
        # join_columns: 指定索引的列名，默认“None”，可以传入数组，比如：['ID', 'Name']
        # 如果对比的两份数据差异较大，可能导致关联列对比匹配不上
        result = datacompy.Compare(df1, df2, join_columns=['COLUMN_NAME'], ignore_spaces=True, ignore_case=True)
        return result.report()

    def primaryKeyCompare(self,get_primary_key_sql1,get_primary_key_sql2):
        table1_primary_key = DatabaseData(self.conn1, self.db1, self.table1).getPrimaryKey(get_primary_key_sql1)
        table2_primary_key = DatabaseData(self.conn2, self.db2, self.table2).getPrimaryKey(get_primary_key_sql2)
        result = datacompy.Compare(table1_primary_key, table2_primary_key, on_index=True, ignore_spaces=True, ignore_case=True)
        return result.report()

    def indexesCompare(self,get_indexes_sql1,get_indexes_sql2):
        table1_indexes = DatabaseData(self.conn1, self.db1, self.table1).getIndexes(get_indexes_sql1)
        table2_indexes = DatabaseData(self.conn2, self.db2, self.table2).getIndexes(get_indexes_sql2)
        result = datacompy.Compare(table1_indexes, table2_indexes, on_index=True, ignore_spaces=True, ignore_case=True)
        return result.report()

    def rowsNumCompare(self,get_rows_num_sql1,get_rows_num_sql2):
        table1_rows_num = DatabaseData(self.conn1, self.db1, self.table1).getRowsNumber(get_rows_num_sql1)
        table2_rows_num = DatabaseData(self.conn2, self.db2, self.table2).getRowsNumber(get_rows_num_sql2)
        if table1_rows_num == table2_rows_num:
            d = {"result": True}
            return d
        d = {"result": False, "table1_rows_num": table1_rows_num, "table2_rows_num": table2_rows_num}
        return d

    def fullTableMD5compare(self,get_data_sql1, get_data_sql2,get_primary_key_sql1, get_primary_key_sql2,get_indexes_sql1, get_indexes_sql2,get_columns_names_sql1,
                                                                                    get_columns_names_sql2, sort_values):
        table1_data = DatabaseData(self.conn1, self.db1, self.table1).getFullData(get_data_sql1,
                                                                                    get_primary_key_sql1,
                                                                                    get_indexes_sql1,
                                                                                    get_columns_names_sql1, sort_values,
                           )
        table2_data = DatabaseData(self.conn2, self.db2, self.table2).getFullData(get_data_sql2,
                                                                                    get_primary_key_sql2,
                                                                                    get_indexes_sql2,
                                                                                    get_columns_names_sql2, sort_values,
                           )
        val1 = hashlib.md5(str(table1_data).encode('utf8')).hexdigest()
        val2 = hashlib.md5(str(table2_data).encode('utf8')).hexdigest()
        if val1 == val2:
            d = {"result": True}
            return d
        d = {"result": False, "val1": val1, "val2": val2}
        return d

    def fullRowsContentCompare(self,get_slice_data_sql1, get_slice_data_sql2, get_primary_key_sql1,get_primary_key_sql2, get_indexes_sql1, get_indexes_sql2, get_columns_names_sql1,get_columns_names_sql2, sort_values,once_get_rows,join_columns):# 每行数据先转换为MD5对比，MD5对比不通过再根据行号获取dataframe数据，使用datacompy对比
        results = []
        count = 0
        initial_row = 0
        while True:
            table1_data = DatabaseData(self.conn1, self.db1, self.table1).getSlicesData(get_slice_data_sql1,get_primary_key_sql1,get_indexes_sql1,get_columns_names_sql1,sort_values,once_get_rows,initial_row=initial_row)
            table2_data = DatabaseData(self.conn2, self.db2, self.table2).getSlicesData(get_slice_data_sql2,get_primary_key_sql2,get_indexes_sql2,get_columns_names_sql2,sort_values,once_get_rows,initial_row=initial_row)
            count += 1
            if table1_data.empty or table2_data.empty:
                logger.info(f'第{count}个切片数据为空，终止循环！')
                print('数据为空，终止循环！')
                return results
            initial_row += once_get_rows
            df1_list = table1_data.values.tolist()
            df2_list = table2_data.values.tolist()
            md5_list1 = [hashlib.md5(str(i).encode('utf8')).hexdigest() for i in df1_list]  # 逐行数据MD5
            md5_list2 = [hashlib.md5(str(i).encode('utf8')).hexdigest() for i in df2_list]  # 逐行数据MD5
            l = [i for i in md5_list1 if i not in md5_list2]  # MD5对比
            r = [i for i in md5_list2 if i not in md5_list1]  # MD5对比
            if not l and not r:
                result = {count: True}
                results.append(result)
                continue
            l_index = [md5_list1.index(i) for i in l]
            r_index = [md5_list2.index(i) for i in r]
            # 在截取数据的语句后加一个.copy()复制一份数据给df,解决“链式索引”（chained indexing）而引起的错误或警告
            df1 = table1_data.iloc[l_index].copy()
            df2 = table2_data.iloc[r_index].copy()
            if eval(join_columns):
                # join_columns: 指定索引的列名，默认“None”，可以传入数组，比如：['ID', 'Name']
                # 如果对比的两份数据差异较大，可能导致关联列对比匹配不上
                res = datacompy.Compare(df1, df2, join_columns=eval(join_columns), ignore_spaces=True,
                                           ignore_case=False)
                result = {count:res.report()}
                results.append(result)
                continue
            primary_key = DatabaseData(self.conn1, self.db1, self.table1).getPrimaryKey(get_primary_key_sql1)
            primary_key_bool = ~primary_key.isnull()['COLUMN_NAME'][0]
            if primary_key_bool:
                res = datacompy.Compare(df1, df2, join_columns=primary_key['COLUMN_NAME'].values.tolist(),
                                           ignore_spaces=True,
                                           ignore_case=False)
                result = {count: res.report()}
                results.append(result)
                continue
            indexes = DatabaseData(self.conn1, self.db1, self.table1).getPrimaryKey(get_indexes_sql1)
            indexes_bool = ~indexes.isnull()['COLUMN_NAME'][0]
            if indexes_bool:
                res = datacompy.Compare(df1, df2, join_columns=indexes.values.tolist(), ignore_spaces=True,
                                           ignore_case=False)
                result = {count: res.report()}
                results.append(result)
                continue
            # on_index: 是否要开启索引，开启之后不需要指定 join_columns，默认“False”;ignore_spaces: 是否忽略空格，默认“False”;ignore_case: 是否忽略大小写，默认“False”
            res = datacompy.Compare(df1, df2, on_index=True, ignore_spaces=True, ignore_case=False)
            result = {count: res.report()}
            results.append(result)
            continue

    def sampleRowsContentCompare(self,get_data_sql1, get_data_sql2, get_primary_key_sql1,get_primary_key_sql2, get_indexes_sql1, get_indexes_sql2, get_columns_names_sql1,get_columns_names_sql2, sort_values,once_get_rows,join_columns,ratio):# 每行数据先转换为MD5对比，MD5对比不通过再根据行号获取dataframe数据，使用datacompy对比
        results = []
        count = 0
        initial_row = 0
        while True:
            table1_data = DatabaseData(self.conn1, self.db1, self.table1).getSlicesData(get_data_sql1,get_primary_key_sql1,get_indexes_sql1,get_columns_names_sql1,sort_values,once_get_rows,initial_row=initial_row)
            table2_data = DatabaseData(self.conn2, self.db2, self.table2).getSlicesData(get_data_sql2,get_primary_key_sql2,get_indexes_sql2,get_columns_names_sql2,sort_values,once_get_rows,initial_row=initial_row)
            count += 1
            if table1_data.empty or table2_data.empty:
                logger.info(f'第{count}个切片数据为空，终止循环！')
                print('数据为空，终止循环！')
                return results
            initial_row += once_get_rows
            # 获取一个切片的抽样数据,random.sample返回的数据会乱序，需重新排序
            l = [i for i in range(len(table1_data))]
            index_list = random.sample(l, math.floor(len(table1_data) * float(ratio)))
            index_list.sort()
            df1_sample = table1_data.iloc[index_list]
            df2_sample = table2_data.iloc[index_list]
            df1_list = df1_sample.values.tolist()
            df2_list = df2_sample.values.tolist()
            md5_list1 = [hashlib.md5(str(i).encode('utf8')).hexdigest() for i in df1_list]  # 逐行数据MD5
            md5_list2 = [hashlib.md5(str(i).encode('utf8')).hexdigest() for i in df2_list]  # 逐行数据MD5
            l = [i for i in md5_list1 if i not in md5_list2]  # MD5对比
            r = [i for i in md5_list2 if i not in md5_list1]  # MD5对比
            if not l and not r:
                result = {count: True}
                results.append(result)
                continue
            l_index = [md5_list1.index(i) for i in l]
            r_index = [md5_list2.index(i) for i in r]
            # 在截取数据的语句后加一个.copy()复制一份数据给df,解决“链式索引”（chained indexing）而引起的错误或警告
            df1 = table1_data.iloc[l_index].copy()
            df2 = table2_data.iloc[r_index].copy()
            if eval(join_columns):
                # join_columns: 指定索引的列名，默认“None”，可以传入数组，比如：['ID', 'Name']
                # 如果对比的两份数据差异较大，可能导致关联列对比匹配不上
                res = datacompy.Compare(df1, df2, join_columns=eval(join_columns), ignore_spaces=True,
                                           ignore_case=False)
                result = {count: res.report()}
                results.append(result)
                continue
            primary_key = DatabaseData(self.conn1, self.db1, self.table1).getPrimaryKey(get_primary_key_sql1)
            primary_key_bool = ~primary_key.isnull()['COLUMN_NAME'][0]
            if primary_key_bool:
                res = datacompy.Compare(df1, df2, join_columns=primary_key['COLUMN_NAME'].values.tolist(),
                                           ignore_spaces=True,
                                           ignore_case=False)
                result = {count: res.report()}
                results.append(result)
                continue
            indexes = DatabaseData(self.conn1, self.db1, self.table1).getPrimaryKey(get_indexes_sql1)
            indexes_bool = ~indexes.isnull()['COLUMN_NAME'][0]
            if indexes_bool:
                res = datacompy.Compare(df1, df2, join_columns=indexes.values.tolist(), ignore_spaces=True,
                                           ignore_case=False)
                result = {count: res.report()}
                results.append(result)
                continue
            # on_index: 是否要开启索引，开启之后不需要指定 join_columns，默认“False”;ignore_spaces: 是否忽略空格，默认“False”;ignore_case: 是否忽略大小写，默认“False”
            res = datacompy.Compare(df1, df2, on_index=True, ignore_spaces=True, ignore_case=False)
            result = {count: res.report()}
            results.append(result)
            continue

    def incrementalRowsContentCompare(self, get_data_sql1, get_data_sql2, get_primary_key_sql1,
                           get_primary_key_sql2, get_indexes_sql1, get_indexes_sql2,
                           get_columns_names_sql1, get_columns_names_sql2, sort_values, once_get_rows,
                           join_columns,incremental_field,incremental_date,date_format):  # 每行数据先转换为MD5对比，MD5对比不通过再根据行号获取dataframe数据，使用datacompy对比
        # 增量比对
        comparison = "{field} >= '{value}'".format(field=incremental_field, value=incremental_date)
        incremental_data_sql1 = get_data_sql1.replace("1 = 1", comparison)
        incremental_data_sql2 = get_data_sql2.replace("1 = 1", comparison)

        results = []
        count = 0
        initial_row = 0
        while True:
            table1_data = DatabaseData(self.conn1, self.db1, self.table1).getSlicesData(incremental_data_sql1,
                                                                                        get_primary_key_sql1,
                                                                                        get_indexes_sql1,
                                                                                        get_columns_names_sql1,
                                                                                        sort_values,
                                                                                        once_get_rows,
                                                                                        initial_row=initial_row)
            table2_data = DatabaseData(self.conn2, self.db2, self.table2).getSlicesData(incremental_data_sql2,
                                                                                        get_primary_key_sql2,
                                                                                        get_indexes_sql2,
                                                                                        get_columns_names_sql2,
                                                                                        sort_values,
                                                                                        once_get_rows,
                                                                                        initial_row=initial_row)
            count += 1
            if table1_data.empty or table2_data.empty:
                logger.info(f'第{count}个切片数据为空，终止循环！')
                print('数据为空，终止循环！')
                return results
            initial_row += once_get_rows
            df1_list = table1_data.values.tolist()
            df2_list = table2_data.values.tolist()
            md5_list1 = [hashlib.md5(str(i).encode('utf8')).hexdigest() for i in df1_list]  # 逐行数据MD5
            md5_list2 = [hashlib.md5(str(i).encode('utf8')).hexdigest() for i in df2_list]  # 逐行数据MD5
            l = [i for i in md5_list1 if i not in md5_list2]  # MD5对比
            r = [i for i in md5_list2 if i not in md5_list1]  # MD5对比
            if not l and not r:
                result = {count: True}
                results.append(result)
                continue
            l_index = [md5_list1.index(i) for i in l]
            r_index = [md5_list2.index(i) for i in r]
            # 在截取数据的语句后加一个.copy()复制一份数据给df,解决“链式索引”（chained indexing）而引起的错误或警告
            df1 = table1_data.iloc[l_index].copy()
            df2 = table2_data.iloc[r_index].copy()
            if eval(join_columns):
                # join_columns: 指定索引的列名，默认“None”，可以传入数组，比如：['ID', 'Name']
                # 如果对比的两份数据差异较大，可能导致关联列对比匹配不上
                res = datacompy.Compare(df1, df2, join_columns=eval(join_columns),
                                           ignore_spaces=True, ignore_case=False)
                result = {count: res.report()}
                results.append(result)
                continue
            primary_key = DatabaseData(self.conn1, self.db1, self.table1).getPrimaryKey(
                get_primary_key_sql1)
            primary_key_bool = ~primary_key.isnull()['COLUMN_NAME'][0]
            if primary_key_bool:
                res = datacompy.Compare(df1, df2,
                                           join_columns=primary_key['COLUMN_NAME'].values.tolist(),
                                           ignore_spaces=True,
                                           ignore_case=False)
                result = {count: res.report()}
                results.append(result)
                continue
            indexes = DatabaseData(self.conn1, self.db1, self.table1).getPrimaryKey(
                get_indexes_sql1)
            indexes_bool = ~indexes.isnull()['COLUMN_NAME'][0]
            if indexes_bool:
                res = datacompy.Compare(df1, df2, join_columns=indexes.values.tolist(),
                                           ignore_spaces=True,
                                           ignore_case=False)
                result = {count: res.report()}
                results.append(result)
                continue
            # on_index: 是否要开启索引，开启之后不需要指定 join_columns，默认“False”;ignore_spaces: 是否忽略空格，默认“False”;ignore_case: 是否忽略大小写，默认“False”
            res = datacompy.Compare(df1, df2, on_index=True, ignore_spaces=True,
                                       ignore_case=False)
            result = {count: res.report()}
            results.append(result)
            continue


if __name__ == '__main__':

    conf_user = r'D:\pythonProject\DataCompare\configuration\userprofile.ini'
    conf_db = r'D:\pythonProject\DataCompare\configuration\databaseinformation.ini'
    conf_sql = r'D:\pythonProject\DataCompare\configuration\sql.ini'
    # 连接数据库
    db_info = GetConfig(conf_user, 'PARAMETERS').getItem('source_db_conect')
    db_connect = GetConfig(conf_db, eval(db_info)[0]).getItems()
    conn = DatabaseConnect(db_connect).connectMysql()
    db = db_connect["database"]
    table1 = GetConfig(conf_user, 'PARAMETERS').getItem('source_table')
    table2 = GetConfig(conf_user, 'PARAMETERS').getItem('target_table')


    # get_columns_num_sql1 = GetConfig(conf_sql,eval(db_info)[1]).getItem('get_columns_num')
    # get_columns_num_sql2 = GetConfig(conf_sql, eval(db_info)[1]).getItem('get_columns_num')
    # table1_columns_num = DatabaseData(conn,db,table1).getColumnsNumber(get_columns_num_sql1)
    # table2_columns_num = DatabaseData(conn, db, table2).getColumnsNumber(get_columns_num_sql2)
    # print(table1_columns_num)
    # print(table2_columns_num)

    # get_columns_names_sql1 = GetConfig(conf_sql, eval(db_info)[1]).getItem('get_columns_names')
    # get_columns_names_sql2 = GetConfig(conf_sql, eval(db_info)[1]).getItem('get_columns_names')
    # r = CompareColumns(conn, db, table1,conn, db, table2).columnsNameCompare(get_columns_names_sql1,get_columns_names_sql2)
    # print(r)

    # get_primary_key_sql1 = GetConfig(conf_sql, eval(db_info)[1]).getItem('get_primary_key')
    # get_primary_key_sql2 = GetConfig(conf_sql, eval(db_info)[1]).getItem('get_primary_key')
    # r = CompareColumns(conn, db, table1,conn, db, table2).primaryKeyCompare(get_primary_key_sql1,get_primary_key_sql2)
    # print(r)

    # get_indexes_sql1 = GetConfig(conf_sql, eval(db_info)[1]).getItem('get_indexes')
    # get_indexes_sql2 = GetConfig(conf_sql, eval(db_info)[1]).getItem('get_indexes')
    # r = CompareColumns(conn, db, table1, conn, db, table2).indexesCompare(get_indexes_sql1, get_indexes_sql2)
    # print(r)

    # get_rows_sql1 = GetConfig(conf_sql, eval(db_info)[1]).getItem('get_rows_num')
    # get_rows_sql2 = GetConfig(conf_sql, eval(db_info)[1]).getItem('get_rows_num')
    # r = CompareColumns(conn, db, table1, conn, db, table2).rowsNumCompare(get_rows_sql1, get_rows_sql2)
    # print(r)

    get_primary_key_sql1 = GetConfig(conf_sql, eval(db_info)[1]).getItem('get_primary_key')
    get_primary_key_sql2 = GetConfig(conf_sql, eval(db_info)[1]).getItem('get_primary_key')
    get_indexes_sql1 = GetConfig(conf_sql, eval(db_info)[1]).getItem('get_indexes')
    get_indexes_sql2 = GetConfig(conf_sql, eval(db_info)[1]).getItem('get_indexes')
    get_columns_names_sql1 = GetConfig(conf_sql, eval(db_info)[1]).getItem('get_columns_names')
    get_columns_names_sql2 = GetConfig(conf_sql, eval(db_info)[1]).getItem('get_columns_names')
    cols = GetConfig(conf_user, 'PARAMETERS').getItem('sort_values')
    once_get_rows  = GetConfig(conf_user, 'PARAMETERS').getItem('once_get_rows')

    get_data_sql1 = GetConfig(conf_sql, eval(db_info)[1]).getItem('get_data')
    get_data_sql2 = GetConfig(conf_sql, eval(db_info)[1]).getItem('get_data')
    r = CompareColumns(conn, db, table1, conn, db, table2).rowsContentCompare(get_data_sql1, get_data_sql2, get_primary_key_sql1,get_primary_key_sql2, get_indexes_sql1,get_indexes_sql2, get_columns_names_sql1,get_columns_names_sql2, cols,once_get_rows)
    print(r)
