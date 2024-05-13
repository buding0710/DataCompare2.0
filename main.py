#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time     : 2023/5/31 19:57
# @Author   : buding
# @Project  : pythonProject
# @File     : main.py
# @Software : PyCharm
import datetime
import os
import time
from loguru import logger

from DataCompare2.comparetype.batchfilecompare import BatchFileCompare
from DataCompare2.comparetype.diffdbtypebatchtablecompare import DiffDbTypeBatchTableCompare
from DataCompare2.comparetype.diffdbtypetwotablecompare import DiffDbTypeTwoTableCompare
from DataCompare2.comparetype.samedbtypebatchtablecompare import SameDbTypeBatchTableCompare
from DataCompare2.comparetype.samedbtypetwotablecompare import SameDbTypeTwoTableCompare
from DataCompare2.comparetype.twofilecompare import TwoFileCompare
from DataCompare2.util.getconfigration import GetConfig

if __name__ == '__main__':
    today = str(datetime.date.today())
    current_time = time.strftime('%H%M%S', time.localtime())
    if not os.path.exists('./log/' + today):
        os.mkdir('./log/' + today)
    if not os.path.exists('./report/' + today):
        os.mkdir('./report/' + today)

    conf_user = r'D:\pythonProject\DataCompare2\configuration\userprofile.ini'
    conf_db = r'D:\pythonProject\DataCompare2\configuration\databaseinformation.ini'
    conf_sql = r'D:\pythonProject\DataCompare2\configuration\sql.ini'
    user_profile = GetConfig(conf_user, 'PARAMETERS').getItems()
    compare_type = user_profile.get('compare_type')

    #不同数据库类型批量表对比
    batch_table = r'D:\pythonProject\DataCompare2\configuration\batchtablenames.txt'
    match compare_type:
        case 'full_compare':
            print('开始全量对比')
            td = DiffDbTypeBatchTableCompare(conf_user,conf_db,conf_sql,batch_table)
            td.fullDataCompare()
        case 'sample_compare':
            print('开始抽样对比')
            td = DiffDbTypeBatchTableCompare(conf_user, conf_db, conf_sql,batch_table)
            td.sampleDataCompare()
        case 'incremental_compare':
            print('开始增量对比')
            td = DiffDbTypeBatchTableCompare(conf_user, conf_db, conf_sql,batch_table)
            td.incrementalDataCompare()

    # 不同数据库类型两表对比
    source_table = user_profile['source_table']
    target_table = user_profile['target_table']
    log_path = './log/{to}/{st}&{tt}_{ct}.log'.format(to=today, ct=current_time, st=source_table,tt=target_table.replace('\n', '').replace('\r', ''))
    logger.add(log_path, level='INFO')
    match compare_type:
        case 'full_compare':
            logger.info('开始全量对比')
            td = DiffDbTypeTwoTableCompare(conf_user, conf_db, conf_sql)
            td.columnsCompare()
            td.primaryKeyCompare()
            td.indexesCompare()
            td.rowsNumCompare()
            td.fullDataCompare()
        case 'sample_compare':
            logger.info('开始抽样对比')
            td = DiffDbTypeTwoTableCompare(conf_user, conf_db, conf_sql)
            td.columnsCompare()
            td.primaryKeyCompare()
            td.indexesCompare()
            td.rowsNumCompare()
            td.sampleDataCompare()
        case 'incremental_compare':
            logger.info('开始增量对比')
            td = DiffDbTypeTwoTableCompare(conf_user, conf_db, conf_sql)
            td.columnsCompare()
            td.primaryKeyCompare()
            td.indexesCompare()
            td.rowsNumCompare()
            td.incrementalDataCompare()

    # 相同数据库类型批量表对比
    batch_file = r'D:\pythonProject\DataCompare2\configuration\batchtablenames.txt'
    match compare_type:
        case 'full_compare':
            print('开始全量对比')
            td = SameDbTypeBatchTableCompare(conf_user,conf_db,conf_sql,batch_file)
            td.fullDataCompare()
        case 'sample_compare':
            print('开始抽样对比')
            td = SameDbTypeBatchTableCompare(conf_user, conf_db, conf_sql,batch_file)
            td.sampleDataCompare()
        case 'incremental_compare':
            print('开始增量对比')
            td = SameDbTypeBatchTableCompare(conf_user, conf_db, conf_sql,batch_file)
            td.incrementalDataCompare()

    # 相同数据库类型两表对比
    source_table = user_profile['source_table']
    target_table = user_profile['target_table']
    log_path = './log/{to}/{st}&{tt}_{ct}.log'.format(to=today, ct=current_time, st=source_table,
                                                      tt=target_table.replace('\n', '').replace('\r', ''))
    logger.add(log_path, level='INFO')
    match compare_type:
        case 'full_compare':
            logger.info('开始全量对比')
            td = SameDbTypeTwoTableCompare(conf_user, conf_db, conf_sql)
            td.tableStructureCompare()
            # td.columnsCompare()
            # td.primaryKeyCompare()
            # td.indexesCompare()
            td.rowsNumCompare()
            td.fullDataCompare()
        case 'sample_compare':
            logger.info('开始抽样对比')
            td = SameDbTypeTwoTableCompare(conf_user, conf_db, conf_sql)
            td.tableStructureCompare()
            # td.columnsCompare()
            # td.primaryKeyCompare()
            # td.indexesCompare()
            td.rowsNumCompare()
            td.sampleDataCompare()
        case 'incremental_compare':
            logger.info('开始增量对比')
            td = SameDbTypeTwoTableCompare(conf_user, conf_db, conf_sql)
            td.tableStructureCompare()
            # td.columnsCompare()
            # td.primaryKeyCompare()
            # td.indexesCompare()
            td.rowsNumCompare()
            td.incrementalDataCompare()

    #批量文件对比
    batch_file = r'D:\pythonProject\DataCompare2\configuration\batchfilepaths.txt'
    match compare_type:
        case 'full_compare':
            print('开始全量对比')
            BatchFileCompare(conf_user, batch_file).fullDataCompare()  # 全量对比
        case 'sample_compare':
            print('开始抽样对比')
            BatchFileCompare(conf_user, batch_file).sampleDataCompare()  # 抽样对比
        case 'incremental_compare':
            print('开始增量对比')
            BatchFileCompare(conf_user, batch_file).incrementalDataCompare()  # 增量对比

    #两个文件对比
    source_file = user_profile['source_file_path'].split('\\')[-1]
    target_file = user_profile['target_file_path'].split('\\')[-1]
    log_path = './log/{to}/{sf}&{tf}_{ct}.log'.format(to=today, ct=current_time, sf=source_file,
                                                      tf=target_file.rstrip('\n'))
    logger.add(log_path, level='INFO')
    match compare_type:
        case 'full_compare':
            logger.info('开始全量对比')
            tf = TwoFileCompare(conf_user)
            tf.columnsCompare()
            tf.rowsNumCompare()
            tf.fullDataCompare()  # 全量对比
        case 'sample_compare':
            logger.info('开始抽样对比')
            tf = TwoFileCompare(conf_user)
            tf.columnsCompare()
            tf.rowsNumCompare()
            tf.sampleDataCompare()  # 抽样对比
        case 'incremental_compare':
            logger.info('开始增量对比')
            tf = TwoFileCompare(conf_user)
            tf.columnsCompare()
            tf.rowsNumCompare()
            tf.incrementalDataCompare()  # 增量对比