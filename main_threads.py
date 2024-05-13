#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time     : 2023/5/31 19:57
# @Author   : buding
# @Project  : pythonProject
# @File     : main.py
# @Software : PyCharm
import datetime
import os.path
import threading
import time
from loguru import logger

from DataCompare2.comparetype.batchfilecompare_threads import BatchFileCompare
from DataCompare2.comparetype.diffdbtypebatchtablecompare_threads import DiffDbTypeBatchTableCompare
from DataCompare2.comparetype.diffdbtypetwotablecompare_threads import DiffDbTypeTwoTableCompare
from DataCompare2.comparetype.samedbtypebatchtablecompare_threads import SameDbTypeBatchTableCompare
from DataCompare2.comparetype.samedbtypetwotablecompare_threads import SameDbTypeTwoTableCompare
from DataCompare2.comparetype.twofilecompare_threads import TwoFileCompare
from DataCompare2.util.getconfigration import GetConfig

if __name__ == '__main__':
    today = str(datetime.date.today())
    current_time = time.strftime('%H%M%S',time.localtime())
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
    logger.info('threads begin...')
    begin = time.time()
    threads = []
    for _, line in enumerate(open(batch_table, 'r')):
        log_path = './log/{to}/{li}_{ct}.log'.format(to = today,ct =current_time,li = line.rstrip('\n'))
        logger.add(log_path,level='INFO')
        match compare_type:
            case 'full_compare':
                logger.info('开始全量对比，当前线程为：'+threading.current_thread().name)
                threads.append(threading.Thread(target=DiffDbTypeBatchTableCompare(conf_user,conf_db,conf_sql,line).fullDataCompare, args=()))
            case 'sample_compare':
                logger.info('开始抽样对比，当前线程为：'+threading.current_thread().name)
                threads.append(threading.Thread(target=DiffDbTypeBatchTableCompare(conf_user,conf_db,conf_sql,line).sampleDataCompare, args=()))
            case 'incremental_compare':
                logger.info('开始增量对比，当前线程为：'+threading.current_thread().name)
                threads.append(threading.Thread(target=DiffDbTypeBatchTableCompare(conf_user,conf_db,conf_sql,line).incrementalDataCompare, args=()))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    end = time.time()
    logger.info('threads end,time cost:', end - begin, "seconds")

    # 不同数据库类型两表对比
    source_table = user_profile['source_table']
    target_table = user_profile['target_table']
    log_path = './log/{to}/{st}&{tt}_{ct}.log'.format(to=today, ct=current_time, st=source_table,tt=target_table.rstrip('\n'))
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
    logger.info('threads begin...')
    begin = time.time()
    threads = []
    for _, line in enumerate(open(batch_file, 'r')):
        log_path = './log/{to}/{li}_{ct}.log'.format(to=today, ct=current_time,
                                                     li=line.rstrip('\n'))
        logger.add(log_path, level='INFO')
        match compare_type:
            case 'full_compare':
                logger.info('开始全量对比，当前线程为：' + threading.current_thread().name)
                threads.append(threading.Thread(
                    target=SameDbTypeBatchTableCompare(conf_user, conf_db, conf_sql, line).fullDataCompare, args=()))
            case 'sample_compare':
                logger.info('开始抽样对比，当前线程为：' + threading.current_thread().name)
                threads.append(threading.Thread(
                    target=SameDbTypeBatchTableCompare(conf_user, conf_db, conf_sql, line).sampleDataCompare, args=()))
            case 'incremental_compare':
                logger.info('开始增量对比，当前线程为：' + threading.current_thread().name)
                threads.append(threading.Thread(
                    target=SameDbTypeBatchTableCompare(conf_user, conf_db, conf_sql, line).incrementalDataCompare, args=()))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
    end = time.time()
    logger.info("thread end,time cost:", end - begin, "seconds")

    # 相同数据库类型两表对比
    source_table = user_profile['source_table']
    target_table = user_profile['target_table']
    log_path = './log/{to}/{st}&{tt}_{ct}.log'.format(to=today, ct=current_time, st=source_table,
                                                      tt=target_table.rstrip('\n'))
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
    logger.info('threads begin...')
    begin = time.time()
    threads = []
    for _, line in enumerate(open(batch_file, 'r')):
        log_path = './log/{to}/{sf}&{tf}_{ct}.log'.format(to=today, ct=current_time,
                                                     sf=line.split(',')[0].split('\\')[-1],
                                                     tf=line.split('\\')[-1].rstrip('\n'))
        logger.add(log_path, level='INFO')
        match compare_type:
            case 'full_compare':
                logger.info('开始全量对比，当前线程为：' + threading.current_thread().name)
                threads.append(threading.Thread(target=BatchFileCompare(conf_user, line).fullDataCompare, args=()))
            case 'sample_compare':
                logger.info('开始抽样对比，当前线程为：' + threading.current_thread().name)
                threads.append(threading.Thread(target=BatchFileCompare(conf_user, line).sampleDataCompare, args=()))
            case 'incremental_compare':
                logger.info('开始增量对比，当前线程为：' + threading.current_thread().name)
                threads.append(
                    threading.Thread(target=BatchFileCompare(conf_user, line).incrementalDataCompare, args=()))
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
    end = time.time()
    logger.info("thread end,time cost:", end - begin, "seconds")


    #两个文件对比
    source_file = user_profile['source_file_path'].split('\\')[-1].split('.')[0]
    target_file = user_profile['target_file_path'].split('\\')[-1].split('.')[0]
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