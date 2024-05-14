#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time     : 2023/4/19 16:30
# @Author   : buding
# @Project  : pythonProject
# @File     : getconfigration.py
# @Software : PyCharm
import configparser

class GetConfig:
    def __init__(self,path,section):
        self.path = path
        self.section = section
        # self.conf = configparser.ConfigParser()
        self.conf = configparser.RawConfigParser()#可解决特殊字符读取
        self.conf.read(self.path)

    def getItem(self,item):
        item = self.conf[self.section][item]
        return item

    def getItems(self):
        items = self.conf[self.section].items()
        # d = {k:v for k,v in items}
        d = dict(items)
        return d

    def getInt(self,item):
        item = self.conf[self.section].getint(item)
        return item

if __name__ == '__main__':
    file_path = r'D:\pythonProject\DataCompare\configuration\databaseinformation.ini'
    conf_user = r'D:\pythonProject\DataCompare\configuration\userprofile.ini'
    conf_sql = r'D:\pythonProject\DataCompare\configuration\sql.ini'
    section = 'MYSQL SIT'
    im = 'get_data'
    d = GetConfig(file_path,section).getItems()
    print(d)

