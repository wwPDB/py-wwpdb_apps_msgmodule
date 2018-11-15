##
# File:  StatusDbApi.py
# Date:  14-Sep-2016
# Updates:
##
"""

This software was developed as part of the World Wide Protein Data Bank
Common Deposition and Annotation System Project

Copyright (c) 2015 wwPDB

This software is provided under a Creative Commons Attribution 3.0 Unported
License described at http://creativecommons.org/licenses/by/3.0/.

"""
__docformat__ = "restructuredtext en"
__author__    = "Zukang Feng"
__email__     = "zfeng@rcsb.rutgers.edu"
__license__   = "Creative Commons Attribution 3.0 Unported"
__version__   = "V0.07"


import os,sys

from wwpdb.api.facade.ConfigInfo       import ConfigInfo
from wwpdb.apps.msgmodule.io.DbApiUtil import DbApiUtil


class StatusDbApi(object):
    __schemaMap = { "GET_GROUP_ID" : "select group_id from group_deposition_information where dep_set_id = '%s'",
                    "GET_DEP_ID": "select dep_set_id from group_deposition_information where group_id = '%s'",
                  }
    """
    """
    def __init__(self, siteId=None, verbose=False, log=sys.stderr):
        """
        """
        self.__lfh      = log
        self.__verbose  = verbose
        self.__siteId   = siteId
        self.__cI       = ConfigInfo(self.__siteId)
        self.__dbServer = self.__cI.get("SITE_DB_SERVER")
        self.__dbHost   = self.__cI.get("SITE_DB_HOST_NAME")
        self.__dbName   = self.__cI.get("SITE_DB_DATABASE_NAME")
        self.__dbUser   = self.__cI.get("SITE_DB_USER_NAME")
        self.__dbPw     = self.__cI.get("SITE_DB_PASSWORD")
        self.__dbSocket = self.__cI.get("SITE_DB_SOCKET")
        self.__dbPort   = int(self.__cI.get("SITE_DB_PORT_NUMBER"))
        #
        self.__dbApi    = DbApiUtil(dbServer=self.__dbServer, dbHost=self.__dbHost, dbName=self.__dbName, dbUser=self.__dbUser, dbPw=self.__dbPw, \
                                    dbSocket=self.__dbSocket, dbPort=self.__dbPort, verbose=self.__verbose, log=self.__lfh)
        self.__dbApi.setSchemaMap(self.__schemaMap)

    def __getDataDir(self, key, parameter, idx):
        list = self.__dbApi.selectData(key=key, parameter=parameter)
        if list:
            return list[idx]
        #
        return None

    def getGroupId(self, depId=None):
        if not depId:
            return None
        #
        rtnDir = self.__getDataDir("GET_GROUP_ID", (depId), 0)
        if rtnDir:
            return rtnDir['group_id']
        #
        return None

    def getEntryIdList(self, groupId=None):
        entryList = []
        if not groupId:
            return entryList
        #
        retList = self.__dbApi.selectData(key="GET_DEP_ID", parameter=(groupId))
        for retDir in retList:
            if retDir.has_key('dep_set_id') and retDir['dep_set_id']:
                entryList.append(str(retDir['dep_set_id']))
            #
        #
        return entryList

    def runUpdate(self, table=None, where=None, data=None):
        return self.__dbApi.runUpdate(table=table, where=where, data=data)
