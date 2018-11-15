##
# File:  DbApiUtil.py
# Date:  04-May-2015
# Updates:
##
"""
Providing general APIs for database access

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
import time
import datetime
import MySQLdb
#
from wwpdb.api.status.dbapi.DbConnection import DbConnection    

class DbApiUtil(object):
    def __init__(self, dbServer=None, dbHost=None, dbName=None, dbUser=None, dbPw=None, dbSocket=None, dbPort=None, verbose=False, log=sys.stderr):
        """
        """
        self.__debug=False
        self.__Nretry = 5
        self.__dbServer  = dbServer
        self.__dbHost    = dbHost
        self.__dbName    = dbName
        self.__dbUser    = dbUser
        self.__dbPw      = dbPw
        self.__dbSocket  = dbSocket
        self.__dbPort    = dbPort
        self.__verbose   = verbose
        self.__lfh       = log
        self.__schemaMap = {}
        self.__dbState   = 0

        if (self.__debug):
            self.__lfh.write("\n+DbApiUtil.__init__() using socket %r\n" % self.__dbSocket)
            self.__lfh.write("+DbApiUtil.__init__() using socket environment reference %r\n" % os.getenv("SITE_DB_SOCKET",None))


        self.__myDb  = DbConnection(dbServer=self.__dbServer,dbHost=self.__dbHost, dbName=self.__dbName, dbUser=self.__dbUser, \
                                    dbPw=self.__dbPw, dbPort=self.__dbPort, dbSocket=self.__dbSocket) 
        
        self.__dbcon = self.__myDb.connect()          

    def __reConnect(self):
        """
        """
        try:
            self.__myDb.close(self.__dbcon)
        except MySQLdb.Error:
            self.__lfh.write("+DbApiUtil.reConnect() DB connection lost - cannot close\n")
            self.__lfh.write("+DbApiUtil.reConnect() Re-connecting to the database ..\n")
            self.__lfh.write("+DbApiUtil.reConnect() UTC time = %s\n" % datetime.datetime.utcnow())

        for i in range(1, self.__Nretry):
          try:
              self.__dbcon   = self.__myDb.connect()
              self.__dbState = 0
              return True
          except MySQLdb.Error:
              self.__lfh.write("+DbApiUtil.reConnect() Cannot get re-connection : trying again\n")
              time.sleep(2*i)

        return False

    def __runSelectSQL(self, query):
        """
        """
        rows = ()
        try:
            self.__dbcon.commit()
            curs = self.__dbcon.cursor(MySQLdb.cursors.DictCursor)
            curs.execute(query)
            rows = curs.fetchall()
        except MySQLdb.Error, e:
            self.__dbState = e.args[0]
            self.__lfh.write("Database error %d: %s\n" % (e.args[0], e.args[1]))

        return rows

    def __runUpdateSQL(self, query):
        """
        """
        try:
            curs = self.__dbcon.cursor()
            curs.execute("set autocommit=0")
            nrows = curs.execute(query)
            self.__dbcon.commit()
            curs.execute("set autocommit=1")
            curs.close()
            return 'OK'
        except MySQLdb.Error, e:
            self.__dbcon.rollback()
            self.__dbState = e.args[0]
            self.__lfh.write("Database error %d: %s\n" % (e.args[0], e.args[1]))
        #
        return None
    
    def setSchemaMap(self, schemaMap):
        """
        """
        self.__schemaMap = schemaMap

    def runSelectSQL(self, sql):
        """ method to run a query
        """
        for retry in range(1, self.__Nretry):
            ret = self.__runSelectSQL(sql)
            if ret == None:
                if self.__dbState > 0:
                    time.sleep(retry*2)
                    if not self.__reConnect(): return None
                else:
                    return None
                #
            else:
                return ret
            #
        #
        return None

    def runUpdateSQL(self, sql):
        """ method to run a query
        """
        for retry in range(1, self.__Nretry):
            ret = self.__runUpdateSQL(sql)
            if ret == None:
                if self.__dbState > 0:
                    time.sleep(retry*2)
                    if not self.__reConnect(): return None
                else:
                    return None
                #
            else:
                return ret
            #
        #
        return None

    def runUpdate(self, table=None, where=None, data=None):
        if not table:
            return None
        #
        if (not where) and (not data):
            return None
        #
        rowExists = False
        if where:
            sql = "select ordinal from " + str(table) + " where " + ' and '.join(["%s = '%s'" % (k, v.replace("'", "\\'")) for k, v in where.iteritems()])
            rows = self.runSelectSQL(sql)
            if rows and len(rows) > 0:
                rowExists = True
            #
        #
        if rowExists and (not data):
            return 'OK'
        #
        if rowExists:
            sql = "update " + str(table) + " set " + ','.join(["%s = '%s'" % (k, v.replace("'", "\\'")) for k, v in data.iteritems()])
            if where:
                sql += ' where ' + ' and '.join(["%s = '%s'" % (k, v.replace("'", "\\'")) for k, v in where.iteritems()])
            #
        else:
            sql = "insert into " + str(table) + " (" + ','.join(['%s' % (k) for k, v in where.iteritems()])
            if data:
                sql += "," + ','.join(['%s' % (k) for k, v in data.iteritems()])
            #
            sql += ") values (" + ','.join(["'%s'" % (v.replace("'", "\\'")) for k, v in where.iteritems()])
            if data:
                sql += "," + ','.join(["'%s'" % (v.replace("'", "\\'")) for k, v in data.iteritems()])
            #
            sql += ")"
        #
        return self.runUpdateSQL(sql)

    def runUpdateSQLwithKey(self, key=None, parameter=()):
        """
        """
        if not key or not self.__schemaMap or not self.__schemaMap.has_key(key):
            return None
        #
        sql = self.__schemaMap[key]
        if parameter:
            sql = self.__schemaMap[key] % parameter
        #
        return self.runUpdateSQL(sql)

    def selectData(self, key=None, parameter=()):
        """
        """
        if not key or not self.__schemaMap or not self.__schemaMap.has_key(key):
            return None
        #
        sql = self.__schemaMap[key]
        if parameter:
            sql = self.__schemaMap[key] % parameter
        #
        return self.runSelectSQL(sql)
