##
# File:  AutoMessage.py
# Date:  28-Jun-2019 E. Peisach
#
# Update:
"""
Support for automatic scripts to send template drive email messages and archive in normal message stream/notes


"""
__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "peisach@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"

import sys
import logging
from wwpdb.utils.config.ConfigInfo import ConfigInfo, getSiteId
from wwpdb.apps.msgmodule.io.MessagingIo import MessagingIo
from wwpdb.utils.session.WebRequest import InputRequest
#

logger = logging.getLogger(__name__)


class AutoMessage(object):
    def __init__(self, siteId=None, topSessionDir=None, verbose=False, log=sys.stderr):
        self.__siteId = getSiteId(siteId)
        self.__cI = ConfigInfo(self.__siteId)
        self.__verbose = verbose
        self.__log = log

        if topSessionDir:
            self.__topSessionDir = topSessionDir
        else:
            self.__topSessionDir = self.__cI.get('SITE_WEB_APPS_TOP_SESSIONS_PATH')

    def __getmsgio(self):
        paramdict = {'TopSessionPath': [self.__topSessionDir]}
        reqobj = InputRequest(paramdict, verbose=True)
        reqobj.setValue('WWPDB_SITE_ID', self.__siteId)
        # Session dir

        mio = MessagingIo(reqobj, verbose=self.__verbose, log=self.__log)
        return mio

    def sendRemindUnlocked(self, depidlist):
        mio = self.__getmsgio()
        mio.autoMsg(depidlist, p_tmpltType="remind-unlocked")
