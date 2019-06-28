##
# File:  ExtractMessage.py
# Date:  28-Jun-2019 E. Peisach
#
# Update:
"""
Support for automatic extracting information from send messages


"""
__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "peisach@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"

import os
import sys
import logging
from wwpdb.utils.config.ConfigInfo import ConfigInfo, getSiteId
from mmcif.io.PdbxReader import PdbxReader
from wwpdb.io.locator.PathInfo  import PathInfo
#

logger = logging.getLogger(__name__)


class ExtractMessage(object):
    def __init__(self, siteId=None, verbose=False, log=sys.stderr):
        self.__siteId = getSiteId(siteId)
        self.__verbose = verbose
        self.__log = log
        self.__pI = PathInfo(siteId=self.__siteId, verbose=self.__verbose, log=self.__log)

    def getLastUnlocked(self, depid):
        """Returns datetime of the last time an unlocked message was sent.  This is parallel to the code in MessagingIo.py

        If no unlock message has been sent, will return None
        """

        msgfile = self.__pI.getFilePath(depid, contentType='messages-to-depositor', formatType='pdbx', fileSource="archive", versionId="1")

        if not os.path.exists(msgfile):
            return None

        ret = None

        myContainerList=[]
        ifh = open(msgfile, "r")
        pRd=PdbxReader(ifh)
        pRd.read(myContainerList)

        if len(myContainerList) >= 1:
            c0=myContainerList[0]
            catObj=c0.getObj("pdbx_deposition_message_info")
            if catObj is None:
                logger.debug("Deposition %s no pdbx_deposition_message_info category" % depid)
                return None
            else:
                #
                # Get column name index.
                #
                itDict={}
                itNameList=catObj.getItemNameList()
                for idxIt,itName in enumerate(itNameList):
                    itDict[str(itName).lower()]=idxIt
                    #
                idxOrdinalId=itDict['_pdbx_deposition_message_info.ordinal_id']
                idxLastCommDate=itDict['_pdbx_deposition_message_info.timestamp']
                idxMsgSubj=itDict['_pdbx_deposition_message_info.message_subject']
                
                maxUnlockOrdId=0
                for row in catObj.getRowList():
                    try:
                        ordinalId = int(row[idxOrdinalId])
                        msgsubj = row[idxMsgSubj]

                        if msgsubj == 'System Unlocked':
                            if ordinalId > maxUnlockOrdId:
                                maxUnlockOrdId = ordinalId
                                ret = str(row[idxLastCommDate])

                    except Exception as e:
                        logger.error("Error processing %s %s" % (depid, e))

                
        
        else:
            logger.debug("Deposition %s empty message file" % depid)
            return None

        return ret

