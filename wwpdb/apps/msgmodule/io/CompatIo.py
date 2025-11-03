# Wrappers for various compatibility functions

import os
import sys
from typing import Dict, List, Optional

from wwpdb.utils.config.ConfigInfo import getSiteId
from wwpdb.utils.config.ConfigInfoApp import ConfigInfoAppMessaging

from mmcif_utils.message.PdbxMessageIo import PdbxMessageIo as PdbxMessageIoLegacy
from wwpdb.apps.msgmodule.db.PdbxMessageIo import PdbxMessageIo as PdbxMessageIoDb
from wwpdb.apps.msgmodule.db.LockFile import LockFile as LockFileDb
from mmcif_utils.persist.LockFile import LockFile as LockFileLegacy

from wwpdb.apps.msgmodule.db.LockFile import FileSizeLogger as FileSizeLoggerDb

import logging

logger = logging.getLogger(__name__)


class PdbxMessageIo:
    """Wrapper between file and DB implementation"""
    def __init__(self, site_id: str = None, verbose=True, log=sys.stderr, db_config: Optional[Dict] = None):
        # Use provided site_id or auto-detect - consistent with LockFile
        actual_site_id = site_id if site_id is not None else getSiteId()
        self.__legacycomm = not ConfigInfoAppMessaging(actual_site_id).get_msgdb_support()
        if self.__legacycomm:
            self.__impl = PdbxMessageIoLegacy(verbose, log)
        else:
            self.__impl = PdbxMessageIoDb(actual_site_id, verbose, log, db_config)

    def read(self, filePath: str, logtag: str = "", deposition_id: str = None) -> bool:
        if self.__legacycomm:
            return self.__impl.read(filePath, logtag)
        else:
            return self.__impl.read(filePath, logtag, deposition_id)  # pylint:  disable=too-many-function-args

    def getCategory(self, catName: str = "pdbx_deposition_message_info") -> List[Dict]:
        return self.__impl.getCategory(catName)

    def getMessageInfo(self) -> List[Dict]:
        return self.__impl.getMessageInfo()

    def getFileReferenceInfo(self) -> List[Dict]:
        return self.__impl.getFileReferenceInfo()

    def getOrigCommReferenceInfo(self) -> List[Dict]:
        return self.__impl.getOrigCommReferenceInfo()

    def getMsgStatusInfo(self) -> List[Dict]:
        return self.__impl.getMsgStatusInfo()

    def update(self, catName: str, attributeName: str, value, iRow: int = 0) -> bool:
        return self.__impl.update(catName, attributeName, value, iRow)

    def appendMessage(self, rowAttribDict: Dict) -> bool:
        return self.__impl.appendMessage(rowAttribDict)

    def appendFileReference(self, rowAttribDict: Dict) -> bool:
        return self.__impl.appendFileReference(rowAttribDict)

    def appendOrigCommReference(self, rowAttribDict: Dict) -> bool:
        return self.__impl.appendOrigCommReference(rowAttribDict)

    def appendMsgReadStatus(self, rowAttribDict: Dict) -> bool:
        return self.__impl.appendMsgReadStatus(rowAttribDict)

    def write(self, filePath: str) -> bool:
        return self.__impl.write(filePath)

    def complyStyle(self) -> bool:
        return self.__impl.complyStyle()

    def setBlock(self, blockId: str) -> bool:
        return self.__impl.setBlock(blockId)

    def newBlock(self, blockId: str) -> None:
        self.__impl.newBlock(blockId)

    def nextMessageOrdinal(self) -> int:
        return self.__impl.nextMessageOrdinal()

    def nextFileReferenceOrdinal(self) -> int:
        return self.__impl.nextFileReferenceOrdinal()

    def nextOrigCommReferenceOrdinal(self) -> int:
        return self.__impl.nextOrigCommReferenceOrdinal()

    def close(self):
        return self.__impl.close()


class LockFile(object):
    """ A simple wrapper for file locking
    """

    def __init__(self, filePath, timeoutSeconds=15, retrySeconds=.2, verbose=False, log=sys.stderr, site_id=None):
        # Use provided site_id or auto-detect - consistent with PdbxMessageIo
        actual_site_id = site_id if site_id is not None else getSiteId()
        msgdb_support = ConfigInfoAppMessaging(actual_site_id).get_msgdb_support()
        self.__legacycomm = not msgdb_support

        # Debug logging to understand routing decisions
        if verbose:
            log.write(f"LockFile: Using site_id: '{actual_site_id}'\n")
            log.write(f"LockFile: msgdb_support={msgdb_support}, legacycomm={self.__legacycomm}\n")
            log.write(f"LockFile: Will use {'Legacy' if self.__legacycomm else 'Database'} implementation\n")

        if self.__legacycomm:
            self.__limpl = LockFileLegacy(filePath, timeoutSeconds, retrySeconds, verbose, log)
        else:
            self.__limpl = LockFileDb(filePath, timeoutSeconds, retrySeconds, verbose, log)
        #

    def acquire(self):
        self.__limpl.acquire()

    def release(self):
        self.__limpl.release()

    def __enter__(self):
        """ Internal method for Context-management support.  Invoked at the beginning of a 'with' clause.
        """
        return self.__limpl.__enter__()

    def __exit__(self, exc_type, value, traceback):
        self.__limpl.__exit__(exc_type, value, traceback)

    def __del__(self):
        self.__limpl.__del__()


class FileSizeLogger(object):
    def __init__(self, filePath, verbose=False, log=sys.stderr):  # pylint: disable=unused-argument
        """Prepare the file size logger. Specify the file to report on"""
        self.__legacycomm = not ConfigInfoAppMessaging().get_msgdb_support()
        if self.__legacycomm:
            self.__limpl = FileSizeLoggerLegacy(filePath, verbose, log)
        else:
            self.__limpl = FileSizeLoggerDb(filePath, verbose, log)

    def __enter__(self):
        return self.__limpl.__enter__()

    def __exit__(self, exc_type, value, tb):
        return self.__limpl.__exit__(exc_type, value, tb)


# FileSizeLogger is now imported from wwpdb.apps.msgmodule.db.LockFile
class FileSizeLoggerLegacy(object):
    """Simple class to support trace logging for file size before and after a given action"""

    def __init__(self, filePath, verbose=False, log=sys.stderr):  # pylint: disable=unused-argument
        """Prepare the file size logger. Specify the file to report on"""
        self.__filePath = filePath
        #
        # self.__lfh = log
        self.__verbose = verbose
        self.__debug = True
        #

    def __enter__(self):
        filesize = os.stat(self.__filePath).st_size
        if self.__verbose and self.__debug:
            logger.debug("+%s -- filesize for %s before call: %s bytes.", self.__class__.__name__, self.__filePath, filesize)

        return self

    def __exit__(self, exc_type, value, tb):
        filesize = os.stat(self.__filePath).st_size
        if self.__verbose and self.__debug:
            logger.debug("+%s -- filesize for %s after call: %s bytes.", self.__class__.__name__, self.__filePath, filesize)
