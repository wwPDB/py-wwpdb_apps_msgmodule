##
# File: MessagingDataRouter.py
# Date: 10-Sep-2025
#
# Routing wrapper for MessagingDataImport/Export that selectively uses
# database-backed or file-based implementations based on content type.
##
"""
Routing wrapper that provides transparent access to both database-backed and 
file-based MessagingData implementations based on content type.

Uses database-backed implementation for: messages-from-depositor, messages-to-depositor, notes-from-annotator
Uses original file-based implementation for: everything else
"""

from wwpdb.apps.msgmodule.db.MessagingDataImport import MessagingDataImport as DbMessagingDataImport
from wwpdb.apps.msgmodule.db.MessagingDataExport import MessagingDataExport as DbMessagingDataExport
from wwpdb.apps.msgmodule.io.MessagingDataImport import MessagingDataImport as IoMessagingDataImport
from wwpdb.apps.msgmodule.io.MessagingDataExport import MessagingDataExport as IoMessagingDataExport


class MessagingDataImport(object):
    """
    Lazy-loading wrapper that routes to the appropriate implementation based on content type.
    
    Uses database-backed implementation for: messages-from-depositor, messages-to-depositor, notes-from-annotator
    Uses original file-based implementation for: everything else
    """
    
    # Content types that should use the database-backed implementation
    DB_BACKED_CONTENT_TYPES = {
        "messages-from-depositor",
        "messages-to-depositor", 
        "notes-from-annotator"
    }
    
    def __init__(self, reqObj=None, verbose=False, log=None):
        self._reqObj = reqObj
        self._verbose = verbose
        self._log = log
        self._db_impl = None
        self._io_impl = None
    
    def _get_db_impl(self):
        """Lazy initialization of database-backed implementation"""
        if self._db_impl is None:
            self._db_impl = DbMessagingDataImport(self._reqObj, self._verbose, self._log)
        return self._db_impl
    
    def _get_io_impl(self):
        """Lazy initialization of file-based implementation"""
        if self._io_impl is None:
            self._io_impl = IoMessagingDataImport(self._reqObj, self._verbose, self._log)
        return self._io_impl
    
    def getFilePath(self, contentType="model", format="pdbx", **kwargs):
        """Route getFilePath call to appropriate implementation based on content type"""
        if contentType in self.DB_BACKED_CONTENT_TYPES:
            return self._get_db_impl().getFilePath(contentType, format, **kwargs)
        else:
            return self._get_io_impl().getFilePath(contentType, format, **kwargs)
    
    def __getattr__(self, name):
        """For any other methods, delegate to the original file-based implementation"""
        return getattr(self._get_io_impl(), name)


class MessagingDataExport(object):
    """
    Similar wrapper for MessagingDataExport that routes based on content type.
    """
    
    # Content types that should use the database-backed implementation
    DB_BACKED_CONTENT_TYPES = {
        "messages-from-depositor",
        "messages-to-depositor", 
        "notes-from-annotator"
    }
    
    def __init__(self, reqObj=None, verbose=False, log=None):
        self._reqObj = reqObj
        self._verbose = verbose
        self._log = log
        self._db_impl = None
        self._io_impl = None
    
    def _get_db_impl(self):
        """Lazy initialization of database-backed implementation"""
        if self._db_impl is None:
            self._db_impl = DbMessagingDataExport(self._reqObj, self._verbose, self._log)
        return self._db_impl
    
    def _get_io_impl(self):
        """Lazy initialization of file-based implementation"""
        if self._io_impl is None:
            self._io_impl = IoMessagingDataExport(self._reqObj, self._verbose, self._log)
        return self._io_impl
    
    def getFilePath(self, contentType="model", format="pdbx", **kwargs):
        """Route getFilePath call to appropriate implementation based on content type"""
        if contentType in self.DB_BACKED_CONTENT_TYPES:
            return self._get_db_impl().getFilePath(contentType, format, **kwargs)
        else:
            return self._get_io_impl().getFilePath(contentType, format, **kwargs)
    
    def __getattr__(self, name):
        """For any other methods, delegate to the original file-based implementation"""
        return getattr(self._get_io_impl(), name)
