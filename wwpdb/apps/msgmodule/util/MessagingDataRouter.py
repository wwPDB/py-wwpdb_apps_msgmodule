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

from wwpdb.utils.config.ConfigInfoApp import ConfigInfoAppMessaging
from wwpdb.apps.msgmodule.db.MessagingDataImport import MessagingDataImport as DbMessagingDataImport
from wwpdb.apps.msgmodule.db.MessagingDataExport import MessagingDataExport as DbMessagingDataExport
from wwpdb.apps.msgmodule.io.MessagingDataImport import MessagingDataImport as IoMessagingDataImport
from wwpdb.apps.msgmodule.io.MessagingDataExport import MessagingDataExport as IoMessagingDataExport


class MessagingDataImport(object):
    """Routing wrapper for MessagingDataImport that selects backend based on content type.

    Transparently routes to either database-backed or file-based MessagingDataImport
    implementation depending on the content type. Messaging content types use the
    database backend, while all other content types use the original file-based backend.

    Args:
        reqObj: Request object passed to the underlying implementation
        verbose: Enable verbose logging (default: False)
        log: File handle for logging output (default: None)

    Example:
        >>> mdi = MessagingDataImport(reqObj)
        >>> # Database backend for messaging
        >>> msg_path = mdi.getFilePath(contentType="messages-to-depositor")
        >>> # File backend for other content
        >>> model_path = mdi.getFilePath(contentType="model")

    Note:
        Database-backed content types: messages-from-depositor, messages-to-depositor, notes-from-annotator
        All other content types use the original file-based implementation.
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
        siteId = str(self._reqObj.getValue("WWPDB_SITE_ID"))
        self.__legacycomm = not ConfigInfoAppMessaging(siteId).get_msgdb_support()

    def _get_db_impl(self):
        """Lazy initialization of database-backed MessagingDataImport.

        Returns:
            DbMessagingDataImport instance (cached after first call)
        """
        if self._db_impl is None:
            self._db_impl = DbMessagingDataImport(self._reqObj, self._verbose, self._log)
        return self._db_impl

    def _get_io_impl(self):
        """Lazy initialization of file-based MessagingDataImport.

        Returns:
            IoMessagingDataImport instance (cached after first call)
        """
        if self._io_impl is None:
            self._io_impl = IoMessagingDataImport(self._reqObj, self._verbose, self._log)
        return self._io_impl

    def getFilePath(self, contentType="model", format="pdbx", **kwargs):  # pylint: disable=redefined-builtin
        """Get file path, routing to appropriate backend based on content type.

        Args:
            contentType: Type of content (default: "model")
            format: File format (default: "pdbx")
            **kwargs: Additional keyword arguments passed to underlying implementation

        Returns:
            File path string (dummy path for database backend, real path for file backend)
        """
        if (not self.__legacycomm) and contentType in self.DB_BACKED_CONTENT_TYPES:
            return self._get_db_impl().getFilePath(contentType, format, **kwargs)
        else:
            return self._get_io_impl().getFilePath(contentType, format, **kwargs)

    def __getattr__(self, name):
        """Delegate undefined method calls to file-based implementation.

        Args:
            name: Method name to call

        Returns:
            Method from file-based implementation

        Note:
            This ensures all methods from the original file-based implementation
            remain accessible for non-messaging content types.
        """
        return getattr(self._get_io_impl(), name)


class MessagingDataExport(object):
    """Routing wrapper for MessagingDataExport that selects backend based on content type.

    Transparently routes to either database-backed or file-based MessagingDataExport
    implementation depending on the content type. Messaging content types use the
    database backend, while all other content types use the original file-based backend.

    Args:
        reqObj: Request object passed to the underlying implementation
        verbose: Enable verbose logging (default: False)
        log: File handle for logging output (default: None)

    Note:
        Database-backed content types: messages-from-depositor, messages-to-depositor, notes-from-annotator
        All other content types use the original file-based implementation.
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
        siteId = str(self._reqObj.getValue("WWPDB_SITE_ID"))
        self.__legacycomm = not ConfigInfoAppMessaging(siteId).get_msgdb_support()

    def _get_db_impl(self):
        """Lazy initialization of database-backed MessagingDataExport.

        Returns:
            DbMessagingDataExport instance (cached after first call)
        """
        if self._db_impl is None:
            self._db_impl = DbMessagingDataExport(self._reqObj, self._verbose, self._log)
        return self._db_impl

    def _get_io_impl(self):
        """Lazy initialization of file-based MessagingDataExport.

        Returns:
            IoMessagingDataExport instance (cached after first call)
        """
        if self._io_impl is None:
            self._io_impl = IoMessagingDataExport(self._reqObj, self._verbose, self._log)
        return self._io_impl

    def getFilePath(self, contentType="model", format="pdbx", **kwargs):  # pylint: disable=redefined-builtin
        """Get file path, routing to appropriate backend based on content type.

        Args:
            contentType: Type of content (default: "model")
            format: File format (default: "pdbx")
            **kwargs: Additional keyword arguments passed to underlying implementation

        Returns:
            File path string (dummy path for database backend, real path for file backend)
        """
        if (not self.__legacycomm) and contentType in self.DB_BACKED_CONTENT_TYPES:
            return self._get_db_impl().getFilePath(contentType, format, **kwargs)
        else:
            return self._get_io_impl().getFilePath(contentType, format, **kwargs)

    def __getattr__(self, name):
        """Delegate undefined method calls to file-based implementation.

        Args:
            name: Method name to call

        Returns:
            Method from file-based implementation

        Note:
            This ensures all methods from the original file-based implementation
            remain accessible for non-messaging content types.
        """
        return getattr(self._get_io_impl(), name)
