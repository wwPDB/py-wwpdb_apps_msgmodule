##
# File: MessagingDataExport.py (Database-backed stub)
# Date: 27-Aug-2025
#
# Stub implementation that provides the same interface as the original
# MessagingDataExport but works with database storage instead of file I/O.
##
"""
Database-backed stub for MessagingDataExport that maintains compatibility
with existing code while the database backend handles message storage.
"""

import sys
import os
import logging

logger = logging.getLogger(__name__)


class MessagingDataExport(object):
    """Database-backed stub for MessagingDataExport interface compatibility.

    Provides the same API as the original file-based MessagingDataExport but returns
    dummy file paths instead of performing actual file operations. The database backend
    (PdbxMessageIo) parses these paths to extract deposition ID and content type context.

    Args:
        reqObj: Request object containing identifier, groupid, and WWPDB_SITE_ID
        verbose: Enable verbose logging (default: False)
        log: File handle for logging output (default: sys.stderr)

    Note:
        This is a compatibility shim - no actual file I/O occurs. All data is stored
        in the database, and file paths are used only for context parsing.
    """

    def __init__(self, reqObj=None, verbose=False, log=sys.stderr):  # pylint: disable=unused-argument
        self.__verbose = verbose
        # self.__lfh = log
        self.__reqObj = reqObj
        # self.__debug = False

    def getFilePath(self, contentType="model", format="pdbx", **kwargs):  # pylint: disable=redefined-builtin,unused-argument
        """Get dummy file path containing deposition ID and content type for database context.

        Returns a path that looks like a real file path for compatibility with existing
        code, but no actual file exists. The database backend parses this path to extract
        deposition ID and content type for querying.

        Args:
            contentType: Type of content (e.g., "messages-to-depositor", "messages-from-depositor",
                "notes-from-annotator")
            format: File format (default: "pdbx")
            **kwargs: Additional keyword arguments (ignored)

        Returns:
            Dummy file path string in format: /dummy/messaging/{depId}/{depId}_{contentType}_P1.{format}.V1

        Raises:
            ValueError: If request object is missing or lacks required identifier

        Note:
            For messaging content types, uses .cif extension regardless of format parameter.
        """
        if not self.__reqObj:
            raise ValueError("Request object is required for MessagingDataExport in production")

        try:
            # Get deposition ID from request object
            depId = self.__reqObj.getValue("identifier")
            if not depId:
                raise ValueError("Deposition identifier is required")

            # Get group ID if available
            groupId = self.__reqObj.getValue("groupid") or ""
            if groupId and groupId.startswith("G_"):
                depId = groupId

            # Construct a dummy path that contains the necessary information
            # for the database backend to parse
            if contentType in ["messages-to-depositor", "messages-from-depositor", "notes-from-annotator"]:
                filename = f"{depId}_{contentType}_P1.cif.V1"
            else:
                filename = f"{depId}_{contentType}_P1.{format}.V1"

            # Return a path that looks like a real file path for compatibility
            dummy_path = os.path.join("/dummy", "messaging", depId, filename)

            if self.__verbose:
                logger.debug("MessagingDataExport stub returning dummy path: %s", dummy_path)

            return dummy_path

        except Exception as e:
            logger.error("Error in MessagingDataExport.getFilePath: %s", e)
            raise

    def getFilePathExt(self, **kwargs):
        """Get tuple of (current_path, depositor_path) dummy paths for compatibility.

        Args:
            **kwargs: Keyword arguments passed to getFilePath()

        Returns:
            Tuple of (path, path) where both elements are the same dummy path
        """
        path = self.getFilePath(**kwargs)
        return path, path

    def exportFile(self, **kwargs):  # pylint: disable=unused-argument
        """Stub method for file export compatibility (no-op for database backend).

        Args:
            **kwargs: Keyword arguments (ignored)

        Returns:
            True (always succeeds - no actual export occurs)
        """
        return True

    def getMileStoneFilePaths(self, contentType, format, version="latest", partitionNum=None):  # pylint: disable=unused-argument,redefined-builtin
        """Get dummy milestone file paths for deposit and archive versions.

        Returns a dictionary with dummy paths that the database backend can parse for
        context information. Mimics the original interface that returns both deposit
        and annotation archive paths.

        Args:
            contentType: Type of content (e.g., "messages-to-depositor")
            format: File format (e.g., "pdbx")
            version: Version selector (default: "latest", currently ignored)
            partitionNum: Partition number (currently ignored)

        Returns:
            Dictionary with keys 'dpstPth' (deposit path) and 'annotPth' (archive path),
            both containing dummy paths for database context parsing

        Raises:
            ValueError: If request object is missing or lacks required identifier
        """
        if not self.__reqObj:
            raise ValueError("Request object is required for MessagingDataExport milestone operations")

        try:
            # Get deposition ID from request object
            depId = self.__reqObj.getValue("identifier")
            if not depId:
                raise ValueError("Deposition identifier is required")

            # Get group ID if available
            groupId = self.__reqObj.getValue("groupid") or ""
            if groupId and groupId.startswith("G_") and contentType in ["messages-to-depositor", "messages-from-depositor", "notes-from-annotator"]:
                depId = groupId

            # Construct dummy paths for both deposit and annotation versions
            if contentType in ["messages-to-depositor", "messages-from-depositor", "notes-from-annotator"]:
                filename = f"{depId}_{contentType}_P1.cif.V1"
            else:
                filename = f"{depId}_{contentType}_P1.{format}.V1"

            # Return dictionary matching original interface
            pathDict = {}
            pathDict["dpstPth"] = os.path.join("/dummy", "messaging", "deposit", depId, filename)
            pathDict["annotPth"] = os.path.join("/dummy", "messaging", "archive", depId, filename)

            if self.__verbose:
                logger.debug("MessagingDataExport stub returning milestone paths: %s", pathDict)

            return pathDict

        except Exception as e:
            logger.error("Error in getMileStoneFilePaths: %s", e)
            raise
