##
# File: MessagingDataImport.py (Database-backed stub)
# Date: 27-Aug-2025
#
# Stub implementation that provides the same interface as the original
# MessagingDataImport but works with database storage instead of file I/O.
##
"""
Database-backed stub for MessagingDataImport that maintains compatibility
with existing code while the database backend handles message storage.
"""

import sys
import os
import logging

logger = logging.getLogger(__name__)


class MessagingDataImport(object):
    """Database-backed stub for MessagingDataImport interface compatibility.

    Provides the same API as the original file-based MessagingDataImport but returns
    dummy file paths instead of performing actual file operations. The database backend
    (PdbxMessageIo) parses these paths to extract deposition ID and content type context.

    Args:
        reqObj: Request object containing identifier, instance, groupid, and WWPDB_SITE_ID
        verbose: Enable verbose logging (default: False)
        log: File handle for logging output (default: sys.stderr)

    Raises:
        ValueError: If request object is missing or lacks required fields

    Note:
        This is a compatibility shim - no actual file I/O occurs. All data is loaded
        from the database, and file paths are used only for context parsing.
    """

    def __init__(self, reqObj=None, verbose=False, log=sys.stderr):  # pylint: disable=unused-argument
        self.__verbose = verbose
        # self.__lfh = log
        self.__reqObj = reqObj
        # self.__debug = False

        # Set up instance variables to match original interface
        self.__setup()

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
            raise ValueError("Request object is required for MessagingDataImport in production")

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
                logger.debug("MessagingDataImport stub returning dummy path: %s", dummy_path)

            return dummy_path

        except Exception as e:
            logger.error("Error in MessagingDataImport.getFilePath: %s", e)
            raise

    def checkFilePathExists(self, filePath):  # pylint: disable=unused-argument
        """Check if file path exists (always returns True for database backend).

        Args:
            filePath: File path to check (ignored)

        Returns:
            True (always - database storage doesn't depend on file existence)
        """
        return True

    def getFileReference(self, **kwargs):  # pylint: disable=unused-argument
        """Get file reference (stub method returning None for database backend).

        Args:
            **kwargs: Keyword arguments (ignored)

        Returns:
            None (no file references in database backend)
        """
        return None

    def __setup(self):
        """Initialize instance variables from request object.

        Extracts and validates required configuration values from the request object:
        identifier, instance, WWPDB_SITE_ID, and optionally groupid.

        Raises:
            ValueError: If request object is missing or lacks required fields
        """
        if not self.__reqObj:
            raise ValueError("Request object is required for MessagingDataImport initialization")

        try:
            identifier = self.__reqObj.getValue("identifier")
            if not identifier:
                raise ValueError("Deposition identifier is required")

            self.__identifier = str(identifier).upper()
            self.__instance = str(self.__reqObj.getValue("instance") or "").upper()

            siteId = self.__reqObj.getValue("WWPDB_SITE_ID")
            if not siteId:
                raise ValueError("WWPDB_SITE_ID is required")
            # self.__siteId = str(siteId)

            self.__groupId = str(self.__reqObj.getValue("groupid") or "").upper()
            # self.__fileSource = "archive"  # Default file source

            if self.__verbose:
                logger.debug("Database stub initialized - identifier: %s, instance: %s",
                             self.__identifier, self.__instance)

        except Exception as e:
            logger.exception("Error in __setup: %s", e)
            raise ValueError(f"Failed to initialize MessagingDataImport: {e}") from e

    def getMileStoneFilePaths(self, contentType, format, version="latest", partitionNum=None):  # pylint: disable=redefined-builtin,unused-argument
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
            ValueError: If MessagingDataImport not properly initialized
        """
        if not hasattr(self, '_MessagingDataImport__identifier'):
            raise ValueError("MessagingDataImport not properly initialized - missing identifier")

        try:
            # Get base identifier
            depId = self.__identifier

            # Use group ID if available for messaging files
            if (
                    hasattr(self, '_MessagingDataImport__groupId')
                    and self.__groupId
                    and contentType in ["messages-to-depositor", "messages-from-depositor", "notes-from-annotator"]):
                depId = self.__groupId

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
                logger.debug("MessagingDataImport stub returning milestone paths: %s", pathDict)

            return pathDict

        except Exception as e:
            logger.error("Error in getMileStoneFilePaths: %s", e)
            raise

    # def __getWfFilePath(self, contentType, fmt="pdbx", fileSource="archive", version="latest", createAsNeeded=False, partitionNum=None):    # pylint: disable=unused-argument
    #     """Get workflow file path (internal method for original interface compatibility).

    #     Args:
    #         contentType: Type of content
    #         fmt: File format (default: "pdbx")
    #         fileSource: File source location (default: "archive", ignored)
    #         version: Version selector (default: "latest", ignored)
    #         createAsNeeded: Whether to create file if needed (default: False, ignored)
    #         partitionNum: Partition number (ignored)

    #     Returns:
    #         Dummy file path for database backend compatibility
    #     """
    #     return self.getFilePath(contentType=contentType, format=fmt)
