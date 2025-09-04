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
    """
    Stub class to maintain compatibility with existing MessagingIo code.
    
    Since we're using database storage, most file operations are no longer needed.
    This class provides the same interface but returns dummy file paths.
    """

    def __init__(self, reqObj=None, verbose=False, log=sys.stderr):
        self.__verbose = verbose
        self.__lfh = log
        self.__reqObj = reqObj
        self.__debug = False

    def getFilePath(self, contentType="model", format="pdbx", **kwargs):
        """
        Return a dummy file path that contains the deposition ID and content type.
        
        The database-backed PdbxMessageIo will parse this path to extract context
        information without actually accessing the file.
        """
        try:
            # Get deposition ID from request object
            depId = self.__reqObj.getValue("identifier") if self.__reqObj else "D_000000"
            
            # Get group ID if available
            groupId = self.__reqObj.getValue("groupid") if self.__reqObj else ""
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
            return None

    def getFilePathExt(self, **kwargs):
        """
        Stub method for compatibility - returns tuple of (current_path, depositor_path).
        """
        path = self.getFilePath(**kwargs)
        return path, path

    def exportFile(self, **kwargs):
        """
        Stub method for compatibility.
        """
        return True
