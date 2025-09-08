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
    """
    Stub class to maintain compatibility with existing MessagingIo code.
    
    Since we're using database storage, most file operations are no longer needed.
    This class provides the same interface but returns dummy file paths that 
    the PdbxMessageIo can parse for deposition_id and content_type.
    """

    def __init__(self, reqObj=None, verbose=False, log=sys.stderr):
        self.__verbose = verbose
        self.__lfh = log
        self.__reqObj = reqObj
        self.__debug = False
        
        # Initialize attributes that the original MessagingDataImport sets up
        self.__identifier = None
        self.__instance = None
        self.__siteId = None
        self.__fileSource = "archive"
        self.__groupId = None
        self.__sessionObj = None
        
        # Setup if we have a request object
        if self.__reqObj:
            self.__setup()

    def __setup(self):
        """Initialize attributes from request object, similar to original."""
        try:
            self.__sessionObj = self.__reqObj.getSessionObj() if hasattr(self.__reqObj, 'getSessionObj') else None
            self.__identifier = str(self.__reqObj.getValue("identifier")).upper() if self.__reqObj.getValue("identifier") else None
            self.__instance = str(self.__reqObj.getValue("instance")).upper() if self.__reqObj.getValue("instance") else None
            self.__siteId = str(self.__reqObj.getValue("WWPDB_SITE_ID")) if self.__reqObj.getValue("WWPDB_SITE_ID") else None
            self.__fileSource = "archive"  # Default to archive
            
            # Handle group ID
            self.__groupId = str(self.__reqObj.getValue("groupid")).upper() if self.__reqObj.getValue("groupid") else None
            if (not self.__groupId) and self.__instance and len(self.__instance) == 9 and self.__instance.startswith("G_"):
                self.__groupId = self.__instance
                self.__fileSource = "autogroup"
                
            if self.__verbose:
                logger.debug("MessagingDataImport stub setup - identifier: %s, instance: %s, groupId: %s", 
                           self.__identifier, self.__instance, self.__groupId)
        except Exception as e:
            logger.error("MessagingDataImport stub setup failed: %s", e)

    def getFilePath(self, contentType, format):  # pylint: disable=redefined-builtin
        """
        Return a dummy file path that contains the deposition ID and content type.
        
        Mimics the original getFilePath interface and behavior.
        """
        createAsNeeded = True if (contentType in ["messages-from-depositor", "messages-to-depositor", "notes-from-annotator"]) else False
        return self.__getWfFilePath(contentType=contentType, fmt=format, fileSource=self.__fileSource, version="latest", createAsNeeded=createAsNeeded)

    def __getWfFilePath(self, contentType, fmt="pdbx", fileSource="archive", version="latest", createAsNeeded=False, partitionNum=None):
        """
        Database stub version of __getWfFilePath that returns dummy paths instead of real file operations.
        """
        try:
            # Use the properly initialized identifier
            depId = self.__identifier or self.__groupId
            if not depId:
                raise ValueError("No deposition ID available")
            
            # Construct a dummy path that contains the necessary information
            # for the database backend to parse
            if contentType in ["messages-to-depositor", "messages-from-depositor", "notes-from-annotator"]:
                filename = f"{depId}_{contentType}_P1.cif.V1"
            else:
                filename = f"{depId}_{contentType}_P1.{fmt}.V1"
            
            # Return a path that looks like a real file path for compatibility
            dummy_path = os.path.join("/dummy", "messaging", depId, filename)
            
            if self.__verbose:
                logger.debug("MessagingDataImport stub returning dummy path: %s", dummy_path)
                
            return dummy_path
            
        except Exception as e:
            logger.error("Error in MessagingDataImport.__getWfFilePath: %s", e)
            return None

    def getMileStoneFilePaths(self, contentType, format, version="latest", partitionNum=None):  # pylint: disable=redefined-builtin
        """
        Return milestone file paths for compatibility - mimics original interface.
        """
        pathDict = {}
        pathDict["dpstPth"] = self.__getWfFilePath(contentType=contentType, fmt=format, fileSource="deposit", version=version)
        pathDict["annotPth"] = self.__getWfFilePath(
            contentType=contentType, fmt=format, fileSource=self.__fileSource, version=version, createAsNeeded=False, partitionNum=partitionNum
        )
        return pathDict

    def checkFilePathExists(self, filePath):
        """
        Always return True since database storage doesn't depend on file existence.
        """
        return True

    def getFileReference(self, **kwargs):
        """
        Stub method for compatibility.
        """
        return None
