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
        
        # Set up instance variables to match original interface
        self.__setup()

    def getFilePath(self, contentType="model", format="pdbx", **kwargs):
        """
        Return a dummy file path that contains the deposition ID and content type.
        
        The database-backed PdbxMessageIo will parse this path to extract context
        information without actually accessing the file.
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

    def __setup(self):
        """
        Initialize instance variables to match original interface.
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
            self.__siteId = str(siteId)
            
            self.__groupId = str(self.__reqObj.getValue("groupid") or "").upper()
            self.__fileSource = "archive"  # Default file source
                
            if self.__verbose:
                logger.debug("Database stub initialized - identifier: %s, instance: %s", 
                           self.__identifier, self.__instance)
                           
        except Exception as e:
            logger.exception("Error in __setup: %s", e)
            raise ValueError(f"Failed to initialize MessagingDataImport: {e}") from e

    def getMileStoneFilePaths(self, contentType, format, version="latest", partitionNum=None):
        """
        Return dummy milestone file paths for database backend compatibility.
        
        Returns a dictionary with 'dpstPth' and 'annotPth' keys containing dummy paths
        that the database backend can parse for context information.
        """
        if not hasattr(self, '_MessagingDataImport__identifier'):
            raise ValueError("MessagingDataImport not properly initialized - missing identifier")
            
        try:
            # Get base identifier
            depId = self.__identifier
            
            # Use group ID if available for messaging files
            if (hasattr(self, '_MessagingDataImport__groupId') and 
                self.__groupId and 
                contentType in ["messages-to-depositor", "messages-from-depositor", "notes-from-annotator"]):
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

    def __getWfFilePath(self, contentType, fmt="pdbx", fileSource="archive", version="latest", createAsNeeded=False, partitionNum=None):
        """
        Internal method to match original interface.
        Returns a dummy file path for database backend compatibility.
        """
        return self.getFilePath(contentType=contentType, format=fmt)
