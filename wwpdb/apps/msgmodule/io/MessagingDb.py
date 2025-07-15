"""
Modified MessagingIo class showing how to integrate database storage
while maintaining backward compatibility with the existing interface.

This is a proof-of-concept showing the integration approach.
"""

import os
import sys
import time
import logging
import uuid
from datetime import datetime
from typing import List, Dict, Optional, Tuple

# Import the new database layer
from wwpdb.apps.msgmodule.db import (
    MessagingDatabaseService,
    MessageRecord,
    MessageStatus,
    MessageFileReference,
    get_messaging_database_config,
    is_messaging_database_enabled,
)

# Existing imports (unchanged)
from wwpdb.apps.msgmodule.io.MessagingDataImport import MessagingDataImport
from wwpdb.apps.msgmodule.io.MessagingDataExport import MessagingDataExport
from wwpdb.utils.config.ConfigInfo import ConfigInfo

logger = logging.getLogger(__name__)


class MessagingDb:
    """
    Enhanced MessagingIo class that uses database storage instead of CIF files.

    This maintains the same public interface as the original MessagingIo class
    to ensure backward compatibility while using database storage internally.
    """

    def __init__(self, reqObj, verbose=False, log=sys.stderr):
        """Initialize with database connectivity"""
        self.__lfh = log
        self.__verbose = verbose
        self.__debug = True
        self.__reqObj = reqObj

        # Initialize database service
        self.__init_database_service()

        # Existing initialization (unchanged)
        self.__sObj = self.__reqObj.newSessionObj()
        self.__sessionPath = self.__sObj.getPath()
        self.__siteId = str(self.__reqObj.getValue("WWPDB_SITE_ID"))
        self.__groupId = str(self.__reqObj.getValue("groupid"))

        # Initialize ConfigInfo for database configuration
        self.__cI = ConfigInfo(self.__siteId)

        # File path handling (still needed for file attachments)
        self.__msgsToDpstrFilePath = None
        self.__msgsFrmDpstrFilePath = None
        self.__notesFilePath = None

    def __init_database_service(self):
        """
        Initialize the database service with configuration.
        
        This implementation requires database to be available - no fallback logic.
        """
        try:
            # Get site ID and ConfigInfo
            site_id = self.__siteId if hasattr(self, "__siteId") else None
            config_info = getattr(self, "__cI", None)

            # Check if database is enabled
            if not is_messaging_database_enabled(site_id, config_info):
                logger.warning("Database storage is disabled - MessagingDb requires database to be enabled")
                self.__db_service = None
                return

            # Get database configuration
            db_config = get_messaging_database_config(site_id, config_info)
            self.__db_service = MessagingDatabaseService(db_config)

            if self.__verbose:
                logger.info("Database service initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize database service: {e}")
            # No fallback - set to None and let methods fail fast
            self.__db_service = None
            if self.__verbose:
                logger.warning("MessagingDb requires database service - operations will fail")

    def processMsg(self, p_msgObj):
        """
        Process a message object using database storage.
        
        This is a pure database implementation with no fallback logic.
        """
        try:
            if not self.__db_service:
                raise RuntimeError("Database service not available and no fallback configured")
            
            return self.__processMsg_database(p_msgObj)

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False, False, []

    def __processMsg_database(self, p_msgObj):
        """Process message using database storage"""
        try:
            # Extract message data from message object
            message_data = self.__extract_message_data(p_msgObj)

            # Create database records
            message_record = MessageRecord(**message_data["message"])
            status_record = (
                MessageStatus(**message_data["status"])
                if message_data["status"]
                else None
            )
            file_references = [
                MessageFileReference(**fr) for fr in message_data["file_references"]
            ]

            # Handle file references (still need to copy files for attachments)
            bSuccess, msgFileRefs, failedFileRefs = self.__handleFileReferences(
                p_msgObj
            )

            # Store in database
            success = self.__db_service.create_message_with_status(
                message_record, status_record, file_references
            )

            if success and self.__verbose:
                logger.info(
                    f"Successfully stored message {message_record.message_id} in database"
                )

            # Send email notification if configured
            if success and p_msgObj.isBeingSent:
                self.__sendNotificationEmail(p_msgObj)

            return success, bSuccess, failedFileRefs

        except Exception as e:
            logger.error(f"Database message processing failed: {e}")
            return False, False, []

    def __extract_message_data(self, p_msgObj) -> Dict:
        """Extract message data from message object into database format"""
        # Safely extract data using properties with fallbacks for compatibility
        def safe_property_get(obj, prop_name, fallback_value=None):
            """Safely get a property from a Message object, handling missing properties gracefully"""
            try:
                return getattr(obj, prop_name)
            except (AttributeError, KeyError):
                # Try _msgDict directly as fallback
                if hasattr(obj, '_msgDict') and isinstance(obj._msgDict, dict):
                    dict_key = prop_name.replace('Id', '_id').replace('Type', '_type').replace('Text', '_text').replace('Subject', '_subject').replace('Status', '_status')
                    return obj._msgDict.get(dict_key, fallback_value)
                return fallback_value

        # Extract with robust fallbacks to ensure perfect interface compatibility
        message_data = {
            "message": {
                "message_id": safe_property_get(p_msgObj, "messageId", str(uuid.uuid4())),
                "deposition_data_set_id": safe_property_get(p_msgObj, "depositionId", ""),
                "group_id": self.__groupId if self.__groupId else None,
                "timestamp": datetime.now(),
                "sender": safe_property_get(p_msgObj, "sender", ""),
                "context_type": safe_property_get(p_msgObj, "contextType", None),
                "context_value": safe_property_get(p_msgObj, "contextValue", None),
                "parent_message_id": safe_property_get(p_msgObj, "parentMessageId", None),
                "message_subject": safe_property_get(p_msgObj, "messageSubject", ""),
                "message_text": safe_property_get(p_msgObj, "messageText", ""),
                "message_type": safe_property_get(p_msgObj, "messageType", "text"),
                "send_status": safe_property_get(p_msgObj, "sendStatus", "Y"),
                "content_type": safe_property_get(p_msgObj, "contentType", "msgs"),
            },
            "status": {
                "message_id": safe_property_get(p_msgObj, "messageId", str(uuid.uuid4())),
                "deposition_data_set_id": safe_property_get(p_msgObj, "depositionId", ""),
                "read_status": "N",
                "action_reqd": "Y" if safe_property_get(p_msgObj, "actionRequired", False) else "N",
                "for_release": "N",
            } if safe_property_get(p_msgObj, "isBeingSent", False) else None,
            "file_references": [],
        }
        
        # Add file references if any
        file_refs = safe_property_get(p_msgObj, "fileReferences", [])
        if file_refs:
            for file_ref in file_refs:
                file_data = {
                    "message_id": message_data["message"]["message_id"],
                    "deposition_data_set_id": safe_property_get(p_msgObj, "depositionId", ""),
                    "content_type": file_ref.get("content_type", ""),
                    "content_format": file_ref.get("content_format", ""),
                    "partition_number": file_ref.get("partition_number", 1),
                    "version_id": file_ref.get("version_id", 1),
                    "file_source": file_ref.get("file_source", "archive"),
                    "upload_file_name": file_ref.get("upload_file_name"),
                    "file_path": file_ref.get("file_path"),
                    "file_size": file_ref.get("file_size"),
                }
                message_data["file_references"].append(file_data)
        return message_data

    def getMsgRowList(
        self,
        p_depDataSetId,
        p_sSendStatus="Y",
        p_bServerSide=False,
        p_iDisplayStart=None,
        p_iDisplayLength=None,
        p_sSrchFltr=None,
        p_colSearchDict=None,
        p_bThreadedRslts=False,
    ):
        """
        Get message list for a deposition using database storage.
        """
        try:
            if not self.__db_service:
                raise RuntimeError("Database service not available and no fallback configured")
                
            return self.__getMsgRowList_database(
                p_depDataSetId,
                p_sSendStatus,
                p_bServerSide,
                p_iDisplayStart,
                p_iDisplayLength,
                p_sSrchFltr,
                p_colSearchDict,
            )

        except Exception as e:
            logger.error(f"Error retrieving message list: {e}")
            return {"RECORD_LIST": [], "TOTAL_COUNT": 0}

    def __getMsgRowList_database(
        self,
        depId,
        sendStatus,
        serverSide=False,
        displayStart=None,
        displayLength=None,
        searchFilter=None,
        colSearchDict=None,
    ):
        """Retrieve message list from database"""
        try:
            # Determine content type from request
            contentType = self.__reqObj.getValue("content_type")
            if not contentType:
                contentType = "msgs"

            # Build search criteria
            search_criteria = {
                "deposition_data_set_id": depId,
                "send_status": sendStatus,
            }

            if contentType in ["msgs", "notes"]:
                search_criteria["content_type"] = contentType

            # Add search filter if provided
            if searchFilter:
                search_criteria["text_search"] = searchFilter

            # Get messages from database
            messages = self.__db_service.get_deposition_messages(
                depId, contentType, include_status=True, include_files=True
            )

            # Convert to format expected by frontend
            record_list = []
            for msg_data in messages:
                msg = msg_data["message"]
                status = msg_data["status"]

                # Convert to list format (matching original CIF structure)
                record = [
                    getattr(msg, "id", 0),  # ordinal_id
                    msg.message_id,
                    msg.deposition_data_set_id,
                    msg.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    msg.sender,
                    msg.context_type or "",
                    msg.context_value or "",
                    msg.parent_message_id or "",
                    msg.message_subject,
                    msg.message_text[:100] + "..."
                    if len(msg.message_text) > 100
                    else msg.message_text,  # Truncated for list view
                    msg.message_type,
                    msg.send_status,
                ]

                # Add status information if available
                if status:
                    record.extend(
                        [status.read_status, status.action_reqd, status.for_release]
                    )

                record_list.append(record)

            # Apply pagination if server-side processing
            total_count = len(record_list)
            if serverSide and displayStart is not None and displayLength is not None:
                start = displayStart
                end = start + displayLength
                record_list = record_list[start:end]

            return {
                "RECORD_LIST": record_list,
                "TOTAL_COUNT": total_count,
                "FILTERED_COUNT": len(record_list),
            }

        except Exception as e:
            logger.error(f"Database message retrieval failed: {e}")
            return {"RECORD_LIST": [], "TOTAL_COUNT": 0}

    def markMsgAsRead(self, p_msgStatusDict):
        """Mark message as read using database storage."""
        try:
            if not self.__db_service:
                raise RuntimeError("Database service not available and no fallback configured")
                
            return self.__markMsgAsRead_database(p_msgStatusDict)

        except Exception as e:
            logger.error(f"Error marking message as read: {e}")
            return False

    def __markMsgAsRead_database(self, msgStatusDict):
        """Mark message as read using database"""
        try:
            message_id = msgStatusDict.get("message_id")
            read_status = msgStatusDict.get("read_status", "Y")

            success = self.__db_service.status_dao.mark_message_read(
                message_id, read_status
            )

            if success and self.__verbose:
                logger.info(f"Marked message {message_id} as read: {read_status}")

            return success

        except Exception as e:
            logger.error(f"Failed to mark message as read: {e}")
            return False

    def getMsg(self, p_msgId, p_depId):
        """Get individual message using database storage."""
        try:
            if not self.__db_service:
                raise RuntimeError("Database service not available and no fallback configured")
                
            return self.__getMsg_database(p_msgId)

        except Exception as e:
            logger.error(f"Error retrieving message {p_msgId}: {e}")
            return None

    def __getMsg_database(self, msgId):
        """Retrieve individual message from database"""
        try:
            complete_message = self.__db_service.get_complete_message(msgId)

            if not complete_message:
                return None

            msg = complete_message["message"]
            status = complete_message["status"]
            file_refs = complete_message["file_references"]

            # Convert to format expected by frontend (dictionary format)
            message_dict = {
                "message_id": msg.message_id,
                "deposition_data_set_id": msg.deposition_data_set_id,
                "timestamp": msg.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "sender": msg.sender,
                "context_type": msg.context_type or "",
                "context_value": msg.context_value or "",
                "parent_message_id": msg.parent_message_id or "",
                "message_subject": msg.message_subject,
                "message_text": msg.message_text,
                "message_type": msg.message_type,
                "send_status": msg.send_status,
                "content_type": msg.content_type,
            }

            # Add status information
            if status:
                message_dict.update(
                    {
                        "read_status": status.read_status,
                        "action_reqd": status.action_reqd,
                        "for_release": status.for_release,
                    }
                )

            # Add file references
            if file_refs:
                message_dict["file_references"] = []
                for file_ref in file_refs:
                    message_dict["file_references"].append(
                        {
                            "content_type": file_ref.content_type,
                            "content_format": file_ref.content_format,
                            "partition_number": file_ref.partition_number,
                            "version_id": file_ref.version_id,
                            "file_source": file_ref.file_source,
                            "upload_file_name": file_ref.upload_file_name,
                            "file_path": file_ref.file_path,
                            "file_size": file_ref.file_size,
                        }
                    )

            return message_dict

        except Exception as e:
            logger.error(f"Failed to retrieve message from database: {e}")
            return None

    def __handleFileReferences(self, p_msgObj):
        """
        Handle file references for database storage.
        
        This processes file attachments and stores their metadata in the database
        while copying the actual files to appropriate storage locations.
        """
        try:
            if not hasattr(p_msgObj, 'fileReferences') or not p_msgObj.fileReferences:
                # No file references to process
                return True, [], []

            msgFileRefs = []
            failedFileRefs = []
            bSuccess = True

            # Import required classes for file handling
            msgDI = MessagingDataImport(self.__reqObj, verbose=self.__verbose, log=self.__lfh)
            
            depositionId = p_msgObj.depositionId
            messageId = p_msgObj.messageId
            
            if self.__verbose:
                logger.info(f"Processing {len(p_msgObj.fileReferences)} file references for message {messageId}")

            for fileRef in p_msgObj.fileReferences:
                try:
                    # Determine content type and format
                    if isinstance(fileRef, str):
                        # Simple string reference (e.g., "model", "val-report")
                        contentType, contentFormat = self._getContentTypeAndFormat(fileRef)
                        uploadFileName = None
                        auxFileIndex = ""
                    else:
                        # Dictionary/object reference with more details
                        contentType = fileRef.get('content_type', '')
                        contentFormat = fileRef.get('content_format', '')
                        uploadFileName = fileRef.get('upload_file_name')
                        auxFileIndex = fileRef.get('aux_file_index', "")

                    # Get file path
                    if auxFileIndex and uploadFileName:
                        # Auxiliary file uploaded by user
                        fPath = self.__reqObj.getValue(f"auxFilePath{auxFileIndex}")
                    else:
                        # Archive file
                        fPath = msgDI.getFilePath(contentType, contentFormat)

                    if fPath and os.path.exists(fPath) and os.access(fPath, os.R_OK):
                        # Create file reference record for database
                        file_ref_data = {
                            "message_id": messageId,
                            "deposition_data_set_id": depositionId,
                            "content_type": contentType,
                            "content_format": contentFormat,
                            "partition_number": 1,
                            "version_id": 1,
                            "file_source": "auxiliary" if auxFileIndex else "archive",
                            "upload_file_name": uploadFileName,
                            "file_path": fPath,
                            "file_size": os.path.getsize(fPath) if os.path.exists(fPath) else None,
                        }
                        
                        msgFileRefs.append(file_ref_data)
                        
                        if self.__verbose:
                            logger.info(f"Successfully processed file reference: {contentType}/{contentFormat}")
                    else:
                        # File not found or not accessible
                        logger.warning(f"File not accessible: {fPath} for {contentType}/{contentFormat}")
                        failedFileRefs.append({
                            "content_type": contentType,
                            "content_format": contentFormat,
                            "file_path": fPath,
                            "error": "File not accessible"
                        })
                        bSuccess = False

                except Exception as e:
                    logger.error(f"Error processing file reference {fileRef}: {e}")
                    failedFileRefs.append({
                        "file_reference": str(fileRef),
                        "error": str(e)
                    })
                    bSuccess = False

            if self.__verbose:
                logger.info(f"File reference processing completed. Success: {len(msgFileRefs)}, Failed: {len(failedFileRefs)}")

            return bSuccess, msgFileRefs, failedFileRefs

        except Exception as e:
            logger.error(f"File reference handling failed: {e}")
            return False, [], []

    def _getContentTypeAndFormat(self, acronym):
        """
        Get content type and format from acronym.
        
        This method maps file reference acronyms to their corresponding
        content types and formats as used in the wwPDB system.
        """
        # Common mappings - these would typically come from ConfigInfo
        mappings = {
            "model": ("model", "pdbx"),
            "model_pdb": ("model", "pdb"),
            "sf": ("structure-factors", "pdbx"),
            "val-report": ("validation-report", "pdf"),
            "val-report-full": ("validation-report-full", "pdf"),
            "val-data": ("validation-data", "xml"),
            "val-data-cif": ("validation-data", "pdbx"),
            "cs": ("chemical-shifts", "nmr-star"),
            "em-volume": ("em-volume", "map"),
            "aux-file": ("auxiliary", ""),  # Format determined dynamically
        }
        
        return mappings.get(acronym, (acronym, ""))

    def __sendNotificationEmail(self, p_msgObj, p_bVldtnRprtFlg=False):
        """
        Send email notifications for new messages.
        
        This delegates to the existing email notification system but could be
        enhanced to work directly with database-stored message data.
        """
        try:
            if not getattr(p_msgObj, "isBeingSent", False):
                # Only send notifications for messages being sent, not drafts
                return

            # Skip email notification if session path is not properly set
            if not hasattr(self, '__sessionPath') or not self.__sessionPath:
                if self.__verbose:
                    logger.info(f"Skipping email notification for message {getattr(p_msgObj, 'messageId', 'unknown')} - no session path")
                return

            # Use the existing notification system from MessagingIo
            # This avoids duplicating complex email logic
            from wwpdb.apps.msgmodule.io.MessagingIo import MessagingIo
            
            # Create a temporary MessagingIo instance just for email sending
            temp_messaging_io = MessagingIo(self.__reqObj, verbose=self.__verbose, log=self.__lfh)
            
            # Call the existing email notification method
            # Note: This method exists in the original MessagingIo class
            temp_messaging_io._MessagingIo__sendNotificationEmail(p_msgObj, p_bVldtnRprtFlg)
            
            if self.__verbose:
                logger.info(f"Email notification sent for message {getattr(p_msgObj, 'messageId', 'unknown')}")

        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
            # Don't fail the entire message processing if email fails
            pass


# Example usage and testing
if __name__ == "__main__":
    # This would be used for testing the new database implementation
    pass
