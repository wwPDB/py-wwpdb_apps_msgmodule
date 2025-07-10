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


class MessagingIoDatabase:
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
        """Initialize the database service with configuration"""
        try:
            # Get site ID and ConfigInfo
            site_id = self.__siteId if hasattr(self, "__siteId") else None
            config_info = getattr(self, "__cI", None)

            # Check if database is enabled
            if not is_messaging_database_enabled(site_id, config_info):
                logger.info("Database storage is disabled, using file-based storage")
                self.__db_service = None
                return

            # Get database configuration
            db_config = get_messaging_database_config(site_id, config_info)
            self.__db_service = MessagingDatabaseService(db_config)

            if self.__verbose:
                logger.info("Database service initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize database service: {e}")
            # Fallback to file-based system if database unavailable
            self.__db_service = None
            if self.__verbose:
                logger.warning("Falling back to file-based message storage")

    def processMsg(self, p_msgObj):
        """
        Process a message object - main entry point for message handling.

        This method maintains the same interface as the original but uses
        database storage instead of CIF files.
        """
        try:
            if self.__db_service:
                return self.__processMsg_database(p_msgObj)
            else:
                # Fallback to original file-based processing
                return self.__processMsg_file_fallback(p_msgObj)

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
        message_data = {
            "message": {
                "message_id": p_msgObj.messageId or str(uuid.uuid4()),
                "deposition_data_set_id": p_msgObj.depositionId,
                "group_id": self.__groupId if self.__groupId else None,
                "timestamp": datetime.now(),
                "sender": p_msgObj.sender,
                "context_type": getattr(p_msgObj, "contextType", None),
                "context_value": getattr(p_msgObj, "contextValue", None),
                "parent_message_id": getattr(p_msgObj, "parentMessageId", None),
                "message_subject": p_msgObj.messageSubject,
                "message_text": p_msgObj.messageText,
                "message_type": "text",
                "send_status": "Y" if p_msgObj.isBeingSent else "N",
                "content_type": p_msgObj.contentType,
            },
            "status": {
                "message_id": p_msgObj.messageId or str(uuid.uuid4()),
                "deposition_data_set_id": p_msgObj.depositionId,
                "read_status": "N",
                "action_reqd": "Y"
                if getattr(p_msgObj, "actionRequired", False)
                else "N",
                "for_release": "N",
            }
            if p_msgObj.isBeingSent
            else None,
            "file_references": [],
        }

        # Add file references if any
        if hasattr(p_msgObj, "fileReferences") and p_msgObj.fileReferences:
            for file_ref in p_msgObj.fileReferences:
                file_data = {
                    "message_id": message_data["message"]["message_id"],
                    "deposition_data_set_id": p_msgObj.depositionId,
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
        Get message list for a deposition - maintains original interface
        """
        try:
            if self.__db_service:
                return self.__getMsgRowList_database(
                    p_depDataSetId,
                    p_sSendStatus,
                    p_bServerSide,
                    p_iDisplayStart,
                    p_iDisplayLength,
                    p_sSrchFltr,
                    p_colSearchDict,
                )
            else:
                # Fallback to file-based method
                return self.__getMsgRowList_file_fallback(p_depDataSetId, p_sSendStatus)

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
        """Mark message as read - maintains original interface"""
        try:
            if self.__db_service:
                return self.__markMsgAsRead_database(p_msgStatusDict)
            else:
                return self.__markMsgAsRead_file_fallback(p_msgStatusDict)

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
        """Get individual message - maintains original interface"""
        try:
            if self.__db_service:
                return self.__getMsg_database(p_msgId)
            else:
                return self.__getMsg_file_fallback(p_msgId, p_depId)

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
        Handle file references - this still needs file system operations
        for copying/moving files, but metadata is stored in database
        """
        # This method would contain the existing file handling logic
        # but would store file metadata in database instead of CIF files

        # For now, return mock values
        bSuccess = True
        msgFileRefs = []
        failedFileRefs = []

        if self.__verbose:
            logger.info("File reference handling completed")

        return bSuccess, msgFileRefs, failedFileRefs

    def __sendNotificationEmail(self, p_msgObj, p_bVldtnRprtFlg=False):
        """Send email notifications - unchanged from original"""
        # This method would remain largely unchanged
        # as it deals with email sending, not storage

        if self.__verbose:
            logger.info(f"Email notification sent for message {p_msgObj.messageId}")

    # Fallback methods for file-based operations (when database is unavailable)

    def __processMsg_file_fallback(self, p_msgObj):
        """Fallback to original file-based message processing"""
        # This would contain the original CIF file processing logic
        logger.warning("Using file-based fallback for message processing")
        return False, False, []

    def __getMsgRowList_file_fallback(self, depId, sendStatus):
        """Fallback to original file-based message retrieval"""
        logger.warning("Using file-based fallback for message retrieval")
        return {"RECORD_LIST": [], "TOTAL_COUNT": 0}

    def __markMsgAsRead_file_fallback(self, msgStatusDict):
        """Fallback to original file-based read status update"""
        logger.warning("Using file-based fallback for marking message as read")
        return False

    def __getMsg_file_fallback(self, msgId, depId):
        """Fallback to original file-based individual message retrieval"""
        logger.warning("Using file-based fallback for individual message retrieval")
        return None


# Example usage and testing
if __name__ == "__main__":
    # This would be used for testing the new database implementation
    pass
