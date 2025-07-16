"""
Data Models for wwPDB Communication Module

This module contains dataclass definitions that provide a consistent interface
for messaging data structures. These classes serve as DTOs (Data Transfer Objects)
and provide compatibility with both CIF-based and database-based backends.
"""

from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime


@dataclass
class MessageRecord:
    """Data class representing a message record (keeping original interface)"""

    id: Optional[int] = None
    message_id: str = ""
    deposition_data_set_id: str = ""
    group_id: Optional[str] = None
    timestamp: datetime = None
    sender: str = ""
    recipient: Optional[str] = None
    context_type: Optional[str] = None
    context_value: Optional[str] = None
    parent_message_id: Optional[str] = None
    message_subject: str = ""
    message_text: str = ""
    message_type: str = "text"
    send_status: str = "Y"
    content_type: str = "msgs"  # 'msgs' or 'notes'
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # Add property aliases for compatibility with Message class interface
    @property
    def messageId(self):
        return self.message_id

    @property
    def depositionId(self):
        return self.deposition_data_set_id

    @property
    def groupId(self):
        return self.group_id

    @property
    def messageSubject(self):
        return self.message_subject

    @property
    def messageText(self):
        return self.message_text

    @property
    def messageType(self):
        return self.message_type

    @property
    def sendStatus(self):
        return self.send_status

    @property
    def contentType(self):
        return self.content_type

    @property
    def parentMessageId(self):
        return self.parent_message_id

    @property
    def contextType(self):
        return self.context_type

    @property
    def contextValue(self):
        return self.context_value


@dataclass
class MessageFileReference:
    """Data class representing a file reference"""

    id: Optional[int] = None
    message_id: str = ""
    deposition_data_set_id: str = ""
    content_type: str = ""
    content_format: str = ""
    partition_number: int = 1
    version_id: int = 1
    file_source: str = "archive"
    upload_file_name: Optional[str] = None
    file_path: Optional[str] = None
    file_size: Optional[int] = None
    created_at: Optional[datetime] = None

    # Aliases for compatibility
    @property
    def depositionId(self):
        return self.deposition_data_set_id

    @property
    def contentType(self):
        return self.content_type

    @property
    def contentFormat(self):
        return self.content_format

    @property
    def fileSource(self):
        return self.file_source

    @property
    def uploadFileName(self):
        return self.upload_file_name

    @property
    def filePath(self):
        return self.file_path

    @property
    def fileSize(self):
        return self.file_size


@dataclass
class MessageStatus:
    """Data class representing message status"""

    message_id: str = ""
    deposition_data_set_id: str = ""
    read_status: str = "N"
    action_reqd: str = "N"
    for_release: str = "N"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # Aliases for compatibility
    @property
    def messageId(self):
        return self.message_id

    @property
    def depositionId(self):
        return self.deposition_data_set_id

    @property
    def readStatus(self):
        return self.read_status

    @property
    def actionReqd(self):
        return self.action_reqd

    @property
    def forRelease(self):
        return self.for_release
