##
# File: PdbxMessage.py  
# Database-backed implementation for compatibility
#
# This module provides compatibility classes that match the interface of
# the original PdbxMessage classes but use SQLAlchemy models underneath.
##
"""
Database-backed compatibility classes for PdbxMessage.

These classes provide the same interface as the original PdbxMessage classes
but use SQLAlchemy models for data storage and manipulation.
"""

import sys
import time
import uuid
import logging
from datetime import datetime
from typing import Dict, Any, Optional

from .Models import MessageInfo, MessageFileReference, MessageStatus

logger = logging.getLogger(__name__)


class PdbxMessageInfo:
    """Database-backed message info class with original PdbxMessage interface"""
    
    def __init__(self, verbose=True, log=sys.stderr):
        self.__verbose = verbose
        self.__lfh = log
        self.__data = self.__setup()
    
    def __getMessageId(self):
        """Generate a unique message ID"""
        try:
            return str(uuid.uuid4())
        except Exception:
            import random
            import string
            chars = string.ascii_lowercase + string.digits
            parts = [
                ''.join(random.choice(chars) for _ in range(8)),
                ''.join(random.choice(chars) for _ in range(4)),
                ''.join(random.choice(chars) for _ in range(4)),
                ''.join(random.choice(chars) for _ in range(4)),
                ''.join(random.choice(chars) for _ in range(12))
            ]
            return '-'.join(parts)
    
    def __setup(self):
        """Initialize the message data dictionary"""
        d = {
            'ordinal_id': None,
            'message_id': self.__getMessageId(),
            'deposition_data_set_id': None,
            'timestamp': time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
            'sender': None,
            'context_type': None,
            'context_value': None,
            'parent_message_id': None,
            'message_subject': None,
            'message_text': None,
            'message_type': 'text',
            'send_status': 'N',
            'content_type': None,
        }
        d['parent_message_id'] = d['message_id']
        return d
    
    def get(self):
        """Get the message data as dictionary"""
        return self.__data
    
    def set(self, messageDict={}):
        """Set message data from dictionary"""
        d = self.__setup()
        for k, v in messageDict.items():
            if k in d:
                d[k] = v
        self.__data = d
        return d
    
    def write(self, ofh=sys.stdout):
        """Write message data to output handle"""
        for key, value in self.__data.items():
            ofh.write(f"{key:30s} : {value}\n")
    
    # Ordinal ID methods
    def setOrdinalId(self, id=None):
        self.__data['ordinal_id'] = id
    
    def getOrdinalId(self):
        return self.__data['ordinal_id']
    
    # Message ID methods
    def setMessageId(self, id=None):
        if id is None:
            id = self.__getMessageId()
        self.__data['message_id'] = id
    
    def getMessageId(self):
        return self.__data['message_id']
    
    # Deposition ID methods
    def setDepositionId(self, id=None):
        self.__data['deposition_data_set_id'] = id
    
    def getDepositionId(self):
        return self.__data['deposition_data_set_id']
    
    # Sender methods
    def setSender(self, sender=None):
        self.__data['sender'] = sender
    
    def getSender(self):
        return self.__data['sender']
    
    # Context methods
    def setContextType(self, type=None):
        self.__data['context_type'] = type
    
    def getContextType(self):
        return self.__data['context_type']
    
    def setContextValue(self, val=None):
        self.__data['context_value'] = val
    
    def getContextValue(self):
        return self.__data['context_value']
    
    # Parent message methods
    def setParentMessageId(self, id=None):
        self.__data['parent_message_id'] = id
    
    def getParentMessageId(self):
        return self.__data['parent_message_id']
    
    # Message type methods
    def setMessageType(self, type=None):
        self.__data['message_type'] = type
    
    def getMessageType(self):
        return self.__data['message_type']
    
    # Subject methods
    def setMessageSubject(self, txt=None):
        self.__data['message_subject'] = txt
    
    def getMessageSubject(self):
        return self.__data['message_subject']
    
    # Message text methods
    def setMessageText(self, txt=None):
        self.__data['message_text'] = txt
    
    def getMessageText(self):
        return self.__data['message_text']
    
    # Send status methods
    def setSendStatus(self, status='N'):
        self.__data['send_status'] = status
    
    def getSendStatus(self):
        return self.__data['send_status']
    
    # Content type methods
    def setContentType(self, content_type=None):
        self.__data['content_type'] = content_type
    
    def getContentType(self):
        return self.__data['content_type']
    
    def getValueList(self, attributeIdList=[]):
        """Get list of values for specified attributes"""
        vL = []
        for attributeId in attributeIdList:
            if attributeId in self.__data:
                vL.append(self.__data[attributeId])
            else:
                vL.append(None)
        return vL


class PdbxMessageFileReference:
    """Database-backed file reference class with original PdbxMessage interface"""
    
    def __init__(self, verbose=True, log=sys.stderr):
        self.__verbose = verbose
        self.__lfh = log
        self.__data = self.__setup()
    
    def __setup(self):
        """Initialize the file reference data dictionary"""
        return {
            'ordinal_id': None,
            'message_id': None,
            'deposition_data_set_id': None,
            'content_type': None,
            'content_format': None,
            'partition_number': None,
            'version_id': None,
            'storage_type': None,
            'upload_file_name': None,
        }
    
    def get(self):
        """Get the file reference data as dictionary"""
        return self.__data
    
    def set(self, messageDict={}):
        """Set file reference data from dictionary"""
        d = self.__setup()
        for k, v in messageDict.items():
            if k in d:
                d[k] = v
        self.__data = d
        return d
    
    def write(self, ofh=sys.stdout):
        """Write file reference data to output handle"""
        for key, value in self.__data.items():
            ofh.write(f"{key:30s} : {value}\n")
    
    # Ordinal ID methods
    def setOrdinalId(self, id=None):
        self.__data['ordinal_id'] = id
    
    def getOrdinalId(self):
        return self.__data['ordinal_id']
    
    # Message ID methods
    def setMessageId(self, id=None):
        self.__data['message_id'] = id
    
    def getMessageId(self):
        return self.__data['message_id']
    
    # Deposition ID methods
    def setDepositionId(self, id=None):
        self.__data['deposition_data_set_id'] = id
    
    def getDepositionId(self):
        return self.__data['deposition_data_set_id']
    
    # Content type methods
    def setContentType(self, type=None):
        self.__data['content_type'] = type
    
    def getContentType(self):
        return self.__data['content_type']
    
    # Content format methods
    def setContentFormat(self, type=None):
        self.__data['content_format'] = type
    
    def getContentFormat(self):
        return self.__data['content_format']
    
    # Upload file name methods
    def setUploadFileName(self, fileName=""):
        self.__data['upload_file_name'] = fileName
    
    def getUploadFileName(self):
        return self.__data['upload_file_name']
    
    # Storage type methods
    def setStorageType(self, type=None):
        self.__data['storage_type'] = type
    
    def getStorageType(self):
        return self.__data['storage_type']
    
    # Partition number methods
    def setPartitionNumber(self, val=None):
        self.__data['partition_number'] = val
    
    def getPartitionNumber(self):
        return self.__data['partition_number']
    
    # Version ID methods
    def setVersionId(self, val=None):
        self.__data['version_id'] = val
    
    def getVersionId(self):
        return self.__data['version_id']


class PdbxMessageOrigCommReference:
    """Database-backed original communication reference class"""
    
    def __init__(self, verbose=True, log=sys.stderr):
        self.__verbose = verbose
        self.__lfh = log
        self.__data = self.__setup()
    
    def __setup(self):
        """Initialize the original communication reference data dictionary"""
        return {
            'ordinal_id': None,
            'message_id': None,
            'deposition_data_set_id': None,
            'orig_message_id': None,
            'orig_deposition_data_set_id': None,
            'orig_timestamp': None,
            'orig_sender': None,
            'orig_recipient': None,
            'orig_message_subject': None,
            'orig_attachments': None,
        }
    
    def get(self):
        """Get the original communication reference data as dictionary"""
        return self.__data
    
    def set(self, messageDict={}):
        """Set original communication reference data from dictionary"""
        d = self.__setup()
        for k, v in messageDict.items():
            if k in d:
                d[k] = v
        self.__data = d
        return d
    
    def write(self, ofh=sys.stdout):
        """Write original communication reference data to output handle"""
        for key, value in self.__data.items():
            ofh.write(f"{key:30s} : {value}\n")
    
    # Standard methods for compatibility
    def setOrdinalId(self, id=None):
        self.__data['ordinal_id'] = id
    
    def getOrdinalId(self):
        return self.__data['ordinal_id']
    
    def setMessageId(self, id=None):
        self.__data['message_id'] = id
    
    def getMessageId(self):
        return self.__data['message_id']
    
    def setDepositionId(self, id=None):
        self.__data['deposition_data_set_id'] = id
    
    def getDepositionId(self):
        return self.__data['deposition_data_set_id']
    
    # Original message specific methods
    def setOrigMessageId(self, origId=None):
        self.__data['orig_message_id'] = origId
    
    def getOrigMessageId(self):
        return self.__data['orig_message_id']
    
    def setOrigDepositionId(self, origDepId=None):
        self.__data['orig_deposition_data_set_id'] = origDepId
    
    def getOrigDepositionId(self):
        return self.__data['orig_deposition_data_set_id']
    
    def setOrigTimeStamp(self, origTimeStamp=None):
        self.__data['orig_timestamp'] = origTimeStamp
    
    def getOrigTimeStamp(self):
        return self.__data['orig_timestamp']
    
    def setOrigSender(self, origSender=None):
        self.__data['orig_sender'] = origSender
    
    def getOrigSender(self):
        return self.__data['orig_sender']
    
    def setOrigRecipient(self, origRecipient=None):
        self.__data['orig_recipient'] = origRecipient
    
    def getOrigRecipient(self):
        return self.__data['orig_recipient']
    
    def setOrigMessageSubject(self, subject=None):
        self.__data['orig_message_subject'] = subject
    
    def getOrigMessageSubject(self):
        return self.__data['orig_message_subject']
    
    def setOrigAttachments(self, origAttachments=None):
        self.__data['orig_attachments'] = origAttachments
    
    def getOrigAttachments(self):
        return self.__data['orig_attachments']


class PdbxMessageStatus:
    """Database-backed message status class with original PdbxMessage interface"""
    
    def __init__(self, verbose=True, log=sys.stderr):
        self.__verbose = verbose
        self.__lfh = log
        self.__data = self.__setup()
    
    def __setup(self):
        """Initialize the message status data dictionary"""
        return {
            'message_id': None,
            'deposition_data_set_id': None,
            'read_status': 'N',
            'action_reqd': 'N',
            'for_release': 'N',
        }
    
    def get(self):
        """Get the message status data as dictionary"""
        return self.__data
    
    def set(self, messageDict={}):
        """Set message status data from dictionary"""
        d = self.__setup()
        for k, v in messageDict.items():
            if k in d:
                d[k] = v
        self.__data = d
        return d
    
    def write(self, ofh=sys.stdout):
        """Write message status data to output handle"""
        for key, value in self.__data.items():
            ofh.write(f"{key:30s} : {value}\n")
    
    # Message ID methods
    def setMessageId(self, id=None):
        self.__data['message_id'] = id
    
    def getMessageId(self):
        return self.__data['message_id']
    
    # Deposition ID methods
    def setDepositionId(self, id=None):
        self.__data['deposition_data_set_id'] = id
    
    def getDepositionId(self):
        return self.__data['deposition_data_set_id']
    
    # Read status methods
    def setReadStatus(self, status='N'):
        self.__data['read_status'] = status
    
    def getReadStatus(self):
        return self.__data['read_status']
    
    # Action required methods
    def setActionReqdStatus(self, status='N'):
        self.__data['action_reqd'] = status
    
    def getActionReqdStatus(self):
        return self.__data['action_reqd']
    
    # For release methods
    def setReadyForRelStatus(self, status='N'):
        self.__data['for_release'] = status
    
    def getReadyForRelStatus(self):
        return self.__data['for_release']
