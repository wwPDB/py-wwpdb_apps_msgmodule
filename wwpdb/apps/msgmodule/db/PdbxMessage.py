##
# File: PdbxMessage.py (Database-backed implementation)
# Date: 27-Aug-2025
#
# Database-backed drop-in replacement for mmcif_utils.message.PdbxMessage classes
# that maintains the same API interface while leveraging existing SQLAlchemy models.
##
"""
Database-backed message helper classes that provide the same interface as the original
PdbxMessage classes but work with the existing SQLAlchemy models from Models.py.
"""

import sys
import time
import uuid
from datetime import datetime
from typing import Dict, Optional

from wwpdb.apps.msgmodule.db.Models import MessageInfo, MessageFileReference, MessageStatus


def _fmt_timestamp(ts) -> str:
    """Format timestamp for compatibility with CIF backend"""
    if isinstance(ts, datetime):
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(ts, str):
        return ts
    return ""


def _parse_timestamp(ts_str) -> datetime:
    """Parse timestamp string to datetime"""
    if isinstance(ts_str, datetime):
        return ts_str
    if not ts_str:
        return datetime.utcnow()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%b-%Y %H:%M:%S", "%d-%b-%Y"):
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue
    return datetime.utcnow()


class _BasePdbxMessage:
    """Base class that wraps SQLAlchemy models with CIF-compatible interface"""
    
    def __init__(self, model_instance=None, verbose: bool = False, log=sys.stderr):
        self._verbose = verbose
        self._log = log
        self._model = model_instance
        self._row_dict = {}

    def setOrdinalId(self, id: int):
        if self._model:
            self._model.ordinal_id = id
        self._row_dict["ordinal_id"] = id

    def getOrdinalId(self) -> int:
        if self._model and hasattr(self._model, 'ordinal_id'):
            return self._model.ordinal_id or 0
        return self._row_dict.get("ordinal_id", 0)

    def setMessageId(self, v: str):
        if self._model:
            self._model.message_id = v
        self._row_dict["message_id"] = v

    def getMessageId(self) -> str:
        if self._model and hasattr(self._model, 'message_id'):
            return self._model.message_id or ""
        return self._row_dict.get("message_id", "")

    def setDepositionId(self, v: str):
        if self._model:
            self._model.deposition_data_set_id = v
        self._row_dict["deposition_data_set_id"] = v

    def getDepositionId(self) -> str:
        if self._model and hasattr(self._model, 'deposition_data_set_id'):
            return self._model.deposition_data_set_id or ""
        return self._row_dict.get("deposition_data_set_id", "")

    def get(self) -> Dict:
        """Return dict representation compatible with CIF backend"""
        if self._model:
            # Convert SQLAlchemy model to dict
            result = {}
            for attr in dir(self._model):
                if not attr.startswith('_') and not callable(getattr(self._model, attr)):
                    value = getattr(self._model, attr, None)
                    if value is not None:
                        if isinstance(value, datetime):
                            result[attr] = _fmt_timestamp(value)
                        else:
                            result[attr] = value
            # Merge with any additional row_dict values
            result.update(self._row_dict)
            return result
        return dict(self._row_dict)

    def set(self, rowDict: Dict):
        """Set values from dict (for compatibility)"""
        self._row_dict.update(rowDict)
        if self._model:
            for key, value in rowDict.items():
                if hasattr(self._model, key):
                    if key == 'timestamp' and isinstance(value, str):
                        setattr(self._model, key, _parse_timestamp(value))
                    else:
                        setattr(self._model, key, value)

    def get_model(self):
        """Get the underlying SQLAlchemy model instance"""
        return self._model


class PdbxMessageInfo(_BasePdbxMessage):
    """Drop-in helper that wraps MessageInfo SQLAlchemy model"""

    def __init__(self, verbose: bool = False, log=sys.stderr):
        model = MessageInfo()
        super().__init__(model, verbose, log)
        # Initialize defaults like the original CIF implementation
        self._backfill_missing_defaults()

    def set(self, rowDict: Dict):
        """Set values from dict with alias normalization"""
        rd = dict(rowDict or {})
        
        # Normalize common aliases used by upstream code
        if "msg_id" in rd and "message_id" not in rd:
            rd["message_id"] = rd["msg_id"]
        if "identifier" in rd and "deposition_data_set_id" not in rd:
            rd["deposition_data_set_id"] = rd["identifier"]
        
        # Update in-memory dict
        self._row_dict.update(rd)
        
        # Update model where attributes exist
        if self._model:
            for key, value in rd.items():
                if hasattr(self._model, key):
                    if key == 'timestamp' and isinstance(value, str):
                        setattr(self._model, key, _parse_timestamp(value))
                    else:
                        setattr(self._model, key, value)
        
        # Only backfill defaults for fields that are still missing
        # Don't overwrite explicitly provided values
        self._backfill_missing_defaults()
        return self.get()

    def _backfill_missing_defaults(self):
        """Backfill only missing required defaults without overwriting existing values"""
        # Only generate message_id if truly missing
        if not self._row_dict.get("message_id") and not (self._model and getattr(self._model, "message_id", None)):
            msg_id = str(uuid.uuid4())
            self._row_dict["message_id"] = msg_id
            if self._model:
                self._model.message_id = msg_id

        # Set parent_message_id to same as message_id if not present
        current_msg_id = self._row_dict.get("message_id") or (self._model and getattr(self._model, "message_id", None))
        if current_msg_id and not self._row_dict.get("parent_message_id") and not (self._model and getattr(self._model, "parent_message_id", None)):
            self._row_dict["parent_message_id"] = current_msg_id
            if self._model:
                self._model.parent_message_id = current_msg_id
        
        # Set timestamp to current GMT time if not present
        if not self._row_dict.get("timestamp") and not (self._model and getattr(self._model, "timestamp", None)):
            gmt_timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            self._row_dict["timestamp"] = gmt_timestamp
            if self._model:
                self._model.timestamp = _parse_timestamp(gmt_timestamp)
        
        # Set message_type default
        if not self._row_dict.get("message_type") and not (self._model and getattr(self._model, "message_type", None)):
            self._row_dict["message_type"] = "text"
            if self._model:
                self._model.message_type = "text"
        
        # Set send_status default to "N" (not "Y")
        if not self._row_dict.get("send_status") and not (self._model and getattr(self._model, "send_status", None)):
            self._row_dict["send_status"] = "N"
            if self._model:
                self._model.send_status = "N"

    def setTimestamp(self, v):
        if isinstance(v, datetime):
            timestamp = v
            self._row_dict["timestamp"] = _fmt_timestamp(v)
        else:
            timestamp = _parse_timestamp(v)
            self._row_dict["timestamp"] = _fmt_timestamp(timestamp)
        
        if self._model:
            self._model.timestamp = timestamp

    def getTimestamp(self) -> str:
        if self._model and self._model.timestamp:
            return _fmt_timestamp(self._model.timestamp)
        return self._row_dict.get("timestamp", "")

    def setSender(self, v: str):
        if self._model:
            self._model.sender = v
        self._row_dict["sender"] = v

    def getSender(self) -> str:
        if self._model:
            return self._model.sender or ""
        return self._row_dict.get("sender", "")

    def setContextType(self, v: Optional[str]):
        if self._model:
            self._model.context_type = v
        self._row_dict["context_type"] = v

    def getContextType(self) -> Optional[str]:
        if self._model:
            return self._model.context_type
        return self._row_dict.get("context_type")

    def setContextValue(self, v: Optional[str]):
        if self._model:
            self._model.context_value = v
        self._row_dict["context_value"] = v

    def getContextValue(self) -> Optional[str]:
        if self._model:
            return self._model.context_value
        return self._row_dict.get("context_value")

    def setParentMessageId(self, v: Optional[str]):
        if self._model:
            self._model.parent_message_id = v
        self._row_dict["parent_message_id"] = v

    def getParentMessageId(self) -> Optional[str]:
        if self._model:
            return self._model.parent_message_id
        return self._row_dict.get("parent_message_id")

    def setMessageSubject(self, v: str):
        if self._model:
            self._model.message_subject = v
        self._row_dict["message_subject"] = v

    def getMessageSubject(self) -> str:
        if self._model:
            return self._model.message_subject or ""
        return self._row_dict.get("message_subject", "")

    def setMessageText(self, v: str):
        if self._model:
            self._model.message_text = v
        self._row_dict["message_text"] = v

    def getMessageText(self) -> str:
        if self._model:
            return self._model.message_text or ""
        return self._row_dict.get("message_text", "")

    def setMessageType(self, v: str):
        if self._model:
            self._model.message_type = v
        self._row_dict["message_type"] = v

    def getMessageType(self) -> str:
        if self._model:
            return self._model.message_type or "text"
        return self._row_dict.get("message_type", "text")

    def setSendStatus(self, v: str):
        if self._model:
            self._model.send_status = v
        self._row_dict["send_status"] = v

    def getSendStatus(self) -> str:
        if self._model:
            return self._model.send_status or "N"
        return self._row_dict.get("send_status", "N")

    def setContentType(self, v: str):
        if self._model:
            self._model.content_type = v
        self._row_dict["content_type"] = v

    def getContentType(self) -> str:
        if self._model:
            return self._model.content_type or "messages-to-depositor"
        return self._row_dict.get("content_type", "messages-to-depositor")


class PdbxMessageFileReference(_BasePdbxMessage):
    """Drop-in helper that wraps MessageFileReference SQLAlchemy model"""

    def __init__(self, verbose: bool = False, log=sys.stderr):
        model = MessageFileReference()
        super().__init__(model, verbose, log)

    def setContentType(self, v: str):
        if self._model:
            self._model.content_type = v
        self._row_dict["content_type"] = v

    def getContentType(self) -> str:
        if self._model:
            return self._model.content_type or ""
        return self._row_dict.get("content_type", "")

    def setContentFormat(self, v: str):
        if self._model:
            self._model.content_format = v
        self._row_dict["content_format"] = v

    def getContentFormat(self) -> str:
        if self._model:
            return self._model.content_format or ""
        return self._row_dict.get("content_format", "")

    def setPartitionNumber(self, v: int):
        if self._model:
            self._model.partition_number = v
        self._row_dict["partition_number"] = v

    def getPartitionNumber(self) -> int:
        if self._model:
            return self._model.partition_number or 1
        return self._row_dict.get("partition_number", 1)

    def setVersionId(self, v: int):
        # API uses 'version_id' but DB schema may use 'version_number'
        if self._model:
            if hasattr(self._model, "version_number"):
                self._model.version_number = v
            else:
                self._model.version_id = v
        # Store in both possible keys for compatibility
        self._row_dict["version_id"] = v
        self._row_dict["version_number"] = v

    def getVersionId(self) -> int:
        if self._model:
            # Check DB attribute first
            if hasattr(self._model, "version_number") and self._model.version_number:
                return self._model.version_number
            return getattr(self._model, "version_id", None) or 1
        # Check both possible keys in row_dict
        return self._row_dict.get("version_number") or self._row_dict.get("version_id", 1)

    def setStorageType(self, v: str):
        if self._model:
            self._model.storage_type = v
        self._row_dict["storage_type"] = v

    def getStorageType(self) -> str:
        if self._model:
            return self._model.storage_type or "archive"
        return self._row_dict.get("storage_type", "archive")

    def setUploadFileName(self, v: Optional[str]):
        if self._model:
            self._model.upload_file_name = v
        self._row_dict["upload_file_name"] = v

    def getUploadFileName(self) -> Optional[str]:
        if self._model:
            return self._model.upload_file_name
        return self._row_dict.get("upload_file_name")


class PdbxMessageOrigCommReference(_BasePdbxMessage):
    """In-memory only class for API compatibility (no database persistence)"""

    def __init__(self, verbose: bool = False, log=sys.stderr):
        super().__init__(None, verbose, log)  # No model - in-memory only

    def setOrigSender(self, v: str):
        self._row_dict["orig_sender"] = v

    def getOrigSender(self) -> str:
        return self._row_dict.get("orig_sender", "")

    def setOrigRecipient(self, v: str):
        self._row_dict["orig_recipient"] = v

    def getOrigRecipient(self) -> str:
        return self._row_dict.get("orig_recipient", "")

    def setOrigDepositionId(self, v: str):
        self._row_dict["orig_deposition_data_set_id"] = v

    def getOrigDepositionId(self) -> str:
        return self._row_dict.get("orig_deposition_data_set_id", "")

    def setOrigMessageSubject(self, v: str):
        self._row_dict["orig_message_subject"] = v

    def getOrigMessageSubject(self) -> str:
        return self._row_dict.get("orig_message_subject", "")

    def setOrigTimeStamp(self, v: str):
        self._row_dict["orig_timestamp"] = v

    def getOrigTimeStamp(self) -> str:
        return self._row_dict.get("orig_timestamp", "")

    def setOrigAttachments(self, v: str):
        self._row_dict["orig_attachments"] = v

    def getOrigAttachments(self) -> str:
        return self._row_dict.get("orig_attachments", "")


class PdbxMessageStatus(_BasePdbxMessage):
    """Drop-in helper that wraps MessageStatus SQLAlchemy model"""

    def __init__(self, verbose: bool = False, log=sys.stderr):
        model = MessageStatus()
        super().__init__(model, verbose, log)

    def setReadStatus(self, status='N'):
        if self._model:
            self._model.read_status = status
        self._row_dict["read_status"] = status

    def getReadStatus(self) -> str:
        if self._model:
            return self._model.read_status or "N"
        return self._row_dict.get("read_status", "N")

    def setActionReqdStatus(self, status='N'):
        if self._model:
            self._model.action_reqd = status
        self._row_dict["action_reqd"] = status

    def getActionReqdStatus(self) -> str:
        if self._model:
            return self._model.action_reqd or "N"
        return self._row_dict.get("action_reqd", "N")

    def setReadyForRelStatus(self, status='N'):
        if self._model:
            self._model.for_release = status
        self._row_dict["for_release"] = status

    def getReadyForRelStatus(self) -> str:
        if self._model:
            return self._model.for_release or "N"
        return self._row_dict.get("for_release", "N")
