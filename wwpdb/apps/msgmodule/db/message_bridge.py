"""
Bridge between existing Message models and SQLAlchemy database models.

Provides conversion utilities to maintain compatibility between the original
CIF-based Message classes and the new database-backed storage.
"""

from typing import Dict, List, Optional
from datetime import datetime
import uuid

from ..models.Message import Message, Note, AutoMessage, ReminderMessage, AutoNote
from ..models.DatabaseModels import MessageRecordModel, MessageFileReferenceModel, MessageStatusModel
from ..db.messaging_dal import MessageRecord, MessageFileReference, MessageStatus


class MessageModelBridge:
    """Bridge for converting between Message objects and database models"""
    
    @staticmethod
    def message_to_record(msg_obj: Message) -> MessageRecord:
        """Convert Message object to MessageRecord dataclass"""
        msg_dict = msg_obj.getMsgDict()
        
        return MessageRecord(
            message_id=msg_dict.get("message_id", str(uuid.uuid4())),
            deposition_data_set_id=msg_dict.get("deposition_data_set_id", ""),
            group_id=msg_dict.get("group_id"),
            timestamp=datetime.now(),  # Use current time if not provided
            sender=msg_dict.get("sender", ""),
            recipient=msg_dict.get("recipient"),
            context_type=msg_dict.get("context_type"),
            context_value=msg_dict.get("context_value"),
            parent_message_id=msg_dict.get("parent_message_id"),
            message_subject=msg_dict.get("message_subject", ""),
            message_text=msg_dict.get("message_text", ""),
            message_type=msg_dict.get("message_type", "text"),
            send_status=msg_dict.get("send_status", "Y"),
            content_type=msg_obj.contentType,  # Use the property
        )
    
    @staticmethod
    def file_refs_to_records(file_refs: List, message_id: str, deposition_id: str) -> List[MessageFileReference]:
        """Convert file reference list to MessageFileReference dataclasses"""
        records = []
        for file_ref in file_refs:
            # file_ref might be a dict or object - handle both
            if isinstance(file_ref, dict):
                records.append(MessageFileReference(
                    message_id=message_id,
                    deposition_data_set_id=deposition_id,
                    content_type=file_ref.get("content_type", ""),
                    content_format=file_ref.get("content_format", ""),
                    partition_number=file_ref.get("partition_number", 1),
                    version_id=file_ref.get("version_id", 1),
                    file_source=file_ref.get("file_source", "archive"),
                    upload_file_name=file_ref.get("upload_file_name"),
                    file_path=file_ref.get("file_path"),
                    file_size=file_ref.get("file_size"),
                ))
            else:
                # Assume it's an object with attributes
                records.append(MessageFileReference(
                    message_id=message_id,
                    deposition_data_set_id=deposition_id,
                    content_type=getattr(file_ref, "content_type", ""),
                    content_format=getattr(file_ref, "content_format", ""),
                    partition_number=getattr(file_ref, "partition_number", 1),
                    version_id=getattr(file_ref, "version_id", 1),
                    file_source=getattr(file_ref, "file_source", "archive"),
                    upload_file_name=getattr(file_ref, "upload_file_name", None),
                    file_path=getattr(file_ref, "file_path", None),
                    file_size=getattr(file_ref, "file_size", None),
                ))
        return records
    
    @staticmethod
    def create_default_status(message_id: str, deposition_id: str) -> MessageStatus:
        """Create default status for a new message"""
        return MessageStatus(
            message_id=message_id,
            deposition_data_set_id=deposition_id,
            read_status="N",
            action_reqd="N",
            for_release="N",
        )
    
    @staticmethod
    def record_to_message_dict(record: MessageRecord) -> Dict:
        """Convert MessageRecord back to dict format for Message creation"""
        return {
            "message_id": record.message_id,
            "deposition_data_set_id": record.deposition_data_set_id,
            "group_id": record.group_id,
            "sender": record.sender,
            "recipient": record.recipient,
            "context_type": record.context_type,
            "context_value": record.context_value,
            "parent_message_id": record.parent_message_id,
            "message_subject": record.message_subject,
            "message_text": record.message_text,
            "message_type": record.message_type,
            "send_status": record.send_status,
            "message_state": "livemsg",  # Assume live when coming from database
        }
    
    @staticmethod
    def create_message_from_record(record: MessageRecord, file_refs: List = None) -> Message:
        """Create appropriate Message subclass from database record"""
        msg_dict = MessageModelBridge.record_to_message_dict(record)
        file_refs = file_refs or []
        
        if record.content_type == "notes":
            return Note(msg_dict, file_refs)
        elif record.message_type == "reminder":
            return ReminderMessage(msg_dict, file_refs)
        else:
            return Message(msg_dict, file_refs)
