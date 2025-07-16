"""
Models module for wwPDB messaging system.

Message model classes and related entities for representing different types
of messages (standard messages, notes, auto messages, reminders, etc.).
Also includes SQLAlchemy database models for ORM operations and data transfer objects.
"""

# Original CIF-based message models
from .Message import Message, Note, AutoMessage, ReminderMessage, AutoNote

# Data Transfer Objects (DTOs)
from .DataModels import (
    MessageRecord,
    MessageFileReference,
    MessageStatus
)

# SQLAlchemy database models
from .DatabaseModels import Base, MessageRecordModel, MessageFileReferenceModel, MessageStatusModel

# Model conversion utilities
from .ModelUtils import (
    model_to_dataclass,
    dataclass_to_model_data,
    dataclass_to_model,
    dict_to_dataclass,
    dataclass_to_dict,
    update_dataclass_from_dict
)

__all__ = [
    # CIF-based models
    "Message",
    "Note", 
    "AutoMessage",
    "ReminderMessage",
    "AutoNote",
    
    # DTO models
    "MessageRecord",
    "MessageFileReference", 
    "MessageStatus",
    
    # SQLAlchemy models
    "Base",
    "MessageRecordModel",
    "MessageFileReferenceModel", 
    "MessageStatusModel",
    
    # Model utilities
    "model_to_dataclass",
    "dataclass_to_model_data",
    "dataclass_to_model",
    "dict_to_dataclass",
    "dataclass_to_dict",
    "update_dataclass_from_dict"
]