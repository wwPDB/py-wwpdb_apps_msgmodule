"""
Models module for wwPDB messaging system.

Message model classes and SQLAlchemy database models with compatibility interfaces.
"""

# Original CIF-based message models
from .Message import Message, Note, AutoMessage, ReminderMessage, AutoNote

# SQLAlchemy database models with compatibility interface
from .DatabaseModels import Base, MessageRecordModel, MessageFileReferenceModel, MessageStatusModel

__all__ = [
    # CIF-based models
    "Message",
    "Note", 
    "AutoMessage",
    "ReminderMessage",
    "AutoNote",
    
    # SQLAlchemy models with compatibility interface
    "Base",
    "MessageRecordModel",
    "MessageFileReferenceModel", 
    "MessageStatusModel",
]