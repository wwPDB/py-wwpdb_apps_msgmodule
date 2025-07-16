"""
Models module for wwPDB messaging system.

Message model classes and SQLAlchemy database models with compatibility interfaces.
"""

# Original CIF-based message models
from .Message import Message, Note, AutoMessage, ReminderMessage, AutoNote

# SQLAlchemy database models with compatibility interface  
from .Models import Base, MessageRecord, MessageFileReference, MessageStatus

__all__ = [
    # CIF-based models
    "Message",
    "Note", 
    "AutoMessage",
    "ReminderMessage",
    "AutoNote",
    
    # SQLAlchemy models with compatibility interface
    "Base",
    "MessageRecord",
    "MessageFileReference", 
    "MessageStatus",
]