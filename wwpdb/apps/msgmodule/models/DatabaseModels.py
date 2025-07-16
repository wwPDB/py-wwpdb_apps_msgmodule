"""
SQLAlchemy database models for wwPDB messaging system.

Database table definitions using SQLAlchemy ORM for message storage.
"""

from sqlalchemy import create_engine, Column, String, Text, DateTime, Integer, ForeignKey, CHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

# Create SQLAlchemy base
Base = declarative_base()


class MessageRecordModel(Base):
    """SQLAlchemy model for message records"""
    __tablename__ = 'messages'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(String(255), unique=True, nullable=False, index=True)
    deposition_data_set_id = Column(String(50), nullable=False, index=True)
    group_id = Column(String(50), nullable=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    sender = Column(String(100), nullable=False, index=True)
    recipient = Column(String(100), nullable=True)
    context_type = Column(String(50), nullable=True, index=True)
    context_value = Column(String(255), nullable=True)
    parent_message_id = Column(String(255), ForeignKey('messages.message_id'), nullable=True, index=True)
    message_subject = Column(Text, nullable=False)
    message_text = Column(Text, nullable=False)
    message_type = Column(String(20), nullable=True, default='text')
    send_status = Column(CHAR(1), nullable=True, default='Y')
    content_type = Column(String(20), nullable=False, default='msgs', index=True)
    created_at = Column(DateTime, nullable=True, default=func.current_timestamp(), index=True)
    updated_at = Column(DateTime, nullable=True, default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    # Relationships
    status = relationship("MessageStatusModel", back_populates="message", uselist=False, cascade="all, delete-orphan")
    file_references = relationship("MessageFileReferenceModel", back_populates="message", cascade="all, delete-orphan")


class MessageFileReferenceModel(Base):
    """SQLAlchemy model for message file references"""
    __tablename__ = 'message_file_references'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(String(255), ForeignKey('messages.message_id'), nullable=False, index=True)
    deposition_data_set_id = Column(String(50), nullable=False, index=True)
    content_type = Column(String(50), nullable=False, index=True)
    content_format = Column(String(20), nullable=False)
    partition_number = Column(Integer, nullable=True, default=1)
    version_id = Column(Integer, nullable=True, default=1)
    file_source = Column(String(20), nullable=True, default='archive', index=True)
    upload_file_name = Column(String(255), nullable=True)
    file_path = Column(Text, nullable=True)
    file_size = Column(Integer, nullable=True)
    created_at = Column(DateTime, nullable=True, default=func.current_timestamp())
    
    # Relationships
    message = relationship("MessageRecordModel", back_populates="file_references")


class MessageStatusModel(Base):
    """SQLAlchemy model for message status"""
    __tablename__ = 'message_status'
    
    message_id = Column(String(255), ForeignKey('messages.message_id'), primary_key=True)
    deposition_data_set_id = Column(String(50), nullable=False, index=True)
    read_status = Column(CHAR(1), nullable=True, default='N', index=True)
    action_reqd = Column(CHAR(1), nullable=True, default='N', index=True)
    for_release = Column(CHAR(1), nullable=True, default='N', index=True)
    created_at = Column(DateTime, nullable=True, default=func.current_timestamp())
    updated_at = Column(DateTime, nullable=True, default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    # Relationships
    message = relationship("MessageRecordModel", back_populates="status")
