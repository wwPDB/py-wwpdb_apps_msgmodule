"""
SQLAlchemy models that exactly mirror the mmCIF messaging categories.

This provides direct database mapping for the messaging system that matches
the CIF structure:
- pdbx_deposition_message_info
- pdbx_deposition_message_file_reference  
- pdbx_deposition_message_status
"""

from sqlalchemy import Column, String, Text, DateTime, Integer, ForeignKey, CHAR, Enum, BigInteger, UniqueConstraint
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

# Create SQLAlchemy base
Base = declarative_base()


class MessageInfo(Base):
    """SQLAlchemy model mapping to _pdbx_deposition_message_info mmCIF category"""
    __tablename__ = 'pdbx_deposition_message_info'
    
    # Database columns - exactly matching mmCIF attributes
    ordinal_id = Column(BigInteger, primary_key=True, autoincrement=True)
    message_id = Column(String(255), unique=True, nullable=False, index=True)
    deposition_data_set_id = Column(String(50), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    sender = Column(String(255), nullable=False, index=True)
    context_type = Column(String(50), nullable=True, index=True)
    context_value = Column(String(255), nullable=True)
    parent_message_id = Column(String(255), ForeignKey('pdbx_deposition_message_info.message_id'), nullable=True, index=True)
    message_subject = Column(Text, nullable=False)
    message_text = Column(LONGTEXT, nullable=False)
    message_type = Column(String(20), nullable=True, default='text')
    send_status = Column(CHAR(1), nullable=True, default='Y')
    content_type = Column(Enum('messages-to-depositor', 'messages-from-depositor', 'notes-from-annotator', name='content_type_enum'), nullable=False, index=True)
    created_at = Column(DateTime, nullable=True, default=func.current_timestamp(), index=True)
    updated_at = Column(DateTime, nullable=True, default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    # Relationships
    status = relationship("MessageStatus", back_populates="message", uselist=False, cascade="all, delete-orphan")
    file_references = relationship("MessageFileReference", back_populates="message", cascade="all, delete-orphan")


class MessageFileReference(Base):
    """SQLAlchemy model mapping to _pdbx_deposition_message_file_reference mmCIF category"""
    __tablename__ = 'pdbx_deposition_message_file_reference'
    
    # Database columns - exactly matching mmCIF attributes
    ordinal_id = Column(BigInteger, primary_key=True, autoincrement=True)
    message_id = Column(String(255), ForeignKey('pdbx_deposition_message_info.message_id'), nullable=False, index=True)
    deposition_data_set_id = Column(String(50), nullable=False, index=True)
    content_type = Column(String(50), nullable=False, index=True)
    content_format = Column(String(20), nullable=False)
    partition_number = Column(Integer, nullable=True, default=1)
    version_id = Column(Integer, nullable=True, default=1)
    storage_type = Column(String(20), nullable=True, default='archive', index=True)
    upload_file_name = Column(String(255), nullable=True)
    created_at = Column(DateTime, nullable=True, default=func.current_timestamp())
    
    # Add unique constraint to ensure idempotent inserts
    __table_args__ = (
        UniqueConstraint('message_id', 'content_type', 'version_id', 'partition_number', 
                        name='uq_file_ref_message_content_version_partition'),
    )
    
    # Relationships
    message = relationship("MessageInfo", back_populates="file_references")


class MessageStatus(Base):
    """SQLAlchemy model mapping to _pdbx_deposition_message_status mmCIF category"""
    __tablename__ = 'pdbx_deposition_message_status'
    
    # Database columns - exactly matching mmCIF attributes
    message_id = Column(String(255), ForeignKey('pdbx_deposition_message_info.message_id'), primary_key=True)
    deposition_data_set_id = Column(String(50), nullable=False, index=True)
    read_status = Column(CHAR(1), nullable=True, default='N', index=True)
    action_reqd = Column(CHAR(1), nullable=True, default='N', index=True)
    for_release = Column(CHAR(1), nullable=True, default='N', index=True)
    created_at = Column(DateTime, nullable=True, default=func.current_timestamp())
    updated_at = Column(DateTime, nullable=True, default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    # Relationships
    message = relationship("MessageInfo", back_populates="status")
