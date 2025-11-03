"""
SQLAlchemy models that exactly mirror the mmCIF messaging categories.

This module provides direct database mapping for the messaging system that matches
the CIF structure used by the PDBx/mmCIF format:

- **pdbx_deposition_message_info** - Core message data
- **pdbx_deposition_message_file_reference** - File attachment metadata
- **pdbx_deposition_message_status** - Message status tracking

The models use SQLAlchemy ORM to provide a Pythonic interface while maintaining
exact correspondence with the mmCIF category definitions.
"""

from sqlalchemy import Column, String, Text, DateTime, Integer, ForeignKey, CHAR, Enum, BigInteger, UniqueConstraint
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

# Create SQLAlchemy base
Base = declarative_base()


class MessageInfo(Base):
    """SQLAlchemy model mapping to _pdbx_deposition_message_info mmCIF category.

    Represents the core message data including content, metadata, and relationships.
    Each message has a unique message_id and belongs to a specific deposition.

    Attributes:
        ordinal_id (BigInteger): Auto-incrementing primary key
        message_id (String): Unique message identifier (indexed, unique)
        deposition_data_set_id (String): Deposition ID this message belongs to (indexed)
        timestamp (DateTime): When the message was created (indexed)
        sender (String): Email or identifier of message sender (indexed)
        context_type (String): Type of context (e.g., 'EM', 'validation') (indexed)
        context_value (String): Context-specific value
        parent_message_id (String): ID of parent message for threading (indexed, FK)
        message_subject (Text): Message subject line
        message_text (LONGTEXT): Full message body text
        message_type (String): Message format type (default: 'text')
        send_status (CHAR): Send status flag ('Y'/'N', default: 'Y')
        content_type (Enum): Message category (indexed). One of:
            - 'messages-to-depositor': Annotator to depositor messages
            - 'messages-from-depositor': Depositor to annotator messages
            - 'notes-from-annotator': Internal annotator notes
        created_at (DateTime): Record creation timestamp (indexed)
        updated_at (DateTime): Record last update timestamp

    Relationships:
        status: One-to-one relationship with MessageStatus
        file_references: One-to-many relationship with MessageFileReference

    Table:
        pdbx_deposition_message_info
    """
    __tablename__ = 'pdbx_deposition_message_info'

    # Database columns - exactly matching mmCIF attributes
    ordinal_id = Column(BigInteger, primary_key=True, autoincrement=True)
    message_id = Column(String(64), unique=True, nullable=False, index=True)
    deposition_data_set_id = Column(String(50), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    sender = Column(String(150), nullable=False, index=True)
    context_type = Column(String(50), nullable=True, index=True)
    context_value = Column(String(255), nullable=True)
    parent_message_id = Column(String(64), ForeignKey('pdbx_deposition_message_info.message_id'), nullable=True, index=True)
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
    """SQLAlchemy model mapping to _pdbx_deposition_message_file_reference mmCIF category.

    Stores metadata about file attachments referenced in messages. This table
    controls the attachment download links displayed to depositors and ensures
    a complete audit trail of what files were sent and when.

    Attributes:
        ordinal_id (BigInteger): Auto-incrementing primary key
        message_id (String): ID of parent message (indexed, FK to MessageInfo)
        deposition_data_set_id (String): Deposition ID (indexed)
        content_type (String): File content type (e.g., 'model', 'sf') (indexed)
        content_format (String): File format (e.g., 'pdbx', 'pdf')
        partition_number (Integer): Partition number for versioning (default: 1)
        version_id (Integer): Version identifier (default: 1)
        storage_type (String): Storage location type (indexed, default: 'archive')
        upload_file_name (String): Original uploaded filename
        created_at (DateTime): Record creation timestamp

    Relationships:
        message: Many-to-one relationship with MessageInfo

    Constraints:
        Unique constraint on (message_id, content_type, version_id, partition_number)
        to ensure idempotent inserts

    Table:
        pdbx_deposition_message_file_reference
    """
    __tablename__ = 'pdbx_deposition_message_file_reference'

    # Database columns - exactly matching mmCIF attributes
    ordinal_id = Column(BigInteger, primary_key=True, autoincrement=True)
    message_id = Column(String(64), ForeignKey('pdbx_deposition_message_info.message_id'), nullable=False, index=True)
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
    """SQLAlchemy model mapping to _pdbx_deposition_message_status mmCIF category.

    Tracks the lifecycle status of messages including read state, action requirements,
    and release flags. One status record per message.

    Attributes:
        message_id (String): Message identifier (primary key, FK to MessageInfo)
        deposition_data_set_id (String): Deposition ID (indexed)
        read_status (CHAR): Whether message has been read (indexed). Values:
            - 'Y': Message has been read
            - 'N': Message is unread (default)
        action_reqd (CHAR): Whether action is required (indexed). Values:
            - 'Y': Action required
            - 'N': No action required (default)
        for_release (CHAR): Whether message is flagged for release (indexed). Values:
            - 'Y': Flagged for release
            - 'N': Not for release (default)
        created_at (DateTime): Record creation timestamp
        updated_at (DateTime): Record last update timestamp

    Relationships:
        message: One-to-one relationship with MessageInfo

    Table:
        pdbx_deposition_message_status
    """
    __tablename__ = 'pdbx_deposition_message_status'

    # Database columns - exactly matching mmCIF attributes
    message_id = Column(String(64), ForeignKey('pdbx_deposition_message_info.message_id'), primary_key=True)
    deposition_data_set_id = Column(String(50), nullable=False, index=True)
    read_status = Column(CHAR(1), nullable=True, default='N', index=True)
    action_reqd = Column(CHAR(1), nullable=True, default='N', index=True)
    for_release = Column(CHAR(1), nullable=True, default='N', index=True)
    created_at = Column(DateTime, nullable=True, default=func.current_timestamp())
    updated_at = Column(DateTime, nullable=True, default=func.current_timestamp(), onupdate=func.current_timestamp())

    # Relationships
    message = relationship("MessageInfo", back_populates="status")
