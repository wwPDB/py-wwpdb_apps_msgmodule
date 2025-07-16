"""
Streamlined SQLAlchemy models with Message interface compatibility.

This eliminates the need for intermediate dataclass conversions, making
the SQLAlchemy models themselves provide the same interface as the original
CIF-based Message classes.
"""

from sqlalchemy import create_engine, Column, String, Text, DateTime, Integer, ForeignKey, CHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

# Create SQLAlchemy base
Base = declarative_base()


class MessageRecord(Base):
    """SQLAlchemy model with Message interface compatibility"""
    __tablename__ = 'messages'
    
    # Database columns
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
    status = relationship("MessageStatus", back_populates="message", uselist=False, cascade="all, delete-orphan")
    file_references = relationship("FileReference", back_populates="message", cascade="all, delete-orphan")
    
    # ========== Compatibility Properties (Original Message interface) ==========
    
    @property
    def messageId(self):
        """Compatibility with original Message.messageId"""
        return self.message_id
    
    @messageId.setter
    def messageId(self, value):
        self.message_id = value
        
    @property
    def depositionId(self):
        """Compatibility with original Message.depositionId"""
        return self.deposition_data_set_id
        
    @property
    def groupId(self):
        """Compatibility with original Message.groupId"""
        return self.group_id
        
    @property
    def messageSubject(self):
        """Compatibility with original Message.messageSubject"""
        return self.message_subject
        
    @property
    def messageText(self):
        """Compatibility with original Message.messageText"""
        return self.message_text
        
    @property
    def messageType(self):
        """Compatibility with original Message.messageType"""
        return self.message_type
        
    @property
    def sendStatus(self):
        """Compatibility with original Message.sendStatus"""
        return self.send_status
        
    @property
    def contentType(self):
        """Compatibility with original Message.contentType"""
        return self.content_type
        
    @property
    def parentMessageId(self):
        """Compatibility with original Message.parentMessageId"""
        return self.parent_message_id
        
    @property
    def contextType(self):
        """Compatibility with original Message.contextType"""
        return self.context_type
        
    @property
    def contextValue(self):
        """Compatibility with original Message.contextValue"""
        return self.context_value
    
    # ========== Factory Methods ==========
    
    @classmethod
    def from_message_obj(cls, msg_obj):
        """Create Message from original Message object"""
        from datetime import datetime
        import uuid
        
        def safe_get(obj, attr, fallback=None):
            try:
                return getattr(obj, attr, fallback)
            except (AttributeError, KeyError):
                return fallback
        
        return cls(
            message_id=safe_get(msg_obj, 'messageId', str(uuid.uuid4())),
            deposition_data_set_id=safe_get(msg_obj, 'depositionId', ''),
            group_id=safe_get(msg_obj, 'groupId'),
            timestamp=safe_get(msg_obj, 'timestamp', datetime.now()),
            sender=safe_get(msg_obj, 'sender', ''),
            recipient=safe_get(msg_obj, 'recipient'),
            context_type=safe_get(msg_obj, 'contextType'),
            context_value=safe_get(msg_obj, 'contextValue'),
            parent_message_id=safe_get(msg_obj, 'parentMessageId'),
            message_subject=safe_get(msg_obj, 'messageSubject', ''),
            message_text=safe_get(msg_obj, 'messageText', ''),
            message_type=safe_get(msg_obj, 'messageType', 'text'),
            send_status=safe_get(msg_obj, 'sendStatus', 'Y'),
            content_type=safe_get(msg_obj, 'contentType', 'msgs')
        )


class FileReference(Base):
    """SQLAlchemy model for message file references with compatibility interface"""
    __tablename__ = 'file_reference'
    
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
    message = relationship("MessageRecord", back_populates="file_references")
    
    # ========== Compatibility Properties ==========
    
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


class MessageStatus(Base):
    """SQLAlchemy model for message status with compatibility interface"""
    __tablename__ = 'message_status'
    
    message_id = Column(String(255), ForeignKey('messages.message_id'), primary_key=True)
    deposition_data_set_id = Column(String(50), nullable=False, index=True)
    read_status = Column(CHAR(1), nullable=True, default='N', index=True)
    action_reqd = Column(CHAR(1), nullable=True, default='N', index=True)
    for_release = Column(CHAR(1), nullable=True, default='N', index=True)
    created_at = Column(DateTime, nullable=True, default=func.current_timestamp())
    updated_at = Column(DateTime, nullable=True, default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    # Relationships
    message = relationship("MessageRecord", back_populates="status")
    
    # ========== Compatibility Properties ==========
    
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
