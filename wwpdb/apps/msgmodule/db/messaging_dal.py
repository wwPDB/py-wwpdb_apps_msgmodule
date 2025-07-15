"""
Database Access Layer (DAL) for wwPDB Communication Module

This module provides database-agnostic abstraction for message storage,
supporting multiple database backends. Now uses SQLAlchemy ORM for better 
maintainability and type safety while maintaining the original interface.
"""

import logging
import time
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
from abc import ABC, abstractmethod
from contextlib import contextmanager

# SQLAlchemy imports
from sqlalchemy import create_engine, Column, String, Text, DateTime, Integer, ForeignKey, CHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
from sqlalchemy.sql import func
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

# Create SQLAlchemy base
Base = declarative_base()


# SQLAlchemy Models (replacing raw SQL)
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


@dataclass
class MessageRecord:
    """Data class representing a message record (keeping original interface)"""

    id: Optional[int] = None
    message_id: str = ""
    deposition_data_set_id: str = ""
    group_id: Optional[str] = None
    timestamp: datetime = None
    sender: str = ""
    recipient: Optional[str] = None
    context_type: Optional[str] = None
    context_value: Optional[str] = None
    parent_message_id: Optional[str] = None
    message_subject: str = ""
    message_text: str = ""
    message_type: str = "text"
    send_status: str = "Y"
    content_type: str = "msgs"  # 'msgs' or 'notes'
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # Add property aliases for compatibility with Message class interface
    @property
    def messageId(self):
        return self.message_id

    @property
    def depositionId(self):
        return self.deposition_data_set_id

    @property
    def groupId(self):
        return self.group_id

    @property
    def messageSubject(self):
        return self.message_subject

    @property
    def messageText(self):
        return self.message_text

    @property
    def messageType(self):
        return self.message_type

    @property
    def sendStatus(self):
        return self.send_status

    @property
    def contentType(self):
        return self.content_type

    @property
    def parentMessageId(self):
        return self.parent_message_id

    @property
    def contextType(self):
        return self.context_type

    @property
    def contextValue(self):
        return self.context_value


@dataclass
class MessageFileReference:
    """Data class representing a file reference"""

    id: Optional[int] = None
    message_id: str = ""
    deposition_data_set_id: str = ""
    content_type: str = ""
    content_format: str = ""
    partition_number: int = 1
    version_id: int = 1
    file_source: str = "archive"
    upload_file_name: Optional[str] = None
    file_path: Optional[str] = None
    file_size: Optional[int] = None
    created_at: Optional[datetime] = None

    # Aliases for compatibility
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


@dataclass
class MessageStatus:
    """Data class representing message status"""

    message_id: str = ""
    deposition_data_set_id: str = ""
    read_status: str = "N"
    action_reqd: str = "N"
    for_release: str = "N"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # Aliases for compatibility
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


class DatabaseConnectionManager:
    """SQLAlchemy-based connection manager (replacing legacy backends)"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.engine = self._create_engine()
        self.Session = sessionmaker(bind=self.engine)

    def _create_engine(self):
        """Create SQLAlchemy engine from configuration"""
        db_type = self.config.get("type", "mysql").lower()
        
        if db_type == "mysql":
            username = self.config.get('user', self.config.get('username', 'root'))
            password = self.config.get('password', '')
            host = self.config.get('host', 'localhost')
            port = self.config.get('port', 3306)
            database = self.config.get('database', 'messages')
            charset = self.config.get('charset', 'utf8mb4')
            
            connection_string = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}?charset={charset}"
            
        elif db_type == "postgresql":
            username = self.config.get('user', self.config.get('username', 'postgres'))
            password = self.config.get('password', '')
            host = self.config.get('host', 'localhost')
            port = self.config.get('port', 5432)
            database = self.config.get('database', 'messages')
            
            connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
            
        elif db_type == "sqlite":
            db_path = self.config.get('database', self.config.get('path', ':memory:'))
            connection_string = f"sqlite:///{db_path}"
            
        else:
            # Default to SQLite in-memory for testing
            connection_string = "sqlite:///:memory:"
        
        # Engine configuration
        engine_config = {
            'echo': self.config.get('echo', False),
            'pool_size': self.config.get('pool_size', 5),
            'pool_timeout': self.config.get('pool_timeout', 30),
            'pool_recycle': self.config.get('pool_recycle', 3600),
            'pool_pre_ping': self.config.get('pool_pre_ping', True),
        }
        
        # Remove SQLite incompatible options
        if db_type == 'sqlite':
            engine_config = {k: v for k, v in engine_config.items() 
                           if k not in ['pool_size', 'pool_timeout', 'pool_recycle', 'pool_pre_ping']}
        
        try:
            engine = create_engine(connection_string, **engine_config)
            logger.info(f"Created SQLAlchemy engine for {db_type} database")
            return engine
        except Exception as e:
            logger.error(f"Failed to create database engine: {e}")
            # Fallback to in-memory SQLite
            logger.warning("Falling back to in-memory SQLite database")
            return create_engine("sqlite:///:memory:", echo=False)

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations"""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_connection(self):
        """Get a session (for compatibility with legacy interface)"""
        return self.Session()

    def create_tables(self):
        """Create all database tables"""
        try:
            Base.metadata.create_all(self.engine)
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create database tables: {e}")
            raise


# Helper functions for ORM conversion
def model_to_dataclass(model_instance, dataclass_type):
    """Convert SQLAlchemy model instance to dataclass"""
    if not model_instance:
        return None
    
    # Get all fields from the dataclass
    model_data = {}
    for field_name in dataclass_type.__dataclass_fields__.keys():
        if hasattr(model_instance, field_name):
            model_data[field_name] = getattr(model_instance, field_name)
    
    return dataclass_type(**model_data)

def dataclass_to_model_data(dataclass_instance):
    """Convert dataclass to dict for model creation"""
    # Exclude None/empty id fields for auto-increment
    data = {}
    for field, value in dataclass_instance.__dict__.items():
        if field == 'id' and (value is None or value == 0):
            continue
        data[field] = value
    return data

def dataclass_to_model(dataclass_instance, model_class):
    """Convert dataclass to SQLAlchemy model instance"""
    data = {}
    for field, value in dataclass_instance.__dict__.items():
        if field == 'id' and (value is None or value == 0):
            continue
        if hasattr(model_class, field):
            data[field] = value
    return model_class(**data)


class BaseDAO:
    """Base Data Access Object with SQLAlchemy ORM functionality"""

    def __init__(self, connection_manager: DatabaseConnectionManager):
        self.connection_manager = connection_manager


class MessageDAO(BaseDAO):
    """Data Access Object for message operations using SQLAlchemy ORM"""

    def create_message(self, message: MessageRecord) -> int:
        """Create a new message record using ORM"""
        with self.connection_manager.session_scope() as session:
            try:
                # Convert dataclass to model data
                model_data = dataclass_to_model_data(message)
                
                # Create model instance
                message_model = MessageRecordModel(**model_data)
                
                # Add and flush to get the ID
                session.add(message_model)
                session.flush()
                
                logger.debug(f"Created message with ID: {message_model.id}")
                return message_model.id
                
            except SQLAlchemyError as e:
                logger.error(f"Failed to create message: {e}")
                raise

    def get_message_by_id(self, message_id: str) -> Optional[MessageRecord]:
        """Retrieve a message by its ID using ORM"""
        with self.connection_manager.session_scope() as session:
            try:
                message_model = session.query(MessageRecordModel).filter(
                    MessageRecordModel.message_id == message_id
                ).first()
                
                return model_to_dataclass(message_model, MessageRecord)
                
            except SQLAlchemyError as e:
                logger.error(f"Failed to retrieve message {message_id}: {e}")
                raise

    def get_messages_by_deposition(
        self,
        deposition_id: str,
        content_type: str = None,
        limit: int = None,
        offset: int = 0,
    ) -> List[MessageRecord]:
        """Retrieve messages for a deposition using ORM"""
        with self.connection_manager.session_scope() as session:
            try:
                query = session.query(MessageRecordModel).filter(
                    MessageRecordModel.deposition_data_set_id == deposition_id
                )

                if content_type:
                    query = query.filter(MessageRecordModel.content_type == content_type)

                query = query.order_by(MessageRecordModel.timestamp.desc())

                if limit:
                    query = query.limit(limit).offset(offset)

                message_models = query.all()
                return [model_to_dataclass(model, MessageRecord) for model in message_models]
                
            except SQLAlchemyError as e:
                logger.error(f"Failed to retrieve messages for deposition {deposition_id}: {e}")
                raise

    def update_message(self, message_id: str, updates: Dict) -> bool:
        """Update a message record using ORM"""
        with self.connection_manager.session_scope() as session:
            try:
                message_model = session.query(MessageRecordModel).filter(
                    MessageRecordModel.message_id == message_id
                ).first()
                
                if not message_model:
                    return False

                # Apply updates
                for field, value in updates.items():
                    if hasattr(message_model, field):
                        setattr(message_model, field, value)

                # SQLAlchemy will automatically handle the updated_at timestamp
                session.flush()
                return True
                
            except SQLAlchemyError as e:
                logger.error(f"Failed to update message {message_id}: {e}")
                raise

    def delete_message(self, message_id: str) -> bool:
        """Delete a message record using ORM"""
        with self.connection_manager.session_scope() as session:
            try:
                message_model = session.query(MessageRecordModel).filter(
                    MessageRecordModel.message_id == message_id
                ).first()
                
                if not message_model:
                    return False

                session.delete(message_model)
                session.flush()
                return True
                
            except SQLAlchemyError as e:
                logger.error(f"Failed to delete message {message_id}: {e}")
                raise

    def search_messages(self, search_criteria: Dict) -> List[MessageRecord]:
        """Search messages based on various criteria using ORM"""
        with self.connection_manager.session_scope() as session:
            try:
                query = session.query(MessageRecordModel)

                for field, value in search_criteria.items():
                    if field == "text_search":
                        search_term = f"%{value}%"
                        query = query.filter(
                            (MessageRecordModel.message_subject.like(search_term)) |
                            (MessageRecordModel.message_text.like(search_term))
                        )
                    elif field == "date_from":
                        query = query.filter(MessageRecordModel.timestamp >= value)
                    elif field == "date_to":
                        query = query.filter(MessageRecordModel.timestamp <= value)
                    elif hasattr(MessageRecordModel, field):
                        query = query.filter(getattr(MessageRecordModel, field) == value)

                query = query.order_by(MessageRecordModel.timestamp.desc())
                message_models = query.all()
                
                return [model_to_dataclass(model, MessageRecord) for model in message_models]
                
            except SQLAlchemyError as e:
                logger.error(f"Failed to search messages: {e}")
                raise


class MessageFileDAO(BaseDAO):
    """Data Access Object for message file reference operations"""

    def create_file_reference(self, file_ref: MessageFileReference) -> int:
        """Create a new file reference"""
        with self.connection_manager.session_scope() as session:
            try:
                # Convert dataclass to model data
                model_data = dataclass_to_model_data(file_ref)
                
                # Create model instance
                file_model = MessageFileReferenceModel(**model_data)
                
                # Add and flush to get the ID
                session.add(file_model)
                session.flush()
                
                logger.debug(f"Created file reference with ID: {file_model.id}")
                return file_model.id
                
            except SQLAlchemyError as e:
                logger.error(f"Failed to create file reference: {e}")
                raise

    def get_file_references_by_message(
        self, message_id: str
    ) -> List[MessageFileReference]:
        """Get all file references for a message"""
        with self.connection_manager.session_scope() as session:
            try:
                file_models = session.query(MessageFileReferenceModel).filter(
                    MessageFileReferenceModel.message_id == message_id
                ).all()
                return [model_to_dataclass(model, MessageFileReference) for model in file_models]
            except SQLAlchemyError as e:
                logger.error(f"Failed to get file references by message: {e}")
                raise

    def get_file_references_by_deposition(
        self, deposition_id: str
    ) -> List[MessageFileReference]:
        """Get all file references for a deposition"""
        with self.connection_manager.session_scope() as session:
            try:
                file_models = session.query(MessageFileReferenceModel).filter(
                    MessageFileReferenceModel.deposition_data_set_id == deposition_id
                ).all()
                return [model_to_dataclass(model, MessageFileReference) for model in file_models]
            except SQLAlchemyError as e:
                logger.error(f"Failed to get file references by deposition: {e}")
                raise


class MessageStatusDAO(BaseDAO):
    """Data Access Object for message status operations"""

    def create_or_update_status(self, status: MessageStatus) -> bool:
        """Create or update message status"""
        with self.connection_manager.session_scope() as session:
            try:
                # Check if status already exists
                existing_status = session.query(MessageStatusModel).filter(
                    MessageStatusModel.message_id == status.message_id
                ).first()
                
                if existing_status:
                    # Update existing status
                    existing_status.read_status = status.read_status
                    existing_status.action_reqd = status.action_reqd
                    existing_status.for_release = status.for_release
                    existing_status.updated_at = datetime.utcnow()
                else:
                    # Create new status
                    model_data = dataclass_to_model_data(status)
                    status_model = MessageStatusModel(**model_data)
                    session.add(status_model)
                
                session.flush()
                return True
                
            except SQLAlchemyError as e:
                logger.error(f"Failed to create or update status: {e}")
                raise

    def get_status_by_message(self, message_id: str) -> Optional[MessageStatus]:
        """Get status for a specific message"""
        with self.connection_manager.session_scope() as session:
            try:
                status_model = session.query(MessageStatusModel).filter(
                    MessageStatusModel.message_id == message_id
                ).first()
                return model_to_dataclass(status_model, MessageStatus)
            except SQLAlchemyError as e:
                logger.error(f"Failed to get status by message: {e}")
                raise

    def get_statuses_by_deposition(self, deposition_id: str) -> List[MessageStatus]:
        """Get all message statuses for a deposition"""
        with self.connection_manager.session_scope() as session:
            try:
                status_models = session.query(MessageStatusModel).filter(
                    MessageStatusModel.deposition_data_set_id == deposition_id
                ).all()
                return [model_to_dataclass(model, MessageStatus) for model in status_models]
            except SQLAlchemyError as e:
                logger.error(f"Failed to get statuses by deposition: {e}")
                raise

    def mark_message_read(self, message_id: str, read_status: str = "Y") -> bool:
        """Mark a message as read/unread"""
        with self.connection_manager.session_scope() as session:
            try:
                status_model = session.query(MessageStatusModel).filter(
                    MessageStatusModel.message_id == message_id
                ).first()
                
                if status_model:
                    status_model.read_status = read_status
                    status_model.updated_at = datetime.utcnow()
                    session.flush()
                    return True
                return False
            except SQLAlchemyError as e:
                logger.error(f"Failed to mark message read: {e}")
                raise

    def set_action_required(self, message_id: str, action_reqd: str = "Y") -> bool:
        """Set action required flag for a message"""
        with self.connection_manager.session_scope() as session:
            try:
                status_model = session.query(MessageStatusModel).filter(
                    MessageStatusModel.message_id == message_id
                ).first()
                
                if status_model:
                    status_model.action_reqd = action_reqd
                    status_model.updated_at = datetime.utcnow()
                    session.flush()
                    return True
                return False
            except SQLAlchemyError as e:
                logger.error(f"Failed to set action required: {e}")
                raise


class MessagingDatabaseService:
    """High-level service for messaging database operations"""

    def __init__(self, db_config: Dict):
        self.connection_manager = DatabaseConnectionManager(db_config)
        self.message_dao = MessageDAO(self.connection_manager)
        self.file_dao = MessageFileDAO(self.connection_manager)
        self.status_dao = MessageStatusDAO(self.connection_manager)

    def create_message_with_status(
        self,
        message: MessageRecord,
        status: MessageStatus = None,
        file_references: List[MessageFileReference] = None,
    ) -> bool:
        """Create a complete message record with status and file references"""
        try:
            # Create the main message
            message_id = self.message_dao.create_message(message)
            if not message_id:
                return False

            # Create status record if provided
            if status:
                status.message_id = message.message_id
                self.status_dao.create_or_update_status(status)

            # Create file references if provided
            if file_references:
                for file_ref in file_references:
                    file_ref.message_id = message.message_id
                    self.file_dao.create_file_reference(file_ref)

            logger.info(f"Successfully created message {message.message_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to create message {message.message_id}: {e}")
            return False

    def get_complete_message(self, message_id: str) -> Optional[Dict]:
        """Get complete message with status and file references"""
        try:
            message = self.message_dao.get_message_by_id(message_id)
            if not message:
                return None

            status = self.status_dao.get_status_by_message(message_id)
            file_refs = self.file_dao.get_file_references_by_message(message_id)

            return {"message": message, "status": status, "file_references": file_refs}

        except Exception as e:
            logger.error(f"Failed to retrieve complete message {message_id}: {e}")
            return None

    def get_deposition_messages(
        self,
        deposition_id: str,
        content_type: str = None,
        include_status: bool = True,
        include_files: bool = True,
    ) -> List[Dict]:
        """Get all messages for a deposition with optional status and file info"""
        try:
            messages = self.message_dao.get_messages_by_deposition(
                deposition_id, content_type
            )
            result = []

            for message in messages:
                message_data = {"message": message}

                if include_status:
                    status = self.status_dao.get_status_by_message(message.message_id)
                    message_data["status"] = status

                if include_files:
                    file_refs = self.file_dao.get_file_references_by_message(
                        message.message_id
                    )
                    message_data["file_references"] = file_refs

                result.append(message_data)

            return result

        except Exception as e:
            logger.error(
                f"Failed to retrieve messages for deposition {deposition_id}: {e}"
            )
            return []
