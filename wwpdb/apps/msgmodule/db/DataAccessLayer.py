"""
Database Access Layer (DAL) for wwPDB Communication Module

This module provides database-agnostic abstraction for message storage,
supporting multiple database backends. Uses SQLAlchemy ORM for better 
maintainability and type safety while maintaining the original interface.
"""

import logging
from typing import List, Dict, Optional, Any
from datetime import datetime
from contextlib import contextmanager

# SQLAlchemy imports
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError

# Import models from models module
from ..models.Models import Base, MessageRecord, FileReference, MessageStatus

logger = logging.getLogger(__name__)


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
                # Add and flush to get the ID
                session.add(message)
                session.flush()
                
                logger.debug(f"Created message with ID: {message.id}")
                return message.id
                
            except SQLAlchemyError as e:
                logger.error(f"Failed to create message: {e}")
                raise

    def get_message_by_id(self, message_id: str) -> Optional[MessageRecord]:
        """Retrieve a message by its ID using ORM"""
        with self.connection_manager.session_scope() as session:
            try:
                message_model = session.query(MessageRecord).filter(
                    MessageRecord.message_id == message_id
                ).first()
                
                return message_model
                
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
                query = session.query(MessageRecord).filter(
                    MessageRecord.deposition_data_set_id == deposition_id
                )

                if content_type:
                    query = query.filter(MessageRecord.content_type == content_type)

                query = query.order_by(MessageRecord.timestamp.desc())

                if limit:
                    query = query.limit(limit).offset(offset)

                message_models = query.all()
                return message_models
                
            except SQLAlchemyError as e:
                logger.error(f"Failed to retrieve messages for deposition {deposition_id}: {e}")
                raise

    def update_message(self, message_id: str, updates: Dict) -> bool:
        """Update a message record using ORM"""
        with self.connection_manager.session_scope() as session:
            try:
                message_model = session.query(MessageRecord).filter(
                    MessageRecord.message_id == message_id
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
                message_model = session.query(MessageRecord).filter(
                    MessageRecord.message_id == message_id
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
                query = session.query(MessageRecord)

                for field, value in search_criteria.items():
                    if field == "text_search":
                        search_term = f"%{value}%"
                        query = query.filter(
                            (MessageRecord.message_subject.like(search_term)) |
                            (MessageRecord.message_text.like(search_term))
                        )
                    elif field == "date_from":
                        query = query.filter(MessageRecord.timestamp >= value)
                    elif field == "date_to":
                        query = query.filter(MessageRecord.timestamp <= value)
                    elif hasattr(MessageRecord, field):
                        query = query.filter(getattr(MessageRecord, field) == value)

                query = query.order_by(MessageRecord.timestamp.desc())
                message_models = query.all()
                
                return message_models
                
            except SQLAlchemyError as e:
                logger.error(f"Failed to search messages: {e}")
                raise


class MessageFileDAO(BaseDAO):
    """Data Access Object for message file reference operations"""

    def create_file_reference(self, file_ref: FileReference) -> int:
        """Create a new file reference"""
        with self.connection_manager.session_scope() as session:
            try:
                # Add and flush to get the ID
                session.add(file_ref)
                session.flush()
                
                logger.debug(f"Created file reference with ID: {file_ref.id}")
                return file_ref.id
                
            except SQLAlchemyError as e:
                logger.error(f"Failed to create file reference: {e}")
                raise

    def get_file_references_by_message(
        self, message_id: str
    ) -> List[FileReference]:
        """Get all file references for a message"""
        with self.connection_manager.session_scope() as session:
            try:
                file_models = session.query(FileReference).filter(
                    FileReference.message_id == message_id
                ).all()
                return file_models
            except SQLAlchemyError as e:
                logger.error(f"Failed to get file references by message: {e}")
                raise

    def get_file_references_by_deposition(
        self, deposition_id: str
    ) -> List[FileReference]:
        """Get all file references for a deposition"""
        with self.connection_manager.session_scope() as session:
            try:
                file_models = session.query(FileReference).filter(
                    FileReference.deposition_data_set_id == deposition_id
                ).all()
                return file_models
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
                existing_status = session.query(MessageStatus).filter(
                    MessageStatus.message_id == status.message_id
                ).first()
                
                if existing_status:
                    # Update existing status
                    existing_status.read_status = status.read_status
                    existing_status.action_reqd = status.action_reqd
                    existing_status.for_release = status.for_release
                    existing_status.updated_at = datetime.utcnow()
                else:
                    # Create new status
                    session.add(status)
                
                session.flush()
                return True
                
            except SQLAlchemyError as e:
                logger.error(f"Failed to create or update status: {e}")
                raise

    def get_status_by_message(self, message_id: str) -> Optional[MessageStatus]:
        """Get status for a specific message"""
        with self.connection_manager.session_scope() as session:
            try:
                status_model = session.query(MessageStatus).filter(
                    MessageStatus.message_id == message_id
                ).first()
                return status_model
            except SQLAlchemyError as e:
                logger.error(f"Failed to get status by message: {e}")
                raise

    def get_statuses_by_deposition(self, deposition_id: str) -> List[MessageStatus]:
        """Get all message statuses for a deposition"""
        with self.connection_manager.session_scope() as session:
            try:
                status_models = session.query(MessageStatus).filter(
                    MessageStatus.deposition_data_set_id == deposition_id
                ).all()
                return status_models
            except SQLAlchemyError as e:
                logger.error(f"Failed to get statuses by deposition: {e}")
                raise

    def mark_message_read(self, message_id: str, read_status: str = "Y") -> bool:
        """Mark a message as read/unread"""
        with self.connection_manager.session_scope() as session:
            try:
                status_model = session.query(MessageStatus).filter(
                    MessageStatus.message_id == message_id
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
                status_model = session.query(MessageStatus).filter(
                    MessageStatus.message_id == message_id
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
        file_references: List[FileReference] = None,
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
