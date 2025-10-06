"""
Data Access Layer for messaging system.

Provides streamlined database operations using SQLAlchemy ORM models with
inheritance and polymorphism for clean, maintainable code.
"""

import logging
from typing import Dict, List, Optional, Type, TypeVar, Generic
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError

from wwpdb.apps.msgmodule.db.Models import Base, MessageInfo, MessageFileReference, MessageStatus

logger = logging.getLogger(__name__)

# Generic type for SQLAlchemy models
ModelType = TypeVar('ModelType', bound=Base)


class DatabaseConnection:
    """Manages database connection and session factory"""
    
    def __init__(self, db_config: Dict):
        """Initialize database connection with configuration"""
        self.db_config = db_config
        self._engine = None
        self._session_factory = None
        self._setup_database()
    
    def _setup_database(self):
        """Setup database connection and session factory"""
        try:
            # Create connection string
            connection_string = (
                f"mysql+pymysql://{self.db_config['username']}:{self.db_config['password']}"
                f"@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
                f"?charset={self.db_config.get('charset', 'utf8mb4')}"
            )
            
            logger.info("Attempting database connection to: mysql+pymysql://%s:***@%s:%s/%s",
                        self.db_config['username'], self.db_config['host'],
                        self.db_config['port'], self.db_config['database'])
            
            # Create engine
            self._engine = create_engine(
                connection_string,
                pool_size=self.db_config.get('pool_size', 10),
                max_overflow=20,
                pool_pre_ping=True,
                echo=False  # Set to True for SQL debugging
            )
            
            # Test connection
            with self._engine.connect() as conn:
                conn.execute("SELECT 1")
                logger.info("Database connection test successful")
            
            # Create session factory
            self._session_factory = sessionmaker(bind=self._engine)
            
            logger.info("Database connection initialized successfully")
            
        except Exception as e:
            logger.error("Failed to setup database: %s", e)
            logger.error("Database config: host=%s, port=%s, database=%s, username=%s",
                        self.db_config.get('host'), self.db_config.get('port'),
                        self.db_config.get('database'), self.db_config.get('username'))
            raise
    
    def get_session(self) -> Session:
        """Get a new database session"""
        return self._session_factory()
    
    def create_tables(self):
        """Create all tables"""
        try:
            Base.metadata.create_all(self._engine)
            logger.info("All tables created successfully")
        except Exception as e:
            logger.error("Error creating tables: %s", e)
            raise
    
    def close(self):
        """Close database connections"""
        if self._engine:
            self._engine.dispose()
            logger.info("Database connections closed")


class BaseDAO(Generic[ModelType]):
    """Base Data Access Object with common CRUD operations"""
    
    def __init__(self, db_connection: DatabaseConnection, model_class: Type[ModelType]):
        self.db_connection = db_connection
        self.model_class = model_class
    
    def create(self, obj: ModelType) -> bool:
        """Create a new record"""
        try:
            with self.db_connection.get_session() as session:
                session.add(obj)
                session.commit()
                logger.info("Created %s record", self.model_class.__name__)
                return True
        except SQLAlchemyError as e:
            logger.error("Error creating %s: %s", self.model_class.__name__, e)
            logger.error("Record data: %s", obj.__dict__ if hasattr(obj, '__dict__') else str(obj))
            return False
        except Exception as e:
            logger.error("Unexpected error creating %s: %s", self.model_class.__name__, e)
            logger.error("Record data: %s", obj.__dict__ if hasattr(obj, '__dict__') else str(obj))
            return False
    
    def get_by_id(self, record_id: str, id_field: str = 'ordinal_id') -> Optional[ModelType]:
        """Get record by ID"""
        try:
            with self.db_connection.get_session() as session:
                return session.query(self.model_class).filter(
                    getattr(self.model_class, id_field) == record_id
                ).first()
        except SQLAlchemyError as e:
            logger.error("Error getting %s %s: %s", self.model_class.__name__, record_id, e)
            return None
    
    def get_all(self) -> List[ModelType]:
        """Get all records"""
        try:
            with self.db_connection.get_session() as session:
                return session.query(self.model_class).all()
        except SQLAlchemyError as e:
            logger.error("Error getting all %s records: %s", self.model_class.__name__, e)
            return []
    
    def update(self, obj: ModelType) -> bool:
        """Update an existing record"""
        try:
            with self.db_connection.get_session() as session:
                session.merge(obj)
                session.commit()
                logger.info("Updated %s record", self.model_class.__name__)
                return True
        except SQLAlchemyError as e:
            logger.error("Error updating %s: %s", self.model_class.__name__, e)
            return False
    
    def delete(self, record_id: str, id_field: str = 'ordinal_id') -> bool:
        """Delete a record by ID"""
        try:
            with self.db_connection.get_session() as session:
                obj = session.query(self.model_class).filter(
                    getattr(self.model_class, id_field) == record_id
                ).first()
                if obj:
                    session.delete(obj)
                    session.commit()
                    logger.info("Deleted %s %s", self.model_class.__name__, record_id)
                    return True
                return False
        except SQLAlchemyError as e:
            logger.error("Error deleting %s %s: %s", self.model_class.__name__, record_id, e)
            return False


class MessageDAO(BaseDAO[MessageInfo]):
    """Data Access Object for Message operations with specialized methods"""
    
    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection, MessageInfo)
    
    def get_by_message_id(self, message_id: str) -> Optional[MessageInfo]:
        """Get message by message_id"""
        return self.get_by_id(message_id, 'message_id')
    
    def get_by_deposition(self, deposition_id: str) -> List[MessageInfo]:
        """Get all messages for a deposition"""
        try:
            with self.db_connection.get_session() as session:
                return session.query(MessageInfo).filter(
                    MessageInfo.deposition_data_set_id == deposition_id
                ).all()
        except SQLAlchemyError as e:
            logger.error("Error getting messages for deposition %s: %s", deposition_id, e)
            return []
    
    def get_by_content_type(self, content_type: str) -> List[MessageInfo]:
        """Get messages by content type"""
        try:
            with self.db_connection.get_session() as session:
                return session.query(MessageInfo).filter(
                    MessageInfo.content_type == content_type
                ).all()
        except SQLAlchemyError as e:
            logger.error("Error getting messages by content type %s: %s", content_type, e)
            return []


class FileReferenceDAO(BaseDAO[MessageFileReference]):
    """Data Access Object for File Reference operations with specialized methods"""
    
    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection, MessageFileReference)
    
    def get_by_message_id(self, message_id: str) -> List[MessageFileReference]:
        """Get all file references for a message"""
        try:
            with self.db_connection.get_session() as session:
                return session.query(MessageFileReference).filter(
                    MessageFileReference.message_id == message_id
                ).all()
        except SQLAlchemyError as e:
            logger.error("Error getting file references for message %s: %s", message_id, e)
            return []


class MessageStatusDAO(BaseDAO[MessageStatus]):
    """Data Access Object for Message Status operations with specialized methods"""
    
    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection, MessageStatus)
    
    def get_by_message_id(self, message_id: str) -> Optional[MessageStatus]:
        """Get status by message_id"""
        return self.get_by_id(message_id, 'message_id')
    
    def create_or_update(self, status: MessageStatus) -> bool:
        """Create or update message status"""
        try:
            with self.db_connection.get_session() as session:
                existing = session.query(MessageStatus).filter(
                    MessageStatus.message_id == status.message_id
                ).first()
                
                if existing:
                    # Update existing
                    existing.read_status = status.read_status
                    existing.action_reqd = status.action_reqd
                    existing.for_release = status.for_release
                    existing.deposition_data_set_id = status.deposition_data_set_id
                else:
                    # Create new
                    session.add(status)
                
                session.commit()
                logger.info("Created/updated status for message %s", status.message_id)
                return True
        except SQLAlchemyError as e:
            logger.error("Error creating/updating status for %s: %s", status.message_id, e)
            return False


class DataAccessLayer:
    """Main data access facade that provides all messaging database operations"""
    
    def __init__(self, db_config: Dict):
        """Initialize data access layer with database configuration"""
        self.db_connection = DatabaseConnection(db_config)
        
        # Initialize DAOs
        self.messages = MessageDAO(self.db_connection)
        self.file_references = FileReferenceDAO(self.db_connection)
        self.status = MessageStatusDAO(self.db_connection)
    
    def create_tables(self):
        """Create all database tables"""
        self.db_connection.create_tables()
    
    def close(self):
        """Close database connections"""
        self.db_connection.close()
    
    # Convenience methods for common operations
    def create_message(self, message: MessageInfo) -> bool:
        """Create a new message"""
        return self.messages.create(message)
    
    def get_message_by_id(self, message_id: str) -> Optional[MessageInfo]:
        """Get message by ID"""
        return self.messages.get_by_message_id(message_id)
    
    def get_deposition_messages(self, deposition_id: str) -> List[MessageInfo]:
        """Get all messages for a deposition"""
        return self.messages.get_by_deposition(deposition_id)
    
    def create_file_reference(self, file_ref: MessageFileReference) -> bool:
        """Create a new file reference"""
        return self.file_references.create(file_ref)
    
    def create_or_update_status(self, status: MessageStatus) -> bool:
        """Create or update message status"""
        return self.status.create_or_update(status)
