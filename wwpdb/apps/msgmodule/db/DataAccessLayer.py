"""
Data Access Layer for messaging system.

Provides streamlined database operations using SQLAlchemy ORM models with
inheritance and polymorphism for clean, maintainable code.
"""

import logging
import time
import threading
from typing import Dict, List, Optional, Type, TypeVar, Generic
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError, OperationalError

from wwpdb.apps.msgmodule.db.Models import Base, MessageInfo, MessageFileReference, MessageStatus

logger = logging.getLogger(__name__)

# Generic type for SQLAlchemy models
ModelType = TypeVar('ModelType', bound=Base)

# Global engine cache - one engine per unique connection string
# This ensures all PdbxMessageIo instances share the same connection pool
_engine_cache = {}
_engine_cache_lock = threading.Lock()


def _get_or_create_engine(db_config: Dict):
    """Get or create a shared engine for the given database configuration.

    This implements a singleton pattern per connection string, ensuring that all
    PdbxMessageIo instances share the same connection pool, preventing connection
    leaks even if __del__ is not called promptly.

    Args:
        db_config (Dict): Database configuration dictionary containing:
            - host (str): Database server hostname
            - port (int): Database server port
            - database (str): Database name
            - username (str): Database username
            - password (str): Database password
            - charset (str, optional): Character set (default: 'utf8mb4')
            - pool_size (int, optional): Connection pool size (default: 3)

    Returns:
        sqlalchemy.engine.Engine: Shared SQLAlchemy engine instance

    Note:
        Uses pool_size=3 to avoid exceeding MySQL connection limits.
        With 10 WSGI processes * 3 connections/process * 3 servers = 90 connections
        (vs default MySQL limit of 150).
    """
    # Create connection string for cache key
    connection_string = (
        f"mysql+pymysql://{db_config['username']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        f"?charset={db_config.get('charset', 'utf8mb4')}"
    )

    # Thread-safe cache access
    with _engine_cache_lock:
        if connection_string not in _engine_cache:
            logger.info("Creating new shared engine for: mysql+pymysql://%s:***@%s:%s/%s",
                        db_config['username'], db_config['host'],
                        db_config['port'], db_config['database'])

            # Create engine with enhanced connection handling for large messages
            # Using pool_size=3 to avoid exceeding MySQL connection limits:
            # 10 WSGI processes * 3 connections/process * 3 servers = 90 connections (vs default limit of 150)
            engine = create_engine(
                connection_string,
                pool_size=db_config.get('pool_size', 3),  # Reduced from 10 to 3
                max_overflow=10,  # Reduced from 20 to 10
                pool_pre_ping=True,  # Verify connections before use
                pool_recycle=3600,   # Recycle connections after 1 hour to prevent stale connections
                connect_args={
                    'connect_timeout': 300,  # 5 minute timeout for large operations
                },
                isolation_level="READ COMMITTED",  # Avoid stale reads from REPEATABLE READ
                echo=False  # Set to True for SQL debugging
            )

            # Test connection and set max_allowed_packet for large messages
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                try:
                    conn.execute(text("SET SESSION max_allowed_packet=67108864"))
                    logger.info("Set SESSION max_allowed_packet to 64MB for large messages")
                except Exception as e:
                    logger.warning(f"Could not set max_allowed_packet (may require SUPER privilege): {e}")  # pylint: disable=logging-fstring-interpolation

                logger.info("Database connection test successful")

            _engine_cache[connection_string] = engine
            logger.info("Shared engine cached - all instances will reuse this connection pool")
        else:
            logger.debug("Reusing existing shared engine for database connection")

    return _engine_cache[connection_string]


class DatabaseConnection:
    """Manages database connection and session factory.

    This class provides a centralized interface for database connectivity,
    using a shared connection pool to optimize resource usage across multiple
    instances.

    Attributes:
        db_config (Dict): Database configuration dictionary
        _engine: SQLAlchemy engine instance (shared across instances)
        _session_factory: SQLAlchemy session maker
        _is_shared_engine (bool): Flag indicating shared engine usage
    """

    def __init__(self, db_config: Dict):
        """Initialize database connection with configuration.

        Args:
            db_config (Dict): Database configuration dictionary containing connection parameters
        """
        self.db_config = db_config
        self._engine = None
        self._session_factory = None
        self._is_shared_engine = True  # Track that we're using a shared engine
        self._setup_database()

    def _setup_database(self):
        """Setup database connection and session factory.

        Creates or retrieves a shared engine and initializes the session factory
        with appropriate configuration for message handling.

        Raises:
            Exception: If database connection setup fails
        """
        try:
            # Use shared engine instead of creating a new one
            self._engine = _get_or_create_engine(self.db_config)

            # Create session factory with explicit configuration to avoid stale reads
            self._session_factory = sessionmaker(
                bind=self._engine,
                autoflush=True,
                expire_on_commit=True  # Ensure objects are refreshed after commit
            )

            logger.info("Database connection initialized successfully with shared engine")

        except Exception as e:
            logger.error("Failed to setup database: %s", e)
            logger.error("Database config: host=%s, port=%s, database=%s, username=%s",
                         self.db_config.get('host'), self.db_config.get('port'),
                         self.db_config.get('database'), self.db_config.get('username'))
            raise

    def get_session(self) -> Session:
        """Get a new database session.

        Returns:
            Session: A new SQLAlchemy session for database operations
        """
        return self._session_factory()

    def create_tables(self):
        """Create all messaging tables in the database.

        Creates the following tables if they don't exist:
            - pdbx_deposition_message_info
            - pdbx_deposition_message_file_reference
            - pdbx_deposition_message_status

        Raises:
            Exception: If table creation fails
        """
        try:
            Base.metadata.create_all(self._engine)
            logger.info("All tables created successfully")
        except Exception as e:
            logger.error("Error creating tables: %s", e)
            raise

    def close(self):
        """Close database connections.

        Note: Since we're using a shared engine, we don't dispose of it here.
        The engine and its connection pool are managed globally and shared across
        all instances. Individual sessions are automatically closed after use.
        """
        # Don't dispose of shared engine - it's managed globally
        logger.debug("DatabaseConnection.close() called - session factory cleared (shared engine retained)")
        self._session_factory = None

    def __del__(self):
        """Ensure connections are closed when object is garbage collected"""
        try:
            self.close()
        except Exception:
            pass  # Ignore errors during cleanup


class BaseDAO(Generic[ModelType]):
    """Base Data Access Object with common CRUD operations.

    Provides generic CRUD (Create, Read, Update, Delete) operations for
    SQLAlchemy ORM models, with built-in error handling and logging.

    Attributes:
        db_connection (DatabaseConnection): Database connection manager
        model_class (Type[ModelType]): SQLAlchemy model class

    Type Parameters:
        ModelType: The SQLAlchemy model type this DAO operates on
    """

    def __init__(self, db_connection: DatabaseConnection, model_class: Type[ModelType]):
        """Initialize DAO with database connection and model class.

        Args:
            db_connection (DatabaseConnection): Database connection manager
            model_class (Type[ModelType]): SQLAlchemy model class for this DAO
        """
        self.db_connection = db_connection
        self.model_class = model_class

    def create(self, obj: ModelType) -> bool:
        """Create a new record with retry logic for large messages.

        Implements exponential backoff retry logic to handle connection failures
        and large message insertions that may exceed packet size limits.

        Args:
            obj (ModelType): SQLAlchemy model instance to create

        Returns:
            bool: True if creation succeeded, False otherwise

        Note:
            Automatically sets SESSION max_allowed_packet to 64MB for large messages
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with self.db_connection.get_session() as session:
                    # Ensure max_allowed_packet is set for this session
                    try:
                        session.execute(text("SET SESSION max_allowed_packet=67108864"))
                    except Exception:
                        pass  # Ignore if we can't set it

                    session.add(obj)
                    session.commit()
                    logger.info("Created %s record", self.model_class.__name__)
                    return True
            except OperationalError as e:
                error_msg = str(e)
                if "MySQL server has gone away" in error_msg or "Broken pipe" in error_msg:
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt  # Exponential backoff
                        logger.warning(
                            "Connection lost (attempt %d/%d). Retrying in %ds... Error: %s",
                            attempt + 1, max_retries, wait_time, error_msg
                        )
                        time.sleep(wait_time)
                        # Force connection pool refresh
                        self.db_connection._engine.dispose()  # pylint:  disable=protected-access
                        continue
                    else:
                        logger.error(
                            "Failed to create %s after %d attempts: %s",
                            self.model_class.__name__, max_retries, e
                        )
                        logger.error("Record data: %s", obj.__dict__ if hasattr(obj, '__dict__') else str(obj))
                        return False
                else:
                    # Other operational errors
                    logger.error("Error creating %s: %s", self.model_class.__name__, e)
                    logger.error("Record data: %s", obj.__dict__ if hasattr(obj, '__dict__') else str(obj))
                    return False
            except SQLAlchemyError as e:
                logger.error("Error creating %s: %s", self.model_class.__name__, e)
                logger.error("Record data: %s", obj.__dict__ if hasattr(obj, '__dict__') else str(obj))
                return False
            except Exception as e:
                logger.error("Unexpected error creating %s: %s", self.model_class.__name__, e)
                logger.error("Record data: %s", obj.__dict__ if hasattr(obj, '__dict__') else str(obj))
                return False
        return False

    def get_by_id(self, record_id: str, id_field: str = 'ordinal_id') -> Optional[ModelType]:
        """Get record by ID.

        Args:
            record_id (str): The ID value to search for
            id_field (str): The name of the ID field (default: 'ordinal_id')

        Returns:
            Optional[ModelType]: The model instance if found, None otherwise
        """
        try:
            with self.db_connection.get_session() as session:
                return session.query(self.model_class).filter(
                    getattr(self.model_class, id_field) == record_id
                ).first()
        except SQLAlchemyError as e:
            logger.error("Error getting %s %s: %s", self.model_class.__name__, record_id, e)
            return None

    def get_all(self) -> List[ModelType]:
        """Get all records.

        Returns:
            List[ModelType]: List of all model instances, empty list if none found or on error
        """
        try:
            with self.db_connection.get_session() as session:
                return session.query(self.model_class).all()
        except SQLAlchemyError as e:
            logger.error("Error getting all %s records: %s", self.model_class.__name__, e)
            return []

    def update(self, obj: ModelType) -> bool:
        """Update an existing record.

        Args:
            obj (ModelType): SQLAlchemy model instance with updated values

        Returns:
            bool: True if update succeeded, False otherwise
        """
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
        """Delete a record by ID.

        Args:
            record_id (str): The ID value of the record to delete
            id_field (str): The name of the ID field (default: 'ordinal_id')

        Returns:
            bool: True if deletion succeeded or record not found, False on error
        """
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
    """Data Access Object for Message operations with specialized methods.

    Provides message-specific database operations beyond basic CRUD,
    including queries by deposition ID and content type.

    Inherits from:
        BaseDAO[MessageInfo]: Base DAO with generic CRUD operations
    """

    def __init__(self, db_connection: DatabaseConnection):
        """Initialize MessageDAO.

        Args:
            db_connection (DatabaseConnection): Database connection manager
        """
        super().__init__(db_connection, MessageInfo)

    def get_by_message_id(self, message_id: str) -> Optional[MessageInfo]:
        """Get message by message_id.

        Args:
            message_id (str): Unique message identifier

        Returns:
            Optional[MessageInfo]: Message instance if found, None otherwise
        """
        return self.get_by_id(message_id, 'message_id')

    def get_by_deposition(self, deposition_id: str) -> List[MessageInfo]:
        """Get all messages for a deposition.

        Args:
            deposition_id (str): Deposition dataset ID (e.g., 'D_1000000001')

        Returns:
            List[MessageInfo]: List of messages for the deposition, empty list if none found
        """
        try:
            with self.db_connection.get_session() as session:
                return session.query(MessageInfo).filter(
                    MessageInfo.deposition_data_set_id == deposition_id
                ).all()
        except SQLAlchemyError as e:
            logger.error("Error getting messages for deposition %s: %s", deposition_id, e)
            return []

    def get_by_content_type(self, content_type: str) -> List[MessageInfo]:
        """Get messages by content type.

        Args:
            content_type (str): Message content type. One of:
                - 'messages-to-depositor'
                - 'messages-from-depositor'
                - 'notes-from-annotator'

        Returns:
            List[MessageInfo]: List of messages with specified content type
        """
        try:
            with self.db_connection.get_session() as session:
                return session.query(MessageInfo).filter(
                    MessageInfo.content_type == content_type
                ).all()
        except SQLAlchemyError as e:
            logger.error("Error getting messages by content type %s: %s", content_type, e)
            return []

    def get_by_deposition_and_content_type(self, deposition_id: str, content_type: str) -> List[MessageInfo]:
        """Get messages for a deposition filtered by content type.

        Args:
            deposition_id (str): Deposition dataset ID (e.g., 'D_1000000001')
            content_type (str): Message content type

        Returns:
            List[MessageInfo]: List of messages, ordered by timestamp ASC
        """
        try:
            with self.db_connection.get_session() as session:
                return session.query(MessageInfo).filter(
                    MessageInfo.deposition_data_set_id == deposition_id,
                    MessageInfo.content_type == content_type
                ).order_by(MessageInfo.timestamp.asc()).all()
        except SQLAlchemyError as e:
            logger.error("Error getting messages for deposition %s, content type %s: %s",
                         deposition_id, content_type, e)
            return []

    def get_by_date_range(self, start_date, end_date=None,
                          deposition_ids: List[str] = None, content_types: List[str] = None,
                          sender: str = None, keywords: List[str] = None) -> List[MessageInfo]:
        """Get messages within a date range with optional filters.

        Args:
            start_date (datetime): Start of date range (inclusive)
            end_date (datetime): End of date range (inclusive, defaults to now)
            deposition_ids (List[str]): Optional list of deposition IDs to filter by
            content_types (List[str]): Optional list of content types to filter by
            sender (str): Optional sender email/identifier to filter by
            keywords (List[str]): Optional keywords to search in subject and text

        Returns:
            List[MessageInfo]: List of messages matching the criteria, ordered by timestamp descending
        """
        try:
            with self.db_connection.get_session() as session:
                from sqlalchemy.orm import joinedload

                # Build base query with date range and eager load relationships
                query = session.query(MessageInfo).options(
                    joinedload(MessageInfo.file_references),
                    joinedload(MessageInfo.status)
                ).filter(
                    MessageInfo.timestamp >= start_date
                )

                # Add end date filter if provided
                if end_date:
                    query = query.filter(MessageInfo.timestamp <= end_date)

                # Add optional filters
                if deposition_ids:
                    query = query.filter(MessageInfo.deposition_data_set_id.in_(deposition_ids))

                if content_types:
                    query = query.filter(MessageInfo.content_type.in_(content_types))

                if sender:
                    query = query.filter(MessageInfo.sender.like(f"%{sender}%"))

                # Add keyword search if provided (search in subject and text)
                if keywords:
                    from sqlalchemy import or_, and_
                    keyword_filters = []
                    for keyword in keywords:
                        keyword_filter = or_(
                            MessageInfo.message_subject.like(f"%{keyword}%"),
                            MessageInfo.message_text.like(f"%{keyword}%")
                        )
                        keyword_filters.append(keyword_filter)
                    # AND all keyword filters together (all keywords must match)
                    if keyword_filters:
                        query = query.filter(and_(*keyword_filters))

                # Order by timestamp descending (most recent first)
                query = query.order_by(MessageInfo.timestamp.desc())

                # Execute query and expunge objects from session to avoid DetachedInstanceError
                results = query.all()
                for result in results:
                    session.expunge(result)

                return results

        except SQLAlchemyError as e:
            logger.error("Error getting messages by date range: %s", e)
            return []


class FileReferenceDAO(BaseDAO[MessageFileReference]):
    """Data Access Object for File Reference operations with specialized methods.

    Manages file attachment metadata associated with messages.

    Inherits from:
        BaseDAO[MessageFileReference]: Base DAO with generic CRUD operations
    """

    def __init__(self, db_connection: DatabaseConnection):
        """Initialize FileReferenceDAO.

        Args:
            db_connection (DatabaseConnection): Database connection manager
        """
        super().__init__(db_connection, MessageFileReference)

    def get_by_message_id(self, message_id: str) -> List[MessageFileReference]:
        """Get all file references for a message.

        Args:
            message_id (str): Message identifier

        Returns:
            List[MessageFileReference]: List of file references, empty list if none found
        """
        try:
            with self.db_connection.get_session() as session:
                return session.query(MessageFileReference).filter(
                    MessageFileReference.message_id == message_id
                ).all()
        except SQLAlchemyError as e:
            logger.error("Error getting file references for message %s: %s", message_id, e)
            return []


class MessageStatusDAO(BaseDAO[MessageStatus]):
    """Data Access Object for Message Status operations with specialized methods.

    Manages message status tracking (read status, action required, release flags).

    Inherits from:
        BaseDAO[MessageStatus]: Base DAO with generic CRUD operations
    """

    def __init__(self, db_connection: DatabaseConnection):
        """Initialize MessageStatusDAO.

        Args:
            db_connection (DatabaseConnection): Database connection manager
        """
        super().__init__(db_connection, MessageStatus)

    def get_by_message_id(self, message_id: str) -> Optional[MessageStatus]:
        """Get status by message_id.

        Args:
            message_id (str): Message identifier

        Returns:
            Optional[MessageStatus]: Status instance if found, None otherwise
        """
        return self.get_by_id(message_id, 'message_id')

    def create_or_update(self, status: MessageStatus) -> bool:
        """Create or update message status with retry logic.

        If a status record already exists for the message, updates it.
        Otherwise creates a new status record. Includes exponential backoff
        retry logic for connection failures.

        Args:
            status (MessageStatus): Status instance to create or update

        Returns:
            bool: True if operation succeeded, False otherwise
        """
        max_retries = 3
        for attempt in range(max_retries):
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
            except OperationalError as e:
                error_msg = str(e)
                if "MySQL server has gone away" in error_msg or "Broken pipe" in error_msg:
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        logger.warning(
                            "Connection lost during status update (attempt %d/%d). Retrying in %ds...",
                            attempt + 1, max_retries, wait_time
                        )
                        time.sleep(wait_time)
                        self.db_connection._engine.dispose()  # pylint:  disable=protected-access
                        continue
                    else:
                        logger.error(
                            "Failed to create/update status for %s after %d attempts: %s",
                            status.message_id, max_retries, e
                        )
                        return False
                else:
                    logger.error("Error creating/updating status for %s: %s", status.message_id, e)
                    return False
            except SQLAlchemyError as e:
                logger.error("Error creating/updating status for %s: %s", status.message_id, e)
                return False
        return False


class DataAccessLayer:
    """Main data access facade that provides all messaging database operations.

    This class serves as the primary interface for all database operations
    related to the messaging system. It provides a unified API and manages
    specialized DAO instances for different entity types.

    Attributes:
        db_connection (DatabaseConnection): Database connection manager
        messages (MessageDAO): DAO for message operations
        file_references (FileReferenceDAO): DAO for file reference operations
        status (MessageStatusDAO): DAO for status operations

    Example:
        >>> db_config = {
        ...     'host': 'localhost',
        ...     'port': 3306,
        ...     'database': 'messaging',
        ...     'username': 'user',
        ...     'password': 'pass'
        ... }
        >>> dal = DataAccessLayer(db_config)
        >>> message = MessageInfo(message_id='MSG001', ...)
        >>> dal.create_message(message)
        True
    """

    def __init__(self, db_config: Dict):
        """Initialize data access layer with database configuration.

        Args:
            db_config (Dict): Database configuration dictionary
        """
        self.db_connection = DatabaseConnection(db_config)

        # Initialize DAOs
        self.messages = MessageDAO(self.db_connection)
        self.file_references = FileReferenceDAO(self.db_connection)
        self.status = MessageStatusDAO(self.db_connection)

    def create_tables(self):
        """Create all database tables.

        Delegates to DatabaseConnection.create_tables() to create all
        messaging-related tables.
        """
        self.db_connection.create_tables()

    def close(self):
        """Close database connections.

        Note: Since shared engine is used, this only clears session factory.
        The connection pool remains active for other instances.
        """
        self.db_connection.close()

    def __del__(self):
        """Ensure connections are closed when object is garbage collected"""
        try:
            self.close()
        except Exception:
            pass  # Ignore errors during cleanup

    # Convenience methods for common operations
    def create_message(self, message: MessageInfo) -> bool:
        """Create a new message.

        Args:
            message (MessageInfo): Message instance to create

        Returns:
            bool: True if creation succeeded, False otherwise
        """
        return self.messages.create(message)

    def get_message_by_id(self, message_id: str) -> Optional[MessageInfo]:
        """Get message by ID.

        Args:
            message_id (str): Unique message identifier

        Returns:
            Optional[MessageInfo]: Message instance if found, None otherwise
        """
        return self.messages.get_by_message_id(message_id)

    def get_deposition_messages(self, deposition_id: str) -> List[MessageInfo]:
        """Get all messages for a deposition.

        Args:
            deposition_id (str): Deposition dataset ID (e.g., 'D_1000000001')

        Returns:
            List[MessageInfo]: List of messages, empty list if none found
        """
        return self.messages.get_by_deposition(deposition_id)

    def get_deposition_messages_by_content_type(self, deposition_id: str, content_type: str) -> List[MessageInfo]:
        """Get messages for a deposition filtered by content type.

        Args:
            deposition_id (str): Deposition dataset ID
            content_type (str): Message content type

        Returns:
            List[MessageInfo]: List of messages, ordered by timestamp
        """
        return self.messages.get_by_deposition_and_content_type(deposition_id, content_type)

    def get_file_references_for_message(self, message_id: str) -> List[MessageFileReference]:
        """Get file references for a message.

        Args:
            message_id (str): Message identifier

        Returns:
            List[MessageFileReference]: List of file references
        """
        return self.file_references.get_by_message_id(message_id)

    def create_file_reference(self, file_ref: MessageFileReference) -> bool:
        """Create a new file reference.

        Args:
            file_ref (MessageFileReference): File reference instance to create

        Returns:
            bool: True if creation succeeded, False otherwise
        """
        return self.file_references.create(file_ref)

    def create_or_update_status(self, status: MessageStatus) -> bool:
        """Create or update message status.

        Args:
            status (MessageStatus): Status instance to create or update

        Returns:
            bool: True if operation succeeded, False otherwise
        """
        return self.status.create_or_update(status)
