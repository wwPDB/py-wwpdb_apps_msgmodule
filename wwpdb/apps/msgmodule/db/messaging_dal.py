"""
Database Access Layer (DAL) for wwPDB Communication Module

This module provides database abstraction for message storage,
replacing the current CIF file-based approach.
"""

import logging
import time
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import mysql.connector
from mysql.connector import pooling

logger = logging.getLogger(__name__)


@dataclass
class MessageRecord:
    """Data class representing a message record"""
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


class DatabaseConnectionManager:
    """Manages database connections and connection pooling"""
    
    def __init__(self, config: Dict):
        self.config = config
        self._pool = None
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize the connection pool"""
        try:
            pool_config = {
                'host': self.config['host'],
                'port': self.config['port'],
                'database': self.config['database'],
                'user': self.config['user'],
                'password': self.config['password'],
                'charset': self.config.get('charset', 'utf8mb4'),
                'pool_name': 'msgmodule_pool',
                'pool_size': self.config.get('pool_size', 20),
                'pool_reset_session': True,
                'autocommit': False
            }
            self._pool = pooling.MySQLConnectionPool(**pool_config)
            logger.info("Database connection pool initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database connection pool: {e}")
            raise
    
    def get_connection(self):
        """Get a connection from the pool"""
        try:
            return self._pool.get_connection()
        except Exception as e:
            logger.error(f"Failed to get database connection: {e}")
            raise


class BaseDAO:
    """Base Data Access Object with common functionality"""
    
    def __init__(self, connection_manager: DatabaseConnectionManager):
        self.connection_manager = connection_manager
    
    def execute_query(self, query: str, params: Tuple = None, fetch_one: bool = False, 
                     fetch_all: bool = False, commit: bool = False) -> Optional[any]:
        """Execute a database query with proper connection handling"""
        connection = None
        cursor = None
        try:
            connection = self.connection_manager.get_connection()
            cursor = connection.cursor(dictionary=True)
            
            cursor.execute(query, params or ())
            
            if commit:
                connection.commit()
                return cursor.lastrowid
            elif fetch_one:
                return cursor.fetchone()
            elif fetch_all:
                return cursor.fetchall()
            else:
                return cursor.rowcount
                
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Database query failed: {query[:100]}... Error: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()


class MessageDAO(BaseDAO):
    """Data Access Object for message operations"""
    
    def create_message(self, message: MessageRecord) -> int:
        """Create a new message record"""
        query = """
        INSERT INTO messages (
            message_id, deposition_data_set_id, group_id, timestamp, sender,
            recipient, context_type, context_value, parent_message_id,
            message_subject, message_text, message_type, send_status, content_type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            message.message_id, message.deposition_data_set_id, message.group_id,
            message.timestamp, message.sender, message.recipient, message.context_type,
            message.context_value, message.parent_message_id, message.message_subject,
            message.message_text, message.message_type, message.send_status, message.content_type
        )
        return self.execute_query(query, params, commit=True)
    
    def get_message_by_id(self, message_id: str) -> Optional[MessageRecord]:
        """Retrieve a message by its ID"""
        query = "SELECT * FROM messages WHERE message_id = %s"
        result = self.execute_query(query, (message_id,), fetch_one=True)
        return MessageRecord(**result) if result else None
    
    def get_messages_by_deposition(self, deposition_id: str, content_type: str = None,
                                  limit: int = None, offset: int = 0) -> List[MessageRecord]:
        """Retrieve messages for a deposition"""
        query = "SELECT * FROM messages WHERE deposition_data_set_id = %s"
        params = [deposition_id]
        
        if content_type:
            query += " AND content_type = %s"
            params.append(content_type)
        
        query += " ORDER BY timestamp DESC"
        
        if limit:
            query += " LIMIT %s OFFSET %s"
            params.extend([limit, offset])
        
        results = self.execute_query(query, tuple(params), fetch_all=True)
        return [MessageRecord(**row) for row in results] if results else []
    
    def update_message(self, message_id: str, updates: Dict) -> bool:
        """Update a message record"""
        if not updates:
            return False
        
        set_clauses = []
        params = []
        
        for field, value in updates.items():
            set_clauses.append(f"{field} = %s")
            params.append(value)
        
        params.append(message_id)
        query = f"UPDATE messages SET {', '.join(set_clauses)} WHERE message_id = %s"
        
        rows_affected = self.execute_query(query, tuple(params), commit=True)
        return rows_affected > 0
    
    def delete_message(self, message_id: str) -> bool:
        """Delete a message record"""
        query = "DELETE FROM messages WHERE message_id = %s"
        rows_affected = self.execute_query(query, (message_id,), commit=True)
        return rows_affected > 0
    
    def search_messages(self, search_criteria: Dict) -> List[MessageRecord]:
        """Search messages based on various criteria"""
        conditions = []
        params = []
        
        for field, value in search_criteria.items():
            if field == 'text_search':
                conditions.append("(message_subject LIKE %s OR message_text LIKE %s)")
                search_term = f"%{value}%"
                params.extend([search_term, search_term])
            elif field == 'date_from':
                conditions.append("timestamp >= %s")
                params.append(value)
            elif field == 'date_to':
                conditions.append("timestamp <= %s")
                params.append(value)
            else:
                conditions.append(f"{field} = %s")
                params.append(value)
        
        query = "SELECT * FROM messages"
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY timestamp DESC"
        
        results = self.execute_query(query, tuple(params), fetch_all=True)
        return [MessageRecord(**row) for row in results] if results else []


class MessageFileDAO(BaseDAO):
    """Data Access Object for message file reference operations"""
    
    def create_file_reference(self, file_ref: MessageFileReference) -> int:
        """Create a new file reference"""
        query = """
        INSERT INTO message_file_references (
            message_id, deposition_data_set_id, content_type, content_format,
            partition_number, version_id, file_source, upload_file_name,
            file_path, file_size
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            file_ref.message_id, file_ref.deposition_data_set_id, file_ref.content_type,
            file_ref.content_format, file_ref.partition_number, file_ref.version_id,
            file_ref.file_source, file_ref.upload_file_name, file_ref.file_path, file_ref.file_size
        )
        return self.execute_query(query, params, commit=True)
    
    def get_file_references_by_message(self, message_id: str) -> List[MessageFileReference]:
        """Get all file references for a message"""
        query = "SELECT * FROM message_file_references WHERE message_id = %s"
        results = self.execute_query(query, (message_id,), fetch_all=True)
        return [MessageFileReference(**row) for row in results] if results else []
    
    def get_file_references_by_deposition(self, deposition_id: str) -> List[MessageFileReference]:
        """Get all file references for a deposition"""
        query = "SELECT * FROM message_file_references WHERE deposition_data_set_id = %s"
        results = self.execute_query(query, (deposition_id,), fetch_all=True)
        return [MessageFileReference(**row) for row in results] if results else []


class MessageStatusDAO(BaseDAO):
    """Data Access Object for message status operations"""
    
    def create_or_update_status(self, status: MessageStatus) -> bool:
        """Create or update message status"""
        query = """
        INSERT INTO message_status (
            message_id, deposition_data_set_id, read_status, action_reqd, for_release
        ) VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            read_status = VALUES(read_status),
            action_reqd = VALUES(action_reqd),
            for_release = VALUES(for_release),
            updated_at = CURRENT_TIMESTAMP
        """
        params = (
            status.message_id, status.deposition_data_set_id,
            status.read_status, status.action_reqd, status.for_release
        )
        rows_affected = self.execute_query(query, params, commit=True)
        return rows_affected > 0
    
    def get_status_by_message(self, message_id: str) -> Optional[MessageStatus]:
        """Get status for a specific message"""
        query = "SELECT * FROM message_status WHERE message_id = %s"
        result = self.execute_query(query, (message_id,), fetch_one=True)
        return MessageStatus(**result) if result else None
    
    def get_statuses_by_deposition(self, deposition_id: str) -> List[MessageStatus]:
        """Get all message statuses for a deposition"""
        query = "SELECT * FROM message_status WHERE deposition_data_set_id = %s"
        results = self.execute_query(query, (deposition_id,), fetch_all=True)
        return [MessageStatus(**row) for row in results] if results else []
    
    def mark_message_read(self, message_id: str, read_status: str = "Y") -> bool:
        """Mark a message as read/unread"""
        query = """
        UPDATE message_status 
        SET read_status = %s, updated_at = CURRENT_TIMESTAMP 
        WHERE message_id = %s
        """
        rows_affected = self.execute_query(query, (read_status, message_id), commit=True)
        return rows_affected > 0
    
    def set_action_required(self, message_id: str, action_reqd: str = "Y") -> bool:
        """Set action required flag for a message"""
        query = """
        UPDATE message_status 
        SET action_reqd = %s, updated_at = CURRENT_TIMESTAMP 
        WHERE message_id = %s
        """
        rows_affected = self.execute_query(query, (action_reqd, message_id), commit=True)
        return rows_affected > 0


class MessagingDatabaseService:
    """High-level service for messaging database operations"""
    
    def __init__(self, db_config: Dict):
        self.connection_manager = DatabaseConnectionManager(db_config)
        self.message_dao = MessageDAO(self.connection_manager)
        self.file_dao = MessageFileDAO(self.connection_manager)
        self.status_dao = MessageStatusDAO(self.connection_manager)
    
    def create_message_with_status(self, message: MessageRecord, 
                                  status: MessageStatus = None,
                                  file_references: List[MessageFileReference] = None) -> bool:
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
            
            return {
                'message': message,
                'status': status,
                'file_references': file_refs
            }
            
        except Exception as e:
            logger.error(f"Failed to retrieve complete message {message_id}: {e}")
            return None
    
    def get_deposition_messages(self, deposition_id: str, content_type: str = None,
                               include_status: bool = True, include_files: bool = True) -> List[Dict]:
        """Get all messages for a deposition with optional status and file info"""
        try:
            messages = self.message_dao.get_messages_by_deposition(deposition_id, content_type)
            result = []
            
            for message in messages:
                message_data = {'message': message}
                
                if include_status:
                    status = self.status_dao.get_status_by_message(message.message_id)
                    message_data['status'] = status
                
                if include_files:
                    file_refs = self.file_dao.get_file_references_by_message(message.message_id)
                    message_data['file_references'] = file_refs
                
                result.append(message_data)
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to retrieve messages for deposition {deposition_id}: {e}")
            return []
