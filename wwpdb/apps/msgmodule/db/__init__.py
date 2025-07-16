"""
Database module for wwPDB messaging system.

Database access for the messaging system migration from CIF file-based storage
to relational database storage. Provides database backend implementation,
configuration management, data access layer, and bridge to existing Message models.
"""

# Configuration
from .config import (
    MessagingDatabaseConfig,
    get_messaging_database_config,
    is_messaging_database_enabled,
)

# Data Access Layer (with SQLAlchemy ORM)
from .DataAccessLayer import (
    MessagingDatabaseService,
    DatabaseConnectionManager,
)

# Import SQLAlchemy models directly
from ..models.Models import (
    Base,
    MessageRecord,
    MessageFileReference,
    MessageStatus,
)

# Database Backend Implementation
from .MessagingDb import MessagingDb

__all__ = [
    # Configuration
    "MessagingDatabaseConfig",
    "get_messaging_database_config", 
    "is_messaging_database_enabled",
    
    # Data Access Layer (SQLAlchemy ORM)
    "MessagingDatabaseService",
    "DatabaseConnectionManager",
    
    # SQLAlchemy Models
    "Base",
    "MessageRecord",
    "MessageFileReference", 
    "MessageStatus",
    
    # Database Backend
    "MessagingDb",
]
