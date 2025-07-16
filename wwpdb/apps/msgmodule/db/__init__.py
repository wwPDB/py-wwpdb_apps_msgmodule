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

# Data Access Layer (with your SQLAlchemy ORM)
from .messaging_dal import (
    MessagingDatabaseService,
    DatabaseConnectionManager,
)

# Import data models from models module for backward compatibility
from ..models.DataModels import (
    MessageRecord,
    MessageFileReference,
    MessageStatus,
)

# Database Backend Implementation
from .MessagingDb import MessagingDb

# Bridge for Message model compatibility
from .message_bridge import MessageModelBridge

__all__ = [
    # Configuration
    "MessagingDatabaseConfig",
    "get_messaging_database_config", 
    "is_messaging_database_enabled",
    
    # Data Access Layer (SQLAlchemy ORM)
    "MessagingDatabaseService",
    "MessageRecord",
    "MessageFileReference", 
    "MessageStatus",
    "DatabaseConnectionManager",
    
    # Database Backend
    "MessagingDb",
    
    # Model Bridge
    "MessageModelBridge",
]
