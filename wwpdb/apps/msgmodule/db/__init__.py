"""
Database module for wwPDB messaging system.

Simple database access for the messaging system migration from CIF file-based storage
to relational database storage. No dual-mode complexity.
"""

from .config import (
    MessagingDatabaseConfig,
    get_messaging_database_config,
    is_messaging_database_enabled,
)

from .messaging_dal import (
    MessagingDatabaseService,
    MessageRecord,
    MessageFileReference,
    MessageStatus,
    DatabaseConnectionManager,
)

__all__ = [
    "MessagingDatabaseConfig",
    "get_messaging_database_config",
    "is_messaging_database_enabled",
    "MessagingDatabaseService",
    "MessageRecord",
    "MessageFileReference",
    "MessageStatus",
    "DatabaseConnectionManager",
]
