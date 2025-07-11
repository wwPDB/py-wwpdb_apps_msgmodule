"""
Database module for wwPDB messaging system.

This module provides database access and configuration for the messaging system
migration from CIF file-based storage to relational database storage.
"""

from .config import (
    MessagingDatabaseConfig,
    get_messaging_database_config,
    is_messaging_database_enabled,
    is_messaging_database_writes_enabled,
    is_messaging_database_reads_enabled,
    is_messaging_cif_writes_enabled,
    is_messaging_cif_reads_enabled,
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
    "is_messaging_database_writes_enabled",
    "is_messaging_database_reads_enabled",
    "is_messaging_cif_writes_enabled",
    "is_messaging_cif_reads_enabled",
    "MessagingDatabaseService",
    "MessageRecord",
    "MessageFileReference",
    "MessageStatus",
    "DatabaseConnectionManager",
]
