"""
Database module for wwPDB messaging system.

Database access for the messaging system migration from CIF file-based storage
to relational database storage. Provides database backend implementation,
data access layer, and bridge to existing Message models.
"""

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
    # Data Access Layer (SQLAlchemy ORM)
    "MessagingDatabaseService",
    "DatabaseConnectionManager",
    
    # SQLAlchemy Models
    "Base",
    "MessageRecord",
    "MessageFileReference", 
    "MessageStatus",
    
    # Database Backend Implementation
    "MessagingDb",
]
