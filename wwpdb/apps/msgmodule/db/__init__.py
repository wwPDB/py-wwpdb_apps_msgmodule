"""
Database module for wwPDB messaging system.

Database access for the messaging system migration from CIF file-based storage
to relational database storage. Provides database backend implementation,
data access layer, and bridge to existing Message models.
"""

# Import SQLAlchemy models directly
from .Models import (
    Base,
    MessageInfo,
    MessageFileReference,
    MessageStatus,
)

# Import database services
from .DataAccessLayer import (
    DataAccessLayer,
    DatabaseConnection,
    BaseDAO,
    MessageDAO,
    FileReferenceDAO,
    MessageStatusDAO,
)

__all__ = [
    # SQLAlchemy Models
    "Base",
    "MessageInfo",
    "MessageFileReference", 
    "MessageStatus",
    # Database Services
    "DataAccessLayer",
    "DatabaseConnection",
    "BaseDAO",
    "MessageDAO",
    "FileReferenceDAO",
    "MessageStatusDAO",
]
