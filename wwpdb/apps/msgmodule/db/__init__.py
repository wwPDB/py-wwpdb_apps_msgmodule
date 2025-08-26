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
)

# Import database-backed message I/O
from .PdbxMessageIo import (
    PdbxMessageIo,
    create_message_io,
)

# Import compatibility classes for drop-in replacement
from .PdbxMessage import (
    PdbxMessageInfo,
    PdbxMessageFileReference,
    PdbxMessageOrigCommReference,
    PdbxMessageStatus,
)

__all__ = [
    # SQLAlchemy Models
    "Base",
    "MessageInfo",
    "MessageFileReference", 
    "MessageStatus",
    # Database Services
    "DataAccessLayer",
    # Message I/O
    "PdbxMessageIo",
    "create_message_io",
    # Compatibility Classes (for drop-in replacement)
    "PdbxMessageInfo",
    "PdbxMessageFileReference", 
    "PdbxMessageStatus",
    "PdbxMessageOrigCommReference",
]
