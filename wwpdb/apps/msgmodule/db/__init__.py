"""
Database module for wwPDB messaging system.

Database access for the messaging system migration from CIF file-based storage
to relational database storage. Provides database backend implementation,
data access layer, and bridge to existing Message models.
"""

# Import SQLAlchemy models directly
from wwpdb.apps.msgmodule.db.Models import (
    Base,
    MessageInfo,
    MessageFileReference,
    MessageStatus,
)

# Import database services
from wwpdb.apps.msgmodule.db.DataAccessLayer import (
    DataAccessLayer,
)

# Import database-backed message I/O classes
from wwpdb.apps.msgmodule.db.PdbxMessageIo import PdbxMessageIo
from wwpdb.apps.msgmodule.db.PdbxMessage import (
    PdbxMessageInfo,
    PdbxMessageFileReference,
    PdbxMessageOrigCommReference,
    PdbxMessageStatus,
)

# Import compatibility stubs
from wwpdb.apps.msgmodule.db.MessagingDataImport import MessagingDataImport
from wwpdb.apps.msgmodule.db.MessagingDataExport import MessagingDataExport

# Import database-compatible file system utilities
from wwpdb.apps.msgmodule.db.LockFile import LockFile, FileSizeLogger

__all__ = [
    # SQLAlchemy Models
    "Base",
    "MessageInfo",
    "MessageFileReference",
    "MessageStatus",
    # Database Services
    "DataAccessLayer",
    # Database-backed message I/O classes
    "PdbxMessageIo",
    "PdbxMessageInfo",
    "PdbxMessageFileReference",
    "PdbxMessageOrigCommReference",
    "PdbxMessageStatus",
    # Compatibility stubs
    "MessagingDataImport",
    "MessagingDataExport",
    # Database-compatible file system utilities
    "LockFile",
    "FileSizeLogger",
]
