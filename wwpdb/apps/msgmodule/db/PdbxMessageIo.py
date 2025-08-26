##
# File: PdbxMessageIo.py
# Date: 26-Aug-2025  Database Migration
#
# Database-backed implementation of PdbxMessageIo interface.
#
# This class provides the same API as the original PdbxMessageIo but uses 
# the database instead of CIF files for storage and retrieval of message data.
# It maintains complete compatibility with existing code while providing
# the benefits of relational database storage.
##
"""
Database-backed drop-in replacement for the legacy CIF-based `PdbxMessageIo`.

Goal
----
Keep the *same public interface* that callers expect, while persisting and
reading messages from the relational database via SQLAlchemy models.

This class mimics the original methods that OneDep code typically uses:
  - newBlock(blockId)
  - setBlock(blockId)
  - read(filePath, logtag=None)
  - write(filePath)
  - appendMessage(rowAttribDict)
  - appendFileReference(rowAttribDict)
  - appendOrigCommReference(rowAttribDict)
  - appendMsgReadStatus(rowAttribDict)
  - update(catName, attributeName, value, iRow=0)
  - getCategory(catName)
  - getMessageInfo()
  - getFileReferenceInfo()
  - getOrigCommReferenceInfo()
  - nextMessageOrdinal()
  - nextFileReferenceOrdinal()
  - nextOrigCommReferenceOrdinal()

Usage
-----
# Direct import and use with site_id:
from wwpdb.apps.msgmodule.db.PdbxMessageIo import PdbxMessageIo

io = PdbxMessageIo(site_id="WWPDB_DEV", verbose=True)

# Or use the factory function:
from wwpdb.apps.msgmodule.utils.config import create_message_io_instance

io = create_message_io_instance(site_id="WWPDB_DEV", verbose=True)

Idempotency
-----------
- `write()` performs "upserts": it will insert new rows and ignore duplicates
  keyed by (`message_id`) where sensible.
- Duplicate detection uses obvious unique keys from the schema.
"""

__docformat__ = "restructuredtext en"
__author__ = "wwPDB Database Migration Team"
__email__ = "help@wwpdb.org"
__license__ = "Apache 2.0"

import sys
import uuid
import time
import re
import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, List, Optional, Any

from .DataAccessLayer import DataAccessLayer
from .Models import MessageInfo, MessageFileReference, MessageStatus

# Import compatibility classes that match original PdbxMessage interface
from .PdbxMessage import (
    PdbxMessageInfo,
    PdbxMessageFileReference,
    PdbxMessageOrigCommReference,
    PdbxMessageStatus
)

try:
    from sqlalchemy.exc import IntegrityError, OperationalError
except ImportError:
    # Fallback for environments without SQLAlchemy
    class IntegrityError(Exception):
        pass
    class OperationalError(Exception):
        pass

logger = logging.getLogger(__name__)


# ------------------------------- helpers --------------------------------- #

def _retry_db(fn):
    """Simple retry wrapper for transient DB errors."""
    def wrapper(*args, **kwargs):
        delays = [0.1, 0.5, 1.0]
        last_exc = None
        for d in delays + [None]:
            try:
                return fn(*args, **kwargs)
            except OperationalError as e:
                last_exc = e
                if d is None:
                    raise
                time.sleep(d)
        # If we get here, re-raise the last exception
        raise last_exc
    return wrapper


def _extract_deposition_id_from_path(path: str) -> Optional[str]:
    """
    Best-effort extraction of 'D_xxx' from legacy file paths.
    Works with both depositor messages and annotator notes.
    """
    if not path:
        return None
    
    # Try direct match first (D_1234567890)
    m = re.search(r'(D_[0-9]+)', path)
    if m:
        return m.group(1)
    
    # Try extracting from directory structure
    parts = path.split('/')
    for part in parts:
        if part.startswith('D_') and part[2:].isdigit():
            return part
    
    return None


# ------------------------------ main class -------------------------------- #

class PdbxMessageIo:
    """
    DB-backed implementation with the *same* public interface that callers use.
    Internally, this buffers rows exactly like the CIF object did, but persists
    to SQL on `write()`, and loads from SQL on `read()`.
    """

    # Category names preserved for compatibility
    CAT_MSG_INFO = "pdbx_deposition_message_info"
    CAT_FILE_REF = "pdbx_deposition_message_file_reference"
    CAT_ORIG_REF = "pdbx_deposition_message_origcomm_reference"
    CAT_STATUS   = "pdbx_deposition_message_status"

    def __init__(self, verbose: bool = True, log = sys.stderr, site_id: Optional[str] = None, dal: Optional[DataAccessLayer] = None):
        """Initialize database-backed message I/O
        
        Args:
            verbose: Enable verbose logging (matches original mmcif_utils interface)
            log: Log file handle (matches original mmcif_utils interface)
            site_id: Site ID for ConfigInfo configuration (optional - will try to auto-detect)
            dal: Optional DataAccessLayer instance (will create one if not provided)
        """
        self.__verbose = verbose
        self.__debug = False
        self.__lfh = log
        self.__site_id = site_id
        self.__deposition_id: Optional[str] = None
        self.__filePath: Optional[str] = None

        # in-memory buffers, list[dict] like the CIF object returned
        self.__rows_msg: List[Dict[str, Any]] = []
        self.__rows_file: List[Dict[str, Any]] = []
        self.__rows_orig: List[Dict[str, Any]] = []
        self.__rows_status: List[Dict[str, Any]] = []

        # DAL / session
        if dal is not None:
            self.__dal = dal
        else:
            # Initialize DAL with ConfigInfo
            try:
                from wwpdb.utils.config.ConfigInfo import ConfigInfo
                
                # Try to auto-detect site_id if not provided
                if self.__site_id is None:
                    # Try common environment variables or defaults
                    import os
                    self.__site_id = (
                        os.getenv('WWPDB_SITE_ID') or 
                        os.getenv('SITE_ID') or 
                        'WWPDB_DEV'
                    )
                    if self.__verbose:
                        logger.info(f"Auto-detected site_id: {self.__site_id}")
                
                config_info = ConfigInfo(self.__site_id)
                
                db_config = {
                    "host": config_info.get("SITE_DB_HOST_NAME"),
                    "port": int(config_info.get("SITE_DB_PORT_NUMBER", "3306")),
                    "database": config_info.get("WWPDB_MESSAGING_DB_NAME"),
                    "username": config_info.get("SITE_DB_ADMIN_USER"),
                    "password": config_info.get("SITE_DB_ADMIN_PASS", ""),
                    "charset": "utf8mb4",
                }
                
                self.__dal = DataAccessLayer(db_config)
            except Exception as e:
                logger.error(f"Failed to initialize DataAccessLayer: {e}")
                raise RuntimeError(f"Database configuration error: {e}")

    # -------------- compatibility "category" accessors ---------------- #

    def getCategory(self, catName: str = CAT_MSG_INFO) -> List[Dict[str, Any]]:
        """Get category data as list of dictionaries (compatibility method)"""
        return self.getItemDictList(catName)

    def getMessageInfo(self) -> List[Dict[str, Any]]:
        """Get message info as list of attribute dictionaries"""
        return self.getAttribDictList(catName=self.CAT_MSG_INFO)

    def getFileReferenceInfo(self) -> List[Dict[str, Any]]:
        """Get file reference info as list of attribute dictionaries"""
        return self.getAttribDictList(catName=self.CAT_FILE_REF)
    
    def getMessageList(self) -> List[str]:
        """Get list of message IDs (compatibility method)"""
        try:
            with self.__dal.get_session() as session:
                result = session.query(MessageInfo.message_id).all()
                return [row[0] for row in result if row[0]]
        except Exception as e:
            logger.error(f"Error getting message list: {e}")
            return []
        return self.getAttribDictList(catName=self.CAT_FILE_REF)

    def getOrigCommReferenceInfo(self) -> List[Dict[str, Any]]:
        """Get original communication reference info (for compatibility)"""
        return self.getAttribDictList(catName=self.CAT_ORIG_REF)

    def getMsgStatusInfo(self) -> List[Dict[str, Any]]:
        """Get message status info as list of attribute dictionaries"""
        return self.getAttribDictList(catName=self.CAT_STATUS)

    # ------------------ compatibility "style" shim --------------------- #

    def complyStyle(self) -> bool:
        """Check style compliance (always True for database backend)"""
        return True

    # ----------------- block/container compatibility ------------------- #

    def setBlock(self, blockId: str) -> bool:
        """Set current block/deposition ID (compatibility method)"""
        self.__deposition_id = blockId
        return True

    def newBlock(self, blockId: str) -> None:
        """Create new block (initialize for new deposition)"""
        self.__deposition_id = blockId
        self._reset_buffers()

    # ----------------------- update & append --------------------------- #

    def update(self, catName: str, attributeName: str, value: Any, iRow: int = 0) -> bool:
        """Update attribute value in specified category"""
        rows = self.getItemDictList(catName)
        if not rows:
            return False
        if iRow < 0 or iRow >= len(rows):
            return False
        rows[iRow][attributeName] = value
        return True

    def appendMessage(self, rowAttribDict: Dict[str, Any]) -> bool:
        """Append message to in-memory storage"""
        # Ensure required fields have defaults
        message_data = dict(rowAttribDict)
        if 'ordinal_id' not in message_data:
            message_data['ordinal_id'] = str(len(self.__rows_msg) + 1)
        if 'deposition_data_set_id' not in message_data and self.__deposition_id:
            message_data['deposition_data_set_id'] = self.__deposition_id
        if 'message_id' not in message_data:
            message_data['message_id'] = str(uuid.uuid4())
        if 'timestamp' not in message_data:
            message_data['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        self.__rows_msg.append(message_data)
        return True

    def appendFileReference(self, rowAttribDict: Dict[str, Any]) -> bool:
        """Append file reference to in-memory storage"""
        file_ref_data = dict(rowAttribDict)
        if 'ordinal_id' not in file_ref_data:
            file_ref_data['ordinal_id'] = str(len(self.__rows_file) + 1)
        if 'deposition_data_set_id' not in file_ref_data and self.__deposition_id:
            file_ref_data['deposition_data_set_id'] = self.__deposition_id
        
        self.__rows_file.append(file_ref_data)
        return True

    def appendOrigCommReference(self, rowAttribDict: Dict[str, Any]) -> bool:
        """Append original communication reference (for compatibility)"""
        self.__rows_orig.append(dict(rowAttribDict))
        return True

    def appendMsgReadStatus(self, rowAttribDict: Dict[str, Any]) -> bool:
        """Append message status to in-memory storage"""
        status_data = dict(rowAttribDict)
        if 'deposition_data_set_id' not in status_data and self.__deposition_id:
            status_data['deposition_data_set_id'] = self.__deposition_id
        
        self.__rows_status.append(status_data)
        return True

    # --------------------------- read/write ---------------------------- #

    @_retry_db
    def read(self, filePath: str, logtag: str = "") -> bool:
        """
        Load message data from database instead of file.
        
        The filePath is used to extract deposition_id for database lookup.
        Maintains compatibility with original interface.
        
        Args:
            filePath: File path (used to extract deposition ID)
            logtag: Log tag (for compatibility, ignored)
            
        Returns:
            True if data could be loaded successfully
        """
        self.__filePath = filePath
        dep_id = _extract_deposition_id_from_path(filePath)
        
        if dep_id is None:
            # Try treating the whole path as a block id if it looks like one
            if re.fullmatch(r'D_[0-9]+', filePath):
                dep_id = filePath
                
        if dep_id is None:
            self._log(f"[read] Could not parse deposition id from path: {filePath}")
            return False

        self.__deposition_id = dep_id
        try:
            self._load_from_db()
            return True
        except Exception as e:
            logger.error(f"Error reading from database for {dep_id}: {e}")
            return False

    @_retry_db
    def write(self, filePath: str) -> bool:
        """
        Write data to database instead of file.
        
        The filePath parameter is ignored - data is written to database.
        Maintains compatibility with original interface.
        
        Args:
            filePath: File path (ignored but accepted for compatibility)
            
        Returns:
            True if data was written successfully
        """
        if not self.__deposition_id:
            # Try to recover from filePath if set
            dep = _extract_deposition_id_from_path(filePath) if filePath else None
            if dep:
                self.__deposition_id = dep
            else:
                raise ValueError("No deposition block set. Call newBlock()/setBlock() or read() first.")

        try:
            return self._write_to_database()
        except Exception as e:
            logger.error(f"Error writing to database: {e}")
            return False

    # ------------------------ ordinals / counts ------------------------ #

    def nextMessageOrdinal(self) -> int:
        """Get next message ordinal number"""
        return len(self.__rows_msg) + 1

    def nextFileReferenceOrdinal(self) -> int:
        """Get next file reference ordinal number"""
        return len(self.__rows_file) + 1

    def nextOrigCommReferenceOrdinal(self) -> int:
        """Get next original communication reference ordinal number"""
        return len(self.__rows_orig) + 1

    def getRowCount(self, catName: str) -> int:
        """Get row count for specified category"""
        if catName == self.CAT_MSG_INFO:
            return len(self.__rows_msg)
        elif catName == self.CAT_FILE_REF:
            return len(self.__rows_file)
        elif catName == self.CAT_ORIG_REF:
            return len(self.__rows_orig)
        elif catName == self.CAT_STATUS:
            return len(self.__rows_status)
        else:
            return 0

    # -------------------------- private bits -------------------------- #

    def _reset_buffers(self) -> None:
        """Clear all in-memory buffers"""
        self.__rows_msg.clear()
        self.__rows_file.clear()
        self.__rows_orig.clear()
        self.__rows_status.clear()

    def _log(self, s: str) -> None:
        """Log message if verbose mode enabled"""
        if self.__verbose and self.__lfh:
            try:
                self.__lfh.write(s + "\n")
            except Exception:
                pass

    def getItemDictList(self, catName: str) -> List[Dict[str, Any]]:
        """Get items for specified category as list of dictionaries"""
        if catName == self.CAT_MSG_INFO:
            return self.__rows_msg
        elif catName == self.CAT_FILE_REF:
            return self.__rows_file
        elif catName == self.CAT_ORIG_REF:
            return self.__rows_orig
        elif catName == self.CAT_STATUS:
            return self.__rows_status
        else:
            return []

    def getAttribDictList(self, catName: str) -> List[Dict[str, Any]]:
        """Get attributes for specified category (alias for getItemDictList)"""
        return self.getItemDictList(catName)

    def _load_from_db(self) -> None:
        """Populate buffers from the database for the current deposition."""
        self._reset_buffers()
        if not self.__deposition_id:
            return

        try:
            # Load messages for the deposition
            messages = self.__dal.get_deposition_messages(self.__deposition_id)
            
            for msg in messages:
                # Convert SQLAlchemy model to dictionary format
                msg_dict = {
                    'ordinal_id': str(msg.ordinal_id),
                    'message_id': msg.message_id,
                    'deposition_data_set_id': msg.deposition_data_set_id,
                    'timestamp': msg.timestamp.strftime('%Y-%m-%d %H:%M:%S') if msg.timestamp else '',
                    'sender': msg.sender or '',
                    'context_type': msg.context_type or '',
                    'context_value': msg.context_value or '',
                    'parent_message_id': msg.parent_message_id or '',
                    'message_subject': msg.message_subject or '',
                    'message_text': msg.message_text or '',
                    'message_type': msg.message_type or 'text',
                    'send_status': msg.send_status or 'Y',
                    'content_type': msg.content_type or '',
                }
                self.__rows_msg.append(msg_dict)
                
                # Load file references for this message
                file_refs = self.__dal.file_references.get_by_message_id(msg.message_id)
                for ref in file_refs:
                    ref_dict = {
                        'ordinal_id': str(ref.ordinal_id),
                        'message_id': ref.message_id,
                        'deposition_data_set_id': ref.deposition_data_set_id,
                        'content_type': ref.content_type,
                        'content_format': ref.content_format,
                        'partition_number': str(ref.partition_number),
                        'version_id': str(ref.version_id),
                        'storage_type': ref.storage_type or 'archive',
                        'upload_file_name': ref.upload_file_name or '',
                    }
                    self.__rows_file.append(ref_dict)
                
                # Load status for this message
                status = self.__dal.status.get_by_message_id(msg.message_id)
                if status:
                    status_dict = {
                        'message_id': status.message_id,
                        'deposition_data_set_id': status.deposition_data_set_id,
                        'read_status': status.read_status or 'N',
                        'action_reqd': status.action_reqd or 'N',
                        'for_release': status.for_release or 'N',
                    }
                    self.__rows_status.append(status_dict)
            
            if self.__verbose:
                self._log(f"Loaded {len(self.__rows_msg)} messages for {self.__deposition_id}")
                
        except Exception as e:
            logger.error(f"Error loading data from database for {self.__deposition_id}: {e}")
            raise

    def _write_to_database(self) -> bool:
        """Write all in-memory data to database"""
        try:
            success = True
            
            # Write messages
            for msg_data in self.__rows_msg:
                message = self._dict_to_message_info(msg_data)
                if not self.__dal.create_message(message):
                    success = False
                    logger.error(f"Failed to create message {msg_data.get('message_id')}")
            
            # Write file references
            for ref_data in self.__rows_file:
                file_ref = self._dict_to_file_reference(ref_data)
                if not self.__dal.create_file_reference(file_ref):
                    success = False
                    logger.error(f"Failed to create file reference for message {ref_data.get('message_id')}")
            
            # Write message statuses
            for status_data in self.__rows_status:
                status = self._dict_to_message_status(status_data)
                if not self.__dal.create_or_update_status(status):
                    success = False
                    logger.error(f"Failed to create/update status for message {status_data.get('message_id')}")
            
            if self.__verbose and success:
                self._log(f"Successfully wrote {len(self.__rows_msg)} messages to database")
            
            return success
            
        except Exception as e:
            logger.error(f"Error writing to database: {e}")
            return False

    def _dict_to_message_info(self, msg_dict: Dict[str, Any]) -> MessageInfo:
        """Convert dictionary to MessageInfo model"""
        # Parse timestamp
        timestamp = datetime.now()
        if msg_dict.get('timestamp'):
            try:
                timestamp = datetime.strptime(msg_dict['timestamp'], '%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    timestamp = datetime.strptime(msg_dict['timestamp'], '%Y-%m-%d')
                except ValueError:
                    pass  # Use default
        
        return MessageInfo(
            message_id=msg_dict.get('message_id', ''),
            deposition_data_set_id=msg_dict.get('deposition_data_set_id', ''),
            timestamp=timestamp,
            sender=msg_dict.get('sender', ''),
            context_type=msg_dict.get('context_type'),
            context_value=msg_dict.get('context_value'),
            parent_message_id=msg_dict.get('parent_message_id'),
            message_subject=msg_dict.get('message_subject', ''),
            message_text=msg_dict.get('message_text', ''),
            message_type=msg_dict.get('message_type', 'text'),
            send_status=msg_dict.get('send_status', 'Y'),
            content_type=msg_dict.get('content_type', 'messages-to-depositor'),
        )

    def _dict_to_file_reference(self, ref_dict: Dict[str, Any]) -> MessageFileReference:
        """Convert dictionary to MessageFileReference model"""
        return MessageFileReference(
            message_id=ref_dict.get('message_id', ''),
            deposition_data_set_id=ref_dict.get('deposition_data_set_id', ''),
            content_type=ref_dict.get('content_type', ''),
            content_format=ref_dict.get('content_format', ''),
            partition_number=int(ref_dict.get('partition_number', 1)),
            version_id=int(ref_dict.get('version_id', 1)),
            storage_type=ref_dict.get('storage_type', 'archive'),
            upload_file_name=ref_dict.get('upload_file_name'),
        )

    def _dict_to_message_status(self, status_dict: Dict[str, Any]) -> MessageStatus:
        """Convert dictionary to MessageStatus model"""
        return MessageStatus(
            message_id=status_dict.get('message_id', ''),
            deposition_data_set_id=status_dict.get('deposition_data_set_id', ''),
            read_status=status_dict.get('read_status', 'N'),
            action_reqd=status_dict.get('action_reqd', 'N'),
            for_release=status_dict.get('for_release', 'N'),
        )

    def close(self):
        """Close database connection"""
        if self.__dal:
            self.__dal.close()
            if self.__verbose:
                self._log("Database connection closed")


# Factory function to create appropriate message I/O instance
def create_message_io(site_id: Optional[str] = None, use_database: bool = True, **kwargs):
    """Factory function to create message I/O instance
    
    Args:
        site_id: Site ID for ConfigInfo configuration (optional - will auto-detect if not provided)
        use_database: If True, use database backend; if False, use CIF files
        **kwargs: Additional arguments passed to constructor
    
    Returns:
        PdbxMessageIo instance (database-backed or CIF-based)
    """
    if use_database:
        return PdbxMessageIo(site_id=site_id, **kwargs)
    else:
        # Fall back to original CIF-based implementation
        try:
            from mmcif_utils.message.PdbxMessageIo import PdbxMessageIo as CifPdbxMessageIo
            return CifPdbxMessageIo(**kwargs)
        except ImportError:
            logger.warning("CIF-based PdbxMessageIo not available, using database backend")
            return PdbxMessageIo(site_id=site_id, **kwargs)
