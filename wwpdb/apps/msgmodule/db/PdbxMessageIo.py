##
# File: PdbxMessageIo.py (Database-backed implementation)
# Date: 27-Aug-2025
#
# Database-backed drop-in replacement for mmcif_utils.message.PdbxMessageIo
# that maintains the same API interface while using the messaging database.
##
"""
Database-backed message I/O that provides the same interface as the original
PdbxMessageIo but stores data in the messaging database instead of CIF files.
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from wwpdb.utils.config.ConfigInfo import ConfigInfo

from wwpdb.apps.msgmodule.db.DataAccessLayer import DataAccessLayer
from wwpdb.apps.msgmodule.db.Models import MessageInfo as ORMMessageInfo, MessageFileReference as ORMFileRef, MessageStatus as ORMStatus

from wwpdb.io.locator.PathInfo import PathInfo

logger = logging.getLogger(__name__)

def _parse_context_from_path(file_path: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract deposition id and content_type from file path using PathInfo.splitFileName.
    
    For database-backed operation, we handle multiple path formats:
    1. Workflow dummy paths: /dummy/messaging/D_1000000001/D_1000000001_messages-to-depositor_P1.cif.V1
    2. Legacy hardcoded paths: /net/wwpdb_da/.../messages-to-depositor.cif
    3. Any other format: try to extract deposition ID from context or return None for "all messages"
    """
    # Handle legacy hardcoded file paths (non-workflow mode)
    if not file_path.startswith("/dummy"):
        # This is a legacy hardcoded path like "/net/wwpdb_da/.../messages-to-depositor.cif"
        filename = os.path.basename(file_path)
        if "messages-to-depositor" in filename:
            return None, "messages-to-depositor"  # No specific deposition, but filter by content type
        elif "messages-from-depositor" in filename:
            return None, "messages-from-depositor"
        elif "notes-from-annotator" in filename:
            return None, "notes-from-annotator"
        else:
            return None, None  # No filtering
    
    # Handle workflow dummy paths
    pi = PathInfo()
    filename = os.path.basename(file_path)
    result = pi.splitFileName(filename)
    
    if result and len(result) >= 2:
        dep_id = result[0]
        content_type = result[1]
        
        # Validate that content_type is one of the expected values
        valid_content_types = {'messages-from-depositor', 'messages-to-depositor', 'notes-from-annotator'}
        if content_type in valid_content_types:
            return dep_id, content_type
        else:
            # Invalid content_type, return dep_id but no content filtering
            return dep_id, None
    
    # If we can't parse properly with splitFileName, try a simpler parse
    # This handles cases where MessagingIo doesn't provide proper dummy paths
    # but we might still be able to extract a deposition ID
    simple_result = pi.splitFileName(filename)
    if simple_result and len(simple_result) >= 1:
        # Even if content_type parsing failed, we might have a valid deposition ID
        dep_id = simple_result[0]
        if dep_id and dep_id.startswith('D_'):
            return dep_id, None
    
    return None, None

def _fmt_ts(ts) -> str:
    if isinstance(ts, datetime):
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(ts, str):
        return ts
    return ""

def _parse_ts(ts_str: Optional[str]) -> datetime:
    if isinstance(ts_str, datetime):
        return ts_str
    if not ts_str:
        return datetime.utcnow()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%b-%Y %H:%M:%S", "%d-%b-%Y"):
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue
    return datetime.utcnow()


class PdbxMessageIo:
    """
    DB-backed drop-in for mmcif_utils.message.PdbxMessageIo.

    read(filePath) becomes a context selector (deposition_id + content_type).
    write(filePath) commits pending rows to DB.
    """

    def __init__(self, site_id: str, verbose=True, log=sys.stderr, db_config: Optional[Dict] = None):
        self.__verbose = verbose
        self.__lfh = log
        
        # site_id is now required - no fallback to environment
        if not site_id:
            raise ValueError("site_id parameter is required for PdbxMessageIo.")
        
        self.__site_id = site_id
        if self.__verbose:
            self.__lfh.write("PdbxMessageIo: Using site_id: %r\n" % self.__site_id)
        if not db_config:
            cI = ConfigInfo(self.__site_id)
            
            # Debug configuration values
            host = cI.get("SITE_MESSAGE_DB_HOST_NAME")
            port = cI.get("SITE_MESSAGE_DB_PORT_NUMBER", "3306")
            database = cI.get("SITE_MESSAGE_DB_NAME")
            username = cI.get("SITE_MESSAGE_DB_USER_NAME")
            password = cI.get("SITE_MESSAGE_DB_PASSWORD", "")
            socket = cI.get("SITE_MESSAGE_DB_SOCKET")  # Optional socket parameter
            
            if self.__verbose:
                self.__lfh.write("PdbxMessageIo: Database config for site_id=%s:\n" % self.__site_id)
                self.__lfh.write("  SITE_MESSAGE_DB_HOST_NAME: %s\n" % host)
                self.__lfh.write("  SITE_MESSAGE_DB_PORT_NUMBER: %s\n" % port)
                self.__lfh.write("  SITE_MESSAGE_DB_NAME: %s\n" % database)
                self.__lfh.write("  SITE_MESSAGE_DB_USER_NAME: %s\n" % username)
                self.__lfh.write("  SITE_MESSAGE_DB_PASSWORD: %s\n" % ('***' if password else 'None'))
                if socket:
                    self.__lfh.write("  SITE_MESSAGE_DB_SOCKET: %s\n" % socket)
            
            db_config = {
                "host": host,
                "port": int(port),
                "database": database,
                "username": username,
                "password": password,
                "charset": "utf8mb4",
            }
            
            # Add socket if specified
            if socket:
                db_config["unix_socket"] = socket
            
            # Validate critical configuration
            if not host:
                if self.__verbose:
                    self.__lfh.write("PdbxMessageIo: ERROR - SITE_MESSAGE_DB_HOST_NAME not configured for site_id=%s\n" % self.__site_id)
                raise ValueError(f"Database host not configured for site_id={self.__site_id}. Check SITE_MESSAGE_DB_HOST_NAME in ConfigInfo.")
            
            if not database:
                if self.__verbose:
                    self.__lfh.write("PdbxMessageIo: ERROR - SITE_MESSAGE_DB_NAME not configured for site_id=%s\n" % self.__site_id)
                raise ValueError(f"Database name not configured for site_id={self.__site_id}. Check SITE_MESSAGE_DB_NAME in ConfigInfo.")
            
            if not username:
                if self.__verbose:
                    self.__lfh.write("PdbxMessageIo: ERROR - SITE_MESSAGE_DB_USER_NAME not configured for site_id=%s\n" % self.__site_id)
                raise ValueError(f"Database username not configured for site_id={self.__site_id}. Check SITE_MESSAGE_DB_USER_NAME in ConfigInfo.")
        
        try:
            self._dal = DataAccessLayer(db_config)
            
            if self.__verbose:
                self.__lfh.write("PdbxMessageIo: Database adapter initialized successfully\n")
        except Exception as e:
            if self.__verbose:
                self.__lfh.write("PdbxMessageIo: ERROR - Failed to initialize database adapter: %s\n" % e)
            raise ValueError(f"Failed to initialize database adapter for site_id={self.__site_id}: {e}") from e

        # Current context selected by read(filePath)
        self._deposition_id: Optional[str] = None
        self._content_type: Optional[str] = None

        # Loaded data (from DB) for current context
        self._loaded_messages: List[Dict] = []
        self._loaded_file_refs: List[Dict] = []
        self._loaded_statuses: List[Dict] = []
        self._loaded_origcomm_refs: List[Dict] = []  # no DB persistence in current schema

        # Pending (to be committed on write())
        self._pending_messages: List[Dict] = []
        self._pending_file_refs: List[Dict] = []
        self._pending_statuses: List[Dict] = []
        self._pending_origcomm_refs: List[Dict] = []

        # Current blockId (no-op for DB)
        self._block_id: Optional[str] = None

    # --------- Query (read) API ---------

    def read(self, filePath: str, logtag: str = "", deposition_id: str = None) -> bool:
        """Select deposition_id and content_type context; load existing rows from DB.
        
        Args:
            filePath: File path to parse for context (workflow) or content type (legacy)
            logtag: Optional logging tag
            deposition_id: Optional explicit deposition ID (overrides path parsing)
        """
        dep_id, content_type = _parse_context_from_path(filePath)
        
        # Use explicit deposition_id if provided, otherwise use parsed
        if deposition_id:
            self._deposition_id = deposition_id
        elif dep_id:
            self._deposition_id = dep_id
            
        if content_type:
            self._content_type = content_type

        if self.__verbose:
            logger.info("DB MessageIo read() filePath=%s dep_id=%s content_type=%s logtag=%s", 
                       filePath, self._deposition_id, self._content_type, logtag)

        # Always try to load from DB, even if no deposition_id
        # _load_from_db() will handle the no-deposition case gracefully
        self._load_from_db()
        return True

    def getCategory(self, catName: str = "pdbx_deposition_message_info") -> List[Dict]:
        if catName == "pdbx_deposition_message_info":
            return self.getMessageInfo()
        if catName == "pdbx_deposition_message_file_reference":
            return self.getFileReferenceInfo()
        if catName == "pdbx_deposition_message_origcomm_reference":
            return self.getOrigCommReferenceInfo()
        if catName == "pdbx_deposition_message_status":
            return self.getMsgStatusInfo()
        return []

    def getMessageInfo(self) -> List[Dict]:
        return list(self._loaded_messages)

    def getFileReferenceInfo(self) -> List[Dict]:
        return list(self._loaded_file_refs)

    def getOrigCommReferenceInfo(self) -> List[Dict]:
        # Not persisted in current DB schema; return what was appended in-session.
        return list(self._loaded_origcomm_refs)

    def getMsgStatusInfo(self) -> List[Dict]:
        return list(self._loaded_statuses)

    # --------- Mutation API (append/update) ---------

    def update(self, catName: str, attributeName: str, value, iRow: int = 0) -> bool:
        """Update attribute in loaded row and move to pending for DB write.
        
        For database-backed storage, we need to:
        1. Update the loaded row (what the caller indexed into)
        2. Add/update the corresponding pending row for write()
        """
        loaded_target = None
        pending_target = None
        
        if catName == "pdbx_deposition_message_info":
            loaded_target = self._loaded_messages
            pending_target = self._pending_messages
        elif catName == "pdbx_deposition_message_file_reference":
            loaded_target = self._loaded_file_refs
            pending_target = self._pending_file_refs
        elif catName == "pdbx_deposition_message_status":
            loaded_target = self._loaded_statuses
            pending_target = self._pending_statuses
        elif catName == "pdbx_deposition_message_origcomm_reference":
            loaded_target = self._loaded_origcomm_refs
            pending_target = self._pending_origcomm_refs

        if loaded_target is None or iRow < 0 or iRow >= len(loaded_target):
            return False
        
        # Update the loaded row
        loaded_target[iRow][attributeName] = value
        
        # For status updates, we need to ensure the row gets written to DB
        if catName == "pdbx_deposition_message_status":
            # Check if this status is already in pending list
            msg_id = loaded_target[iRow]["message_id"]
            found = False
            for pending_row in pending_target:
                if pending_row["message_id"] == msg_id:
                    # Update existing pending row
                    pending_row[attributeName] = value
                    found = True
                    break
            
            if not found:
                # Add updated loaded row to pending for write
                pending_target.append(dict(loaded_target[iRow]))
        else:
            # For other categories, just update pending if it exists
            if iRow < len(pending_target):
                pending_target[iRow][attributeName] = value
        
        return True

    def appendMessage(self, rowAttribDict: Dict) -> bool:
        self._ensure_defaults(rowAttribDict, kind="message")
        self._pending_messages.append(dict(rowAttribDict))
        return True

    def appendFileReference(self, rowAttribDict: Dict) -> bool:
        self._ensure_defaults(rowAttribDict, kind="file_ref")
        self._pending_file_refs.append(dict(rowAttribDict))
        return True

    def appendOrigCommReference(self, rowAttribDict: Dict) -> bool:
        # In-memory only; not persisted (no table)
        self._pending_origcomm_refs.append(dict(rowAttribDict))
        return True

    def appendMsgReadStatus(self, rowAttribDict: Dict) -> bool:
        self._ensure_defaults(rowAttribDict, kind="status")
        self._pending_statuses.append(dict(rowAttribDict))
        return True

    def write(self, filePath: str) -> bool:
        """Commit pending rows to DB. filePath is ignored (retained for API compatibility)."""
        if self.__verbose:
            logger.info("DB MessageIo write() pending: msgs=%d files=%d statuses=%d",
                        len(self._pending_messages), len(self._pending_file_refs), len(self._pending_statuses))
        
        success = True
        
        # Messages
        for m in self._pending_messages:
            if self.__verbose:
                logger.info("Processing message data: %s", m)
            
            msg = ORMMessageInfo(
                message_id=m["message_id"],
                deposition_data_set_id=m.get("deposition_data_set_id", self._deposition_id),
                timestamp=_parse_ts(m.get("timestamp")),
                sender=m.get("sender", ""),
                context_type=m.get("context_type"),
                context_value=m.get("context_value"),
                parent_message_id=m.get("parent_message_id"),
                message_subject=m.get("message_subject", ""),
                message_text=m.get("message_text", ""),
                message_type=m.get("message_type", "text"),
                send_status=m.get("send_status", "Y"),
                content_type=m.get("content_type", self._content_type),
            )
            
            if self.__verbose:
                logger.info("Created ORM object: message_id=%s, deposition_data_set_id=%s, timestamp=%s",
                          msg.message_id, msg.deposition_data_set_id, msg.timestamp)
            
            if not self._dal.create_message(msg):
                logger.error("Failed to create message with ID: %s", m['message_id'])
                success = False
            else:
                if self.__verbose:
                    logger.info("Successfully created message with ID: %s", m['message_id'])

        # File references
        for fr in self._pending_file_refs:
            ref = ORMFileRef(
                message_id=fr["message_id"],
                deposition_data_set_id=fr.get("deposition_data_set_id", self._deposition_id),
                content_type=fr.get("content_type", self._content_type),
                content_format=fr.get("content_format", ""),
                partition_number=int(fr.get("partition_number", 1)),
                version_id=int(fr.get("version_id", 1)),
                storage_type=fr.get("storage_type", "archive"),
                upload_file_name=fr.get("upload_file_name"),
            )
            if not self._dal.create_file_reference(ref):
                logger.error("Failed to create file reference for message ID: %s", fr['message_id'])
                success = False

        # Status
        for st in self._pending_statuses:
            status = ORMStatus(
                message_id=st["message_id"],
                deposition_data_set_id=st.get("deposition_data_set_id", self._deposition_id),
                read_status=st.get("read_status", "N"),
                action_reqd=st.get("action_reqd", "N"),
                for_release=st.get("for_release", "N"),
            )
            if not self._dal.create_or_update_status(status):
                logger.error("Failed to create/update status for message ID: %s", st['message_id'])
                success = False

        # merge in-memory origcomm refs (not persisted)
        self._loaded_origcomm_refs.extend(self._pending_origcomm_refs)

        # clear pendings and refresh loaded view only if all operations succeeded
        if success:
            self._pending_messages.clear()
            self._pending_file_refs.clear()
            self._pending_statuses.clear()
            self._pending_origcomm_refs.clear()
            self._load_from_db()
        else:
            logger.error("Database write operations failed - keeping pending data for potential retry")
        
        return success

    # --------- Style/container compatibility (no-ops for DB) ---------

    def complyStyle(self) -> bool:
        return True

    def setBlock(self, blockId: str) -> bool:
        self._block_id = blockId
        return True

    def newBlock(self, blockId: str) -> None:
        self._block_id = blockId

    # --------- Ordinal helpers (kept for API compatibility) ---------

    def nextMessageOrdinal(self) -> int:
        return len(self._loaded_messages) + len(self._pending_messages) + 1

    def nextFileReferenceOrdinal(self) -> int:
        return len(self._loaded_file_refs) + len(self._pending_file_refs) + 1

    def nextOrigCommReferenceOrdinal(self) -> int:
        return len(self._loaded_origcomm_refs) + len(self._pending_origcomm_refs) + 1

    # --------- Internal helpers ---------

    def _ensure_defaults(self, row: Dict, kind: str) -> None:
        if kind in ("message", "file_ref", "status"):
            row.setdefault("deposition_data_set_id", self._deposition_id)
            if self._content_type and kind in ("message", "file_ref"):
                row.setdefault("content_type", self._content_type)

    def _load_from_db(self) -> None:
        """Load current context from DB into dict lists compatible with CIF backend."""
        self._loaded_messages.clear()
        self._loaded_file_refs.clear()
        self._loaded_statuses.clear()

        if self._deposition_id:
            # Load messages for specific deposition (optionally filter content_type)
            msgs = self._dal.get_deposition_messages(self._deposition_id)
            if self.__verbose:
                logger.info("DB _load_from_db: Found %d total messages for deposition %s", len(msgs), self._deposition_id)
                for m in msgs:
                    logger.info("  Message %s: content_type=%s, subject=%s", m.message_id, m.content_type, m.message_subject)
        else:
            # No deposition ID available - this is an error condition
            if self.__verbose:
                logger.error("DB _load_from_db: No deposition_id specified - cannot load messages without deposition context")
            return  # Return with empty loaded_messages
        
        # Only filter by content_type if explicitly set (not empty/None)
        if self._content_type:
            msgs = [m for m in msgs if m.content_type == self._content_type]
            if self.__verbose:
                logger.info("DB _load_from_db: After filtering by content_type=%s, found %d messages", self._content_type, len(msgs))
        else:
            if self.__verbose:
                logger.info("DB _load_from_db: No content_type filter, returning all %d messages for deposition", len(msgs))

        self._loaded_messages = [
            {
                "ordinal_id": m.ordinal_id,
                "message_id": m.message_id,
                "deposition_data_set_id": m.deposition_data_set_id,
                "timestamp": _fmt_ts(m.timestamp),
                "sender": m.sender,
                "context_type": m.context_type,
                "context_value": m.context_value,
                "parent_message_id": m.parent_message_id,
                "message_subject": m.message_subject,
                "message_text": m.message_text,
                "message_type": m.message_type,
                "send_status": m.send_status,
                "content_type": m.content_type,
            }
            for m in msgs
        ]

        # File references for deposition (+ content type if set)
        with self._dal.db_connection.get_session() as sess:
            q = sess.query(ORMFileRef).filter(ORMFileRef.deposition_data_set_id == self._deposition_id)
            # Remove content_type filtering for file references since they use different content types
            # than messages (e.g., 'auxiliary-file-annotate' vs 'messages-to-depositor')
            file_refs = q.all()
        
        self._loaded_file_refs = [
            {
                "ordinal_id": fr.ordinal_id,
                "message_id": fr.message_id,
                "deposition_data_set_id": fr.deposition_data_set_id,
                "content_type": fr.content_type,
                "content_format": fr.content_format,
                "partition_number": fr.partition_number,
                "version_id": fr.version_id,
                "storage_type": fr.storage_type,
                "upload_file_name": fr.upload_file_name,
            }
            for fr in file_refs
        ]

        # Status for all messages in this deposition
        msg_ids = [m["message_id"] for m in self._loaded_messages]
        with self._dal.db_connection.get_session() as sess:
            q = sess.query(ORMStatus).filter(ORMStatus.deposition_data_set_id == self._deposition_id)
            if msg_ids:
                q = q.filter(ORMStatus.message_id.in_(msg_ids))
            statuses = q.all()
        
        self._loaded_statuses = [
            {
                "message_id": st.message_id,
                "deposition_data_set_id": st.deposition_data_set_id,
                "read_status": st.read_status,
                "action_reqd": st.action_reqd,
                "for_release": st.for_release,
            }
            for st in statuses
        ]
