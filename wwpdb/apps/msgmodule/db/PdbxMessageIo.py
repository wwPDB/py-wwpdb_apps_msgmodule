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
    """Extract deposition ID and content type from file path.

    Handles multiple path formats for backward compatibility with both workflow-mode
    dummy paths and legacy hardcoded file paths. Uses PathInfo.splitFileName for
    standardized parsing.

    Args:
        file_path: File path to parse. Supported formats:
            - Workflow dummy: /dummy/messaging/D_1000000001/D_1000000001_messages-to-depositor_P1.cif.V1
            - Legacy hardcoded: /net/wwpdb_da/.../messages-to-depositor.cif

    Returns:
        Tuple of (deposition_id, content_type). Either may be None if not parseable.
        Valid content types: messages-from-depositor, messages-to-depositor, notes-from-annotator

    Note:
        For database-backed operation, this provides the filtering context for queries.
        Legacy paths without deposition IDs will filter by content type only.
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
    """Format timestamp to string representation.

    Args:
        ts: Timestamp as datetime object or string

    Returns:
        Formatted timestamp string in "%Y-%m-%d %H:%M:%S" format, or empty string if invalid
    """
    if isinstance(ts, datetime):
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(ts, str):
        return ts
    return ""


def _parse_ts(ts_str: Optional[str]) -> datetime:
    """Parse timestamp string to datetime object.

    Attempts multiple common timestamp formats. Returns current UTC time if parsing fails.

    Args:
        ts_str: Timestamp string or datetime object

    Returns:
        Parsed datetime object, or current UTC time if parsing fails

    Note:
        Supported formats: "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%b-%Y %H:%M:%S", "%d-%b-%Y"
    """
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
    """Database-backed drop-in replacement for mmcif_utils.message.PdbxMessageIo.

    Provides the same API interface as the original PdbxMessageIo class but stores
    message data in a MySQL database instead of CIF files. Maintains full backward
    compatibility with existing code that uses the CIF-based interface.

    The class operates on a context-based model where read() selects a deposition ID
    and content type, and all subsequent operations (get*/append*) work within that
    context. The write() method commits pending changes to the database.

    Args:
        site_id: WWPDB site identifier (required). Used to load database configuration
            from ConfigInfo settings (SITE_MESSAGE_DB_HOST_NAME, etc.)
        verbose: Enable verbose logging output (default: True)
        log: File handle for log output (default: sys.stderr)
        db_config: Optional explicit database configuration dict. If not provided,
            configuration is loaded from ConfigInfo using site_id. Dict should contain:
            host, port, database, username, password, charset, and optionally unix_socket.

    Raises:
        ValueError: If site_id is not provided or database configuration is invalid

    Example:
        >>> with PdbxMessageIo(site_id="WWPDB_DEPLOY_TEST") as msg_io:
        ...     msg_io.read("/dummy/messaging/D_1000000001/D_1000000001_messages-to-depositor_P1.cif.V1")
        ...     messages = msg_io.getMessageInfo()
        ...     msg_io.appendMessage({"message_id": "msg_001", "message_text": "Hello"})
        ...     msg_io.write("/dummy/path")

    Note:
        This class should be used as a context manager (with statement) to ensure
        proper cleanup of database connections. Alternatively, call close() explicitly.
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

    def close(self):
        """Close database connections and clean up resources.

        Should be called when done with this instance to ensure proper cleanup.
        Automatically called when using context manager (with statement).
        """
        if self._dal:
            self._dal.close()
            if self.__verbose:
                self.__lfh.write("PdbxMessageIo: Database connections closed\n")

    def __del__(self):
        """Destructor - ensures database connections are closed when object is garbage collected."""
        try:
            self.close()
        except Exception:
            pass  # Ignore errors during cleanup

    def __enter__(self):
        """Context manager entry - allows usage with 'with' statement.

        Returns:
            self: This PdbxMessageIo instance
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures connections are closed.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred

        Returns:
            False to propagate any exceptions that occurred
        """
        self.close()
        return False  # Don't suppress exceptions

    # --------- Query (read) API ---------

    def read(self, filePath: str, logtag: str = "", deposition_id: str = None) -> bool:
        """Select deposition context and load existing messages from database.

        Sets the current deposition ID and content type context by parsing the file path,
        then loads all matching messages, file references, and status records from the
        database. Subsequent get* and append* operations work within this context.

        Args:
            filePath: File path to parse for context. Supported formats:
                - Workflow dummy path: /dummy/messaging/D_1000000001/D_1000000001_messages-to-depositor_P1.cif.V1
                - Legacy hardcoded path: /net/wwpdb_da/.../messages-to-depositor.cif
            logtag: Optional logging tag for debugging (default: "")
            deposition_id: Optional explicit deposition ID that overrides path parsing (default: None)

        Returns:
            True on success, False on failure

        Note:
            The file path is used only for context parsing - no actual file I/O occurs.
            All data is loaded from the database based on the parsed deposition ID and content type.
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
        """Get all rows for a specific mmCIF category.

        Provides unified access to different message data categories using mmCIF category names.

        Args:
            catName: Category name. Valid values:
                - "pdbx_deposition_message_info" (default): Message data
                - "pdbx_deposition_message_file_reference": File attachment metadata
                - "pdbx_deposition_message_origcomm_reference": Original communication references
                - "pdbx_deposition_message_status": Message status flags

        Returns:
            List of dictionaries containing category data, or empty list if category unknown
        """
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
        """Get all message info rows loaded from database for current context.

        Returns:
            List of message dictionaries with keys: ordinal_id, message_id, deposition_data_set_id,
            timestamp, sender, context_type, context_value, parent_message_id, message_subject,
            message_text, message_type, send_status, content_type
        """
        return list(self._loaded_messages)

    def getFileReferenceInfo(self) -> List[Dict]:
        """Get all file reference rows loaded from database for current context.

        Returns:
            List of file reference dictionaries with keys: ordinal_id, message_id,
            deposition_data_set_id, content_type, content_format, partition_number,
            version_id, storage_type, upload_file_name
        """
        return list(self._loaded_file_refs)

    def getOrigCommReferenceInfo(self) -> List[Dict]:
        """Get original communication reference data (in-memory only, not persisted to database).

        Returns:
            List of original communication reference dictionaries. Note: This data is maintained
            in-memory for API compatibility but is not persisted to the database in the current schema.
        """
        return list(self._loaded_origcomm_refs)

    def getMsgStatusInfo(self) -> List[Dict]:
        """Get all message status rows loaded from database for current context.

        Returns:
            List of status dictionaries with keys: message_id, deposition_data_set_id,
            read_status, action_reqd, for_release
        """
        return list(self._loaded_statuses)

    # --------- Mutation API (append/update) ---------

    def update(self, catName: str, attributeName: str, value, iRow: int = 0) -> bool:
        """Update an attribute in a loaded row and mark for database write.

        Updates the specified attribute in the loaded data and ensures the change
        is tracked for writing to the database on the next write() call.

        Args:
            catName: Category name to update. Valid values:
                - "pdbx_deposition_message_info"
                - "pdbx_deposition_message_file_reference"
                - "pdbx_deposition_message_status"
                - "pdbx_deposition_message_origcomm_reference"
            attributeName: Name of the attribute/column to update
            value: New value to set
            iRow: Row index to update (default: 0)

        Returns:
            True on success, False if category unknown or row index invalid

        Note:
            For status updates, automatically ensures the row is added to pending
            writes even if it wasn't originally in the pending list.
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
        """Append a new message to pending writes.

        Args:
            rowAttribDict: Dictionary of message attributes. Should include at minimum:
                message_id, message_subject, message_text. Other fields are auto-populated
                with defaults if not provided.

        Returns:
            True on success

        Note:
            Message is added to pending list and will be written to database on next write() call.
        """
        self._ensure_defaults(rowAttribDict, kind="message")
        self._pending_messages.append(dict(rowAttribDict))
        return True

    def appendFileReference(self, rowAttribDict: Dict) -> bool:
        """Append a new file reference to pending writes.

        Args:
            rowAttribDict: Dictionary of file reference attributes. Should include:
                message_id, and optionally content_type, content_format, partition_number,
                version_id, storage_type, upload_file_name

        Returns:
            True on success
        """
        self._ensure_defaults(rowAttribDict, kind="file_ref")
        self._pending_file_refs.append(dict(rowAttribDict))
        return True

    def appendOrigCommReference(self, rowAttribDict: Dict) -> bool:
        """Append original communication reference (in-memory only, not persisted).

        Args:
            rowAttribDict: Dictionary of original communication reference attributes

        Returns:
            True on success

        Note:
            This data is maintained in-memory for API compatibility but is not
            persisted to the database in the current schema.
        """
        self._pending_origcomm_refs.append(dict(rowAttribDict))
        return True

    def appendMsgReadStatus(self, rowAttribDict: Dict) -> bool:
        """Append a new message status to pending writes.

        Args:
            rowAttribDict: Dictionary of status attributes. Should include message_id,
                and optionally read_status, action_reqd, for_release (default: "N" for each)

        Returns:
            True on success
        """
        self._ensure_defaults(rowAttribDict, kind="status")
        self._pending_statuses.append(dict(rowAttribDict))
        return True

    def write(self, filePath: str) -> bool:  # pylint: disable=unused-argument
        """Commit all pending changes to the database.

        Writes all pending messages, file references, and status records to the database
        using the DataAccessLayer. On success, clears pending lists and reloads data
        from database to refresh the current context.

        Args:
            filePath: File path (retained for API compatibility but ignored - no file I/O occurs)

        Returns:
            True if all database operations succeeded, False if any failed

        Note:
            If any database operation fails, pending data is retained for potential retry
            and loaded data is not refreshed. Check logs for specific error details.
        """
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
                upload_file_name=fr.get("upload_file_name") or "",
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
        """Comply with mmCIF style requirements (no-op for database backend).

        Returns:
            True (always succeeds - retained for API compatibility)
        """
        return True

    def setBlock(self, blockId: str) -> bool:
        """Set current block ID (no-op for database backend, retained for API compatibility).

        Args:
            blockId: Block identifier

        Returns:
            True (always succeeds)
        """
        self._block_id = blockId
        return True

    def newBlock(self, blockId: str) -> None:
        """Create new block (no-op for database backend, retained for API compatibility).

        Args:
            blockId: Block identifier
        """
        self._block_id = blockId

    # --------- Ordinal helpers (kept for API compatibility) ---------

    def nextMessageOrdinal(self) -> int:
        """Get next available ordinal ID for a message.

        Returns:
            Next ordinal ID (count of loaded + pending messages + 1)
        """
        return len(self._loaded_messages) + len(self._pending_messages) + 1

    def nextFileReferenceOrdinal(self) -> int:
        """Get next available ordinal ID for a file reference.

        Returns:
            Next ordinal ID (count of loaded + pending file references + 1)
        """
        return len(self._loaded_file_refs) + len(self._pending_file_refs) + 1

    def nextOrigCommReferenceOrdinal(self) -> int:
        """Get next available ordinal ID for an original communication reference.

        Returns:
            Next ordinal ID (count of loaded + pending origcomm references + 1)
        """
        return len(self._loaded_origcomm_refs) + len(self._pending_origcomm_refs) + 1

    # --------- Internal helpers ---------

    def _ensure_defaults(self, row: Dict, kind: str) -> None:
        """Ensure default values are set for required fields.

        Auto-populates deposition_data_set_id and content_type from current context
        if not already present in the row dictionary.

        Args:
            row: Dictionary to populate with defaults (modified in-place)
            kind: Type of row - "message", "file_ref", or "status"
        """
        if kind in ("message", "file_ref", "status"):
            row.setdefault("deposition_data_set_id", self._deposition_id)
            if self._content_type and kind in ("message", "file_ref"):
                row.setdefault("content_type", self._content_type)

    def _load_from_db(self) -> None:
        """Load message data from database for current deposition context.

        Queries the database for all messages, file references, and status records
        matching the current deposition ID and content type. Converts ORM objects
        to dictionaries compatible with the CIF backend interface.

        Note:
            Clears existing loaded data before loading. Status records are loaded for
            the entire deposition regardless of content_type filter, matching legacy
            CIF behavior where all statuses are stored in messages-to-depositor.
        """
        try:
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
                # No deposition ID available - this is a FATAL error condition
                error_msg = (
                    f"DB _load_from_db: FATAL - No deposition_id specified (content_type={self._content_type}). "
                    "Cannot load messages without deposition context. This typically means the file path "
                    "could not be parsed or read() was called without proper context."
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
        except Exception as e:
            logger.error("DB _load_from_db: FATAL - Failed to load messages for deposition %s: %s",
                         self._deposition_id, str(e), exc_info=True)
            # Re-raise so caller knows something went wrong
            raise

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
        try:
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
                    "upload_file_name": fr.upload_file_name or "",
                }
                for fr in file_refs
            ]
        except Exception:
            logger.error("DB _load_from_db: FATAL - Failed to load file references for deposition %s", self._deposition_id, exc_info=True)
            raise

        # Status for all messages in this deposition
        # NOTE: Status records are deposition-scoped, not file-scoped.
        # In legacy CIF files, all status records are stored in messages-to-depositor,
        # regardless of which file (messages-from-depositor, messages-to-depositor, notes)
        # contains the actual message. Therefore, we must NOT filter by msg_ids here,
        # as that would exclude status records for messages in other content_types.
        try:
            with self._dal.db_connection.get_session() as sess:
                q = sess.query(ORMStatus).filter(ORMStatus.deposition_data_set_id == self._deposition_id)
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
        except Exception:
            logger.error("DB _load_from_db: FATAL - Failed to load status records for deposition %s", self._deposition_id, exc_info=True)
            raise
