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

try:
    from wwpdb.utils.config.ConfigInfo import ConfigInfo
    _HAS_CONFIG_INFO = True
except ImportError:
    _HAS_CONFIG_INFO = False

from .DataAccessLayer import DataAccessLayer
from .Models import MessageInfo as ORMMessageInfo, MessageFileReference as ORMFileRef, MessageStatus as ORMStatus

logger = logging.getLogger(__name__)

_CONTENT_TYPES = (
    "messages-to-depositor",
    "messages-from-depositor", 
    "notes-from-annotator",
)

def _parse_context_from_path(file_path: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Heuristic: extract deposition id (D_* or G_*) from parent dirs and content_type from basename.
    Works with existing MessagingDataImport-derived paths.
    """
    dep_id = None
    content_type = None
    try:
        # deposition id from any path component starting with D_ or G_
        parts = os.path.normpath(file_path).split(os.sep)
        for p in reversed(parts):
            if p.startswith(("D_", "G_")):
                dep_id = p
                break
        base = os.path.basename(file_path).lower()
        for ct in _CONTENT_TYPES:
            if ct in base:
                content_type = ct
                break
    except Exception as e:
        logger.debug("Failed to parse context from path %s: %s", file_path, e)
    return dep_id, content_type

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

    def __init__(self, verbose=True, log=sys.stderr, site_id: Optional[str] = None, db_config: Optional[Dict] = None):
        self.__verbose = verbose
        self.__lfh = log
        self.__site_id = site_id or os.environ.get("WWPDB_SITE_ID")
        if not db_config:
            if not _HAS_CONFIG_INFO or not self.__site_id:
                # Use default database config for testing if ConfigInfo is not available
                db_config = {
                    "host": os.environ.get("DB_HOST", "localhost"),
                    "port": int(os.environ.get("DB_PORT", "3306")),
                    "database": os.environ.get("DB_NAME", "wwpdb_messaging"),
                    "username": os.environ.get("DB_USER", "root"),
                    "password": os.environ.get("DB_PASS", ""),
                    "charset": "utf8mb4",
                }
            else:
                cI = ConfigInfo(self.__site_id)
                db_config = {
                    "host": cI.get("SITE_DB_HOST_NAME"),
                    "port": int(cI.get("SITE_DB_PORT_NUMBER", "3306")),
                    "database": cI.get("WWPDB_MESSAGING_DB_NAME"),
                    "username": cI.get("SITE_DB_ADMIN_USER"),
                    "password": cI.get("SITE_DB_ADMIN_PASS", ""),
                    "charset": "utf8mb4",
                }
        self._dal = DataAccessLayer(db_config)

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

    def read(self, filePath: str, logtag: str = "") -> bool:
        """Select deposition_id and content_type context; load existing rows from DB."""
        dep_id, content_type = _parse_context_from_path(filePath)
        self._deposition_id = dep_id or self._deposition_id
        self._content_type = content_type or self._content_type

        if self.__verbose:
            logger.info("DB MessageIo read() dep_id=%s content_type=%s logtag=%s", self._deposition_id, self._content_type, logtag)

        if not self._deposition_id:
            logger.debug("read(): missing deposition id in path %s", filePath)
            return False
        # content_type can be absent for some paths; we will still load messages without content_type filter if needed.

        try:
            self._load_from_db()
            return True
        except Exception as e:
            logger.error("Failed DB load for dep_id=%s content_type=%s: %s", self._deposition_id, self._content_type, e)
            return False

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
        """Update attribute in pending row of the selected category."""
        target = None
        if catName == "pdbx_deposition_message_info":
            target = self._pending_messages
        elif catName == "pdbx_deposition_message_file_reference":
            target = self._pending_file_refs
        elif catName == "pdbx_deposition_message_status":
            target = self._pending_statuses
        elif catName == "pdbx_deposition_message_origcomm_reference":
            target = self._pending_origcomm_refs

        if target is None or iRow < 0 or iRow >= len(target):
            return False
        target[iRow][attributeName] = value
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
        try:
            # Messages
            for m in self._pending_messages:
                msg = ORMMessageInfo(
                    message_id=m.get("message_id"),
                    deposition_data_set_id=m.get("deposition_data_set_id") or self._deposition_id or "",
                    timestamp=_parse_ts(m.get("timestamp")),
                    sender=m.get("sender", ""),
                    context_type=m.get("context_type"),
                    context_value=m.get("context_value"),
                    parent_message_id=m.get("parent_message_id"),
                    message_subject=m.get("message_subject", ""),
                    message_text=m.get("message_text", ""),
                    message_type=m.get("message_type", "text"),
                    send_status=m.get("send_status", "Y"),
                    content_type=m.get("content_type") or self._content_type or "messages-to-depositor",
                )
                self._dal.create_message(msg)

            # File references
            for fr in self._pending_file_refs:
                ref = ORMFileRef(
                    message_id=fr.get("message_id", ""),
                    deposition_data_set_id=fr.get("deposition_data_set_id") or self._deposition_id or "",
                    content_type=fr.get("content_type") or self._content_type or "messages-to-depositor",
                    content_format=fr.get("content_format", ""),
                    partition_number=int(fr.get("partition_number", 1)) if fr.get("partition_number") is not None else 1,
                    version_id=int(fr.get("version_id", 1)) if fr.get("version_id") is not None else 1,
                    storage_type=fr.get("storage_type", "archive"),
                    upload_file_name=fr.get("upload_file_name"),
                )
                self._dal.create_file_reference(ref)

            # Status
            for st in self._pending_statuses:
                status = ORMStatus(
                    message_id=st.get("message_id", ""),
                    deposition_data_set_id=st.get("deposition_data_set_id") or self._deposition_id or "",
                    read_status=st.get("read_status", "N"),
                    action_reqd=st.get("action_reqd", "N"),
                    for_release=st.get("for_release", "N"),
                )
                self._dal.create_or_update_status(status)

            # merge in-memory origcomm refs (not persisted)
            self._loaded_origcomm_refs.extend(self._pending_origcomm_refs)

            # clear pendings and refresh loaded view
            self._pending_messages.clear()
            self._pending_file_refs.clear()
            self._pending_statuses.clear()
            self._pending_origcomm_refs.clear()
            self._load_from_db()
            return True
        except Exception as e:
            logger.error("DB write failed: %s", e)
            return False

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
            row.setdefault("deposition_data_set_id", self._deposition_id or "")
            if self._content_type and kind in ("message", "file_ref"):
                row.setdefault("content_type", self._content_type)

    def _load_from_db(self) -> None:
        """Load current context from DB into dict lists compatible with CIF backend."""
        self._loaded_messages.clear()
        self._loaded_file_refs.clear()
        self._loaded_statuses.clear()
        # Keep origcomm (in-memory only)

        if not self._deposition_id:
            return

        # Load messages for deposition (optionally filter content_type)
        msgs = self._dal.get_deposition_messages(self._deposition_id) or []
        if self._content_type:
            msgs = [m for m in msgs if m.content_type == self._content_type]

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
                if self._content_type:
                    q = q.filter(ORMFileRef.content_type == self._content_type)
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
        except Exception as e:
            logger.debug("FileRef load failed: %s", e)
            self._loaded_file_refs = []

        # Status for all messages in this deposition (no content_type column there)
        try:
            msg_ids = [m["message_id"] for m in self._loaded_messages] if self._loaded_messages else []
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
        except Exception as e:
            logger.debug("Status load failed: %s", e)
            self._loaded_statuses = []
