"""
Database to CIF Export Utility for Messaging System

This script exports message data from the database back to CIF format files,
serving as the reverse operation of migrate_cif_to_db.py.

Examples:
    # Export single deposition
    python dump_db_to_cif.py --deposition D_1000000001 --site-id WWPDB_DEPLOY_TEST

    # Export multiple depositions with custom output
    python dump_db_to_cif.py --depositions D_1000000001 D_1000000002 \\
        --output-dir /path/to/export --overwrite --site-id WWPDB_DEPLOY_TEST

    # Export all depositions with JSON logging
    python dump_db_to_cif.py --all --site-id WWPDB_DEPLOY_TEST \\
        --json-log export.log --log-level DEBUG

    # Export group deposition
    python dump_db_to_cif.py --deposition G_1000000001 --site-id WWPDB_DEPLOY_TEST
"""
# pylint: disable=unnecessary-pass,logging-fstring-interpolation

import os
import sys
import logging
import argparse
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any
from pathlib import Path

try:
    import gemmi
except ImportError as e:  # noqa: F841
    sys.stderr.write("Error: gemmi library is required but not installed.\n")
    sys.stderr.write("Please install it with: pip install gemmi\n")
    sys.exit(1)

# Initialize ConfigInfo to get database configuration
from wwpdb.utils.config.ConfigInfo import ConfigInfo

# Database imports
from wwpdb.apps.msgmodule.db import (
    DataAccessLayer,
    MessageInfo,
    MessageFileReference,
    MessageStatus,
)
from wwpdb.io.locator.PathInfo import PathInfo


# Custom exceptions for better error handling
class ExportError(Exception):
    """Base exception for export-related errors"""
    pass


class DatabaseConfigError(ExportError):
    """Raised when database configuration is invalid"""
    pass


class DepositionIdError(ExportError):
    """Raised when deposition ID validation fails"""
    pass


# Constants
VALID_DEPOSITION_PREFIXES = ("D_", "G_")


# Configuration constants
class DbConfigKeys:
    """Database configuration key names"""
    HOST = "SITE_MESSAGE_DB_HOST_NAME"
    USER = "SITE_MESSAGE_DB_USER_NAME"
    DATABASE = "SITE_MESSAGE_DB_NAME"
    PORT = "SITE_MESSAGE_DB_PORT_NUMBER"
    PASSWORD = "SITE_MESSAGE_DB_PASSWORD"


def validate_deposition_id(deposition_id: str) -> None:
    """
    Validate deposition ID format.

    Args:
        deposition_id: The ID to validate (e.g., 'D_1000000001' or 'G_1000000001')

    Raises:
        DepositionIdError: If validation fails
    """
    if not deposition_id:
        raise DepositionIdError("Deposition ID cannot be empty")

    if not deposition_id.startswith(VALID_DEPOSITION_PREFIXES):
        raise DepositionIdError(
            f"Deposition ID must start with one of: {', '.join(VALID_DEPOSITION_PREFIXES)}"
        )

    # Check that there's an identifier after the prefix (more than just "D_" or "G_")
    if len(deposition_id) <= 2:
        raise DepositionIdError(
            "Deposition ID must have an identifier after the prefix"
        )


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str
    port: int
    database: str
    username: str
    password: str = ""
    charset: str = "utf8mb4"

    def to_dict(self) -> Dict:
        """Convert to dictionary for DataAccessLayer"""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "username": self.username,
            "password": self.password,
            "charset": self.charset,
        }


@dataclass
class BulkExportResult:
    """Result of bulk export operations"""
    successful: List[str] = field(default_factory=list)
    failed: List[str] = field(default_factory=list)

    @property
    def total_processed(self) -> int:
        """Total number of depositions processed"""
        return len(self.successful) + len(self.failed)

    @property
    def success_rate(self) -> float:
        """Success rate as percentage"""
        if self.total_processed == 0:
            return 0.0
        return (len(self.successful) / self.total_processed) * 100


@dataclass
class ExportStatistics:
    """Track export operation statistics"""
    depositions_processed: int = 0
    files_created: int = 0
    messages_exported: int = 0
    errors: int = 0

    def print_summary(self):
        """Print statistics summary"""
        print("Export Summary:")
        print(f"  Depositions processed: {self.depositions_processed}")
        print(f"  Files created: {self.files_created}")
        print(f"  Messages exported: {self.messages_exported}")
        print(f"  Errors: {self.errors}")


# Enhanced logging setup (reused from migrate_cif_to_db.py)
class JsonFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        """Return RFC3339 timestamp with microseconds and Z suffix."""
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.isoformat(timespec="microseconds").replace("+00:00", "Z")

    def format(self, record):
        obj = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "event": getattr(record, "event", "general"),
            "message": record.getMessage(),
        }
        # Add structured data if present
        if hasattr(record, 'extra_data'):
            obj.update(record.extra_data)
        return json.dumps(obj, ensure_ascii=False)


# Map events to log levels so JSON logs reflect severity
EVENT_LEVELS = {
    # errors
    "db_connection_failed": logging.ERROR,
    "export_failed": logging.ERROR,
    "file_write_failed": logging.ERROR,
    "deposition_export_failed": logging.ERROR,
    "directory_creation_failed": logging.ERROR,
    "db_query_failed": logging.ERROR,
    "invalid_deposition_id": logging.ERROR,
    "gemmi_import_error": logging.ERROR,
    # warnings
    "no_messages_found": logging.WARNING,
    "file_exists_skip": logging.WARNING,
    "missing_dependency": logging.WARNING,
    # info
    "init_exporter": logging.INFO,
    "db_connected": logging.INFO,
    "start_deposition_export": logging.INFO,
    "deposition_export_complete": logging.INFO,
    "start_bulk_export": logging.INFO,
    "bulk_export_complete": logging.INFO,
    "file_exported": logging.INFO,
    "progress_update": logging.INFO,
    "export_summary": logging.INFO,
    "export_start": logging.INFO,
    # debug
    "process_messages": logging.DEBUG,
    "cif_structure_created": logging.DEBUG,
}


def setup_logging(json_log_file=None, log_level: str = "INFO"):
    """Setup dual logging: console for humans, JSON for parsing"""
    root_logger = logging.getLogger()
    root_logger.handlers = []  # Clear existing handlers
    lvl = getattr(logging, (log_level or "INFO").upper(), logging.INFO)
    root_logger.setLevel(lvl)

    # Console handler for humans
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(lvl)
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    root_logger.addHandler(console_handler)

    # JSON file handler for parsing
    if json_log_file:
        file_handler = logging.FileHandler(json_log_file, encoding='utf-8')
        file_handler.setLevel(lvl)
        file_handler.setFormatter(JsonFormatter())
        root_logger.addHandler(file_handler)


def log_event(event: str, level: Optional[int] = None, **kwargs):
    """Log structured events for easy querying"""
    # logger = logging.getLogger(__name__)
    lvl = level if level is not None else EVENT_LEVELS.get(event, logging.INFO)
    # Avoid duplicating 'message' in extra_data; it is already the log message
    extra_data = {k: v for k, v in kwargs.items() if k != 'message'}
    logger.log(lvl, kwargs.get('message', ''), extra={'event': event, 'extra_data': extra_data})


# Setup basic logging initially
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def escape_non_ascii(text: str, preserve_newlines: bool = False) -> str:
    """Escape non-ASCII characters to ASCII using Unicode escape notation.

    Handles characters outside BMP (>U+FFFF) by encoding as UTF-16 surrogate pairs.

    Args:
        text: Text to escape
        preserve_newlines: If True, preserve actual newlines instead of escaping them

    Returns:
        Text with non-ASCII characters escaped
    """
    if not text:
        return text

    def escape_char(c):
        """Escape a single character, handling surrogate pairs for emoji"""
        code = ord(c)
        if code < 128:
            return c
        elif code <= 0xFFFF:
            # BMP character: single \uXXXX escape
            return f'\\u{code:04x}'
        else:
            # Non-BMP character: encode as UTF-16 surrogate pair
            # Formula: code = 0x10000 + (high - 0xD800) * 0x400 + (low - 0xDC00)
            code -= 0x10000
            high = 0xD800 + (code >> 10)
            low = 0xDC00 + (code & 0x3FF)
            return f'\\u{high:04x}\\u{low:04x}'

    if preserve_newlines:
        # Split on newlines, escape each part, rejoin with actual newlines
        lines = text.split('\n')
        escaped_lines = []
        for line in lines:
            escaped = ''.join(escape_char(c) for c in line)
            escaped_lines.append(escaped)
        return '\n'.join(escaped_lines)
    else:
        # Original behavior: escape everything including newlines
        return text.encode('unicode_escape').decode('ascii')


def format_cif_value(value: Any, is_multiline: bool = False) -> str:
    """
    Format value for CIF output with proper escaping.

    Args:
        value: The value to format
        is_multiline: Whether this is a multiline text field

    Returns:
        str: Properly formatted and escaped value
    """
    if value is None:
        return "?"

    # For multiline values, preserve newlines
    escaped = escape_non_ascii(str(value), preserve_newlines=is_multiline)

    if is_multiline and "\n" in str(value):
        return f"\n;{escaped}\n;"
    elif " " in escaped or any(c in escaped for c in "'\"[]{}()"):
        return f"'{escaped}'"

    return escaped


def format_cif_loop_value(value: Any, allow_multiline: bool = False) -> str:
    """
    Format value for CIF loop.

    Args:
        value: The value to format
        allow_multiline: If True and value contains newlines, use semicolon format

    Returns:
        str: Formatted value for loop
    """
    if value is None or value == "":
        return "?"

    # Numeric values (int, float) should never be quoted
    if isinstance(value, (int, float)):
        return str(value)

    str_value = str(value)

    # Check if string value is actually a number - don't quote it
    try:
        float(str_value)
        return str_value
    except ValueError:
        pass

    # For multiline text in loops, use semicolon format to preserve newlines
    # Need newline after close
    if allow_multiline and "\n" in str_value:
        escaped = escape_non_ascii(str_value, preserve_newlines=True)
        return f"\n;{escaped}\n;\n"

    # For single-line values, escape and flatten
    escaped = escape_non_ascii(str_value, preserve_newlines=False)
    # Replace any escaped newlines with spaces (from unicode_escape)
    escaped = escaped.replace("\\n", " ").replace("\\r", " ")
    # Collapse multiple spaces into single space
    while "  " in escaped:
        escaped = escaped.replace("  ", " ")
    escaped = escaped.strip()

    # Check if we need quoting
    needs_quoting = " " in escaped or any(c in escaped for c in "'\"[]{}()")

    if not needs_quoting:
        return escaped

    # Choose quote style: use double quotes if text contains single quotes
    if "'" in escaped:
        escaped = escaped.replace('"', '\\"')
        return f'"{escaped}"'
    else:
        return f"'{escaped}'"


class DbToCifExporter:
    """
    Exports message data from database to CIF files using gemmi.

    This exporter handles both individual depositions (D_*) and group depositions (G_*),
    writing them to CIF format files using the gemmi library.
    """

    def __init__(self, site_id: str, config_info: Optional[ConfigInfo] = None):
        """
        Initialize exporter with database and path configuration.

        Args:
            site_id: Site identifier (e.g., 'WWPDB_DEPLOY_TEST', 'RCSB')
            config_info: Optional ConfigInfo instance for testing/dependency injection
        """
        self.site_id = site_id
        self.config_info = config_info or ConfigInfo(site_id)

        # Get database configuration
        db_config = self._get_database_config()
        log_event("init_exporter", site_id=site_id, db_host=db_config.host,
                  db_name=db_config.database)

        self.data_access = DataAccessLayer(db_config.to_dict())
        log_event("db_connected", site_id=site_id)

        self.path_info = PathInfo(siteId=site_id)
        self.stats = ExportStatistics()

    def _get_database_config(self) -> DatabaseConfig:
        """
        Get database configuration from ConfigInfo.

        Returns:
            DatabaseConfig: Structured database configuration

        Raises:
            DatabaseConfigError: If required configuration is missing
        """
        host = self.config_info.get(DbConfigKeys.HOST)
        user = self.config_info.get(DbConfigKeys.USER)
        database = self.config_info.get(DbConfigKeys.DATABASE)
        port = self.config_info.get(DbConfigKeys.PORT, "3306")
        password = self.config_info.get(DbConfigKeys.PASSWORD, "")

        if not all([host, user, database]):
            raise DatabaseConfigError(
                f"Missing required database configuration. "
                f"Required: {DbConfigKeys.HOST}, {DbConfigKeys.USER}, {DbConfigKeys.DATABASE}"
            )

        return DatabaseConfig(
            host=host,
            port=int(port),
            database=database,
            username=user,
            password=password
        )

    def export_deposition(self, deposition_id: str, output_dir: str = None,
                          overwrite: bool = False) -> bool:
        """
        Export all message files for a single deposition.

        Args:
            deposition_id: Deposition or group ID (e.g., 'D_1000000001', 'G_1000000001')
            output_dir: Optional custom output directory (uses PathInfo if None)
            overwrite: Whether to overwrite existing files

        Returns:
            bool: True if export succeeded or no messages found, False on error
        """
        log_event("start_deposition_export", deposition_id=deposition_id,
                  output_dir=output_dir, overwrite=overwrite)

        # Validate deposition ID format
        try:
            validate_deposition_id(deposition_id)
        except DepositionIdError as e:
            log_event("invalid_deposition_id", deposition_id=deposition_id, message=str(e))
            return False

        # Get all messages for the deposition
        try:
            messages = self.data_access.get_deposition_messages(deposition_id)
        except Exception as e:
            log_event("db_query_failed", deposition_id=deposition_id, error=str(e))
            return False

        if not messages:
            log_event("no_messages_found", deposition_id=deposition_id,
                      message="No messages found in database for deposition")
            self.stats.depositions_processed += 1
            return True  # Not an error, just no data

        # Group messages by content type
        messages_by_type = {}
        for msg in messages:
            content_type = msg.content_type
            if content_type not in messages_by_type:
                messages_by_type[content_type] = []
            messages_by_type[content_type].append(msg)

        success = True
        files_created = []

        for content_type, content_messages in messages_by_type.items():
            try:
                file_path = self._get_output_file_path(deposition_id, content_type, output_dir)

                if file_path and self._export_messages_to_cif(
                    content_messages, file_path, content_type, overwrite
                ):
                    files_created.append({"content_type": content_type, "path": file_path})
                    self.stats.files_created += 1
                    self.stats.messages_exported += len(content_messages)
                else:
                    success = False
                    self.stats.errors += 1

            except Exception as e:
                log_event("deposition_export_failed", deposition_id=deposition_id,
                          content_type=content_type, error=str(e))
                success = False
                self.stats.errors += 1

        self.stats.depositions_processed += 1
        log_event("deposition_export_complete", deposition_id=deposition_id, success=success,
                  files_created=len(files_created), messages_exported=self.stats.messages_exported)
        return success

    def export_bulk(self, depositions: List[str] = None, output_dir: str = None,
                    overwrite: bool = False) -> BulkExportResult:
        """
        Export multiple depositions or all depositions from database.

        Args:
            depositions: List of deposition IDs (None = export all)
            output_dir: Optional custom output directory
            overwrite: Whether to overwrite existing files

        Returns:
            BulkExportResult: Results of the bulk export operation
        """
        log_event("start_bulk_export", deposition_count=len(depositions) if depositions else "all",
                  output_dir=output_dir, overwrite=overwrite)

        if not depositions:
            # Get all unique deposition IDs from database
            try:
                with self.data_access.db_connection.get_session() as session:
                    result = session.query(MessageInfo.deposition_data_set_id).distinct().all()
                    depositions = [row[0] for row in result]
            except Exception as e:
                log_event("db_connection_failed", error=str(e))
                return BulkExportResult()

        result = BulkExportResult()

        for i, deposition_id in enumerate(depositions, 1):
            try:
                if self.export_deposition(deposition_id, output_dir, overwrite):
                    result.successful.append(deposition_id)
                else:
                    result.failed.append(deposition_id)

                # Log progress every 100 depositions
                if i % 100 == 0:
                    log_event("progress_update", processed=i, total=len(depositions),
                              successful=len(result.successful), failed=len(result.failed))

            except Exception as e:
                log_event("deposition_export_failed", deposition_id=deposition_id, error=str(e))
                result.failed.append(deposition_id)

        log_event("bulk_export_complete", total=result.total_processed,
                  successful=len(result.successful), failed=len(result.failed),
                  success_rate=f"{result.success_rate:.1f}%")
        return result

    def _get_output_file_path(self, deposition_id: str, content_type: str,
                              output_dir: str = None) -> Optional[str]:
        """
        Get output file path for deposition and content type using PathInfo.

        Args:
            deposition_id: Deposition or group ID
            content_type: Message content type (e.g., 'messages-to-depositor')
            output_dir: Optional custom output directory

        Returns:
            Optional[str]: File path or None on error
        """
        if output_dir:
            # Custom output directory - manually construct path
            output_path = Path(output_dir) / deposition_id
            try:
                output_path.mkdir(parents=True, exist_ok=True)
                filename = f"{deposition_id}_{content_type}_P1.cif.V1"
                return str(output_path / filename)
            except Exception as e:
                log_event("directory_creation_failed", deposition_id=deposition_id,
                          output_dir=str(output_path), error=str(e))
                return None
        else:
            # Use standard PathInfo API to get next version for writing
            try:
                return self.path_info.getFilePath(
                    dataSetId=deposition_id,
                    contentType=content_type,
                    formatType="pdbx",
                    fileSource="archive",
                    versionId="next",
                )
            except Exception as e:
                logger.debug(f"PathInfo failed for {deposition_id} {content_type}: {e}")
                return None

    def _export_messages_to_cif(self, messages: List[MessageInfo], file_path: str,
                                content_type: str, overwrite: bool = False) -> bool:
        """Export messages to CIF file using gemmi"""
        deposition_id = messages[0].deposition_data_set_id if messages else "unknown"

        # Check if file exists and overwrite setting
        if os.path.exists(file_path) and not overwrite:
            log_event("file_exists_skip", deposition_id=deposition_id,
                      file_path=file_path, content_type=content_type,
                      message="File exists and overwrite=False, skipping")
            return True

        try:
            # Create CIF document
            doc = gemmi.cif.Document()  # pylint: disable=no-member
            block = doc.add_new_block("messages")

            log_event("process_messages", deposition_id=deposition_id,
                      message_count=len(messages), content_type=content_type)

            # Add message info category
            self._add_message_info_category(block, messages)

            # Get and add file references
            file_refs = []
            statuses = []
            for message in messages:
                # Get file references for this message
                msg_file_refs = self.data_access.file_references.get_by_message_id(message.message_id)
                file_refs.extend(msg_file_refs)

                # Get status for this message
                msg_status = self.data_access.status.get_by_message_id(message.message_id)
                if msg_status:
                    statuses.append(msg_status)

            # Add file reference category if we have file references
            if file_refs:
                self._add_file_reference_category(block, file_refs)

            # Add status category only for messages-to-depositor (not messages-from-depositor)
            # to maintain compatibility with existing infrastructure
            if statuses and content_type == "messages-to-depositor":
                self._add_status_category(block, statuses)

            log_event("cif_structure_created", deposition_id=deposition_id,
                      messages=len(messages), file_refs=len(file_refs), statuses=len(statuses))

            # Ensure output directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            doc.write_file(file_path)

            log_event("file_exported", deposition_id=deposition_id, file_path=file_path,
                      content_type=content_type, messages=len(messages))
            return True

        except Exception as e:
            import traceback
            sys.stderr.write(f"\n{'='*60}\n")
            sys.stderr.write("EXCEPTION in _export_messages_to_cif:\n")
            sys.stderr.write(f"  Deposition: {deposition_id}\n")
            sys.stderr.write(f"  File: {file_path}\n")
            sys.stderr.write(f"  Error: {type(e).__name__}: {e}\n")
            sys.stderr.write(f"{'='*60}\n")
            traceback.print_exc(file=sys.stderr)
            sys.stderr.write(f"{'='*60}\n\n")
            log_event("file_write_failed", deposition_id=deposition_id,
                      file_path=file_path, content_type=content_type, error=str(e))
            return False

    def _add_message_info_category(self, block: gemmi.cif.Block, messages: List[MessageInfo]):  # pylint: disable=no-member
        """Add _pdbx_deposition_message_info category to CIF block

        Note: This implementation manually distinguishes between pairs (single item) and
        loops (multiple items). A better approach for future refactoring would be to use
        gemmi's block.find_or_add() which handles this automatically:
            table = block.find_or_add(prefix, tags)
            table.append_row(values)
        This would let gemmi decide whether to write as pairs or loops, and handle quoting
        automatically. See: https://gemmi.readthedocs.io/en/latest/cif.html#pairs-and-loops
        """
        # Define the columns we want to include
        columns = [
            "ordinal_id",
            "message_id",
            "deposition_data_set_id",
            "timestamp",
            "sender",
            "context_type",
            "context_value",
            "parent_message_id",
            "message_subject",
            "message_text",
            "message_type",
            "send_status"
        ]

        # Add loop for multiple messages or single item for one message
        if len(messages) > 1:
            loop = block.init_loop("_pdbx_deposition_message_info.", columns)

            for message in messages:
                row_values = []
                for col in columns:
                    value = self._get_message_attribute_value(message, col)
                    # Preserve newlines in message_text and message_subject
                    allow_multiline = col in ("message_text", "message_subject")
                    row_values.append(format_cif_loop_value(value, allow_multiline=allow_multiline))
                loop.add_row(row_values)
        else:
            # Single message - use item format
            message = messages[0]
            for col in columns:
                value = self._get_message_attribute_value(message, col)
                is_multiline = col in ("message_text", "message_subject")
                formatted_value = format_cif_value(value, is_multiline)
                block.set_pair(f"_pdbx_deposition_message_info.{col}", formatted_value)

    def _add_file_reference_category(self, block: gemmi.cif.Block, file_refs: List[MessageFileReference]):  # pylint: disable=no-member
        """Add _pdbx_deposition_message_file_reference category to CIF block

        Note: See _add_message_info_category() for recommended refactoring approach using
        gemmi's find_or_add() to avoid manual pair/loop branching.
        """
        columns = [
            "ordinal_id",
            "message_id",
            "deposition_data_set_id",
            "content_type",
            "content_format",
            "partition_number",
            "version_id",
            "storage_type",
            "upload_file_name"
        ]

        if len(file_refs) > 1:
            loop = block.init_loop("_pdbx_deposition_message_file_reference.", columns)

            for file_ref in file_refs:
                row_values = []
                for col in columns:
                    value = self._get_file_ref_attribute_value(file_ref, col)
                    row_values.append(format_cif_loop_value(value))
                loop.add_row(row_values)
        else:
            file_ref = file_refs[0]
            for col in columns:
                value = self._get_file_ref_attribute_value(file_ref, col)
                formatted_value = format_cif_value(value)
                block.set_pair(f"_pdbx_deposition_message_file_reference.{col}", formatted_value)

    def _add_status_category(self, block: gemmi.cif.Block, statuses: List[MessageStatus]):  # pylint: disable=no-member
        """Add _pdbx_deposition_message_status category to CIF block

        Note: See _add_message_info_category() for recommended refactoring approach using
        gemmi's find_or_add() to avoid manual pair/loop branching.
        """
        columns = [
            "message_id",
            "deposition_data_set_id",
            "read_status",
            "action_reqd",
            "for_release"
        ]

        if len(statuses) > 1:
            loop = block.init_loop("_pdbx_deposition_message_status.", columns)

            for status in statuses:
                row_values = []
                for col in columns:
                    value = self._get_status_attribute_value(status, col)
                    row_values.append(format_cif_loop_value(value))
                loop.add_row(row_values)
        else:
            status = statuses[0]
            for col in columns:
                value = self._get_status_attribute_value(status, col)
                formatted_value = format_cif_value(value)
                block.set_pair(f"_pdbx_deposition_message_status.{col}", formatted_value)

    def _get_message_attribute_value(self, message: MessageInfo, attribute: str):
        """Get attribute value from MessageInfo object"""
        if attribute == "timestamp":
            return message.timestamp.strftime("%Y-%m-%d %H:%M:%S") if message.timestamp else None
        return getattr(message, attribute, None)

    def _get_file_ref_attribute_value(self, file_ref: MessageFileReference, attribute: str):
        """Get attribute value from MessageFileReference object"""
        return getattr(file_ref, attribute, None)

    def _get_status_attribute_value(self, status: MessageStatus, attribute: str):
        """Get attribute value from MessageStatus object"""
        return getattr(status, attribute, None)

    def print_stats(self):
        """Print export statistics summary"""
        self.stats.print_summary()

    def close(self):
        """Close database connection and cleanup resources"""
        try:
            self.data_access.close()
        except Exception as e:
            logger.debug(f"Error closing data access: {e}")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensure resources are cleaned up"""
        self.close()
        return False  # Don't suppress exceptions


def create_argument_parser() -> argparse.ArgumentParser:
    """
    Create and configure the command-line argument parser.

    Returns:
        argparse.ArgumentParser: Configured argument parser
    """
    parser = argparse.ArgumentParser(
        description="Export message data from database to CIF files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export single deposition
  %(prog)s --deposition D_1000000001 --site-id WWPDB_DEPLOY_TEST

  # Export multiple depositions with custom output
  %(prog)s --depositions D_1000000001 D_1000000002 \\
      --output-dir /path/to/export --overwrite --site-id WWPDB_DEPLOY_TEST

  # Export all depositions with JSON logging
  %(prog)s --all --site-id WWPDB_DEPLOY_TEST \\
      --json-log export.log --log-level DEBUG
        """
    )

    # Mutually exclusive group for deposition selection
    deposition_group = parser.add_mutually_exclusive_group(required=True)
    deposition_group.add_argument(
        "--deposition",
        help="Single deposition ID to export (e.g., D_1000000001 or G_1000000001)"
    )
    deposition_group.add_argument(
        "--depositions",
        nargs="+",
        help="Multiple deposition IDs to export"
    )
    deposition_group.add_argument(
        "--all",
        action="store_true",
        help="Export all depositions from database"
    )

    # Output options
    parser.add_argument(
        "--output-dir",
        help="Output directory for CIF files (default: use PathInfo locations)"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing CIF files"
    )

    # Required site configuration
    parser.add_argument(
        "--site-id",
        required=True,
        help="Site ID (e.g., WWPDB_DEPLOY_TEST, RCSB, PDBe, PDBj, BMRB)"
    )

    # Logging options
    parser.add_argument(
        "--json-log",
        help="Path to JSON log file for structured logging"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Console and JSON log level (default: %(default)s)"
    )

    return parser


def main():
    """Main entry point for the export utility"""
    parser = create_argument_parser()
    args = parser.parse_args()

    # Setup enhanced logging
    setup_logging(args.json_log, args.log_level)

    try:
        log_event("export_start", site_id=args.site_id, deposition=args.deposition,
                  depositions=args.depositions, all_depositions=args.all,
                  output_dir=args.output_dir, overwrite=args.overwrite)

        with DbToCifExporter(args.site_id) as exporter:
            if args.deposition:
                success = exporter.export_deposition(args.deposition, args.output_dir, args.overwrite)
                log_event("export_summary", mode="single", success=success)
            elif args.depositions:
                results = exporter.export_bulk(args.depositions, args.output_dir, args.overwrite)
                log_event("export_summary", mode="multiple",
                          successful=len(results.successful), failed=len(results.failed))
            elif args.all:
                results = exporter.export_bulk(None, args.output_dir, args.overwrite)
                log_event("export_summary", mode="all",
                          successful=len(results.successful), failed=len(results.failed))
            else:
                parser.print_help()
                return

            # Final stats
            exporter.print_stats()

    except DatabaseConfigError as e:
        log_event("export_failed", error=str(e), error_type="database_config")
        logger.error(f"Database configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        log_event("export_failed", error=str(e), error_type="general")
        logger.error(f"Export failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
