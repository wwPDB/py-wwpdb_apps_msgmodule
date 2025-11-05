"""
Migration utility to convert existing CIF message files to database records.

This script migrates CIF message files to the new database format.
"""

import os
import sys
import logging
import argparse
import json
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Optional

# Initialize ConfigInfo to get database configuration
try:
    from wwpdb.utils.config.ConfigInfo import ConfigInfo
    CONFIG_INFO_AVAILABLE = True
except ImportError:
    CONFIG_INFO_AVAILABLE = False
    ConfigInfo = None

# CIF parsing imports
from mmcif_utils.message.PdbxMessageIo import PdbxMessageIo

# Database imports - embed directly
import pymysql
import time

# PathInfo for file location (optional)
try:
    from wwpdb.io.locator.PathInfo import PathInfo
    PATH_INFO_AVAILABLE = True
except ImportError:
    PATH_INFO_AVAILABLE = False
    PathInfo = None

# Enhanced logging setup
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
    "parse_exception": logging.ERROR,
    "store_exception": logging.ERROR,
    "corrupted_file": logging.ERROR,
    "store_failed": logging.ERROR,
    "message_store_failed": logging.ERROR,
    "deposition_exception": logging.ERROR,
    "migration_exception": logging.ERROR,
    # warnings
    "empty_file": logging.WARNING,
    "no_messages": logging.WARNING,
    # debug/noisy
    "process_file": logging.DEBUG,
    "parse_ok": logging.DEBUG,
    # info
    "init_migrator": logging.INFO,
    "tables_created": logging.INFO,
    "db_connected": logging.INFO,
    "start_deposition": logging.INFO,
    "deposition_complete": logging.INFO,
    "start_directory": logging.INFO,
    "found_depositions": logging.INFO,
    "progress_update": logging.INFO,
    "directory_complete": logging.INFO,
    "store_success": logging.INFO,
    "store_complete": logging.INFO,
    "dry_run": logging.INFO,
    "message_duplicate": logging.INFO,
    "migration_start": logging.INFO,
    "migration_complete": logging.INFO,
    "single_migration_complete": logging.INFO,
    "bulk_migration_complete": logging.INFO,
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
    logger = logging.getLogger(__name__)
    lvl = level if level is not None else EVENT_LEVELS.get(event, logging.INFO)
    # Avoid duplicating 'message' in extra_data; it is already the log message
    extra_data = {k: v for k, v in kwargs.items() if k != 'message'}
    logger.log(lvl, kwargs.get('message', ''), extra={'event': event, 'extra_data': extra_data})

# Setup basic logging initially
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def unescape_non_ascii(text: str) -> str:
    """
    Decode Unicode escape sequences in text, including surrogate pairs for emoji.
    
    Converts \\uXXXX sequences back to actual Unicode characters.
    This is the reverse operation of escape_non_ascii() in dump_db_to_cif.py.
    
    Handles both BMP characters (\\uXXXX) and surrogate pairs (\\uD8XX\\uDCXX)
    for characters outside the Basic Multilingual Plane (like emoji).
    
    Args:
        text: String potentially containing \\uXXXX escape sequences
        
    Returns:
        String with escape sequences decoded to Unicode characters
        
    Examples:
        >>> unescape_non_ascii("caf\\u00e9")
        'café'
        >>> unescape_non_ascii("\\u4f60\\u597d")
        '你好'
        >>> unescape_non_ascii("\\ud83e\\uddec")  # Surrogate pair for 🧬
        '🧬'
    """
    if not text or '\\u' not in text:
        return text
    
    try:
        # Python's unicode-escape codec doesn't handle surrogate pairs correctly
        # We need to decode them manually
        import re
        
        def decode_match(match):
            escape_seq = match.group(0)
            try:
                # Try direct unicode-escape decoding first
                return escape_seq.encode('utf-8').decode('unicode-escape')
            except:
                return escape_seq
        
        # First pass: decode individual \uXXXX sequences
        # This will create surrogate characters that need to be combined
        result = re.sub(r'\\u[0-9a-fA-F]{4}', decode_match, text)
        
        # Second pass: encode to UTF-16, then decode back to UTF-8
        # This properly combines surrogate pairs into full Unicode characters
        try:
            # Encode as UTF-16 (which handles surrogates), then decode as UTF-8
            result = result.encode('utf-16', 'surrogatepass').decode('utf-16')
        except (UnicodeDecodeError, UnicodeEncodeError):
            # If surrogate handling fails, return the first-pass result
            pass
        
        return result
        
    except Exception as e:
        # If decoding fails, return original text
        logger.warning(f"Failed to unescape text: {e}")
        return text


# ==================== EMBEDDED DATABASE LOGIC ====================

class DatabaseConnection:
    """Simple database connection manager with retry logic"""
    
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = None
        self._connect()
    
    def _connect(self):
        """Establish database connection with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.connection = pymysql.connect(**self.db_config)
                logger.info("Database connection established")
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Connection attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to connect to database after {max_retries} attempts: {e}")
                    raise
    
    def execute_query(self, query, params=None, fetch=False):
        """Execute a SQL query with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if not self.connection.is_connected():
                    self._connect()
                
                cursor = self.connection.cursor()
                cursor.execute(query, params or ())
                
                result = None
                if fetch:
                    result = cursor.fetchall()
                else:
                    self.connection.commit()
                
                cursor.close()
                return result
                
            except Exception as e:
                error_msg = str(e)
                if "MySQL server has gone away" in error_msg or "Broken pipe" in error_msg:
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        logger.warning(f"Connection lost (attempt {attempt + 1}), retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        self._connect()
                        continue
                logger.error(f"Database query failed: {e}")
                raise
    
    def close(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Database connection closed")


def get_database_config_from_args(args):
    """Get database configuration from command line arguments"""
    if all([args.host, args.user, args.database]):
        config = {
            "host": args.host,
            "port": args.port or 3306,
            "database": args.database,
            "user": args.user,
            "password": args.password or "",
            "charset": "utf8mb4",
        }
        
        if hasattr(args, 'socket') and args.socket:
            config["unix_socket"] = args.socket
            
        logger.info("Using database configuration from command line arguments")
        config_display = dict((k, "***" if k == "password" else v) for k, v in config.items())
        logger.info(f"Database config: {config_display}")
        return config
    
    return None


class CifToDbMigrator:
    """Migrates message data from CIF files to database"""

    def __init__(self, site_id: str = None, create_tables: bool = False, db_config: Dict = None, archive_directory: str = None):
        """Initialize migrator"""
        self.site_id = site_id
        self.archive_directory = archive_directory
        
        if site_id and CONFIG_INFO_AVAILABLE:
            self.config_info = ConfigInfo(site_id)
        else:
            self.config_info = None
            if site_id and not CONFIG_INFO_AVAILABLE:
                logger.warning("ConfigInfo not available - ignoring site_id parameter")
        
        # Get database configuration
        if db_config:
            self.db_config = db_config
        else:
            self.db_config = self._get_database_config()
        log_event("init_migrator", site_id=site_id, db_host=self.db_config["host"], 
                 db_name=self.db_config["database"], create_tables=create_tables)
        
        self.db_connection = DatabaseConnection(self.db_config)
        
        # Create tables only if explicitly requested
        if create_tables:
            self._create_tables()
            log_event("tables_created", site_id=site_id)
        
        log_event("db_connected", site_id=site_id)
        
        if site_id and PATH_INFO_AVAILABLE:
            self.path_info = PathInfo(siteId=site_id)
        else:
            self.path_info = None
            if site_id and not PATH_INFO_AVAILABLE:
                logger.warning("PathInfo not available - file path resolution may be limited")
        
        self.stats = {"processed": 0, "migrated": 0, "errors": 0}

    def _create_tables(self):
        """Create database tables using raw SQL"""
        
        # Main messages table
        create_message_table = """
        CREATE TABLE IF NOT EXISTS pdbx_deposition_message_info (
            ordinal_id BIGINT PRIMARY KEY AUTO_INCREMENT,
            message_id VARCHAR(64) UNIQUE NOT NULL,
            deposition_data_set_id VARCHAR(50) NOT NULL,
            timestamp DATETIME NOT NULL,
            sender VARCHAR(150) NOT NULL,
            context_type VARCHAR(50),
            context_value VARCHAR(255),
            parent_message_id VARCHAR(64),
            message_subject TEXT NOT NULL,
            message_text LONGTEXT NOT NULL,
            message_type VARCHAR(20) DEFAULT 'text',
            send_status CHAR(1) DEFAULT 'Y',
            content_type ENUM('messages-to-depositor', 'messages-from-depositor', 'notes-from-annotator') NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            
            INDEX idx_deposition_id (deposition_data_set_id),
            INDEX idx_message_id (message_id),
            INDEX idx_timestamp (timestamp),
            INDEX idx_sender (sender),
            INDEX idx_context_type (context_type),
            INDEX idx_content_type (content_type),
            INDEX idx_created_at (created_at),
            INDEX idx_ordinal_id (ordinal_id),
            
            FOREIGN KEY (parent_message_id) REFERENCES pdbx_deposition_message_info(message_id) ON DELETE SET NULL
        ) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """
        
        # File references table
        create_file_ref_table = """
        CREATE TABLE IF NOT EXISTS pdbx_deposition_message_file_reference (
            ordinal_id BIGINT PRIMARY KEY AUTO_INCREMENT,
            message_id VARCHAR(64) NOT NULL,
            deposition_data_set_id VARCHAR(50) NOT NULL,
            content_type VARCHAR(50) NOT NULL,
            content_format VARCHAR(20) NOT NULL,
            partition_number INT DEFAULT 1,
            version_id INT DEFAULT 1,
            storage_type VARCHAR(20) DEFAULT 'archive',
            upload_file_name VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            INDEX idx_message_id (message_id),
            INDEX idx_deposition_id (deposition_data_set_id),
            INDEX idx_content_type (content_type),
            INDEX idx_storage_type (storage_type),
            INDEX idx_ordinal_id (ordinal_id),
            
            FOREIGN KEY (message_id) REFERENCES pdbx_deposition_message_info(message_id) ON DELETE CASCADE
        ) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """
        
        # Status table
        create_status_table = """
        CREATE TABLE IF NOT EXISTS pdbx_deposition_message_status (
            message_id VARCHAR(64) PRIMARY KEY,
            deposition_data_set_id VARCHAR(50) NOT NULL,
            read_status CHAR(1) DEFAULT 'N',
            action_reqd CHAR(1) DEFAULT 'N',
            for_release CHAR(1) DEFAULT 'N',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            
            INDEX idx_deposition_id (deposition_data_set_id),
            INDEX idx_read_status (read_status),
            INDEX idx_action_reqd (action_reqd),
            INDEX idx_for_release (for_release),
            
            FOREIGN KEY (message_id) REFERENCES pdbx_deposition_message_info(message_id) ON DELETE CASCADE
        ) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """
        
        self.db_connection.execute_query(create_message_table)
        self.db_connection.execute_query(create_file_ref_table)
        self.db_connection.execute_query(create_status_table)
        logger.info("Database tables created successfully")

    def _get_database_config(self) -> Dict:
        """Get database configuration from ConfigInfo"""
        if not self.config_info:
            raise RuntimeError("ConfigInfo not available and no database configuration provided")
            
        # Try messaging-specific configuration first
        host = self.config_info.get("SITE_MESSAGE_DB_HOST_NAME")
        user = self.config_info.get("SITE_MESSAGE_DB_USER_NAME") 
        database = self.config_info.get("SITE_MESSAGE_DB_NAME")
        port = self.config_info.get("SITE_MESSAGE_DB_PORT_NUMBER", "3306")
        password = self.config_info.get("SITE_MESSAGE_DB_PASSWORD", "")
        # socket = self.config_info.get("SITE_MESSAGE_DB_SOCKET")  # Optional socket parameter

        if not all([host, user, database]):
            raise RuntimeError("Missing required database configuration")

        return {
            "host": host,
            "port": int(port),
            "database": database,
            "user": user,
            "password": password,
            "charset": "utf8mb4",
        }

    def migrate_deposition(self, deposition_id: str, dry_run: bool = False) -> bool:
        """Migrate all message files for a deposition"""
        log_event("start_deposition", deposition_id=deposition_id, dry_run=dry_run)
        
        message_types = ["messages-from-depositor", "messages-to-depositor", "notes-from-annotator"]
        success = True
        files_found = []
        files_missing = []
        deferred_statuses = []  # Collect statuses to insert after all messages
        
        for msg_type in message_types:
            file_path = self._get_file_path(deposition_id, msg_type)
            if file_path and os.path.exists(file_path):
                files_found.append({"type": msg_type, "path": file_path})
                if not self._migrate_file(file_path, msg_type, dry_run, deferred_statuses):
                    success = False
            else:
                files_missing.append({"type": msg_type, "expected_path": file_path})
        
        # Now insert all deferred statuses after all messages are in the database
        if not dry_run and success and deferred_statuses:
            for status in deferred_statuses:
                insert_query = """
                    INSERT INTO pdbx_deposition_message_status
                    (message_id, deposition_data_set_id, read_status, action_reqd, for_release)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    read_status = VALUES(read_status),
                    action_reqd = VALUES(action_reqd), 
                    for_release = VALUES(for_release),
                    updated_at = CURRENT_TIMESTAMP
                """
                
                params = (
                    status["message_id"], status["deposition_data_set_id"],
                    status["read_status"], status["action_reqd"], status["for_release"]
                )
                
                self.db_connection.execute_query(insert_query, params)
        
        log_event("deposition_complete", deposition_id=deposition_id, success=success,
                 files_found=len(files_found), files_missing=len(files_missing))
        return success

    def migrate_directory(self, directory_path: str, dry_run: bool = False) -> Dict:
        """Migrate all depositions in a directory"""
        log_event("start_directory", directory=directory_path, dry_run=dry_run)
        
        # Optimized directory scanning - avoid expensive isdir() calls on NFS
        logger.info(f"Scanning directory: {directory_path}")
        try:
            # Use os.scandir() for better performance on NFS
            with os.scandir(directory_path) as entries:
                deposition_ids = [
                    entry.name for entry in entries
                    if entry.is_dir() and entry.name.startswith('D_')
                ]
        except Exception as e:
            logger.error(f"Failed to scan directory {directory_path}: {e}")
            return {"successful": [], "failed": []}
        
        log_event("found_depositions", directory=directory_path, count=len(deposition_ids),
                 sample_ids=deposition_ids[:10])
        
        successful = []
        failed = []
        
        for i, deposition_id in enumerate(deposition_ids, 1):
            try:
                if self.migrate_deposition(deposition_id, dry_run):
                    successful.append(deposition_id)
                else:
                    failed.append(deposition_id)
                
                # Log progress every 1000 depositions
                if i % 1000 == 0:
                    log_event("progress_update", processed=i, total=len(deposition_ids),
                             successful=len(successful), failed=len(failed))
                             
            except Exception as e:
                log_event("deposition_exception", deposition_id=deposition_id, error=str(e))
                failed.append(deposition_id)
        
        log_event("directory_complete", directory=directory_path, 
                 successful=len(successful), failed=len(failed),
                 success_rate=f"{len(successful)/len(deposition_ids)*100:.1f}%" if deposition_ids else "0%")
        return {"successful": successful, "failed": failed}


    def _get_file_path(self, deposition_id: str, message_type: str) -> Optional[str]:
        """Get file path using PathInfo or fallback to direct search"""
        if self.path_info:
            try:
                return self.path_info.getFilePath(
                    dataSetId=deposition_id,
                    contentType=message_type,
                    formatType="pdbx",
                    fileSource="archive",
                    versionId="latest",
                )
            except Exception as e:
                logger.debug(f"PathInfo failed for {message_type} file for {deposition_id}: {e}")
                
        # Fallback: direct file search when PathInfo not available
        deposition_dir = os.path.join(self.archive_directory, deposition_id)
        if not os.path.exists(deposition_dir):
            logger.debug(f"Deposition directory not found: {deposition_dir}")
            return None
            
        # Search for files matching the pattern
        import glob
        pattern = f"{deposition_id}_{message_type}_P1.cif.V*"
        search_pattern = os.path.join(deposition_dir, pattern)
        
        matching_files = glob.glob(search_pattern)
        if not matching_files:
            logger.debug(f"No files found matching pattern: {search_pattern}")
            return None
            
        # Return the latest version (highest V number)
        latest_file = max(matching_files, key=lambda f: self._extract_version(f))
        logger.debug(f"Found {message_type} file: {latest_file}")
        return latest_file
        
    def _extract_version(self, file_path: str) -> int:
        """Extract version number from file path (e.g., V1, V2, etc.)"""
        import re
        match = re.search(r'\.V(\d+)(?:\.|$)', file_path)
        return int(match.group(1)) if match else 0

    def _migrate_file(self, file_path: str, message_type: str, dry_run: bool = False, deferred_statuses: List = None) -> bool:
        """Migrate a single CIF file"""
        deposition_id = os.path.basename(os.path.dirname(file_path))
        filename = os.path.basename(file_path)
        
        log_event("process_file", deposition_id=deposition_id, file_path=file_path,
                 filename=filename, message_type=message_type)
        self.stats["processed"] += 1

        try:
            # Check if file is empty first
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                log_event("empty_file", deposition_id=deposition_id, filename=filename,
                         file_path=file_path, message_type=message_type,
                         message="CIF file is empty (0 bytes)")
                return True  # Empty files are not errors
            
            # Parse CIF file
            msg_io = PdbxMessageIo(verbose=False)
            if not msg_io.read(file_path):
                log_event("corrupted_file", deposition_id=deposition_id, filename=filename,
                         file_path=file_path, message_type=message_type, file_size=file_size,
                         message="CIF file exists but cannot be parsed (possibly corrupted)")
                return False  # Corrupted files are actual errors

            # Convert data
            messages = self._convert_messages(msg_io.getMessageInfo(), message_type)
            file_refs = self._convert_file_refs(msg_io.getFileReferenceInfo())
            statuses = self._convert_statuses(msg_io.getMsgStatusInfo())

            log_event("parse_ok", deposition_id=deposition_id, filename=filename,
                     messages=len(messages), file_refs=len(file_refs), statuses=len(statuses))

            if not messages:
                log_event("no_messages", deposition_id=deposition_id, filename=filename,
                         message="File parsed successfully but contains no messages")
                return True

            # Store messages and file refs immediately, but defer statuses
            if not dry_run:
                success = self._store_data(messages, file_refs, [])  # Empty list for statuses
                if success:
                    self.stats["migrated"] += len(messages)
                    # Collect statuses to be inserted later
                    if deferred_statuses is not None:
                        deferred_statuses.extend(statuses)
                    log_event("store_success", deposition_id=deposition_id, 
                             messages_stored=len(messages))
                else:
                    self.stats["errors"] += 1
                    log_event("store_failed", deposition_id=deposition_id, filename=filename)
                    return False
            else:
                log_event("dry_run", deposition_id=deposition_id, 
                         would_migrate=len(messages))

            return True

        except Exception as e:
            log_event("parse_exception", deposition_id=deposition_id, filename=filename,
                     file_path=file_path, message_type=message_type, error=str(e))
            self.stats["errors"] += 1
            return False

    def _convert_messages(self, msg_infos: List[Dict], message_type: str) -> List[Dict]:
        """Convert CIF message data to dictionaries"""
        messages = []
        
        # Map message type to content type
        if "notes" in message_type:
            content_type = "notes-from-annotator"
        elif "from-depositor" in message_type:
            content_type = "messages-from-depositor"
        else:
            content_type = "messages-to-depositor"

        for msg_info in msg_infos:
            # Parse timestamp
            timestamp_str = msg_info.get("timestamp", "")
            timestamp = datetime.now()
            
            if timestamp_str:
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%b-%Y %H:%M:%S", "%d-%b-%Y"]:
                    try:
                        timestamp = datetime.strptime(timestamp_str, fmt)
                        break
                    except ValueError:
                        continue

            # Get message text and check size
            message_text = msg_info.get("message_text", "")
            message_text_size = len(message_text.encode('utf-8'))
            
            # Log if message is unusually large (> 1MB)
            if message_text_size > 1024 * 1024:
                logger.warning(
                    "Large message detected: %s (%.2f MB) for deposition %s",
                    msg_info.get("message_id", "unknown"),
                    message_text_size / (1024 * 1024),
                    msg_info.get("deposition_data_set_id", "unknown")
                )

            message = {
                "message_id": msg_info.get("message_id", str(uuid.uuid4())),
                "deposition_data_set_id": msg_info.get("deposition_data_set_id", ""),
                "timestamp": timestamp,
                "sender": msg_info.get("sender", ""),
                "context_type": msg_info.get("context_type"),
                "context_value": msg_info.get("context_value"),
                "parent_message_id": msg_info.get("parent_message_id"),
                "message_subject": unescape_non_ascii(msg_info.get("message_subject", "")),
                "message_text": unescape_non_ascii(message_text),
                "message_type": msg_info.get("message_type", "text"),
                "send_status": msg_info.get("send_status", "Y"),
                "content_type": content_type,
            }
            messages.append(message)
        
        return messages

    def _convert_file_refs(self, file_refs: List[Dict]) -> List[Dict]:
        """Convert file references to dictionaries"""
        converted_refs = []
        for ref in file_refs:
            converted_ref = {
                "message_id": ref.get("message_id", ""),
                "deposition_data_set_id": ref.get("deposition_data_set_id", ""),
                "content_type": ref.get("content_type", ""),
                "content_format": ref.get("content_format", ""),
                "partition_number": int(ref.get("partition_number", 1)),
                "version_id": int(ref.get("version_id", 1)),
                "storage_type": ref.get("storage_type", "archive"),
                "upload_file_name": ref.get("upload_file_name"),
            }
            converted_refs.append(converted_ref)
        return converted_refs

    def _convert_statuses(self, statuses: List[Dict]) -> List[Dict]:
        """Convert status records to dictionaries"""
        converted_statuses = []
        for status in statuses:
            converted_status = {
                "message_id": status.get("message_id", ""),
                "deposition_data_set_id": status.get("deposition_data_set_id", ""),
                "read_status": status.get("read_status", "N"),
                "action_reqd": status.get("action_reqd", "N"),
                "for_release": status.get("for_release", "N"),
            }
            converted_statuses.append(converted_status)
        return converted_statuses

    def _store_data(self, messages: List[Dict], file_refs: List[Dict], statuses: List[Dict]) -> bool:
        """Store data in database using raw SQL"""
        deposition_id = messages[0]["deposition_data_set_id"] if messages else "unknown"
        
        try:
            # Store messages
            stored_count = 0
            duplicate_count = 0
            
            for message in messages:
                # Check for duplicates
                check_query = "SELECT COUNT(*) FROM pdbx_deposition_message_info WHERE message_id = %s"
                result = self.db_connection.execute_query(check_query, (message["message_id"],), fetch=True)
                
                if result[0][0] > 0:
                    duplicate_count += 1
                    log_event("message_duplicate", deposition_id=deposition_id, 
                             message_id=message["message_id"])
                    continue
                
                # Insert message
                insert_query = """
                    INSERT INTO pdbx_deposition_message_info 
                    (message_id, deposition_data_set_id, timestamp, sender, context_type, 
                     context_value, parent_message_id, message_subject, message_text, 
                     message_type, send_status, content_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                params = (
                    message["message_id"], message["deposition_data_set_id"], message["timestamp"],
                    message["sender"], message["context_type"], message["context_value"],
                    message["parent_message_id"], message["message_subject"], message["message_text"],
                    message["message_type"], message["send_status"], message["content_type"]
                )
                
                self.db_connection.execute_query(insert_query, params)
                stored_count += 1

            # Store file references
            for file_ref in file_refs:
                insert_query = """
                    INSERT INTO pdbx_deposition_message_file_reference
                    (message_id, deposition_data_set_id, content_type, content_format,
                     partition_number, version_id, storage_type, upload_file_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                params = (
                    file_ref["message_id"], file_ref["deposition_data_set_id"], file_ref["content_type"],
                    file_ref["content_format"], file_ref["partition_number"], file_ref["version_id"],
                    file_ref["storage_type"], file_ref["upload_file_name"]
                )
                
                self.db_connection.execute_query(insert_query, params)
            
            # Store statuses
            for status in statuses:
                insert_query = """
                    INSERT INTO pdbx_deposition_message_status
                    (message_id, deposition_data_set_id, read_status, action_reqd, for_release)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    read_status = VALUES(read_status),
                    action_reqd = VALUES(action_reqd), 
                    for_release = VALUES(for_release),
                    updated_at = CURRENT_TIMESTAMP
                """
                
                params = (
                    status["message_id"], status["deposition_data_set_id"],
                    status["read_status"], status["action_reqd"], status["for_release"]
                )
                
                self.db_connection.execute_query(insert_query, params)

            log_event("store_complete", deposition_id=deposition_id,
                     messages_stored=stored_count, duplicates_skipped=duplicate_count,
                     file_refs_stored=len(file_refs), statuses_stored=len(statuses))
            return True
            
        except Exception as e:
            log_event("store_exception", deposition_id=deposition_id, error=str(e))
            return False

    def print_stats(self):
        """Print simple migration statistics"""
        print(f"Migration Summary:")
        print(f"  Files processed: {self.stats['processed']}")
        print(f"  Messages migrated: {self.stats['migrated']}")
        print(f"  Errors: {self.stats['errors']}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Migrate CIF message files to database")
    parser.add_argument("--deposition", help="Single deposition ID to migrate")
    parser.add_argument("--directory", help="Directory containing deposition subdirectories")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be migrated without writing to database")
    parser.add_argument("--site-id", help="Site ID (RCSB, PDBe, PDBj, BMRB)")
    parser.add_argument("--create-tables", action="store_true", help="Create database tables if they don't exist")
    parser.add_argument("--json-log", help="Path to JSON log file for structured logging")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Console and JSON log level")
    
    # Database connection options (for standalone operation)
    parser.add_argument("--host", help="Database host")
    parser.add_argument("--port", type=int, help="Database port (default: 3306)")
    parser.add_argument("--user", help="Database user")
    parser.add_argument("--password", help="Database password")
    parser.add_argument("--database", help="Database name")
    parser.add_argument("--socket", help="Unix socket path (optional)")

    args = parser.parse_args()

    # Setup enhanced logging
    setup_logging(args.json_log, args.log_level)

    try:
        # Get database configuration - either from command line or ConfigInfo
        db_config = get_database_config_from_args(args)
        
        if not db_config and not args.site_id:
            logger.error("Either --site-id or database connection parameters (--host, --user, --database) must be provided")
            sys.exit(1)
        
        log_event("migration_start", site_id=args.site_id, deposition=args.deposition,
                 directory=args.directory, dry_run=args.dry_run, 
                 create_tables=args.create_tables)
        
        migrator = CifToDbMigrator(args.site_id, create_tables=args.create_tables, db_config=db_config, archive_directory=args.directory)
        
        if args.deposition:
            success = migrator.migrate_deposition(args.deposition, args.dry_run)
            log_event("single_migration_complete", deposition=args.deposition, success=success)
        elif args.directory:
            results = migrator.migrate_directory(args.directory, args.dry_run)
            log_event("bulk_migration_complete", directory=args.directory, 
                     successful=len(results["successful"]), failed=len(results["failed"]))
        else:
            parser.print_help()
            return

        # Final stats
        log_event("migration_complete", stats=migrator.stats)
        migrator.print_stats()

    except Exception as e:
        log_event("migration_exception", error=str(e))
        logger.error(f"Migration failed: {e}")
        sys.exit(1)
    finally:
        try:
            migrator.db_connection.close()
        except:
            pass


if __name__ == "__main__":
    main()
