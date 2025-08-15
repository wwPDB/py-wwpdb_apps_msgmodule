"""
Migration utility to convert existing CIF message files to database records.

This script migrates CIF message files to the new database format.
"""

import os
import sys
import logging
import argparse
from datetime import datetime
from typing import List, Dict, Optional
import uuid
import json
import traceback
import hashlib

# Initialize ConfigInfo to get database configuration
from wwpdb.utils.config.ConfigInfo import ConfigInfo

# CIF parsing imports
from mmcif_utils.message.PdbxMessageIo import PdbxMessageIo

# Database imports
from wwpdb.apps.msgmodule.db import (
    DataAccessLayer,
    MessageInfo,
    MessageFileReference,
    MessageStatus,
)
from wwpdb.io.locator.PathInfo import PathInfo

# Generate unique run ID for this migration session
RUN_ID = str(uuid.uuid4())

class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging"""
    
    def format(self, record: logging.LogRecord) -> str:
        base = {
            "ts": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S.%fZ"),
            "level": record.levelname,
            "logger": record.name,
            "run_id": RUN_ID,
            "event": getattr(record, "event", None),
            "msg": record.getMessage(),
        }
        
        # Merge structured fields passed via logger.extra
        for k, v in getattr(record, "__extra__", {}).items():
            base[k] = v
            
        # Include exception info without newlines (still one json per line)
        if record.exc_info:
            exc_type = record.exc_info[0].__name__
            exc_msg = str(record.exc_info[1])
            base["exc_type"] = exc_type
            base["exc_message"] = exc_msg
            base["exc_trace"] = "".join(traceback.format_exception(*record.exc_info)).strip()
            
        return json.dumps(base, ensure_ascii=False)


def _setup_logging(log_json_path: Optional[str] = None, level: str = "INFO"):
    """Setup dual logging: human-readable console + structured JSON file"""
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level.upper()))
    
    # Clear any existing handlers
    logger.handlers.clear()

    # Human console output
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(ch)

    # Structured JSON file output
    if log_json_path:
        fh = logging.FileHandler(log_json_path, encoding="utf-8")
        fh.setFormatter(JsonFormatter())
        logger.addHandler(fh)


def log_event(level: str, event: str, message: str = "", **fields):
    """Log structured event with consistent fields"""
    extra = {"event": event, "__extra__": fields}
    if not message:
        message = f"{event}: {fields.get('deposition_id', '')} {fields.get('filename', '')}"
    getattr(logging.getLogger(), level.lower())(message, extra=extra)


def _sha256_file(path: str, chunk_size: int = 1024*1024) -> str:
    """Calculate SHA256 hash of file for content fingerprinting"""
    h = hashlib.sha256()
    try:
        with open(path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return ""


logger = logging.getLogger(__name__)


class CifToDbMigrator:
    """Migrates message data from CIF files to database"""

    def __init__(self, site_id: str, create_tables: bool = False):
        """Initialize migrator"""
        self.site_id = site_id
        self.config_info = ConfigInfo(site_id)
        
        log_event("INFO", "init_migrator", 
                 message="Initializing migrator",
                 site_id=site_id, 
                 create_tables=create_tables)
        
        # Get database configuration
        db_config = self._get_database_config()
        self.data_access = DataAccessLayer(db_config)
        
        # Create tables only if explicitly requested
        if create_tables:
            self.data_access.create_tables()
            log_event("INFO", "tables_created", message="Database tables created/verified")
        
        log_event("INFO", "db_connected", message="Database connection established")
        
        self.path_info = PathInfo(siteId=site_id)
        self.stats = {"processed": 0, "migrated": 0, "errors": 0, "skipped": 0}

    def _get_database_config(self) -> Dict:
        """Get database configuration from ConfigInfo"""
        host = self.config_info.get("SITE_DB_HOST_NAME")
        user = self.config_info.get("SITE_DB_ADMIN_USER")
        database = self.config_info.get("WWPDB_MESSAGING_DB_NAME")
        port = self.config_info.get("SITE_DB_PORT_NUMBER", "3306")
        password = self.config_info.get("SITE_DB_ADMIN_PASS", "")
        
        if not all([host, user, database]):
            raise RuntimeError("Missing required database configuration")

        config = {
            "host": host,
            "port": int(port),
            "database": database,
            "username": user,
            "password": password,
            "charset": "utf8mb4",
        }
        
        log_event("INFO", "db_config", 
                 message="Database configuration loaded",
                 host=host, 
                 database=database, 
                 port=port,
                 user=user)
        
        return config

    def migrate_deposition(self, deposition_id: str, dry_run: bool = False) -> bool:
        """Migrate all message files for a deposition"""
        log_event("INFO", "start_deposition", 
                 message=f"Starting deposition migration",
                 deposition_id=deposition_id, 
                 dry_run=dry_run)
        
        message_types = ["messages-from-depositor", "messages-to-depositor", "notes-from-annotator"]
        success = True
        files_found = 0
        
        for msg_type in message_types:
            file_path = self._get_file_path(deposition_id, msg_type)
            if file_path and os.path.exists(file_path):
                files_found += 1
                if not self._migrate_file(file_path, msg_type, dry_run):
                    success = False
            else:
                log_event("DEBUG", "file_not_found",
                         message=f"No {msg_type} file found",
                         deposition_id=deposition_id,
                         message_type=msg_type,
                         expected_path=file_path)
        
        log_event("INFO", "finish_deposition",
                 message=f"Completed deposition migration",
                 deposition_id=deposition_id,
                 success=success,
                 files_found=files_found)
        
        return success

    def migrate_directory(self, directory_path: str, dry_run: bool = False) -> Dict:
        """Migrate all depositions in a directory"""
        log_event("INFO", "start_directory", 
                 message=f"Starting directory scan",
                 directory_path=directory_path)
        
        deposition_ids = [
            item for item in os.listdir(directory_path)
            if os.path.isdir(os.path.join(directory_path, item)) and item.startswith('D_')
        ]
        
        log_event("INFO", "directory_scanned",
                 message=f"Found depositions to process",
                 directory_path=directory_path,
                 deposition_count=len(deposition_ids))
        
        successful = []
        failed = []
        
        for i, deposition_id in enumerate(deposition_ids, 1):
            try:
                log_event("INFO", "deposition_progress",
                         message=f"Processing deposition {i}/{len(deposition_ids)}",
                         deposition_id=deposition_id,
                         progress=f"{i}/{len(deposition_ids)}")
                
                if self.migrate_deposition(deposition_id, dry_run):
                    successful.append(deposition_id)
                else:
                    failed.append(deposition_id)
            except Exception as e:
                log_event("ERROR", "deposition_exception",
                         message=f"Exception processing deposition",
                         deposition_id=deposition_id,
                         error_type=type(e).__name__,
                         error_message=str(e),
                         exc_info=True)
                failed.append(deposition_id)
        
        log_event("INFO", "finish_directory",
                 message="Directory migration completed",
                 directory_path=directory_path,
                 successful_count=len(successful),
                 failed_count=len(failed),
                 total_count=len(deposition_ids))
        
        return {"successful": successful, "failed": failed}

    def _get_file_path(self, deposition_id: str, message_type: str) -> Optional[str]:
        """Get file path using PathInfo"""
        try:
            return self.path_info.getFilePath(
                dataSetId=deposition_id,
                contentType=message_type,
                formatType="pdbx",
                fileSource="archive",
                versionId="latest",
            )
        except Exception as e:
            log_event("DEBUG", "path_lookup_fail",
                     message="Failed to get file path",
                     deposition_id=deposition_id,
                     message_type=message_type,
                     error_message=str(e))
            return None

    def _migrate_file(self, file_path: str, message_type: str, dry_run: bool = False) -> bool:
        """Migrate a single CIF file"""
        filename = os.path.basename(file_path)
        deposition_id = os.path.basename(os.path.dirname(file_path))
        file_sha256 = _sha256_file(file_path)
        
        log_event("INFO", "process_file",
                 message=f"Processing CIF file",
                 file_path=file_path,
                 filename=filename,
                 deposition_id=deposition_id,
                 message_type=message_type,
                 file_sha256=file_sha256)
        
        self.stats["processed"] += 1

        try:
            # Parse CIF file
            msg_io = PdbxMessageIo(verbose=False)
            if not msg_io.read(file_path):
                log_event("ERROR", "read_fail",
                         message="Failed to read CIF file",
                         file_path=file_path,
                         filename=filename,
                         deposition_id=deposition_id,
                         reason="mmCIF read returned False")
                self.stats["errors"] += 1
                return False

            # Convert data
            messages = self._convert_messages(msg_io.getMessageInfo(), message_type)
            file_refs = self._convert_file_refs(msg_io.getFileReferenceInfo())
            statuses = self._convert_statuses(msg_io.getMsgStatusInfo())

            if not messages:
                log_event("WARNING", "no_messages",
                         message="No messages found in file",
                         file_path=file_path,
                         filename=filename,
                         deposition_id=deposition_id)
                self.stats["skipped"] += 1
                return True

            log_event("INFO", "file_parsed",
                     message="File parsed successfully",
                     filename=filename,
                     deposition_id=deposition_id,
                     message_count=len(messages),
                     file_ref_count=len(file_refs),
                     status_count=len(statuses))

            # Store in database
            if not dry_run:
                success = self._store_data(messages, file_refs, statuses)
                if success:
                    self.stats["migrated"] += len(messages)
                    log_event("INFO", "file_migrated",
                             message="File migration successful",
                             filename=filename,
                             deposition_id=deposition_id,
                             migrated_messages=len(messages))
                else:
                    self.stats["errors"] += 1
                    return False
            else:
                log_event("INFO", "dry_run_file",
                         message="Dry run - would migrate messages",
                         filename=filename,
                         deposition_id=deposition_id,
                         would_migrate=len(messages))

            return True

        except Exception as e:
            log_event("ERROR", "file_exception",
                     message="Exception processing file",
                     file_path=file_path,
                     filename=filename,
                     deposition_id=deposition_id,
                     error_type=type(e).__name__,
                     error_message=str(e),
                     exc_info=True)
            self.stats["errors"] += 1
            return False

    def _convert_messages(self, msg_infos: List[Dict], message_type: str) -> List[MessageInfo]:
        """Convert CIF message data to MessageInfo objects"""
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

            message_id = msg_info.get("message_id", str(uuid.uuid4()))
            parent_message_id = msg_info.get("parent_message_id")
            deposition_id = msg_info.get("deposition_data_set_id", "")
            
            # Truncate long text for logging (but not for DB)
            message_text = msg_info.get("message_text", "")
            logged_message_text = message_text[:256] + "..." if len(message_text) > 256 else message_text
            
            log_event("DEBUG", "convert_message",
                     message="Converting message",
                     message_id=message_id,
                     parent_message_id=parent_message_id,
                     deposition_id=deposition_id,
                     content_type=content_type,
                     sender=msg_info.get("sender", ""),
                     logged_message_text=logged_message_text,
                     timestamp=timestamp.isoformat())

            message = MessageInfo(
                message_id=message_id,
                deposition_data_set_id=deposition_id,
                timestamp=timestamp,
                sender=msg_info.get("sender", ""),
                context_type=msg_info.get("context_type"),
                context_value=msg_info.get("context_value"),
                parent_message_id=parent_message_id,
                message_subject=msg_info.get("message_subject", "").encode('utf-8'),
                message_text=message_text.encode('utf-8'),
                message_type=msg_info.get("message_type", "text"),
                send_status=msg_info.get("send_status", "Y"),
                content_type=content_type,
            )
            messages.append(message)
        
        return messages

    def _convert_file_refs(self, file_refs: List[Dict]) -> List[MessageFileReference]:
        """Convert file references"""
        return [
            MessageFileReference(
                message_id=ref.get("message_id", ""),
                deposition_data_set_id=ref.get("deposition_data_set_id", ""),
                content_type=ref.get("content_type", ""),
                content_format=ref.get("content_format", ""),
                partition_number=int(ref.get("partition_number", 1)),
                version_id=int(ref.get("version_id", 1)),
                storage_type=ref.get("storage_type", "archive"),
                upload_file_name=ref.get("upload_file_name"),
            )
            for ref in file_refs
        ]

    def _convert_statuses(self, statuses: List[Dict]) -> List[MessageStatus]:
        """Convert status records"""
        return [
            MessageStatus(
                message_id=status.get("message_id", ""),
                deposition_data_set_id=status.get("deposition_data_set_id", ""),
                read_status=status.get("read_status", "N"),
                action_reqd=status.get("action_reqd", "N"),
                for_release=status.get("for_release", "N"),
            )
            for status in statuses
        ]

    def _sort_messages_by_dependencies(self, messages: List[MessageInfo]) -> List[MessageInfo]:
        """Sort messages to ensure parents are inserted before children"""
        # Create a map of message_id -> message for easy lookup
        message_map = {msg.message_id: msg for msg in messages}
        
        # Track visited and currently processing nodes for cycle detection
        visited = set()
        processing = set()
        sorted_messages = []
        
        def visit(message: MessageInfo):
            if message.message_id in processing:
                # Cycle detected - log warning and skip dependency
                log_event("WARNING", "circular_dependency",
                         message="Circular dependency detected",
                         message_id=message.message_id,
                         parent_message_id=message.parent_message_id,
                         deposition_id=message.deposition_data_set_id)
                return
            
            if message.message_id in visited:
                return
            
            processing.add(message.message_id)
            
            # If this message has a parent, visit the parent first
            if message.parent_message_id and message.parent_message_id in message_map:
                parent = message_map[message.parent_message_id]
                visit(parent)
            
            processing.remove(message.message_id)
            visited.add(message.message_id)
            sorted_messages.append(message)
        
        # Visit all messages
        for message in messages:
            visit(message)
        
        log_event("DEBUG", "messages_sorted",
                 message="Messages sorted by dependencies",
                 original_count=len(messages),
                 sorted_count=len(sorted_messages))
        
        return sorted_messages

    def _store_data(self, messages: List[MessageInfo], file_refs: List[MessageFileReference], 
                    statuses: List[MessageStatus]) -> bool:
        """Store data in database with proper dependency ordering"""
        try:
            # Sort messages to ensure parents are inserted before children
            sorted_messages = self._sort_messages_by_dependencies(messages)
            
            # Store messages in dependency order
            for message in sorted_messages:
                existing = self.data_access.get_message_by_id(message.message_id)
                if existing:
                    log_event("DEBUG", "message_exists",
                             message="Message already exists in database",
                             message_id=message.message_id,
                             parent_message_id=message.parent_message_id,
                             deposition_id=message.deposition_data_set_id)
                    continue
                    
                if not self.data_access.create_message(message):
                    log_event("ERROR", "insert_fail",
                             message="Failed to insert message",
                             message_id=message.message_id,
                             parent_message_id=message.parent_message_id,
                             deposition_id=message.deposition_data_set_id,
                             content_type=message.content_type)
                    return False
                else:
                    log_event("INFO", "insert_ok",
                             message="Message inserted successfully",
                             message_id=message.message_id,
                             parent_message_id=message.parent_message_id,
                             deposition_id=message.deposition_data_set_id,
                             content_type=message.content_type)

            # Store file references and statuses
            for file_ref in file_refs:
                self.data_access.create_file_reference(file_ref)
            
            for status in statuses:
                self.data_access.create_or_update_status(status)

            return True
            
        except Exception as e:
            # Try to extract SQL error code for better debugging
            sql_error_code = None
            error_type = type(e).__name__
            
            try:
                # Try to pull PyMySQL/SQLAlchemy error code
                if hasattr(e, "orig") and hasattr(e.orig, "args") and len(e.orig.args) >= 2:
                    sql_error_code = e.orig.args[0]  # e.g., 1452 (FK constraint), 1406 (data too long)
                elif hasattr(e, "args") and len(e.args) >= 2 and isinstance(e.args[0], int):
                    sql_error_code = e.args[0]
            except Exception:
                pass
            
            log_event("ERROR", "db_error",
                     message="Database operation failed",
                     error_type=error_type,
                     sql_error_code=sql_error_code,
                     error_message=str(e),
                     exc_info=True)
            return False

    def print_stats(self):
        """Print migration statistics"""
        # Emit structured summary for querying
        log_event("INFO", "migration_summary",
                 message="Migration completed",
                 processed=self.stats["processed"],
                 migrated=self.stats["migrated"],
                 errors=self.stats["errors"],
                 skipped=self.stats["skipped"])
        
        # Also emit per-type breakdown if we tracked it
        type_counts = {}
        # Could track by message type during conversion if needed
        
        # Human-readable summary to console
        print(f"Migration Summary:")
        print(f"  Files processed: {self.stats['processed']}")
        print(f"  Messages migrated: {self.stats['migrated']}")
        print(f"  Files skipped (no messages): {self.stats['skipped']}")
        print(f"  Errors: {self.stats['errors']}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Migrate CIF message files to database")
    parser.add_argument("--deposition", help="Single deposition ID to migrate")
    parser.add_argument("--directory", help="Directory containing deposition subdirectories")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be migrated without writing to database")
    parser.add_argument("--site-id", required=True, help="Site ID (RCSB, PDBe, PDBj, BMRB)")
    parser.add_argument("--create-tables", action="store_true", help="Create database tables if they don't exist")
    parser.add_argument("--log-json", default=None, help="Write structured JSONL logs to this file (default: auto-generate logs/migration_TIMESTAMP.jsonl)")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")

    args = parser.parse_args()

    # Setup default log file path if not specified
    if args.log_json is None:
        # Create logs directory if it doesn't exist
        logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs")
        os.makedirs(logs_dir, exist_ok=True)
        
        # Generate timestamped log filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_log_path = os.path.join(logs_dir, f"migration_{timestamp}.jsonl")
    else:
        json_log_path = args.log_json  # Use custom path

    # Setup logging with JSON output (always enabled)
    _setup_logging(json_log_path, args.log_level)
    
    log_event("INFO", "migration_start",
             message="Starting migration",
             deposition=args.deposition,
             directory=args.directory,
             dry_run=args.dry_run,
             site_id=args.site_id,
             create_tables=args.create_tables,
             log_json_path=json_log_path,
             run_id=RUN_ID)

    migrator = None
    try:
        migrator = CifToDbMigrator(args.site_id, create_tables=args.create_tables)
        
        if args.deposition:
            migrator.migrate_deposition(args.deposition, args.dry_run)
        elif args.directory:
            result = migrator.migrate_directory(args.directory, args.dry_run)
            
            # Log final results
            log_event("INFO", "final_results",
                     message="Final migration results",
                     successful_depositions=len(result["successful"]),
                     failed_depositions=len(result["failed"]),
                     successful_list=result["successful"][:10],  # First 10 for brevity
                     failed_list=result["failed"][:10])
        else:
            parser.print_help()
            return

        migrator.print_stats()

    except Exception as e:
        log_event("ERROR", "migration_fatal",
                 message="Migration failed with fatal error",
                 error_type=type(e).__name__,
                 error_message=str(e),
                 exc_info=True)
        sys.exit(1)
    finally:
        if migrator:
            try:
                migrator.data_access.close()
                log_event("INFO", "cleanup_complete", message="Database connection closed")
            except Exception:
                pass


if __name__ == "__main__":
    main()