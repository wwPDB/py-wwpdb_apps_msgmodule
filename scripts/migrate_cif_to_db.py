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


class CifToDbMigrator:
    """Migrates message data from CIF files to database"""

    def __init__(self, site_id: str, create_tables: bool = False):
        """Initialize migrator"""
        self.site_id = site_id
        self.config_info = ConfigInfo(site_id)
        
        # Get database configuration
        db_config = self._get_database_config()
        log_event("init_migrator", site_id=site_id, db_host=db_config["host"], 
                 db_name=db_config["database"], create_tables=create_tables)
        
        self.data_access = DataAccessLayer(db_config)
        
        # Create tables only if explicitly requested
        if create_tables:
            self.data_access.create_tables()
            log_event("tables_created", site_id=site_id)
        
        log_event("db_connected", site_id=site_id)
        
        self.path_info = PathInfo(siteId=site_id)
        self.stats = {"processed": 0, "migrated": 0, "errors": 0}

    def _get_database_config(self) -> Dict:
        """Get database configuration from ConfigInfo"""
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
            "username": user,
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
                self.data_access.create_or_update_status(status)
        
        log_event("deposition_complete", deposition_id=deposition_id, success=success,
                 files_found=len(files_found), files_missing=len(files_missing))
        return success

    def migrate_directory(self, directory_path: str, dry_run: bool = False) -> Dict:
        """Migrate all depositions in a directory"""
        log_event("start_directory", directory=directory_path, dry_run=dry_run)
        
        deposition_ids = [
            item for item in os.listdir(directory_path)
            if os.path.isdir(os.path.join(directory_path, item)) and item.startswith('D_')
        ]
        
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
            logger.debug(f"No {message_type} file for {deposition_id}: {e}")
            return None

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

            message = MessageInfo(
                message_id=msg_info.get("message_id", str(uuid.uuid4())),
                deposition_data_set_id=msg_info.get("deposition_data_set_id", ""),
                timestamp=timestamp,
                sender=msg_info.get("sender", ""),
                context_type=msg_info.get("context_type"),
                context_value=msg_info.get("context_value"),
                parent_message_id=msg_info.get("parent_message_id"),
                message_subject=msg_info.get("message_subject", ""),
                message_text=message_text,
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

    def _store_data(self, messages: List[MessageInfo], file_refs: List[MessageFileReference], 
                    statuses: List[MessageStatus]) -> bool:
        """Store data in database"""
        deposition_id = messages[0].deposition_data_set_id if messages else "unknown"
        
        try:
            # Store messages
            stored_count = 0
            duplicate_count = 0
            
            for message in messages:
                if self.data_access.get_message_by_id(message.message_id):
                    duplicate_count += 1
                    log_event("message_duplicate", deposition_id=deposition_id, 
                             message_id=message.message_id)
                    continue
                    
                if not self.data_access.create_message(message):
                    log_event("message_store_failed", deposition_id=deposition_id,
                             message_id=message.message_id)
                    return False
                stored_count += 1

            # Store file references and statuses
            for file_ref in file_refs:
                self.data_access.create_file_reference(file_ref)
            
            for status in statuses:
                self.data_access.create_or_update_status(status)

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
    parser.add_argument("--site-id", required=True, help="Site ID (RCSB, PDBe, PDBj, BMRB)")
    parser.add_argument("--create-tables", action="store_true", help="Create database tables if they don't exist")
    parser.add_argument("--json-log", help="Path to JSON log file for structured logging")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Console and JSON log level")

    args = parser.parse_args()

    # Setup enhanced logging
    setup_logging(args.json_log, args.log_level)

    try:
        log_event("migration_start", site_id=args.site_id, deposition=args.deposition,
                 directory=args.directory, dry_run=args.dry_run, 
                 create_tables=args.create_tables)
        
        migrator = CifToDbMigrator(args.site_id, create_tables=args.create_tables)
        
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
            migrator.data_access.close()
        except:
            pass


if __name__ == "__main__":
    main()
