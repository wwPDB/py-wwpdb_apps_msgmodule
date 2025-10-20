"""
Database to CIF export utility for messaging system.

This script exports message data from the database back to CIF format files,
serving as the reverse operation of migrate_cif_to_db.py.
"""

import os
import sys
import logging
import argparse
import json
from datetime import datetime, timezone
from typing import List, Dict, Optional, Set
from pathlib import Path

try:
    import gemmi
except ImportError as e:
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


def escape_non_ascii(text: str) -> str:
    """Escape non-ASCII characters to ASCII using Unicode escape notation."""
    if not text:
        return text
    # Fix: Use unicode_escape to get \uXXXX format
    return text.encode('unicode_escape').decode('ascii')


class DbToCifExporter:
    """Exports message data from database to CIF files using gemmi"""

    def __init__(self, site_id: str):
        """Initialize exporter"""
        self.site_id = site_id
        self.config_info = ConfigInfo(site_id)
        
        # Get database configuration
        db_config = self._get_database_config()
        log_event("init_exporter", site_id=site_id, db_host=db_config["host"], 
                 db_name=db_config["database"])
        
        self.data_access = DataAccessLayer(db_config)
        log_event("db_connected", site_id=site_id)
        
        self.path_info = PathInfo(siteId=site_id)
        self.stats = {"depositions_processed": 0, "files_created": 0, "messages_exported": 0, "errors": 0}

    def _get_database_config(self) -> Dict:
        """Get database configuration from ConfigInfo"""
        # Try messaging-specific configuration first
        host = self.config_info.get("SITE_MESSAGE_DB_HOST_NAME")
        user = self.config_info.get("SITE_MESSAGE_DB_USER_NAME") 
        database = self.config_info.get("SITE_MESSAGE_DB_NAME")
        port = self.config_info.get("SITE_MESSAGE_DB_PORT_NUMBER", "3306")
        password = self.config_info.get("SITE_MESSAGE_DB_PASSWORD", "")

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

    def export_deposition(self, deposition_id: str, output_dir: str = None, 
                         overwrite: bool = False) -> bool:
        """Export all message files for a deposition"""
        log_event("start_deposition_export", deposition_id=deposition_id, 
                 output_dir=output_dir, overwrite=overwrite)
        
        # Validate deposition ID format
        if not deposition_id or not deposition_id.startswith('D_'):
            log_event("invalid_deposition_id", deposition_id=deposition_id,
                     message="Deposition ID must start with 'D_'")
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
            self.stats["depositions_processed"] += 1
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
                    self.stats["files_created"] += 1
                    self.stats["messages_exported"] += len(content_messages)
                else:
                    success = False
                    self.stats["errors"] += 1
                    
            except Exception as e:
                log_event("deposition_export_failed", deposition_id=deposition_id,
                         content_type=content_type, error=str(e))
                success = False
                self.stats["errors"] += 1
        
        self.stats["depositions_processed"] += 1
        log_event("deposition_export_complete", deposition_id=deposition_id, success=success,
                 files_created=len(files_created), messages_exported=self.stats["messages_exported"])
        return success

    def export_bulk(self, depositions: List[str] = None, output_dir: str = None,
                   overwrite: bool = False) -> Dict:
        """Export multiple depositions or all depositions from database"""
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
                return {"successful": [], "failed": []}
        
        successful = []
        failed = []
        
        for i, deposition_id in enumerate(depositions, 1):
            try:
                if self.export_deposition(deposition_id, output_dir, overwrite):
                    successful.append(deposition_id)
                else:
                    failed.append(deposition_id)
                
                # Log progress every 100 depositions
                if i % 100 == 0:
                    log_event("progress_update", processed=i, total=len(depositions),
                             successful=len(successful), failed=len(failed))
                             
            except Exception as e:
                log_event("deposition_export_failed", deposition_id=deposition_id, error=str(e))
                failed.append(deposition_id)
        
        log_event("bulk_export_complete", total=len(depositions),
                 successful=len(successful), failed=len(failed),
                 success_rate=f"{len(successful)/len(depositions)*100:.1f}%" if depositions else "0%")
        return {"successful": successful, "failed": failed}

    def _get_output_file_path(self, deposition_id: str, content_type: str, 
                             output_dir: str = None) -> Optional[str]:
        """Get output file path for deposition and content type"""
        if output_dir:
            # Custom output directory
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
            # Use standard PathInfo location
            try:
                return self.path_info.getFilePath(
                    dataSetId=deposition_id,
                    contentType=content_type,
                    formatType="pdbx",
                    fileSource="archive",
                    versionId="latest",
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
            doc = gemmi.cif.Document()
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
            
            # Add status category if we have statuses
            if statuses:
                self._add_status_category(block, statuses)
            
            log_event("cif_structure_created", deposition_id=deposition_id,
                     messages=len(messages), file_refs=len(file_refs), statuses=len(statuses))
            
            # Ensure output directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Write CIF file
            doc.write_file(file_path)
            
            log_event("file_exported", deposition_id=deposition_id, file_path=file_path,
                     content_type=content_type, messages=len(messages))
            return True
            
        except Exception as e:
            log_event("file_write_failed", deposition_id=deposition_id,
                     file_path=file_path, content_type=content_type, error=str(e))
            return False

    def _add_message_info_category(self, block: gemmi.cif.Block, messages: List[MessageInfo]):
        """Add _pdbx_deposition_message_info category to CIF block"""
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
                    escaped_value = escape_non_ascii(str(value)) if value is not None else "?"
                    # In CIF loops, all values must be single-line and quoted to handle spaces
                    # Replace any newlines (real or escaped) with spaces
                    if escaped_value and escaped_value != "?":
                        escaped_value = escaped_value.replace("\n", " ").replace("\\n", " ")
                    # Quote all values to handle spaces (timestamps, text with spaces, etc.)
                    row_values.append(f"'{escaped_value}'")
                loop.add_row(row_values)
        else:
            # Single message - use item format
            message = messages[0]
            for col in columns:
                value = self._get_message_attribute_value(message, col)
                escaped_value = escape_non_ascii(str(value)) if value is not None else "?"
                
                # Handle multiline text with text field format
                if col in ("message_text", "message_subject") and value and "\n" in str(value):
                    block.set_pair(f"_pdbx_deposition_message_info.{col}", f"\n;{escaped_value}\n;")
                else:
                    # Quote strings that contain spaces or special characters
                    if escaped_value and (" " in escaped_value or any(c in escaped_value for c in "'\"[]{}()")):
                        escaped_value = f"'{escaped_value}'"
                    block.set_pair(f"_pdbx_deposition_message_info.{col}", escaped_value)

    def _add_file_reference_category(self, block: gemmi.cif.Block, file_refs: List[MessageFileReference]):
        """Add _pdbx_deposition_message_file_reference category to CIF block"""
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
                    escaped_value = escape_non_ascii(str(value)) if value is not None else "?"
                    # Quote all values to handle spaces
                    row_values.append(f"'{escaped_value}'")
                loop.add_row(row_values)
        else:
            file_ref = file_refs[0]
            for col in columns:
                value = self._get_file_ref_attribute_value(file_ref, col)
                escaped_value = escape_non_ascii(str(value)) if value is not None else "?"
                block.set_pair(f"_pdbx_deposition_message_file_reference.{col}", escaped_value)

    def _add_status_category(self, block: gemmi.cif.Block, statuses: List[MessageStatus]):
        """Add _pdbx_deposition_message_status category to CIF block"""
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
                    escaped_value = escape_non_ascii(str(value)) if value is not None else "?"
                    # Quote all values to handle spaces
                    row_values.append(f"'{escaped_value}'")
                loop.add_row(row_values)
        else:
            status = statuses[0]
            for col in columns:
                value = self._get_status_attribute_value(status, col)
                escaped_value = escape_non_ascii(str(value)) if value is not None else "?"
                block.set_pair(f"_pdbx_deposition_message_status.{col}", escaped_value)

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
        """Print export statistics"""
        print(f"Export Summary:")
        print(f"  Depositions processed: {self.stats['depositions_processed']}")
        print(f"  Files created: {self.stats['files_created']}")
        print(f"  Messages exported: {self.stats['messages_exported']}")
        print(f"  Errors: {self.stats['errors']}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Export message data from database to CIF files")
    parser.add_argument("--deposition", help="Single deposition ID to export")
    parser.add_argument("--depositions", nargs="+", help="Multiple deposition IDs to export")
    parser.add_argument("--all", action="store_true", help="Export all depositions from database")
    parser.add_argument("--output-dir", help="Output directory for CIF files (default: use PathInfo locations)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing CIF files")
    parser.add_argument("--site-id", required=True, help="Site ID (RCSB, PDBe, PDBj, BMRB)")
    parser.add_argument("--json-log", help="Path to JSON log file for structured logging")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Console and JSON log level")

    args = parser.parse_args()

    # Setup enhanced logging
    setup_logging(args.json_log, args.log_level)

    try:
        log_event("export_start", site_id=args.site_id, deposition=args.deposition,
                 depositions=args.depositions, all_depositions=args.all,
                 output_dir=args.output_dir, overwrite=args.overwrite)
        
        exporter = DbToCifExporter(args.site_id)
        
        if args.deposition:
            success = exporter.export_deposition(args.deposition, args.output_dir, args.overwrite)
            log_event("export_summary", mode="single", success=success)
        elif args.depositions:
            results = exporter.export_bulk(args.depositions, args.output_dir, args.overwrite)
            log_event("export_summary", mode="multiple", 
                     successful=len(results["successful"]), failed=len(results["failed"]))
        elif args.all:
            results = exporter.export_bulk(None, args.output_dir, args.overwrite)
            log_event("export_summary", mode="all",
                     successful=len(results["successful"]), failed=len(results["failed"]))
        else:
            parser.print_help()
            return

        # Final stats
        exporter.print_stats()

    except Exception as e:
        log_event("export_failed", error=str(e))
        logger.error(f"Export failed: {e}")
        sys.exit(1)
    finally:
        try:
            exporter.data_access.close()
        except:
            pass


if __name__ == "__main__":
    main()
