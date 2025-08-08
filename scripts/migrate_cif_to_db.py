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
        self.data_access = DataAccessLayer(db_config)
        
        # Create tables only if explicitly requested
        if create_tables:
            self.data_access.create_tables()
            logger.info("Database tables created/verified")
        
        logger.info("Database connection established")
        
        self.path_info = PathInfo(siteId=site_id)
        self.stats = {"processed": 0, "migrated": 0, "errors": 0}

    def _get_database_config(self) -> Dict:
        """Get database configuration from ConfigInfo"""
        host = self.config_info.get("SITE_DB_HOST_NAME")
        user = self.config_info.get("SITE_DB_ADMIN_USER")
        database = self.config_info.get("WWPDB_MESSAGING_DB_NAME")
        port = self.config_info.get("SITE_DB_PORT_NUMBER", "3306")
        password = self.config_info.get("SITE_DB_ADMIN_PASS", "")
        
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
        logger.info(f"Migrating deposition {deposition_id}")
        
        message_types = ["messages-from-depositor", "messages-to-depositor", "notes-from-annotator"]
        success = True
        
        for msg_type in message_types:
            file_path = self._get_file_path(deposition_id, msg_type)
            if file_path and os.path.exists(file_path):
                if not self._migrate_file(file_path, msg_type, dry_run):
                    success = False
            else:
                logger.debug(f"No {msg_type} file for {deposition_id}")
        
        return success

    def migrate_directory(self, directory_path: str, dry_run: bool = False) -> Dict:
        """Migrate all depositions in a directory"""
        logger.info(f"Scanning {directory_path}")
        
        deposition_ids = [
            item for item in os.listdir(directory_path)
            if os.path.isdir(os.path.join(directory_path, item)) and item.startswith('D_')
        ]
        
        logger.info(f"Found {len(deposition_ids)} depositions")
        
        successful = []
        failed = []
        
        for deposition_id in deposition_ids:
            try:
                if self.migrate_deposition(deposition_id, dry_run):
                    successful.append(deposition_id)
                else:
                    failed.append(deposition_id)
            except Exception as e:
                logger.error(f"Error with {deposition_id}: {e}")
                failed.append(deposition_id)
        
        logger.info(f"Completed: {len(successful)} success, {len(failed)} failed")
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

    def _migrate_file(self, file_path: str, message_type: str, dry_run: bool = False) -> bool:
        """Migrate a single CIF file"""
        logger.info(f"Processing {file_path}")
        self.stats["processed"] += 1

        try:
            # Parse CIF file
            msg_io = PdbxMessageIo(verbose=False)
            if not msg_io.read(file_path):
                logger.error(f"Failed to read {file_path}")
                return False

            # Convert data
            messages = self._convert_messages(msg_io.getMessageInfo(), message_type)
            file_refs = self._convert_file_refs(msg_io.getFileReferenceInfo())
            statuses = self._convert_statuses(msg_io.getMsgStatusInfo())

            if not messages:
                logger.warning(f"No messages in {file_path}")
                return True

            # Store in database
            if not dry_run:
                success = self._store_data(messages, file_refs, statuses)
                if success:
                    self.stats["migrated"] += len(messages)
                else:
                    self.stats["errors"] += 1
                    return False
            else:
                logger.info(f"DRY RUN: Would migrate {len(messages)} messages")

            return True

        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
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

            message = MessageInfo(
                message_id=msg_info.get("message_id", str(uuid.uuid4())),
                deposition_data_set_id=msg_info.get("deposition_data_set_id", ""),
                timestamp=timestamp,
                sender=msg_info.get("sender", ""),
                context_type=msg_info.get("context_type"),
                context_value=msg_info.get("context_value"),
                parent_message_id=msg_info.get("parent_message_id"),
                message_subject=msg_info.get("message_subject", ""),
                message_text=msg_info.get("message_text", ""),
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
                logger.warning(f"Circular dependency detected for message {message.message_id}")
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
        
        logger.info(f"Sorted {len(messages)} messages by dependencies")
        return sorted_messages

    def _store_data(self, messages: List[MessageInfo], file_refs: List[MessageFileReference], 
                    statuses: List[MessageStatus]) -> bool:
        """Store data in database with proper dependency ordering"""
        try:
            # Sort messages to ensure parents are inserted before children
            sorted_messages = self._sort_messages_by_dependencies(messages)
            
            # Store messages in dependency order
            for message in sorted_messages:
                if self.data_access.get_message_by_id(message.message_id):
                    logger.debug(f"Message {message.message_id} already exists")
                    continue
                if not self.data_access.create_message(message):
                    return False

            # Store file references and statuses
            for file_ref in file_refs:
                self.data_access.create_file_reference(file_ref)
            
            for status in statuses:
                self.data_access.create_or_update_status(status)

            return True
        except Exception as e:
            logger.error(f"Database error: {e}")
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

    args = parser.parse_args()

    try:
        migrator = CifToDbMigrator(args.site_id, create_tables=args.create_tables)
        
        if args.deposition:
            migrator.migrate_deposition(args.deposition, args.dry_run)
        elif args.directory:
            migrator.migrate_directory(args.directory, args.dry_run)
        else:
            parser.print_help()
            return

        migrator.print_stats()

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)
    finally:
        try:
            migrator.data_access.close()
        except:
            pass


if __name__ == "__main__":
    main()
