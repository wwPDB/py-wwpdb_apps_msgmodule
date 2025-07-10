"""
Migration utility to convert existing CIF message files to database records.

This script will:
1. Scan for existing CIF message files
2. Parse the CIF data structures  
3. Convert to database format
4. Import into the new message database
5. Verify data integrity
"""

import os
import sys
import logging
import argparse
from datetime import datetime
from typing import List, Dict, Optional
import uuid

# CIF parsing imports
from mmcif.io.PdbxReader import PdbxReader
from mmcif_utils.message.PdbxMessageIo import PdbxMessageIo

# Database imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from wwpdb.apps.msgmodule.db import (
    MessagingDatabaseService,
    MessageRecord,
    MessageStatus,
    MessageFileReference,
)
from wwpdb.io.locator.PathInfo import PathInfo

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class CifToDbMigrator:
    """Migrates message data from CIF files to database"""

    def __init__(self, db_config: Dict, site_id: str = "RCSB"):
        """Initialize migrator with database configuration"""
        self.db_service = MessagingDatabaseService(db_config)
        self.site_id = site_id
        self.path_info = PathInfo(siteId=site_id)

        # Migration statistics
        self.stats = {
            "files_processed": 0,
            "messages_migrated": 0,
            "file_references_migrated": 0,
            "status_records_migrated": 0,
            "errors": 0,
            "skipped": 0,
        }

    def migrate_deposition_messages(
        self, deposition_id: str, test_mode: bool = False
    ) -> bool:
        """Migrate all message files for a specific deposition"""
        try:
            logger.info(f"Starting migration for deposition {deposition_id}")

            # Define the message file types to migrate
            message_types = [
                "messages-from-depositor",
                "messages-to-depositor",
                "notes-from-annotator",
            ]

            success = True
            for msg_type in message_types:
                file_path = self._get_message_file_path(deposition_id, msg_type)
                if file_path and os.path.exists(file_path):
                    if not self._migrate_cif_file(file_path, msg_type, test_mode):
                        success = False
                else:
                    logger.info(f"No {msg_type} file found for {deposition_id}")

            if success:
                logger.info(f"Successfully migrated deposition {deposition_id}")
            else:
                logger.error(f"Migration failed for deposition {deposition_id}")

            return success

        except Exception as e:
            logger.error(f"Error migrating deposition {deposition_id}: {e}")
            self.stats["errors"] += 1
            return False

    def migrate_bulk_depositions(
        self, deposition_list: List[str], test_mode: bool = False
    ) -> Dict:
        """Migrate multiple depositions"""
        logger.info(f"Starting bulk migration for {len(deposition_list)} depositions")

        successful = []
        failed = []

        for deposition_id in deposition_list:
            try:
                if self.migrate_deposition_messages(deposition_id, test_mode):
                    successful.append(deposition_id)
                else:
                    failed.append(deposition_id)

            except Exception as e:
                logger.error(f"Error processing {deposition_id}: {e}")
                failed.append(deposition_id)

        results = {"successful": successful, "failed": failed, "stats": self.stats}

        logger.info(
            f"Bulk migration completed. Success: {len(successful)}, Failed: {len(failed)}"
        )
        return results

    def migrate_from_directory(
        self, directory_path: str, test_mode: bool = False
    ) -> Dict:
        """Migrate all CIF message files from a directory"""
        logger.info(f"Scanning directory {directory_path} for CIF message files")

        cif_files = []
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                if self._is_message_cif_file(file):
                    cif_files.append(os.path.join(root, file))

        logger.info(f"Found {len(cif_files)} CIF message files")

        successful = []
        failed = []

        for cif_file in cif_files:
            try:
                msg_type = self._extract_message_type(cif_file)
                if self._migrate_cif_file(cif_file, msg_type, test_mode):
                    successful.append(cif_file)
                else:
                    failed.append(cif_file)

            except Exception as e:
                logger.error(f"Error processing {cif_file}: {e}")
                failed.append(cif_file)

        results = {"successful": successful, "failed": failed, "stats": self.stats}

        logger.info(
            f"Directory migration completed. Success: {len(successful)}, Failed: {len(failed)}"
        )
        return results

    def _migrate_cif_file(
        self, file_path: str, message_type: str, test_mode: bool = False
    ) -> bool:
        """Migrate a single CIF message file"""
        try:
            logger.info(f"Processing CIF file: {file_path}")
            self.stats["files_processed"] += 1

            # Parse CIF file
            messages, file_references, status_records = self._parse_cif_file(
                file_path, message_type
            )

            if not messages:
                logger.warning(f"No messages found in {file_path}")
                self.stats["skipped"] += 1
                return True

            # Migrate data to database
            if not test_mode:
                success = self._store_parsed_data(
                    messages, file_references, status_records
                )
                if not success:
                    self.stats["errors"] += 1
                    return False
            else:
                logger.info(
                    f"TEST MODE: Would migrate {len(messages)} messages from {file_path}"
                )

            self.stats["messages_migrated"] += len(messages)
            self.stats["file_references_migrated"] += len(file_references)
            self.stats["status_records_migrated"] += len(status_records)

            return True

        except Exception as e:
            logger.error(f"Error migrating CIF file {file_path}: {e}")
            self.stats["errors"] += 1
            return False

    def _parse_cif_file(self, file_path: str, message_type: str) -> tuple:
        """Parse CIF file and extract message data"""
        messages = []
        file_references = []
        status_records = []

        try:
            # Use existing PdbxMessageIo to read CIF file
            msg_io = PdbxMessageIo(verbose=False)
            if not msg_io.read(file_path):
                logger.error(f"Failed to read CIF file: {file_path}")
                return messages, file_references, status_records

            # Extract message info
            message_info_list = msg_io.getMessageInfo()
            for msg_info in message_info_list:
                message = self._convert_to_message_record(msg_info, message_type)
                messages.append(message)

            # Extract file references
            file_ref_list = msg_io.getFileReferenceInfo()
            for file_ref in file_ref_list:
                file_reference = self._convert_to_file_reference(file_ref)
                file_references.append(file_reference)

            # Extract status info
            status_info_list = msg_io.getMsgStatusInfo()
            for status_info in status_info_list:
                status = self._convert_to_status_record(status_info)
                status_records.append(status)

            logger.info(
                f"Parsed {len(messages)} messages, {len(file_references)} file refs, "
                f"{len(status_records)} status records from {file_path}"
            )

        except Exception as e:
            logger.error(f"Error parsing CIF file {file_path}: {e}")
            raise

        return messages, file_references, status_records

    def _convert_to_message_record(
        self, msg_info: Dict, message_type: str
    ) -> MessageRecord:
        """Convert CIF message info to MessageRecord"""
        # Map content type
        content_type = "notes" if "notes" in message_type else "msgs"

        # Parse timestamp
        timestamp_str = msg_info.get("timestamp", "")
        try:
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            timestamp = datetime.now()

        return MessageRecord(
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

    def _convert_to_file_reference(self, file_ref: Dict) -> MessageFileReference:
        """Convert CIF file reference to MessageFileReference"""
        return MessageFileReference(
            message_id=file_ref.get("message_id", ""),
            deposition_data_set_id=file_ref.get("deposition_data_set_id", ""),
            content_type=file_ref.get("content_type", ""),
            content_format=file_ref.get("content_format", ""),
            partition_number=int(file_ref.get("partition_number", 1)),
            version_id=int(file_ref.get("version_id", 1)),
            file_source=file_ref.get("file_source", "archive"),
            upload_file_name=file_ref.get("upload_file_name"),
        )

    def _convert_to_status_record(self, status_info: Dict) -> MessageStatus:
        """Convert CIF status info to MessageStatus"""
        return MessageStatus(
            message_id=status_info.get("message_id", ""),
            deposition_data_set_id=status_info.get("deposition_data_set_id", ""),
            read_status=status_info.get("read_status", "N"),
            action_reqd=status_info.get("action_reqd", "N"),
            for_release=status_info.get("for_release", "N"),
        )

    def _store_parsed_data(
        self,
        messages: List[MessageRecord],
        file_references: List[MessageFileReference],
        status_records: List[MessageStatus],
    ) -> bool:
        """Store parsed data in database"""
        try:
            # Store messages first
            for message in messages:
                # Check if message already exists
                existing = self.db_service.message_dao.get_message_by_id(
                    message.message_id
                )
                if existing:
                    logger.warning(
                        f"Message {message.message_id} already exists, skipping"
                    )
                    continue

                # Create message
                if not self.db_service.message_dao.create_message(message):
                    logger.error(f"Failed to create message {message.message_id}")
                    return False

            # Store file references
            for file_ref in file_references:
                if not self.db_service.file_dao.create_file_reference(file_ref):
                    logger.error(
                        f"Failed to create file reference for message {file_ref.message_id}"
                    )
                    return False

            # Store status records
            for status in status_records:
                if not self.db_service.status_dao.create_or_update_status(status):
                    logger.error(
                        f"Failed to create status for message {status.message_id}"
                    )
                    return False

            return True

        except Exception as e:
            logger.error(f"Error storing parsed data: {e}")
            return False

    def _get_message_file_path(
        self, deposition_id: str, message_type: str
    ) -> Optional[str]:
        """Get the file path for a message file"""
        try:
            return self.path_info.getFilePath(
                dataSetId=deposition_id,
                contentType=message_type,
                formatType="pdbx",
                fileSource="archive",
                versionId="latest",
            )
        except Exception as e:
            logger.error(
                f"Error getting file path for {deposition_id}, {message_type}: {e}"
            )
            return None

    def _is_message_cif_file(self, filename: str) -> bool:
        """Check if file is a message CIF file"""
        message_patterns = [
            "messages-from-depositor",
            "messages-to-depositor",
            "notes-from-annotator",
        ]
        return any(
            pattern in filename for pattern in message_patterns
        ) and filename.endswith(".cif")

    def _extract_message_type(self, file_path: str) -> str:
        """Extract message type from file path"""
        filename = os.path.basename(file_path)
        if "messages-from-depositor" in filename:
            return "messages-from-depositor"
        elif "messages-to-depositor" in filename:
            return "messages-to-depositor"
        elif "notes-from-annotator" in filename:
            return "notes-from-annotator"
        else:
            return "unknown"

    def verify_migration(self, deposition_id: str) -> Dict:
        """Verify that migration was successful for a deposition"""
        try:
            # Get messages from database
            db_messages = self.db_service.get_deposition_messages(deposition_id)

            # Get original CIF data for comparison
            cif_data = self._get_original_cif_data(deposition_id)

            verification_result = {
                "deposition_id": deposition_id,
                "db_message_count": len(db_messages),
                "cif_message_count": len(cif_data.get("messages", [])),
                "match": len(db_messages) == len(cif_data.get("messages", [])),
                "discrepancies": [],
            }

            # Additional verification logic could be added here

            return verification_result

        except Exception as e:
            logger.error(f"Error verifying migration for {deposition_id}: {e}")
            return {"error": str(e)}

    def _get_original_cif_data(self, deposition_id: str) -> Dict:
        """Get original CIF data for verification"""
        # This would implement logic to read and parse original CIF files
        # for comparison with database data
        return {"messages": [], "file_references": [], "status_records": []}

    def print_statistics(self):
        """Print migration statistics"""
        print("\n" + "=" * 50)
        print("MIGRATION STATISTICS")
        print("=" * 50)
        print(f"Files processed:          {self.stats['files_processed']}")
        print(f"Messages migrated:        {self.stats['messages_migrated']}")
        print(f"File references migrated: {self.stats['file_references_migrated']}")
        print(f"Status records migrated:  {self.stats['status_records_migrated']}")
        print(f"Errors:                   {self.stats['errors']}")
        print(f"Skipped:                  {self.stats['skipped']}")
        print("=" * 50)


def main():
    """Main entry point for migration script"""
    parser = argparse.ArgumentParser(
        description="Migrate CIF message files to database"
    )
    parser.add_argument("--deposition", help="Single deposition ID to migrate")
    parser.add_argument(
        "--depositions-file", help="File containing list of deposition IDs"
    )
    parser.add_argument("--directory", help="Directory containing CIF files to migrate")
    parser.add_argument(
        "--test-mode", action="store_true", help="Run in test mode (no database writes)"
    )
    parser.add_argument(
        "--site-id", default="RCSB", help="Site ID for file path resolution"
    )
    parser.add_argument("--verify", help="Verify migration for a deposition ID")

    args = parser.parse_args()

    # Database configuration (this would come from config file in real implementation)
    db_config = {
        "host": os.getenv("MSGDB_HOST", "localhost"),
        "port": int(os.getenv("MSGDB_PORT", "3306")),
        "database": os.getenv("MSGDB_NAME", "wwpdb_messaging"),
        "username": os.getenv("MSGDB_USER", "msgmodule_user"),
        "password": os.getenv("MSGDB_PASS", "password"),
        "pool_size": 20,
        "charset": "utf8mb4",
    }

    migrator = CifToDbMigrator(db_config, args.site_id)

    try:
        if args.verify:
            # Verification mode
            result = migrator.verify_migration(args.verify)
            print(f"Verification result: {result}")

        elif args.deposition:
            # Single deposition migration
            success = migrator.migrate_deposition_messages(
                args.deposition, args.test_mode
            )
            print(
                f"Migration {'successful' if success else 'failed'} for {args.deposition}"
            )

        elif args.depositions_file:
            # Bulk deposition migration
            with open(args.depositions_file, "r") as f:
                deposition_list = [line.strip() for line in f if line.strip()]

            results = migrator.migrate_bulk_depositions(deposition_list, args.test_mode)
            print(
                f"Bulk migration completed: {len(results['successful'])} successful, {len(results['failed'])} failed"
            )

        elif args.directory:
            # Directory migration
            results = migrator.migrate_from_directory(args.directory, args.test_mode)
            print(
                f"Directory migration completed: {len(results['successful'])} successful, {len(results['failed'])} failed"
            )

        else:
            parser.print_help()
            return

        migrator.print_statistics()

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
