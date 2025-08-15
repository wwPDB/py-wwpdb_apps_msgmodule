"""
Migration utility to convert existing CIF message files to database records.

Streamlined version with bulk operations, minimal round-trips, and idempotent inserts.
"""

import os, sys, logging, argparse, uuid
from typing import List, Dict, Optional, Tuple, Set
from datetime import datetime

from wwpdb.utils.config.ConfigInfo import ConfigInfo
from mmcif_utils.message.PdbxMessageIo import PdbxMessageIo
from wwpdb.io.locator.PathInfo import PathInfo

from wwpdb.apps.msgmodule.db import MessageInfo, MessageFileReference, MessageStatus
from wwpdb.apps.msgmodule.db.DataAccessLayer import DataAccessLayer

logger = logging.getLogger(__name__)

BATCH_SIZE = 2000

class CifToDbMigrator:
    def __init__(self, site_id: str, create_tables: bool = False):
        self.site_id = site_id
        self.config_info = ConfigInfo(site_id)
        self.path_info = PathInfo(siteId=site_id)
        db_config = self._get_database_config()
        self.dal = DataAccessLayer(db_config)
        if create_tables:
            self.dal.create_tables()
        self.stats = {"processed": 0, "migrated": 0, "errors": 0}

    def _get_database_config(self) -> Dict:
        host = self.config_info.get("SITE_DB_HOST_NAME")
        user = self.config_info.get("SITE_DB_ADMIN_USER")
        database = self.config_info.get("WWPDB_MESSAGING_DB_NAME")
        port = int(self.config_info.get("SITE_DB_PORT_NUMBER", "3306"))
        password = self.config_info.get("SITE_DB_ADMIN_PASS", "")
        if not all([host, user, database]):
            raise RuntimeError("Missing required database configuration")
        return {"host": host, "port": port, "database": database,
                "username": user, "password": password, "charset": "utf8mb4"}

    def migrate_deposition(self, dep_id: str, dry_run: bool = False) -> bool:
        message_types = ["messages-from-depositor", "messages-to-depositor", "notes-from-annotator"]

        all_msgs: List[MessageInfo] = []
        all_refs: List[MessageFileReference] = []
        all_stats: List[MessageStatus] = []

        for ct in message_types:
            fp = self._file_path(dep_id, ct)
            if not (fp and os.path.exists(fp)):
                continue
            ok, msgs, refs, stats = self._parse_file(fp, ct)
            if not ok:
                return False
            all_msgs.extend(msgs)
            all_refs.extend(refs)
            all_stats.extend(stats)

        if not all_msgs:
            logger.info(f"{dep_id}: no messages found")
            return True

        if dry_run:
            logger.info(f"{dep_id}: DRY RUN (msgs={len(all_msgs)}, refs={len(all_refs)}, stats={len(all_stats)})")
            return True

        return self._store_bundle(dep_id, all_msgs, all_refs, all_stats)

    def _file_path(self, dep_id: str, content_type: str) -> Optional[str]:
        try:
            return self.path_info.getFilePath(
                dataSetId=dep_id, contentType=content_type, formatType="pdbx",
                fileSource="archive", versionId="latest"
            )
        except Exception:
            return None

    def _parse_file(self, file_path: str, message_type: str):
        logger.info(f"Processing {file_path}")
        self.stats["processed"] += 1
        try:
            io = PdbxMessageIo(verbose=False)
            if not io.read(file_path):
                logger.error(f"Failed to read {file_path}")
                return False, [], [], []
            msgs = self._convert_messages(io.getMessageInfo(), message_type)
            refs = self._convert_file_refs(io.getFileReferenceInfo())
            stats = self._convert_statuses(io.getMsgStatusInfo())
            return True, msgs, refs, stats
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            self.stats["errors"] += 1
            return False, [], [], []

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
            message_text = msg_info.get("message_text", "")

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
                logger.warning(f"Circular dependency detected: {message.message_id} -> {message.parent_message_id}")
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
        
        logger.debug(f"Messages sorted by dependencies: {len(messages)} -> {len(sorted_messages)}")
        return sorted_messages

    def _store_bundle(self, dep_id: str,
                      messages: List[MessageInfo],
                      file_refs: List[MessageFileReference],
                      statuses: List[MessageStatus]) -> bool:
        """Dependency-safe, batched, idempotent store."""
        # 1) topo sort within bundle
        sorted_msgs = self._sort_messages_by_dependencies(messages)

        # 2) Preload parent presence (outside bundle)
        parent_ids = {m.parent_message_id for m in sorted_msgs if m.parent_message_id}
        with self.dal.session_scope() as s:
            existing_parents = self.dal.messages_exist(s, parent_ids)

        # 3) Iterative passes honoring already-present parents
        pending = {m.message_id: m for m in sorted_msgs}
        inserted: List[MessageInfo] = []
        MAX_PASSES = 10

        with self.dal.session_scope() as s:
            # Build a fast "present" set (parents preloaded + newly inserted)
            present = set(existing_parents)
            for _ in range(MAX_PASSES):
                progressed = 0
                this_pass: List[MessageInfo] = []
                for mid, m in list(pending.items()):
                    if (not m.parent_message_id) or (m.parent_message_id in present):
                        this_pass.append(m)
                        del pending[mid]
                        progressed += 1
                if not this_pass:
                    break
                # bulk insert this pass (ignore duplicates quietly)
                for chunk_start in range(0, len(this_pass), BATCH_SIZE):
                    chunk = this_pass[chunk_start:chunk_start+BATCH_SIZE]
                    self.dal.bulk_insert_messages_ignore(s, chunk)
                present.update(m.message_id for m in this_pass)
                inserted.extend(this_pass)

            # Optional: skip unresolved children (missing parents) strictly
            if pending:
                logger.warning(f"{dep_id}: unresolved children after passes: {len(pending)} (parents missing)")

        self.stats["migrated"] += len(inserted)

        # 4) File refs and statuses after messages
        with self.dal.session_scope() as s:
            for chunk_start in range(0, len(file_refs), BATCH_SIZE):
                self.dal.bulk_insert_file_refs_ignore(s, file_refs[chunk_start:chunk_start+BATCH_SIZE])
            for chunk_start in range(0, len(statuses), BATCH_SIZE):
                self.dal.bulk_upsert_statuses(s, statuses[chunk_start:chunk_start+BATCH_SIZE])

        logger.info(f"{dep_id}: inserted {len(inserted)} msgs, {len(file_refs)} refs, {len(statuses)} statuses")
        return True

    def migrate_directory(self, directory_path: str, dry_run: bool = False) -> Dict:
        """Migrate all depositions in a directory"""
        logger.info(f"Starting directory scan: {directory_path}")
        
        deposition_ids = [
            item for item in os.listdir(directory_path)
            if os.path.isdir(os.path.join(directory_path, item)) and item.startswith('D_')
        ]
        
        logger.info(f"Found {len(deposition_ids)} depositions to process")
        
        successful = []
        failed = []
        
        for i, deposition_id in enumerate(deposition_ids, 1):
            try:
                logger.info(f"Processing deposition {i}/{len(deposition_ids)}: {deposition_id}")
                
                if self.migrate_deposition(deposition_id, dry_run):
                    successful.append(deposition_id)
                else:
                    failed.append(deposition_id)
            except Exception as e:
                logger.error(f"Exception processing deposition {deposition_id}: {e}")
                failed.append(deposition_id)
        
        logger.info(f"Directory migration completed - successful: {len(successful)}, failed: {len(failed)}")
        return {"successful": successful, "failed": failed}

    def close(self):
        """Close database connections"""
        if self.dal:
            self.dal.close()


def main():
    """Main entry point for the migration script"""
    parser = argparse.ArgumentParser(description="Migrate CIF message files to database")
    parser.add_argument("--site-id", required=True, help="Site ID for configuration")
    parser.add_argument("--deposition-id", help="Single deposition ID to migrate")
    parser.add_argument("--directory", help="Directory containing depositions to migrate")
    parser.add_argument("--create-tables", action="store_true", help="Create database tables")
    parser.add_argument("--dry-run", action="store_true", help="Dry run - don't insert into database")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    migrator = None
    try:
        # Initialize migrator
        migrator = CifToDbMigrator(args.site_id, create_tables=args.create_tables)
        
        if args.deposition_id:
            # Migrate single deposition
            success = migrator.migrate_deposition(args.deposition_id, dry_run=args.dry_run)
            if not success:
                sys.exit(1)
        elif args.directory:
            # Migrate directory
            results = migrator.migrate_directory(args.directory, dry_run=args.dry_run)
            if results["failed"]:
                logger.error(f"Some depositions failed: {results['failed']}")
                sys.exit(1)
        else:
            logger.error("Must specify either --deposition-id or --directory")
            sys.exit(1)
            
        logger.info(f"Migration completed. Stats: {migrator.stats}")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)
    finally:
        if migrator:
            migrator.close()


if __name__ == "__main__":
    main()