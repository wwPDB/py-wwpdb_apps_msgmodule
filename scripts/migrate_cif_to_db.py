"""
Migration utility to convert existing CIF message files to database records.

Streamlined version with bulk operations, minimal round-trips, and idempotent inserts.
"""

import os, sys, logging, argparse, uuid, json, traceback
from typing import List, Dict, Optional, Tuple, Set
from datetime import datetime

from wwpdb.utils.config.ConfigInfo import ConfigInfo
from mmcif_utils.message.PdbxMessageIo import PdbxMessageIo
from wwpdb.io.locator.PathInfo import PathInfo

from wwpdb.apps.msgmodule.db import MessageInfo, MessageFileReference, MessageStatus
from wwpdb.apps.msgmodule.db.DataAccessLayer import DataAccessLayer

logger = logging.getLogger(__name__)

RUN_ID = str(uuid.uuid4())
BATCH_SIZE = 2000

class JsonFormatter(logging.Formatter):
    def format(self, record):
        obj = {
            "ts": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S.%fZ"),
            "level": record.levelname,
            "logger": record.name,
            "run_id": RUN_ID,
            "event": getattr(record, "event", None),
            "msg": record.getMessage(),
        }
        # structured extras passed via 'extra={"fields": {...}}'
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            obj.update(fields)
        if record.exc_info:
            etype, e, tb = record.exc_info
            obj["exc_type"] = getattr(etype, "__name__", str(etype))
            obj["exc_message"] = str(e)
            obj["exc_trace"] = "".join(traceback.format_exception(etype, e, tb)).rstrip()
        return json.dumps(obj, ensure_ascii=False)

def setup_logging(human_level="INFO", json_path=None):
    root = logging.getLogger()
    root.handlers[:] = []
    root.setLevel(getattr(logging, human_level.upper()))

    # human console
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    root.addHandler(ch)

    # json file
    if json_path:
        fh = logging.FileHandler(json_path, encoding="utf-8")
        fh.setFormatter(JsonFormatter())
        root.addHandler(fh)

def log_event(level, event, **fields):
    getattr(logger, level.lower())(fields.get("message", ""), extra={"event": event, "fields": fields})

class CifToDbMigrator:
    def __init__(self, site_id: str, create_tables: bool = False):
        self.site_id = site_id
        self.config_info = ConfigInfo(site_id)
        self.path_info = PathInfo(siteId=site_id)
        db_config = self._get_database_config()
        
        log_event("INFO", "init_migrator", site_id=site_id, create_tables=create_tables,
                  db_host=db_config["host"], db_name=db_config["database"])
        
        self.dal = DataAccessLayer(db_config)
        if create_tables:
            try:
                self.dal.create_tables()
                log_event("INFO", "tables_created", site_id=site_id)
            except Exception as e:
                sql_code = self._extract_sql_error_code(e)
                log_event("ERROR", "db_error", operation="create_tables", 
                          sql_error_code=sql_code, message=str(e))
                raise
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

    def _extract_sql_error_code(self, exception) -> Optional[int]:
        """Extract SQL error code from database exception"""
        try:
            # SQLAlchemy+PyMySQL often exposes codes as e.orig.args[0]
            if hasattr(exception, "orig") and getattr(exception.orig, "args", None):
                return exception.orig.args[0]
            # Direct PyMySQL exceptions
            elif hasattr(exception, "args") and exception.args:
                return exception.args[0]
        except Exception:
            pass
        return None

    def migrate_deposition(self, dep_id: str, dry_run: bool = False) -> bool:
        log_event("INFO", "start_deposition", deposition_id=dep_id, dry_run=dry_run)
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
            log_event("INFO", "no_messages", deposition_id=dep_id)
            return True

        if dry_run:
            log_event("INFO", "dry_run_summary", deposition_id=dep_id,
                      messages=len(all_msgs), file_refs=len(all_refs), statuses=len(all_stats))
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
        dep_id = os.path.basename(os.path.dirname(file_path))
        fname = os.path.basename(file_path)
        log_event("INFO", "process_file", deposition_id=dep_id, file_path=file_path,
                  filename=fname, message_type=message_type)
        self.stats["processed"] += 1
        try:
            io = PdbxMessageIo(verbose=False)
            if not io.read(file_path):
                log_event("ERROR", "read_fail", deposition_id=dep_id, filename=fname,
                          file_path=file_path, message_type=message_type,
                          reason="mmCIF read returned False")
                return False, [], [], []
            msgs = self._convert_messages(io.getMessageInfo(), message_type)
            refs = self._convert_file_refs(io.getFileReferenceInfo())
            stats = self._convert_statuses(io.getMsgStatusInfo())
            log_event("INFO", "parse_ok", deposition_id=dep_id, filename=fname,
                      messages=len(msgs), file_refs=len(refs), statuses=len(stats))
            return True, msgs, refs, stats
        except Exception as e:
            log_event("ERROR", "parse_exception", deposition_id=dep_id, filename=fname,
                      file_path=file_path, message_type=message_type, message=str(e))
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
        log_event("DEBUG", "sorted_messages", deposition_id=dep_id, count=len(sorted_msgs))

        # 2) Preload parent presence (outside bundle)
        parent_ids = {m.parent_message_id for m in sorted_msgs if m.parent_message_id}
        try:
            with self.dal.session_scope() as s:
                existing_parents = self.dal.messages_exist(s, parent_ids)
        except Exception as e:
            sql_code = self._extract_sql_error_code(e)
            log_event("ERROR", "db_error", deposition_id=dep_id, operation="preload_parents",
                      sql_error_code=sql_code, message=str(e))
            raise
        
        log_event("DEBUG", "preload_parents", deposition_id=dep_id,
                  requested=len(parent_ids), existing=len(existing_parents))

        # 3) Iterative passes honoring already-present parents
        pending = {m.message_id: m for m in sorted_msgs}
        inserted: List[MessageInfo] = []
        MAX_PASSES = 10

        with self.dal.session_scope() as s:
            # Build a fast "present" set (parents preloaded + newly inserted)
            present = set(existing_parents)
            for pass_no in range(1, MAX_PASSES + 1):
                this_pass: List[MessageInfo] = []
                for mid, m in list(pending.items()):
                    if (not m.parent_message_id) or (m.parent_message_id in present):
                        this_pass.append(m)
                        del pending[mid]
                
                if not this_pass:
                    log_event("INFO", "no_progress_pass", deposition_id=dep_id,
                              pass_no=pass_no, remaining=len(pending))
                    break
                
                # bulk insert this pass (ignore duplicates quietly)
                for chunk_start in range(0, len(this_pass), BATCH_SIZE):
                    chunk = this_pass[chunk_start:chunk_start+BATCH_SIZE]
                    try:
                        self.dal.bulk_insert_messages_ignore(s, chunk)
                        log_event("INFO", "insert_chunk", deposition_id=dep_id,
                                  pass_no=pass_no, batch=len(chunk), start=chunk_start, 
                                  end=chunk_start+len(chunk)-1)
                    except Exception as e:
                        sql_code = self._extract_sql_error_code(e)
                        log_event("ERROR", "db_error", deposition_id=dep_id, 
                                  operation="bulk_insert_messages", pass_no=pass_no,
                                  sql_error_code=sql_code, batch_size=len(chunk), message=str(e))
                        raise
                
                present.update(m.message_id for m in this_pass)
                inserted.extend(this_pass)

            # Optional: skip unresolved children (missing parents) strictly
            if pending:
                unresolved = len(pending)
                sample = list(pending.values())[:5]
                log_event("WARNING", "unresolved_children", deposition_id=dep_id,
                          count=unresolved,
                          examples=[{"message_id": x.message_id, "parent_message_id": x.parent_message_id}
                                    for x in sample])

        self.stats["migrated"] += len(inserted)

        # 4) File refs and statuses after messages
        try:
            with self.dal.session_scope() as s:
                for chunk_start in range(0, len(file_refs), BATCH_SIZE):
                    self.dal.bulk_insert_file_refs_ignore(s, file_refs[chunk_start:chunk_start+BATCH_SIZE])
                for chunk_start in range(0, len(statuses), BATCH_SIZE):
                    self.dal.bulk_upsert_statuses(s, statuses[chunk_start:chunk_start+BATCH_SIZE])
        except Exception as e:
            sql_code = self._extract_sql_error_code(e)
            log_event("ERROR", "db_error", deposition_id=dep_id, 
                      operation="insert_refs_statuses", sql_error_code=sql_code, message=str(e))
            raise

        log_event("INFO", "insert_refs_statuses", deposition_id=dep_id,
                  file_refs=len(file_refs), statuses=len(statuses), batch_size=BATCH_SIZE)
        log_event("INFO", "deposition_summary", deposition_id=dep_id,
                  messages_inserted=len(inserted), unresolved=len(pending))
        return True

    def migrate_directory(self, directory_path: str, dry_run: bool = False) -> Dict:
        """Migrate all depositions in a directory"""
        log_event("INFO", "start_directory", directory=directory_path)
        
        deposition_ids = [
            item for item in os.listdir(directory_path)
            if os.path.isdir(os.path.join(directory_path, item)) and item.startswith('D_')
        ]
        
        log_event("INFO", "found_depositions", directory=directory_path, 
                  count=len(deposition_ids), sample_ids=deposition_ids[:10])
        
        successful = []
        failed = []
        
        for i, deposition_id in enumerate(deposition_ids, 1):
            try:
                log_event("DEBUG", "process_deposition", deposition_id=deposition_id, 
                          progress=f"{i}/{len(deposition_ids)}")
                
                if self.migrate_deposition(deposition_id, dry_run):
                    successful.append(deposition_id)
                else:
                    failed.append(deposition_id)
            except Exception as e:
                log_event("ERROR", "deposition_exception", deposition_id=deposition_id, 
                          message=str(e))
                failed.append(deposition_id)
        
        log_event("INFO", "end_directory", directory=directory_path,
                  successful=len(successful), failed=len(failed),
                  success_ids=successful[:10], failed_ids=failed[:10])
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
    parser.add_argument("--json-log", help="Write structured JSONL logs here")
    
    args = parser.parse_args()
    
    # Setup dual logging (human + JSON)
    setup_logging(args.log_level, args.json_log)
    
    migrator = None
    try:
        log_event("INFO", "migration_start", site_id=args.site_id, 
                  deposition_id=args.deposition_id, directory=args.directory,
                  dry_run=args.dry_run, create_tables=args.create_tables)
        
        # Initialize migrator
        migrator = CifToDbMigrator(args.site_id, create_tables=args.create_tables)
        
        if args.deposition_id:
            # Migrate single deposition
            success = migrator.migrate_deposition(args.deposition_id, dry_run=args.dry_run)
            if not success:
                log_event("ERROR", "migration_failed", deposition_id=args.deposition_id)
                sys.exit(1)
        elif args.directory:
            # Migrate directory
            results = migrator.migrate_directory(args.directory, dry_run=args.dry_run)
            if results["failed"]:
                log_event("ERROR", "migration_partial_failure", failed_count=len(results["failed"]),
                          failed_ids=results["failed"][:10])
                sys.exit(1)
        else:
            logger.error("Must specify either --deposition-id or --directory")
            sys.exit(1)
            
        log_event("INFO", "migration_complete", stats=migrator.stats)
        
    except Exception as e:
        log_event("ERROR", "migration_exception", message=str(e))
        sys.exit(1)
    finally:
        if migrator:
            migrator.close()


if __name__ == "__main__":
    main()


"""
JSON Log Query Examples (with jq):

Assuming you ran with --json-log migration.jsonl:

1. Count FK/DataError by code:
   jq -r 'select(.event=="db_error" and .sql_error_code!=null) | .sql_error_code' migration.jsonl | sort | uniq -c

2. List files that failed to parse:
   jq -r 'select(.event=="read_fail") | .filename' migration.jsonl | sort -u

3. Show unresolved parent links (first 5 examples per deposition):
   jq -r 'select(.event=="unresolved_children") | [.deposition_id,.count,.examples] | @json' migration.jsonl

4. Timeline of chunk inserts:
   jq -r 'select(.event=="insert_chunk") | [.ts,.deposition_id,.pass_no,.batch,.start,.end] | @tsv' migration.jsonl

5. Per-deposition summary table:
   jq -r 'select(.event=="deposition_summary") | [.deposition_id,.messages_inserted,.unresolved] | @tsv' migration.jsonl | column -t

6. Find depositions with missing files:
   jq -r 'select(.event=="no_messages") | .deposition_id' migration.jsonl

7. Track migration performance:
   jq -r 'select(.event=="insert_chunk") | [.deposition_id,.pass_no,.batch] | @tsv' migration.jsonl | awk '{sum[$1] += $3} END {for (dep in sum) print dep, sum[dep]}'

8. Find parsing errors by message type:
   jq -r 'select(.event=="parse_exception") | [.message_type,.filename,.message] | @tsv' migration.jsonl

9. Show directory-level statistics:
   jq -r 'select(.event=="end_directory") | [.directory,.successful,.failed] | @tsv' migration.jsonl

10. List all SQL error codes and their frequency:
    jq -r 'select(.event=="db_error" and .sql_error_code) | .sql_error_code' migration.jsonl | sort -n | uniq -c | sort -nr

Field reference:
- event: process_file, parse_ok, read_fail, parse_exception, start_deposition, 
         no_messages, dry_run_summary, sorted_messages, preload_parents,
         no_progress_pass, insert_chunk, unresolved_children, insert_refs_statuses,
         deposition_summary, start_directory, end_directory, migration_start,
         migration_complete, migration_exception, db_error
- deposition_id, filename, file_path, message_type
- messages, file_refs, statuses, batch, batch_size, pass_no, start, end
- sql_error_code, operation, reason
- run_id (UUID for this migration run)
- ts (ISO timestamp), level, logger
"""