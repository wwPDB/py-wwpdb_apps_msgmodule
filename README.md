[![Build Status](https://dev.azure.com/wwPDB/wwPDB%20Python%20Projects/_apis/build/status%2FwwPDB.py-wwpdb_apps_msgmodule?branchName=master)](https://dev.azure.com/wwPDB/wwPDB%20Python%20Projects/_build/latest?definitionId=19&branchName=master)

# py-wwpdb_apps_msgmodule

OneDep Messaging Application - Communication system for wwPDB deposition pipeline

## Overview

The wwPDB messaging module provides a comprehensive communication system between depositors and annotators during the structure deposition and annotation process. This module handles:

- **Messages to depositors** - Correspondence sent from annotators to depositors
- **Messages from depositors** - Replies and queries from depositors to annotators  
- **Annotator notes** - Internal notes between annotation team members
- **File attachments** - References to attached validation reports, model files, and other documents
- **Message status tracking** - Read status, action requirements, and release flags

## Architecture Evolution: CIF-based → Database-backed

### Historical CIF-based Backend

Previously, all message data was stored in mmCIF format files on the filesystem:

```
D_1000000001/
├── D_1000000001_messages-to-depositor_P1.cif.V1
├── D_1000000001_messages-from-depositor_P1.cif.V1
└── D_1000000001_notes-from-annotator_P1.cif.V1
```

**Advantages of CIF-based storage:**
- Simple file-based architecture
- Easy to inspect and debug
- Natural alignment with PDBx/mmCIF data model
- Version control via filesystem

**Limitations:**
- Inefficient querying across multiple depositions
- File locking contention in high-concurrency scenarios
- Difficult to generate aggregate reports
- No transaction support for atomic updates
- Limited indexing and search capabilities

### New Database-backed Backend

The DAOTHER-10245 project introduces a MySQL database backend that directly mirrors the mmCIF category structure while providing modern database capabilities.

**Key advantages:**
- **Fast querying** - Indexed searches across all messages
- **Transactional integrity** - ACID guarantees for concurrent updates
- **Scalability** - Handles thousands of depositions efficiently
- **Reporting** - Easy aggregate statistics and analytics
- **Connection pooling** - Optimized resource usage
- **Full-text search** - Efficient message content searching

**Backward compatibility:**
- Drop-in replacement for existing CIF-based code
- Same API interface maintained
- Migration scripts provided for historical data
- Export capability back to CIF format for archival

## Database Schema

Three tables mirror the mmCIF categories:

### 1. `pdbx_deposition_message_info`

Core message table storing message content and metadata.

| Column | Type | Description |
|--------|------|-------------|
| `ordinal_id` | BIGINT | Auto-incrementing primary key |
| `message_id` | VARCHAR(255) | Unique message identifier (indexed) |
| `deposition_data_set_id` | VARCHAR(50) | Deposition ID (e.g., D_1000000001) |
| `timestamp` | DATETIME | Message creation timestamp |
| `sender` | VARCHAR(255) | Email or ID of sender |
| `context_type` | VARCHAR(50) | Context category (e.g., 'EM', 'validation') |
| `context_value` | VARCHAR(255) | Context-specific value |
| `parent_message_id` | VARCHAR(255) | Parent message for threading (FK) |
| `message_subject` | TEXT | Subject line |
| `message_text` | LONGTEXT | Full message body |
| `message_type` | VARCHAR(20) | Format type (default: 'text') |
| `send_status` | CHAR(1) | Send status ('Y'/'N') |
| `content_type` | ENUM | Message category (see below) |
| `created_at` | DATETIME | Record creation timestamp |
| `updated_at` | DATETIME | Last update timestamp |

**Content types:**
- `messages-to-depositor` - Annotator → Depositor communication
- `messages-from-depositor` - Depositor → Annotator communication  
- `notes-from-annotator` - Internal annotator notes

### 2. `pdbx_deposition_message_file_reference`

File attachment metadata table.

| Column | Type | Description |
|--------|------|-------------|
| `ordinal_id` | BIGINT | Auto-incrementing primary key |
| `message_id` | VARCHAR(255) | Parent message ID (FK) |
| `deposition_data_set_id` | VARCHAR(50) | Deposition ID |
| `content_type` | VARCHAR(50) | File type (e.g., 'model', 'sf') |
| `content_format` | VARCHAR(20) | Format (e.g., 'pdbx', 'pdf') |
| `partition_number` | INT | Partition for versioning |
| `version_id` | INT | Version number |
| `storage_type` | VARCHAR(20) | Storage location |
| `upload_file_name` | VARCHAR(255) | Original filename |
| `created_at` | DATETIME | Record creation timestamp |

**Unique constraint:** `(message_id, content_type, version_id, partition_number)`

### 3. `pdbx_deposition_message_status`

Message status tracking table.

| Column | Type | Description |
|--------|------|-------------|
| `message_id` | VARCHAR(255) | Message ID (PK, FK) |
| `deposition_data_set_id` | VARCHAR(50) | Deposition ID |
| `read_status` | CHAR(1) | Read flag ('Y'/'N') |
| `action_reqd` | CHAR(1) | Action required flag ('Y'/'N') |
| `for_release` | CHAR(1) | Release flag ('Y'/'N') |
| `created_at` | DATETIME | Record creation timestamp |
| `updated_at` | DATETIME | Last update timestamp |

## Configuration

Add messaging-specific database configuration to your site config:

```bash
# Site configuration file (e.g., site.cfg)

# Messaging database configuration
SITE_MESSAGE_DB_HOST_NAME=your_db_host
SITE_MESSAGE_DB_PORT_NUMBER=3306
SITE_MESSAGE_DB_NAME=wwpdb_messaging
SITE_MESSAGE_DB_USER_NAME=messaging_user
SITE_MESSAGE_DB_PASSWORD=your_password
SITE_MESSAGE_DB_SOCKET=/path/to/socket  # Optional: Unix socket path
```

## Migration Scripts

Three utilities are provided for data migration and maintenance:

### 1. `init_messaging_database.py`

Initialize database schema with proper indexes and foreign keys.

```bash
# Create schema
python init_messaging_database.py --site-id RCSB

# Verify existing schema
python init_messaging_database.py --site-id RCSB --verify-only

# Drop and recreate (DESTRUCTIVE - use with caution)
python init_messaging_database.py --site-id RCSB --drop-and-recreate
```

### 2. `migrate_cif_to_db.py`

Import historical CIF message files into database. **Idempotent** - safe to re-run.

```bash
# Dry run (no database writes)
python migrate_cif_to_db.py --site-id RCSB --directory /path/to/D_* --dry-run

# Single deposition
python migrate_cif_to_db.py --site-id RCSB --deposition D_1000000001

# Bulk migration with structured logging
python migrate_cif_to_db.py --site-id RCSB --directory /path/to/D_* \
    --json-log migration.json --log-level INFO

# Create tables if needed
python migrate_cif_to_db.py --site-id RCSB --directory /path/to/D_* \
    --create-tables
```

**Key features:**
- Duplicate-safe: existing `message_id` entries are skipped
- UTF-8 encoding with Unicode escape handling
- Preserves parent-child message relationships
- Structured JSON logging for audit trails

### 3. `dump_db_to_cif.py`

Export messages from database back to CIF format files. **Reverse operation** of `migrate_cif_to_db.py`.

```bash
# Export single deposition
python dump_db_to_cif.py --site-id RCSB --deposition D_1000000001

# Export multiple depositions
python dump_db_to_cif.py --site-id RCSB \
    --depositions D_1000000001 D_1000000002

# Export all depositions with custom output
python dump_db_to_cif.py --site-id RCSB --all \
    --output-dir /backup/cif-export --overwrite

# With structured logging
python dump_db_to_cif.py --site-id RCSB --all \
    --json-log export.json --log-level DEBUG
```

**Key features:**
- Uses `gemmi` library for proper CIF formatting
- ASCII escaping for non-ASCII characters with Unicode preservation
- Bulk export with progress reporting
- Organized output by content type

## Code Structure

### Core Database Layer (`wwpdb/apps/msgmodule/db/`)

- **`Models.py`** - SQLAlchemy ORM models mapping to mmCIF categories
- **`DataAccessLayer.py`** - Database access facade with DAO pattern
- **`PdbxMessageIo.py`** - Drop-in replacement for CIF-based PdbxMessageIo
- **`MessagingDataImport.py`** - Import adapter for workflow integration
- **`MessagingDataExport.py`** - Export adapter for workflow integration
- **`LockFile.py`** - File locking utilities for backward compatibility

### Application Layer (`wwpdb/apps/msgmodule/`)

- **`io/MessagingIo.py`** - High-level messaging API for web applications
- **`models/Message.py`** - Message data model abstraction
- **`util/`** - Utility modules (auto-messaging, message extraction, routing)
- **`depict/`** - Message rendering and templating
- **`webapp/`** - Web application entry points and WSGI integration

## API Example

```python
from wwpdb.apps.msgmodule.db import DataAccessLayer, MessageInfo
from wwpdb.utils.config.ConfigInfo import ConfigInfo
from datetime import datetime

# Initialize database access
config = ConfigInfo('RCSB')
db_config = {
    'host': config.get('SITE_MESSAGE_DB_HOST_NAME'),
    'port': int(config.get('SITE_MESSAGE_DB_PORT_NUMBER', '3306')),
    'database': config.get('SITE_MESSAGE_DB_NAME'),
    'username': config.get('SITE_MESSAGE_DB_USER_NAME'),
    'password': config.get('SITE_MESSAGE_DB_PASSWORD', ''),
    'charset': 'utf8mb4',
}
dal = DataAccessLayer(db_config)

# Create a message
message = MessageInfo(
    message_id='MSG_20251027_001',
    deposition_data_set_id='D_1000000001',
    timestamp=datetime.now(),
    sender='annotator@rcsb.org',
    message_subject='Validation Report Available',
    message_text='Your validation report is ready for review.',
    content_type='messages-to-depositor',
    send_status='Y'
)
dal.create_message(message)

# Query messages
messages = dal.get_deposition_messages('D_1000000001')
for msg in messages:
    print(f"{msg.timestamp}: {msg.message_subject}")

# Clean up
dal.close()
```

## Testing

Comprehensive test suite covering:

- **Unit tests** - Individual component testing with mocks
- **Integration tests** - Database operations and API behavior
- **Round-trip tests** - CIF → Database → CIF integrity validation
- **Persistence tests** - Message lifecycle verification

Run tests with:

```bash
export WWPDB_SITE_ID=RCSB  # or your site ID
pytest wwpdb/apps/tests-msgmodule/ -v -s
```

## Migration Strategy

For sites migrating from CIF-based to database-backed storage, we recommend a phased approach:

### Phase 1: Setup and Validation
- Configure database connection parameters
- Create database schema using `init_messaging_database.py`
- Perform dry-run migrations to validate configuration
- Test database connectivity and permissions

### Phase 2: Historical Data Migration
- Use `migrate_cif_to_db.py` to import existing CIF files
- Start with a small sample of depositions for validation
- Verify data integrity using round-trip export/import tests
- Scale up to bulk migration after validation passes

### Phase 3: Parallel Operation (Optional)
- Run both CIF and database backends in parallel
- Compare outputs for consistency
- Build confidence in database backend
- Identify and resolve any edge cases

### Phase 4: Incremental Updates
- Periodically re-run migration to catch new CIF messages
- Use idempotent migration to safely update database
- Monitor for any data inconsistencies
- Adjust migration parameters as needed

### Phase 5: Backend Switch-over
- Coordinate with stakeholders for maintenance window
- Pause new message creation during transition
- Run final migration to ensure all data is current
- Switch application configuration to use database backend
- Perform smoke tests on critical workflows
- Resume normal operations

### Phase 6: Monitoring and Optimization
- Monitor database performance and query patterns
- Optimize indexes based on actual usage
- Perform regular integrity checks
- Archive old CIF files as backup
- Document any site-specific considerations

**Key Success Factors:**
- **Idempotent operations** - All migration scripts can be safely re-run
- **Comprehensive logging** - JSON logs provide audit trail
- **Rollback capability** - `dump_db_to_cif.py` enables reverting to CIF if needed
- **Incremental deployment** - Test with small datasets before full migration
- **Data validation** - Round-trip tests ensure no data loss

## Dependencies

New dependencies added for database backend:

```
sqlalchemy >= 1.4.0   # ORM and database abstraction
pymysql >= 1.0.0      # MySQL database connector
gemmi >= 0.6.0        # CIF writing library for export
```

## Documentation

- **Sphinx API Documentation** - All new code includes Sphinx-standard docstrings
- **Migration Guide** - See `scripts/README` for detailed migration procedures
- **Database Schema** - Full schema documentation above

## Support

For questions or issues:

- **GitHub Issues**: [py-wwpdb_apps_msgmodule](https://github.com/wwPDB/py-wwpdb_apps_msgmodule/issues)
- **Email**: Jasmine Young <jasmine@rcsb.rutgers.edu>

## License

Apache 2.0

## Authors

- **Original CIF-based implementation**: Ezra Peisach, John Westbrook
- **Database backend implementation**: Lucas Carrijo de Oliveira (DAOTHER-10245)

