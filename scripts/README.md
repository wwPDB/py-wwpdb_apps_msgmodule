# Scripts Directory - Essential Operations

This directory contains essential scripts for the wwPDB Communication Module.

## Available Scripts

### Core Operations

#### `init_messaging_database.py`

**Purpose**: Initialize the messaging database schema and tables

**Usage**:

```bash
python scripts/init_messaging_database.py --help
python scripts/init_messaging_database.py --env production
```

**When to use**:

- Initial database setup for new environments
- Schema updates during deployment
- Recovery after database corruption

#### `migrate_cif_to_db.py`

**Purpose**: Migrate existing CIF-based message data to the database

**Usage**:

```bash
python scripts/migrate_cif_to_db.py --help
python scripts/migrate_cif_to_db.py --deposition D_1234567890
python scripts/migrate_cif_to_db.py --batch --limit 100
```

**When to use**:

- Initial migration from CIF files to database
- Incremental migration of specific depositions
- Data recovery and synchronization

## Backend Configuration

No additional scripts needed! Backend selection is now controlled by a single environment variable:

```bash
# Use CIF files (default)
export WWPDB_MESSAGING_BACKEND=cif

# Use database backend  
export WWPDB_MESSAGING_BACKEND=database
```

## Environment Configuration

The scripts respect standard database environment variables:

- `MSGDB_HOST`: Database host
- `MSGDB_PORT`: Database port  
- `MSGDB_NAME`: Database name
- `MSGDB_USER`: Database user
- `MSGDB_PASS`: Database password

## Script Integration

Both scripts work seamlessly with the simplified messaging architecture:

- **Database initialization** sets up the schema for database operations
- **Migration script** moves data from CIF files to database storage
- **Backend selection** happens automatically via `WWPDB_MESSAGING_BACKEND`

## Quick Start

1. **Initialize database** (first time setup):
   ```bash
   python scripts/init_messaging_database.py
   ```

2. **Migrate existing data** (optional):
   ```bash
   python scripts/migrate_cif_to_db.py --batch
   ```

3. **Switch to database backend**:
   ```bash
   export WWPDB_MESSAGING_BACKEND=database
   ```

That's it! The system will now use the database for all messaging operations.

### Configuration

- **`WWPDB_MESSAGING_BACKEND=database`** - Use database backend
- **`WWPDB_MESSAGING_BACKEND=cif`** or unset - Use CIF backend (default)

### Backend Modes

1. **📄 CIF-only** (default): Traditional file-based storage
2. **🗃️ Database-only**: Modern database storage

**Always use the factory pattern through `MessagingFactory.create_messaging_backend()` - direct instantiation of MessagingDb/MessagingIo is discouraged.**

## Troubleshooting

- Check that the `WWPDB_MESSAGING_BACKEND` environment variable is set correctly
- Use the `--verbose` flag on scripts to see detailed output
- Monitor script logs for errors during database operations

## Best Practices

1. **Always test scripts in development** before running in production
2. **Use appropriate environment variables** for different environments
3. **Monitor script output** and logs for errors
4. **Backup data** before running migration scripts
5. **Run validation** after any database operations
