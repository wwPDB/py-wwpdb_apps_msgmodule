# Scripts Directory - Database Operations

This directory contains essential scripts for the wwPDB Communication Module database operations.

## Available Scripts

### Production Scripts

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

#### `validate_integration.py`

**Purpose**: Validate database operations integration

**Usage**:

```bash
python scripts/validate_integration.py
```

**When to use**:

- Pre-deployment validation
- Post-deployment verification
- Continuous monitoring and health checks
- Troubleshooting integration issues

### Utility Scripts

#### `makefile_utils.py`

**Purpose**: Utilities for Makefile operations and system health checks

**Usage**: Typically called from Makefile targets

```bash
make health    # Uses this script for health reporting
make status    # Uses this script for system status
```

**When to use**:

- Automated health checks
- Build system operations
- System status reporting

## Environment Configuration

All scripts respect standard environment variables:

- `MSGDB_ENABLED`: Enable/disable database operations
- `MSGDB_HOST`: Database host
- `MSGDB_PORT`: Database port
- `MSGDB_NAME`: Database name
- `MSGDB_USER`: Database user
- `MSGDB_PASS`: Database password

## Integration with Makefile

Scripts are integrated with the Makefile system:

- `make health`: System health check using makefile_utils.py
- `make validate-integration`: Integration validation using validate_integration.py
- Database initialization and migration can be added as Makefile targets

## Backend Selection in Scripts

All scripts now use the simplified backend selection logic via the messaging factory. The backend (CIF or database) is chosen at runtime based on a single environment variable:

### Configuration

- **`WWPDB_MESSAGING_BACKEND=database`** - Use database backend
- **`WWPDB_MESSAGING_BACKEND=cif`** or unset - Use CIF backend (default)

### Backend Modes

1. **📄 CIF-only** (default): Traditional file-based storage
2. **🗃️ Database-only**: Modern database storage

**Always use the factory pattern through `MessagingFactory.create_messaging_service()` or `create_messaging_service()` - direct instantiation of MessagingDb/MessagingIo is discouraged.**

## Migration Support Scripts

The simplified architecture supports clean migration with these helper scripts:

- **`backend_config.py`** - Configure backend modes and show current status  
- **`makefile_utils.py`** - Enhanced to show backend configuration and health
- **`validate_integration.py`** - Validates all backend modes and configurations

### Quick Backend Configuration

```bash
# Show current backend configuration
python scripts/backend_config.py status

# Configure backend modes
python scripts/backend_config.py cif       # Use CIF files
python scripts/backend_config.py database  # Use database
```

## Troubleshooting

- Use `python scripts/backend_config.py show` to see current configuration
- Use `make backend-info` or `make backend-status` to check Makefile integration
- Use the `--verbose` flag on scripts to see backend selection details
- Check that the WWPDB_MESSAGING_BACKEND environment variable is set correctly

## Best Practices

1. **Always test scripts in development** before running in production
2. **Use appropriate environment variables** for different environments
3. **Monitor script output** and logs for errors
4. **Backup data** before running migration scripts
5. **Run validation** after any database operations
