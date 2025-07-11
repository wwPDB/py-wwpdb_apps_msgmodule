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

## Removed Scripts

The following scripts were removed during cleanup:

- `debug_database_integration.py` - Referenced obsolete database classes
- `phase2_integration_demo.py` - Referenced removed hybrid/dual-write functionality  
- `validate_phase2.py` - Superseded by validate_integration.py
- `workflow_demo.py` - Demo script, not production code
- `generate_test_coverage_report.py` - Replaced by proper pytest/tox coverage

## Best Practices

1. **Always test scripts in development** before running in production
2. **Use appropriate environment variables** for different environments
3. **Monitor script output** and logs for errors
4. **Backup data** before running migration scripts
5. **Run validation** after any database operations
