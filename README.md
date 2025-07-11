[![Build Status](https://dev.azure.com/wwPDB/wwPDB%20Python%20Projects/_apis/build/status%2FwwPDB.py-wwpdb_apps_msgmodule?branchName=master)](https://dev.azure.com/wwPDB/wwPDB%20Python%20Projects/_build/latest?definitionId=19&branchName=master)

# wwPDB Communication Module

OneDep Messaging Application with database-primary operations for efficient message handling and storage.

## Overview

The wwPDB Communication Module provides messaging capabilities for the OneDep deposition and annotation system. This implementation features a clean, database-primary architecture with minimal complexity and maximum maintainability.

### Key Features

- **Dual-Mode Backend Support**: Flexible CIF and database backend selection with independent read/write control
- **Gradual Migration Support**: Non-blocking migration from CIF to database with dual-write capability
- **Intelligent Backend Selection**: Configuration-driven backend selection with factory pattern
- **Production-Ready**: Robust feature flags, clean error handling, and comprehensive validation
- **Migration Tools**: Complete CIF-to-database migration and validation utilities

## Architecture

### Core Components

- **MessagingFactory**: Smart factory pattern for backend selection (CIF-only, DB-only, or dual-mode)
- **MessagingDualMode**: Dual-write service for gradual migration scenarios
- **MessagingDb**: Database-primary message operations
- **MessagingIo**: CIF-based operations (standalone or as part of dual-mode)
- **Feature Flag System**: Granular backend control with independent read/write flags
- **Database Layer**: Robust database service with connection management

### Backend Selection

The system supports flexible backend configuration with three modes:

```python
# Automatic backend selection based on configuration
messaging = MessagingFactory.create_messaging_service(
    verbose=True, 
    site_id="RCSB"
)
```

**Available Modes:**

1. **📄 CIF-only** - Traditional file-based storage (default)
2. **🗃️ Database-only** - Pure database storage  
3. **🔄 Dual-mode** - Write to both, configurable read priority (for migration)

## Backend Configuration (Dual-Mode Support)

The backend is configured using four independent feature flags:

### Feature Flags

- **`MSGDB_WRITES_ENABLED`** - Enable database writes (`true`/`false`)
- **`MSGDB_READS_ENABLED`** - Enable database reads (`true`/`false`)
- **`MSGCIF_WRITES_ENABLED`** - Enable CIF file writes (`true`/`false`)
- **`MSGCIF_READS_ENABLED`** - Enable CIF file reads (`true`/`false`)

### Migration Scenarios

#### Phase 1: Dual-write, CIF-read (Validation)
```bash
export MSGDB_WRITES_ENABLED=true
export MSGCIF_WRITES_ENABLED=true
export MSGCIF_READS_ENABLED=true
# Result: Writes to both backends, reads from CIF
```

#### Phase 2: Dual-write, DB-read (Testing)
```bash
export MSGDB_WRITES_ENABLED=true
export MSGDB_READS_ENABLED=true
export MSGCIF_WRITES_ENABLED=true
# Result: Writes to both backends, reads from database
```

#### Phase 3: Database-only (Final)
```bash
export MSGDB_WRITES_ENABLED=true
export MSGDB_READS_ENABLED=true
export MSGCIF_WRITES_ENABLED=false
# Result: Database-only operations
```

### Quick Configuration Commands

```bash
# Show current configuration
make backend-info

# Configure specific modes
make backend-cif-only              # CIF-only mode
make backend-db-only               # Database-only mode  
make backend-dual-write-cif-read   # Migration Phase 1
make backend-dual-write-db-read    # Migration Phase 2

# Migration helpers
make migration-phase-1             # Start Phase 1
make migration-phase-2             # Start Phase 2
make migration-phase-3             # Start Phase 3

# Show migration guide
make backend-migration-guide
```

### Backend Configuration

```bash
# Show current configuration
make backend-info                    # Show detailed backend configuration
make feature-flags                   # Show all feature flag status  
make health                         # Check system health

# Configure backend modes
make backend-cif-only               # Configure CIF-only mode
make backend-db-only                # Configure database-only mode
make backend-dual-write-cif-read    # Configure dual-write, CIF-read (Phase 1)
make backend-dual-write-db-read     # Configure dual-write, DB-read (Phase 2)

# Migration helpers
make migration-phase-1              # Start Migration Phase 1
make migration-phase-2              # Start Migration Phase 2  
make migration-phase-3              # Start Migration Phase 3
make backend-migration-guide        # Show detailed migration guide
```

**All messaging operations use the factory entry point:**

```python
from wwpdb.apps.msgmodule.io.MessagingFactory import create_messaging_service

messaging = create_messaging_service(req_obj, verbose=True)
# Factory automatically selects: MessagingIo, MessagingDb, or MessagingDualMode
```

## Troubleshooting Backend Selection

- Use `make backend-info` to see current configuration
- Use `make feature-flags` to see all feature flag status
- Use `MessagingFactory.get_backend_info(req_obj)` to debug backend selection
- Check that environment variables are set correctly for your desired mode

## Quick Start

### Installation

```bash
# Clone and install
git clone https://github.com/wwPDB/py-wwpdb_apps_msgmodule.git
cd py-wwpdb_apps_msgmodule
make install

# Development setup
make install-dev
```

### Configuration

Set up environment variables:

```bash
# Database configuration
export MSGDB_ENABLED=true
export MSGDB_HOST=localhost
export MSGDB_PORT=3306
export MSGDB_NAME=wwpdb_messaging
export MSGDB_USER=msgmodule_user
export MSGDB_PASS=your_password
```

### Database Setup

```bash
# Initialize database schema
python scripts/init_messaging_database.py --env development

# Migrate existing CIF data (optional)
python scripts/migrate_cif_to_db.py --deposition D_1234567890
```

### Usage Example

```python
from wwpdb.apps.msgmodule.io.MessagingFactory import MessagingFactory

# Get messaging service (automatically selects best backend)
messaging = MessagingFactory.create_messaging_service(site_id="RCSB")

# Store a message
messaging.set(
    deposition_id="D_1234567890",
    message_data={
        "sender": "annotator@rcsb.org",
        "message_text": "Please review the structure validation.",
        "message_type": "annotation_request"
    }
)

# Retrieve messages
messages = messaging.get(deposition_id="D_1234567890")
```

## Available Commands

### Testing

```bash
make test              # Run all database and integration tests
make test-unit         # Run unit tests only (excluding database tests)
make test-integration  # Run integration tests only
make test-database     # Run database operations tests only
make test-coverage     # Generate test coverage report
make test-tox          # Run tests across multiple Python versions
```

### Validation

```bash
make validate              # Run integration validation, lint, and security checks
make validate-integration  # Run only integration validation
make validate-config       # Validate configuration files
```

### Development

```bash
make lint           # Run code linting (pylint, flake8)
make format         # Format code with black
make check-format   # Check code formatting without making changes
make security       # Run security checks (safety, bandit)
make docs           # Generate documentation
make serve-docs     # Serve documentation locally (if using mkdocs)
make docs-check     # Check documentation for issues
```

### Deployment

```bash
make deploy             # Deploy to environment (ENV=development|staging|production)
make deploy-dev         # Deploy to development environment
make deploy-staging     # Deploy to staging environment
make deploy-production  # Deploy to production environment
```

### Feature Flags

```bash
make feature-flags                # Show current feature flag status
make feature-flags-enable FLAG=flag_name   # Enable a feature flag
make feature-flags-disable FLAG=flag_name  # Disable a feature flag
```

### Monitoring and Health

```bash
make health    # System health check
make status    # Show current system status
make monitor   # Start monitoring (placeholder)
```

### Backup and Restore

```bash
make backup                # Create backup of configuration and data
make restore BACKUP=dir    # Restore from backup directory
```

### Development Workflow

```bash
make dev-setup     # Setup complete development environment
make dev-test      # Quick development test cycle
make dev-commit    # Run checks before committing
```

### CI/CD

```bash
make ci-test    # Run CI test pipeline
make ci-deploy  # Run CI deployment pipeline
```

### Troubleshooting

```bash
make troubleshoot   # Show troubleshooting information
make logs           # Show recent logs
```

## Project Structure

```text
wwpdb/apps/msgmodule/
├── io/                     # I/O operations and backend implementations
│   ├── MessagingFactory.py # Factory for backend selection
│   ├── MessagingDb.py      # Database-primary operations
│   └── MessagingIo.py      # Legacy CIF operations
├── db/                     # Database layer
│   ├── messaging_dal.py    # Data access layer
│   └── config.py          # Database configuration
├── util/                   # Utilities and feature management
│   ├── FeatureFlagManager.py
│   └── AutoMessage.py
├── webapp/                # Web application components
└── depict/               # Message rendering and templates

scripts/                   # Operational scripts
├── init_messaging_database.py
├── migrate_cif_to_db.py
├── validate_integration.py
└── makefile_utils.py

wwpdb/apps/tests-msgmodule/ # Test suite
```

## Migration from CIF Files

The system supports migration from existing CIF-based message files:

```bash
# Single deposition
python scripts/migrate_cif_to_db.py --deposition D_1234567890

# Bulk migration
python scripts/migrate_cif_to_db.py --depositions-file deposition_list.txt

# Directory scan
python scripts/migrate_cif_to_db.py --directory /path/to/cif/files
```

## Feature Flags

The system uses a minimal set of production-ready feature flags:

- `database_writes_enabled`: Enable/disable database write operations
- `database_reads_enabled`: Enable/disable database read operations  
- `circuit_breaker`: Database circuit breaker for fault tolerance
- `connection_pooling`: Database connection pooling optimization

## Monitoring and Health Checks

### Built-in Health Check

```bash
make health    # System health check
```

Provides real-time status on:

- Database connectivity
- Feature flag status
- Circuit breaker state
- System resource utilization

### Integration Validation

```bash
python scripts/validate_integration.py
```

Validates:

- Database backend functionality
- CIF backend availability
- Backend selection logic
- Test suite execution

## Production Deployment

1. **Database Setup**: Initialize production database
2. **Configuration**: Set production environment variables
3. **Migration**: Migrate existing CIF data if needed
4. **Validation**: Run integration validation
5. **Monitoring**: Set up health check monitoring

See `scripts/SCRIPTS_README.md` for detailed script documentation.

## Contributing

1. Follow the established patterns for backend selection via MessagingFactory
2. Maintain minimal feature flag usage
3. Ensure all changes pass the full test suite
4. Add appropriate documentation and examples

## Support

For issues, questions, or contributions related to the wwPDB Communication Module, please refer to the project documentation or contact the wwPDB development team.
