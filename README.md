[![Build Status](https://dev.azure.com/wwPDB/wwPDB%20Python%20Projects/_apis/build/status%2FwwPDB.py-wwpdb_apps_msgmodule?branchName=master)](https://dev.azure.com/wwPDB/wwPDB%20Python%20Projects/_build/latest?definitionId=19&branchName=master)

# wwPDB Communication Module

OneDep Messaging Application with database-primary operations for efficient message handling and storage.

## Overview

The wwPDB Communication Module provides messaging capabilities for the OneDep deposition and annotation system. This implementation features a clean, database-primary architecture with minimal complexity and maximum maintainability.

### Key Features

- **Database-Primary Storage**: Efficient relational database storage for messages, file references, and status tracking
- **Intelligent Backend Selection**: Configuration-driven backend selection with factory pattern
- **Production-Ready**: Minimal feature flags, no dual-write complexity, clean error handling
- **Migration Support**: Comprehensive CIF-to-database migration tools
- **Validation & Monitoring**: Built-in health checks and integration validation

## Architecture

### Core Components

- **MessagingFactory**: Clean factory pattern for backend selection
- **MessagingDb**: Database-primary message operations
- **MessagingIo**: Legacy CIF-based operations (fallback only)
- **FeatureFlagManager**: Minimal, production-focused feature flag system
- **Database Layer**: Robust database service with connection management

### Backend Selection

The system uses a configuration-driven approach:

```python
# Automatic backend selection based on configuration
messaging = MessagingFactory.create_messaging_service(
    verbose=True, 
    site_id="RCSB"
)
```

Backend selection priority:

1. **Database** (primary) - when `MSGDB_ENABLED=true` and database is available
2. **CIF files** (fallback) - when database is unavailable or disabled

## Quick Start

### Installation

```bash
# Clone and install
git clone <repository-url>
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
make test              # Run all tests
make test-unit         # Unit tests only
make test-integration  # Integration tests only
make test-database     # Database tests only
```

### Validation

```bash
make validate          # Comprehensive validation
make validate-integration # Integration validation
make health            # System health check
```

### Development

```bash
make lint              # Code linting
make format            # Code formatting
make security          # Security scanning
make docs              # Generate documentation
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
make health
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
