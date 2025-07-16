[![Build Status](https://dev.azure.com/wwPDB/wwPDB%20Python%20Projects/_apis/build/status%2FwwPDB.py-wwpdb_apps_msgmodule?branchName=master)](https://dev.azure.com/wwPDB/wwPDB%20Python%20Projects/_build/latest?definitionId=19&branchName=master)

# wwPDB Communication Module

OneDep Messaging Application with dual backend support for efficient message handling and storage.

## Overview

The wwPDB Communication Module provides messaging capabilities for the OneDep deposition and annotation system. This implementation features a clean, dual-backend architecture with minimal complexity and maximum maintainability.

### Key Features

- **Simple Backend Selection**: Clean choice between CIF and database storage
- **Easy Migration Support**: Migration from CIF to database with single environment variable
- **Factory Pattern**: Configuration-driven backend selection
- **Production-Ready**: Robust configuration, clean error handling, and comprehensive validation
- **Migration Tools**: Complete CIF-to-database migration utilities

## Architecture

### Core Components

- **MessagingFactory**: Simple factory pattern for backend selection (CIF or Database)
- **MessagingDb**: Database message operations
- **MessagingIo**: CIF-based operations (legacy)
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
2. **🗃️ Database-only** - Modern database storage

## Backend Configuration

The backend is configured using a single environment variable:

### Configuration

```bash
# Use database backend
export WWPDB_MESSAGING_BACKEND=database

# Use CIF backend (default)
export WWPDB_MESSAGING_BACKEND=cif
# or
unset WWPDB_MESSAGING_BACKEND
```

### Migration Steps

#### Test Environment Setup
```bash
# In test environment
export WWPDB_MESSAGING_BACKEND=database
export MSGDB_HOST=test-db.example.com
export MSGDB_NAME=wwpdb_messaging_test
# Test and validate database operations
```

#### Production Deployment
```bash
# In production when ready
export WWPDB_MESSAGING_BACKEND=database
export MSGDB_HOST=prod-db.example.com
export MSGDB_NAME=wwpdb_messaging
# Full database operations
```

#### Rollback (if needed)
```bash
# Immediate rollback to CIF files
unset WWPDB_MESSAGING_BACKEND
# or
export WWPDB_MESSAGING_BACKEND=cif
```

### Quick Configuration Commands

```bash
# Show current configuration
make backend-status

# Initialize database (first time)
make init-database

# Migrate existing data
make migrate-data

# Development workflow
make dev-setup                     # Setup development environment
make test                         # Run all tests
make validate                     # Run validations
```

**All messaging operations use the factory entry point:**

```python
from wwpdb.apps.msgmodule.io.MessagingFactory import create_messaging_service

messaging = create_messaging_service(req_obj, verbose=True)
# Factory automatically selects: MessagingIo or MessagingDb
```

## Troubleshooting Backend Selection

- Use `make backend-status` to see current configuration
- Check that `WWPDB_MESSAGING_BACKEND` environment variable is set correctly
- Use `MessagingFactory.create_messaging_service()` for consistent backend selection

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
make test              # Run all tests (currently database operations only)
make test-unit         # Run unit tests (DateUtil, ExtractMessage, etc.)
make test-integration  # Run integration tests (DatabaseIntegration)  
make test-database     # Run database operations tests only
```

**Test Coverage:**
- **Database operations**: 9 tests (core database functionality)
- **Unit tests**: 21 tests (utilities, data models, message extraction)
- **Integration tests**: 6 tests (database configuration and data models)

### Validation

```bash
make validate          # Run validation checks
```

### Development

```bash
make lint           # Run code linting (pylint, flake8)
make format         # Format code with black
make check-format   # Check code formatting without making changes
make security       # Run security checks (safety, bandit)
```

### Deployment

```bash
make deploy             # Deploy to environment (ENV=development|staging|production)
make deploy-dev         # Deploy to development environment
make deploy-production  # Deploy to production environment
```

### Development Workflow

```bash
make dev-setup     # Setup complete development environment
make dev-test      # Quick development test cycle
make dev-commit    # Run checks before committing
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
├── util/                   # Utilities
│   └── AutoMessage.py
├── webapp/                # Web application components
└── depict/               # Message rendering and templates

scripts/                   # Essential operational scripts
├── init_messaging_database.py
└── migrate_cif_to_db.py

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

## Validation and Testing

```bash
make test
```

Validates:

- Database backend functionality  
- CIF backend functionality
- Backend selection logic
- Complete test suite execution

## Production Deployment

1. **Database Setup**: Initialize production database
2. **Configuration**: Set production environment variables
3. **Migration**: Migrate existing CIF data if needed
4. **Validation**: Run integration validation
5. **Monitoring**: Set up health check monitoring

See `scripts/README.md` for detailed script documentation.

## Contributing

1. Follow the established patterns for backend selection via MessagingFactory
2. Use simple environment variable configuration (WWPDB_MESSAGING_BACKEND)
3. Ensure all changes pass the full test suite
4. Add appropriate documentation and examples

## Support

For issues, questions, or contributions related to the wwPDB Communication Module, please refer to the project documentation or contact the wwPDB development team.
