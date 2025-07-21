[![Build Status](https://dev.azure.com/wwPDB/wwPDB%20Python%20Projects/_apis/build/status%2FwwPDB.py-wwpdb_apps_msgmodule?branchName=master)](https://dev.azure.com/wwPDB/wwPDB%20Python%20Projects/_build/latest?definitionId=19&branchName=master)

# wwPDB Communication Module

OneDep Messaging Application with optional database backend support.

## Overview

The wwPDB Communication Module provides messaging capabilities for the OneDep deposition and annotation system. This implementation supports both traditional CIF file storage and an optional database backend.

### Key Features

- **Backend Selection**: Choice between CIF file storage (default) and database storage
- **Factory Pattern**: Clean backend selection via `MessagingFactory`
- **Migration Tools**: Scripts for database setup and CIF-to-database migration

## Configuration

### Backend Selection

The messaging module supports two storage backends, with selection controlled by ConfigInfo:

```
WWPDB_MESSAGING_BACKEND=database  # Use database backend
WWPDB_MESSAGING_BACKEND=cif       # Use CIF backend (default)
```

### Database Configuration (if using database backend)

The messaging module uses ConfigInfo for database configuration, following the standard wwPDB pattern.

Set the following keys in your site's ConfigInfo:

```
MESSAGING_DB_HOST=localhost
MESSAGING_DB_PORT=3306
MESSAGING_DB_NAME=wwpdb_messaging
MESSAGING_DB_USER=msgmodule_user
MESSAGING_DB_PASS=your_password
WWPDB_MESSAGING_BACKEND=database
```

## Usage

```python
from wwpdb.apps.msgmodule.io.MessagingFactory import MessagingFactory
from wwpdb.apps.msgmodule.models.Message import Message

# Backend automatically selected based on ConfigInfo WWPDB_MESSAGING_BACKEND
# Database configuration read from ConfigInfo
messaging = MessagingFactory.create_messaging_backend(reqObj, verbose=True)

# Create and store a message
messageObj = Message.fromReqObj(reqObj)
success, fileSuccess, failedFiles = messaging.processMsg(messageObj)

# Retrieve messages
messages = messaging.getMsgRowList(p_depDataSetId="D_1234567890")
```

## Database Setup (Optional)

```bash
# Initialize database schema
python scripts/init_messaging_database.py --env development

# Migrate existing CIF data (optional)
python scripts/migrate_cif_to_db.py --deposition D_1234567890
```

## Testing

```bash
# Run tests
python -m pytest wwpdb/apps/tests-msgmodule/
```
