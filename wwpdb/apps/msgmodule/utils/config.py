#!/usr/bin/env python
"""
Configuration utility for database-backed messaging.

This utility helps configure the messaging system to use the database backend
instead of CIF files. Uses ConfigInfo for all configuration management.
"""

import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


def create_message_io_instance(site_id: Optional[str] = None, verbose: bool = True, log=None, **kwargs):
    """Factory function to create the database-backed message I/O instance
    
    Args:
        site_id: Site ID for ConfigInfo configuration (optional - will auto-detect if not provided)
        verbose: Enable verbose logging
        log: Log file handle
        **kwargs: Additional arguments passed to constructor
        
    Returns:
        PdbxMessageIo instance (database-backed)
    """
    try:
        from wwpdb.apps.msgmodule.db import PdbxMessageIo
        logger.info(f"Creating database-backed PdbxMessageIo for site {site_id or 'auto-detect'}")
        return PdbxMessageIo(site_id=site_id, verbose=verbose, log=log, **kwargs)
    except ImportError as e:
        logger.error(f"Database backend not available: {e}")
        raise RuntimeError(f"Database backend not available: {e}")


def configure_logging(level: str = "INFO") -> None:
    """Configure logging for the messaging system
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR)
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )


def validate_database_config(site_id: Optional[str] = None) -> Dict[str, Any]:
    """Validate database configuration for the messaging system
    
    Args:
        site_id: Site ID to validate (will auto-detect if not provided)
        
    Returns:
        Dictionary with validation results
        
    Raises:
        RuntimeError: If configuration is invalid
    """
    if site_id is None:
        import os
        site_id = os.getenv('WWPDB_SITE_ID') or os.getenv('SITE_ID') or 'WWPDB_DEV'
    
    try:
        from wwpdb.utils.config.ConfigInfo import ConfigInfo
        config_info = ConfigInfo(site_id)
        
        # Check required configuration keys
        required_keys = [
            "SITE_DB_HOST_NAME",
            "SITE_DB_ADMIN_USER", 
            "WWPDB_MESSAGING_DB_NAME"
        ]
        
        config = {}
        missing_keys = []
        
        for key in required_keys:
            value = config_info.get(key)
            if value:
                config[key] = value
            else:
                missing_keys.append(key)
        
        # Optional keys with defaults
        config["SITE_DB_PORT_NUMBER"] = config_info.get("SITE_DB_PORT_NUMBER", "3306")
        config["SITE_DB_ADMIN_PASS"] = config_info.get("SITE_DB_ADMIN_PASS", "")
        
        if missing_keys:
            raise RuntimeError(f"Missing required configuration keys: {', '.join(missing_keys)}")
        
        return {
            "valid": True,
            "site_id": site_id,
            "config": config,
            "message": f"Database configuration valid for site {site_id}"
        }
        
    except Exception as e:
        return {
            "valid": False,
            "site_id": site_id,
            "config": {},
            "message": f"Database configuration invalid: {e}"
        }


def test_database_connection(site_id: str) -> bool:
    """Test database connection for the messaging system
    
    Args:
        site_id: Site ID to test (required)
        
    Returns:
        True if connection successful, False otherwise
    """
    try:
        from wwpdb.apps.msgmodule.db import DataAccessLayer
        from wwpdb.utils.config.ConfigInfo import ConfigInfo
        
        config_info = ConfigInfo(site_id)
        
        db_config = {
            "host": config_info.get("SITE_DB_HOST_NAME"),
            "port": int(config_info.get("SITE_DB_PORT_NUMBER", "3306")),
            "database": config_info.get("WWPDB_MESSAGING_DB_NAME"),
            "username": config_info.get("SITE_DB_ADMIN_USER"),
            "password": config_info.get("SITE_DB_ADMIN_PASS", ""),
            "charset": "utf8mb4",
        }
        
        # Test connection
        dal = DataAccessLayer(db_config)
        
        # Try a simple query
        messages = dal.get_deposition_messages("TEST_CONNECTION")
        
        dal.close()
        
        logger.info(f"Database connection test successful for site {site_id}")
        return True
        
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False


if __name__ == "__main__":
    import sys
    
    # Simple CLI for testing configuration
    configure_logging("INFO")
    
    if len(sys.argv) < 2:
        print("Usage: python config.py <command> <site_id>")
        print("Commands: test, validate")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if len(sys.argv) < 3:
        print(f"Error: site_id is required for {command} command")
        sys.exit(1)
    
    site_id = sys.argv[2]
    
    if command == "test":
        success = test_database_connection(site_id)
        sys.exit(0 if success else 1)
        
    elif command == "validate":
        result = validate_database_config(site_id)
        print(result["message"])
        sys.exit(0 if result["valid"] else 1)
        
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
