"""
Configuration management for messaging database integration.

Simple configuration - no dual-mode complexity.
Either use database OR CIF files, controlled by environment variable.
"""

import os
import logging
from typing import Dict, Optional, Tuple, List, Any

logger = logging.getLogger(__name__)


class MessagingDatabaseConfig:
    """Simple database configuration for the messaging module"""

    def __init__(self, site_id: str = None, config_info=None):
        """
        Initialize database configuration.

        Args:
            site_id: Site identifier (RCSB, PDBe, PDBj, etc.)
            config_info: ConfigInfo instance (optional)
        """
        self.site_id = site_id
        self.config_info = config_info

    def get_database_config(self) -> Dict:
        """
        Get database configuration from various sources in priority order:
        1. Environment variables (for development/testing)
        2. ConfigInfo system (for production)
        3. Default values (fallback)

        Returns:
            Dict containing database configuration
        """
        config = {}

        # Try environment variables first (highest priority)
        env_config = self._get_env_config()
        if env_config:
            logger.info("Using database configuration from environment variables")
            config.update(env_config)
            return config

        # Try ConfigInfo system
        if self.config_info:
            config_info_config = self._get_config_info_config()
            if config_info_config:
                logger.info("Using database configuration from ConfigInfo")
                config.update(config_info_config)
                return config

        # Fallback to defaults
        logger.warning(
            "Using fallback database configuration - not suitable for production"
        )
        config.update(self._get_default_config())
        return config

    def _get_env_config(self) -> Optional[Dict]:
        """Get configuration from environment variables"""
        # Check if we have the minimum required environment variables
        required_vars = ["MSGDB_HOST", "MSGDB_USER", "MSGDB_NAME"]
        if not all(os.getenv(var) for var in required_vars):
            return None

        return {
            "host": os.getenv("MSGDB_HOST"),
            "port": int(os.getenv("MSGDB_PORT", "3306")),
            "database": os.getenv("MSGDB_NAME"),
            "username": os.getenv("MSGDB_USER"),
            "password": os.getenv("MSGDB_PASS", ""),
            "charset": os.getenv("MSGDB_CHARSET", "utf8mb4"),
            "pool_size": int(os.getenv("MSGDB_POOL_SIZE", "20")),
            "timeout": int(os.getenv("MSGDB_TIMEOUT", "30")),
        }

    def _get_config_info_config(self) -> Optional[Dict]:
        """Get configuration from ConfigInfo system"""
        if not self.config_info:
            return None

        try:
            return {
                "host": self.config_info.get("MESSAGING_DB_HOST"),
                "port": int(self.config_info.get("MESSAGING_DB_PORT", "3306")),
                "database": self.config_info.get("MESSAGING_DB_NAME"),
                "username": self.config_info.get("MESSAGING_DB_USER"),
                "password": self.config_info.get("MESSAGING_DB_PASS"),
                "pool_size": int(self.config_info.get("MESSAGING_DB_POOL_SIZE", "10")),
                "pool_recycle": int(self.config_info.get("MESSAGING_DB_POOL_RECYCLE", "3600")),
                "charset": "utf8mb4",
            }
        except Exception as e:
            logger.error(f"Failed to read database config from ConfigInfo: {e}")
            return None
        # Try ConfigInfo system (production)
        if self.config_info:
            logger.info("Using database configuration from ConfigInfo")
            try:
                config_info_config = self._get_config_info_config()
                if config_info_config:
                    config.update(config_info_config)
                    return config
            except Exception as e:
                logger.warning(f"Failed to get configuration from ConfigInfo: {e}")

        # Use defaults as fallback
        logger.info("Using default database configuration")
        config.update(self._get_default_config())
        return config

    def _get_env_config(self) -> Optional[Dict]:
        """Get configuration from environment variables"""
        # Check if any database env vars are set
        env_vars = [
            "MSGDB_HOST", "MSGDB_PORT", "MSGDB_NAME", 
            "MSGDB_USER", "MSGDB_PASS"
        ]
        
        if not any(os.getenv(var) for var in env_vars):
            return None

        return {
            "host": os.getenv("MSGDB_HOST", "localhost"),
            "port": int(os.getenv("MSGDB_PORT", "3306")),
            "database": os.getenv("MSGDB_NAME", "wwpdb_messaging"),
            "username": os.getenv("MSGDB_USER", "msgdb_user"),
            "password": os.getenv("MSGDB_PASS", ""),
            "pool_size": int(os.getenv("MSGDB_POOL_SIZE", "10")),
            "pool_recycle": int(os.getenv("MSGDB_POOL_RECYCLE", "3600")),
            "charset": "utf8mb4",
        }

    def _get_config_info_config(self) -> Optional[Dict]:
        """Get configuration from ConfigInfo system"""
        if not self.config_info:
            return None

        try:
            return {
                "host": self.config_info.get("MESSAGING_DB_HOST"),
                "port": int(self.config_info.get("MESSAGING_DB_PORT", "3306")),
                "database": self.config_info.get("MESSAGING_DB_NAME"),
                "username": self.config_info.get("MESSAGING_DB_USER"),
                "password": self.config_info.get("MESSAGING_DB_PASS"),
                "pool_size": int(self.config_info.get("MESSAGING_DB_POOL_SIZE", "10")),
                "pool_recycle": int(self.config_info.get("MESSAGING_DB_POOL_RECYCLE", "3600")),
                "charset": "utf8mb4",
            }
        except Exception as e:
            logger.error(f"Failed to read database config from ConfigInfo: {e}")
            return None

    def _get_default_config(self) -> Dict:
        """Get default configuration (for development)"""
        return {
            "host": "localhost",
            "port": 3306,
            "database": "wwpdb_messaging",
            "username": "msgdb_user",
            "password": "",
            "pool_size": 10,
            "pool_recycle": 3600,
            "charset": "utf8mb4",
        }

    def validate_config(self, config: Dict) -> bool:
        """
        Validate database configuration.

        Args:
            config: Configuration dictionary to validate

        Returns:
            True if configuration is valid
        """
        required_keys = ["host", "port", "database", "username"]

        # Check required keys
        for key in required_keys:
            if key not in config:
                logger.error(f"Missing required database configuration: {key}")
                return False

        # Validate data types
        if not isinstance(config["port"], int) or config["port"] <= 0:
            logger.error("Database port must be a positive integer")
            return False

        if "pool_size" in config:
            if not isinstance(config["pool_size"], int) or config["pool_size"] <= 0:
                logger.error("Database pool_size must be a positive integer")
                return False

        return True


def get_messaging_database_config(site_id: str = None, config_info=None) -> Dict:
    """
    Convenience function to get messaging database configuration.

    Args:
        site_id: Site identifier
        config_info: ConfigInfo instance

    Returns:
        Database configuration dictionary
    """
    db_config = MessagingDatabaseConfig(site_id, config_info)
    config = db_config.get_database_config()

    if not db_config.validate_config(config):
        raise ValueError("Invalid database configuration")

    return config


def is_messaging_database_enabled(site_id: str = None, config_info=None) -> bool:
    """
    Check if database backend is enabled via environment variable.
    
    Args:
        site_id: Site identifier (ignored in simplified version)
        config_info: ConfigInfo instance (ignored in simplified version)
        
    Returns:
        True if database backend is enabled, False otherwise
    """
    backend = os.environ.get("WWPDB_MESSAGING_BACKEND", "cif").lower().strip()
    return backend == "database"
