"""
Simplified database configuration for messaging module.
"""

import os
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class MessagingDatabaseConfig:
    """Simple database configuration"""

    def __init__(self, config_info=None):
        """Initialize database configuration."""
        self.config_info = config_info

    def get_database_config(self) -> Dict:
        """
        Get database configuration from environment variables or ConfigInfo.
        
        Returns:
            Dict containing database configuration
        """
        # Try environment variables first
        env_config = self._get_env_config()
        if env_config:
            logger.info("Using database configuration from environment variables")
            return env_config

        # Try ConfigInfo system
        if self.config_info:
            config_info_config = self._get_config_info_config()
            if config_info_config:
                logger.info("Using database configuration from ConfigInfo")
                return config_info_config

        # No valid configuration found
        raise RuntimeError(
            "No database configuration found. Set MSGDB_HOST, MSGDB_USER, MSGDB_NAME "
            "environment variables or provide ConfigInfo."
        )

    def _get_env_config(self) -> Optional[Dict]:
        """Get configuration from environment variables"""
        required_vars = ["MSGDB_HOST", "MSGDB_USER", "MSGDB_NAME"]
        if not all(os.getenv(var) for var in required_vars):
            return None

        return {
            "host": os.getenv("MSGDB_HOST"),
            "port": int(os.getenv("MSGDB_PORT", "3306")),
            "database": os.getenv("MSGDB_NAME"),
            "username": os.getenv("MSGDB_USER"),
            "password": os.getenv("MSGDB_PASS", ""),
            "charset": "utf8mb4",
        }

    def _get_config_info_config(self) -> Optional[Dict]:
        """Get configuration from ConfigInfo system"""
        try:
            return {
                "host": self.config_info.get("MESSAGING_DB_HOST"),
                "port": int(self.config_info.get("MESSAGING_DB_PORT", "3306")),
                "database": self.config_info.get("MESSAGING_DB_NAME"),
                "username": self.config_info.get("MESSAGING_DB_USER"),
                "password": self.config_info.get("MESSAGING_DB_PASS", ""),
                "charset": "utf8mb4",
            }
        except Exception as e:
            logger.error(f"Failed to read database config from ConfigInfo: {e}")
            return None


def get_messaging_database_config(config_info=None) -> Dict:
    """
    Get messaging database configuration.

    Args:
        config_info: ConfigInfo instance

    Returns:
        Database configuration dictionary
    """
    db_config = MessagingDatabaseConfig(config_info)
    return db_config.get_database_config()


def is_messaging_database_enabled() -> bool:
    """
    Check if database backend is enabled via environment variable.
    
    Returns:
        True if database backend is enabled, False otherwise
    """
    backend = os.environ.get("WWPDB_MESSAGING_BACKEND", "cif").lower().strip()
    return backend == "database"
