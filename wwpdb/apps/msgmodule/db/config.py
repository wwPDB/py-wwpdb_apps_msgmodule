"""
Configuration management for messaging database integration.

This module handles database configuration for the messaging module,
integrating with the existing wwPDB ConfigInfo system while providing
fallback mechanisms for development and testing.
"""

import os
import logging
from typing import Dict, Optional, Tuple, List, Any

logger = logging.getLogger(__name__)


class DatabaseConfig:
    """Simple database configuration class for Phase 2 compatibility"""
    
    def __init__(self):
        """Initialize database configuration"""
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from environment variables"""
        return {
            'enabled': os.getenv('MSGDB_ENABLED', 'false').lower() == 'true',
            'host': os.getenv('MSGDB_HOST', 'localhost'),
            'port': int(os.getenv('MSGDB_PORT', '3306')),
            'database': os.getenv('MSGDB_NAME', 'wwpdb_messaging'),
            'user': os.getenv('MSGDB_USER', 'msgdb_user'),
            'password': os.getenv('MSGDB_PASS', ''),
            'pool_size': int(os.getenv('MSGDB_POOL_SIZE', '10')),
            'pool_recycle': int(os.getenv('MSGDB_POOL_RECYCLE', '3600')),
            'autocommit': True,
            'charset': 'utf8mb4'
        }
    
    def is_enabled(self) -> bool:
        """Check if database is enabled"""
        return self.config.get('enabled', False)
    
    def get_config(self) -> Dict[str, Any]:
        """Get the complete configuration"""
        return dict(self.config)
    
    def validate(self) -> Tuple[bool, List[str]]:
        """Validate the configuration"""
        errors = []
        
        if self.is_enabled():
            if not self.config.get('host'):
                errors.append("Missing required database configuration: host")
            
            if not self.config.get('database'):
                errors.append("Missing required database configuration: database")
            
            if not self.config.get('user'):
                errors.append("Missing required database configuration: user")
            
            if not isinstance(self.config.get('port'), int) or self.config.get('port') <= 0:
                errors.append("Database port must be a positive integer")
        
        return len(errors) == 0, errors


class MessagingDatabaseConfig:
    """Manages database configuration for the messaging module"""
    
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
        logger.warning("Using fallback database configuration - not suitable for production")
        config.update(self._get_default_config())
        return config
    
    def _get_env_config(self) -> Optional[Dict]:
        """Get configuration from environment variables"""
        # Check if we have the minimum required environment variables
        required_vars = ['MSGDB_HOST', 'MSGDB_USER', 'MSGDB_NAME']
        if not all(os.getenv(var) for var in required_vars):
            return None
            
        return {
            'host': os.getenv('MSGDB_HOST'),
            'port': int(os.getenv('MSGDB_PORT', '3306')),
            'database': os.getenv('MSGDB_NAME'),
            'username': os.getenv('MSGDB_USER'),
            'password': os.getenv('MSGDB_PASS', ''),
            'charset': os.getenv('MSGDB_CHARSET', 'utf8mb4'),
            'pool_size': int(os.getenv('MSGDB_POOL_SIZE', '20')),
            'timeout': int(os.getenv('MSGDB_TIMEOUT', '30'))
        }
    
    def _get_config_info_config(self) -> Optional[Dict]:
        """Get configuration from ConfigInfo system"""
        try:
            # These would be the expected configuration keys in the ConfigInfo system
            # Adjust these based on actual wwPDB configuration naming conventions
            config_keys = {
                'host': 'MESSAGING_DB_HOST',
                'port': 'MESSAGING_DB_PORT', 
                'database': 'MESSAGING_DB_NAME',
                'username': 'MESSAGING_DB_USER',
                'password': 'MESSAGING_DB_PASSWORD',
                'charset': 'MESSAGING_DB_CHARSET',
                'pool_size': 'MESSAGING_DB_POOL_SIZE',
                'timeout': 'MESSAGING_DB_TIMEOUT'
            }
            
            config = {}
            for key, config_key in config_keys.items():
                value = self.config_info.get(config_key)
                if value:
                    if key in ['port', 'pool_size', 'timeout']:
                        config[key] = int(value)
                    else:
                        config[key] = value
            
            # Check if we have minimum required configuration
            if all(key in config for key in ['host', 'database', 'username']):
                # Set defaults for optional values
                config.setdefault('port', 3306)
                config.setdefault('charset', 'utf8mb4')
                config.setdefault('pool_size', 20)
                config.setdefault('timeout', 30)
                config.setdefault('password', '')
                return config
                
        except Exception as e:
            logger.warning(f"Could not get database config from ConfigInfo: {e}")
            
        return None
    
    def _get_default_config(self) -> Dict:
        """Get fallback default configuration"""
        return {
            'host': 'localhost',
            'port': 3306,
            'database': 'wwpdb_messaging_test',
            'username': 'msgmodule_test',
            'password': '',
            'charset': 'utf8mb4',
            'pool_size': 5,
            'timeout': 30
        }
    
    def validate_config(self, config: Dict) -> bool:
        """
        Validate database configuration.
        
        Args:
            config: Database configuration dictionary
            
        Returns:
            True if configuration is valid
        """
        required_keys = ['host', 'port', 'database', 'username']
        
        for key in required_keys:
            if key not in config:
                logger.error(f"Missing required database configuration: {key}")
                return False
                
        # Validate data types
        if not isinstance(config['port'], int) or config['port'] <= 0:
            logger.error("Database port must be a positive integer")
            return False
            
        if 'pool_size' in config:
            if not isinstance(config['pool_size'], int) or config['pool_size'] <= 0:
                logger.error("Database pool_size must be a positive integer")
                return False
        
        return True
    
    def is_database_enabled(self) -> bool:
        """
        Check if database storage is enabled.
        
        This allows for gradual rollout and easy fallback to file-based storage.
        """
        # Check environment variable first
        env_flag = os.getenv('MSGDB_ENABLED', '').lower()
        if env_flag in ['true', '1', 'yes', 'on']:
            return True
        elif env_flag in ['false', '0', 'no', 'off']:
            return False
            
        # Check ConfigInfo if available
        if self.config_info:
            try:
                config_flag = self.config_info.get('MESSAGING_DB_ENABLED', 'false')
                return str(config_flag).lower() in ['true', '1', 'yes', 'on']
            except Exception:
                pass
                
        # Default to disabled for safety
        return False


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
    Convenience function to check if messaging database is enabled.
    
    Args:
        site_id: Site identifier
        config_info: ConfigInfo instance
        
    Returns:
        True if database storage is enabled
    """
    db_config = MessagingDatabaseConfig(site_id, config_info)
    return db_config.is_database_enabled()
