"""
MessagingFactory - Backend Selection and Creation

This module provides the factory pattern for creating the appropriate messaging backend
based on configuration. This is where the diversion between CIF and Database backends happens.
"""

import logging
from typing import Union

logger = logging.getLogger(__name__)


class MessagingFactory:
    """Factory for creating the appropriate messaging backend implementation"""

    @staticmethod
    def create_messaging_backend(req_obj, verbose=False, log=None) -> Union['MessagingIo', 'MessagingDb']:
        """
        Create the appropriate messaging backend based on configuration.
        
        This is THE CRITICAL DECISION POINT where we choose between:
        - MessagingDb (database storage)
        - MessagingIo (CIF file storage)
        
        Args:
            req_obj: Request object containing configuration
            verbose: Enable verbose logging
            log: Log output stream
            
        Returns:
            Either MessagingDb or MessagingIo instance with identical interfaces
        """
        try:
            # Check if database backend is enabled
            from wwpdb.apps.msgmodule.db import is_messaging_database_enabled
            
            # Get site ID for configuration
            site_id = req_obj.getValue("WWPDB_SITE_ID") if req_obj else None
            
            # THE CRITICAL DECISION POINT
            if is_messaging_database_enabled(site_id):
                # Database backend selected
                if verbose:
                    logger.info("🗃️  SELECTED: Database backend (MessagingDb)")
                
                from wwpdb.apps.msgmodule.io.MessagingDb import MessagingDb
                return MessagingDb(req_obj, verbose=verbose, log=log)
            
            else:
                # CIF backend selected  
                if verbose:
                    logger.info("📄 SELECTED: CIF file backend (MessagingIo)")
                
                from wwpdb.apps.msgmodule.io.MessagingIo import MessagingIo
                return MessagingIo(req_obj, verbose=verbose, log=log)
                
        except Exception as e:
            logger.error(f"Backend selection failed: {e}")
            # Fallback to CIF backend
            if verbose:
                logger.warning("⚠️  FALLBACK: Using CIF file backend due to error")
            
            from wwpdb.apps.msgmodule.io.MessagingIo import MessagingIo
            return MessagingIo(req_obj, verbose=verbose, log=log)

    @staticmethod
    def get_backend_info(req_obj=None) -> dict:
        """
        Get information about which backend would be selected and why.
        
        Returns:
            Dictionary with backend selection information
        """
        try:
            from wwpdb.apps.msgmodule.db import is_messaging_database_enabled, get_messaging_database_config
            
            site_id = req_obj.getValue("WWPDB_SITE_ID") if req_obj else None
            
            is_db_enabled = is_messaging_database_enabled(site_id)
            
            info = {
                "selected_backend": "database" if is_db_enabled else "cif",
                "backend_class": "MessagingDb" if is_db_enabled else "MessagingIo",
                "site_id": site_id,
                "database_enabled": is_db_enabled,
                "reason": "Database enabled in configuration" if is_db_enabled else "Database not enabled, using CIF files"
            }
            
            if is_db_enabled:
                try:
                    config = get_messaging_database_config(site_id)
                    info["database_config"] = {
                        "host": config.get("host", "unknown"),
                        "port": config.get("port", "unknown"),
                        "database": config.get("database", "unknown"),
                        "has_credentials": bool(config.get("username") and config.get("password"))
                    }
                except Exception as e:
                    info["database_config_error"] = str(e)
            
            return info
            
        except Exception as e:
            return {
                "selected_backend": "cif",
                "backend_class": "MessagingIo", 
                "error": str(e),
                "reason": "Error during backend selection, defaulting to CIF"
            }


# Convenience function for direct use
def create_messaging_service(req_obj, verbose=False, log=None):
    """
    Convenience function to create messaging service with backend selection.
    
    This replaces direct instantiation of MessagingIo in existing code.
    """
    return MessagingFactory.create_messaging_backend(req_obj, verbose, log)
