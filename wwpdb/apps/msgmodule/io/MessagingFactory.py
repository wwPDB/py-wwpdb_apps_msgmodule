"""
MessagingFactory - Simple Backend Selection

This module provides simple backend selection between CIF and Database storage.
Controlled by a single environment variable: WWPDB_MESSAGING_BACKEND
- "database" = Use database backend (MessagingDb)  
- "cif" or unset = Use CIF file backend (MessagingIo) [default]
"""

import os
import logging
from typing import Any

logger = logging.getLogger(__name__)


class MessagingFactory:
    """Factory for creating the appropriate messaging backend implementation"""

    @staticmethod
    def create_messaging_backend(req_obj, verbose=False, log=None) -> Any:
        """
        Create the appropriate messaging backend based on simple configuration.
        
        Backend selection controlled by environment variable WWPDB_MESSAGING_BACKEND:
        - "database" -> MessagingDb (database storage)
        - "cif" or unset -> MessagingIo (CIF file storage) [default]
        
        Args:
            req_obj: Request object containing configuration
            verbose: Enable verbose logging
            log: Log output stream
            
        Returns:
            MessagingDb or MessagingIo instance with identical interfaces
        """
        # Simple backend selection via environment variable
        backend_mode = os.environ.get("WWPDB_MESSAGING_BACKEND", "cif").lower().strip()
        
        try:
            if backend_mode == "database":
                if verbose:
                    logger.info("�️ SELECTED: Database backend (MessagingDb)")
                
                from wwpdb.apps.msgmodule.db.MessagingDb import MessagingDb
                return MessagingDb(req_obj, verbose=verbose, log=log)
            
            else:
                if verbose:
                    if backend_mode != "cif":
                        logger.warning(f"Unknown backend mode '{backend_mode}', defaulting to CIF")
                    logger.info("📄 SELECTED: CIF file backend (MessagingIo)")
                
                from wwpdb.apps.msgmodule.io.MessagingIo import MessagingIo
                return MessagingIo(req_obj, verbose=verbose, log=log)
                
        except Exception as e:
            logger.error(f"Backend creation failed: {e}")
            # Always fallback to CIF backend
            if verbose:
                logger.warning("⚠️  FALLBACK: Using CIF file backend due to error")
            
            from wwpdb.apps.msgmodule.io.MessagingIo import MessagingIo
            return MessagingIo(req_obj, verbose=verbose, log=log)

    @staticmethod
    def get_backend_info(req_obj=None) -> dict:
        """
        Get information about which backend would be selected.
        
        Returns:
            Dictionary with backend selection information
        """
        backend_mode = os.environ.get("WWPDB_MESSAGING_BACKEND", "cif").lower().strip()
        
        if backend_mode == "database":
            selected_backend = "database"
            backend_class = "MessagingDb"
            reason = "Database mode enabled via WWPDB_MESSAGING_BACKEND=database"
        else:
            selected_backend = "cif"
            backend_class = "MessagingIo"
            reason = "CIF mode (default)" if backend_mode == "cif" else f"Unknown mode '{backend_mode}', defaulting to CIF"
        
        return {
            "selected_backend": selected_backend,
            "backend_class": backend_class,
            "environment_variable": "WWPDB_MESSAGING_BACKEND",
            "current_value": os.environ.get("WWPDB_MESSAGING_BACKEND", "not set"),
            "reason": reason
        }
