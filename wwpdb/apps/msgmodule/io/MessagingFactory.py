"""
MessagingFactory - Backend Selection using ConfigInfo

This module provides backend selection between CIF and Database storage.
Controlled by ConfigInfo setting: WWPDB_MESSAGING_BACKEND
- "database" = Use database backend (MessagingDb)  
- "cif" or unset = Use CIF file backend (MessagingIo) [default]

No fallback behavior - if database is configured but fails, an exception is raised.
"""

import os
import logging
from typing import Any

from wwpdb.utils.config.ConfigInfo import ConfigInfo
                
logger = logging.getLogger(__name__)


class MessagingFactory:
    """Factory for creating the appropriate messaging backend implementation"""

    @staticmethod
    def create_messaging_backend(req_obj, verbose=False, log=None) -> Any:
        """
        Create the appropriate messaging backend based on ConfigInfo configuration.
        
        Backend selection controlled by ConfigInfo setting WWPDB_MESSAGING_BACKEND:
        - "database" -> MessagingDb (database storage)
        - "cif" or unset -> MessagingIo (CIF file storage) [default]
        
        Args:
            req_obj: Request object containing configuration
            verbose: Enable verbose logging
            log: Log output stream
            
        Returns:
            MessagingDb or MessagingIo instance with identical interfaces
            
        Raises:
            RuntimeError: If backend creation fails or configuration is invalid
        """
        # Get backend selection from ConfigInfo
        backend_mode = MessagingFactory._get_backend_mode(req_obj)
        
        if backend_mode == "database":
            if verbose:
                logger.info("SELECTED: Database backend (MessagingDb)")
            
            try:
                from wwpdb.apps.msgmodule.db.MessagingDb import MessagingDb
                return MessagingDb(req_obj, verbose=verbose, log=log)
            except Exception as e:
                logger.error(f"Failed to create database backend: {e}")
                raise RuntimeError(f"Database backend creation failed: {e}")
        
        else:
            if verbose:
                if backend_mode != "cif":
                    logger.warning(f"Unknown backend mode '{backend_mode}', defaulting to CIF")
                logger.info("SELECTED: CIF file backend (MessagingIo)")
            
            try:
                from wwpdb.apps.msgmodule.io.MessagingIo import MessagingIo
                return MessagingIo(req_obj, verbose=verbose, log=log)
            except Exception as e:
                logger.error(f"Failed to create CIF backend: {e}")
                raise RuntimeError(f"CIF backend creation failed: {e}")

    @staticmethod
    def _get_backend_mode(req_obj) -> str:
        """
        Get backend mode from ConfigInfo.
        
        Args:
            req_obj: Request object containing site information
            
        Returns:
            Backend mode string ("database" or "cif")
        """
        try:
            # Get site ID from request object
            site_id = str(req_obj.getValue("WWPDB_SITE_ID"))
            if site_id:
                cI = ConfigInfo(site_id)
                backend = cI.get("WWPDB_MESSAGING_BACKEND", "cif").lower().strip()
                logger.debug(f"Backend mode from ConfigInfo: {backend}")
                return backend
            else:
                logger.warning("No site ID found in request object, defaulting to CIF backend")
                return "cif"
        except Exception as e:
            logger.error(f"Failed to get backend mode from ConfigInfo: {e}")
            raise RuntimeError(f"ConfigInfo configuration error: {e}")

    @staticmethod
    def get_backend_info(req_obj=None) -> dict:
        """
        Get information about which backend would be selected.
        
        Args:
            req_obj: Request object containing site information (required)
        
        Returns:
            Dictionary with backend selection information
        """
        if not req_obj:
            raise ValueError("req_obj is required to determine backend configuration")
            
        backend_mode = MessagingFactory._get_backend_mode(req_obj)
        
        if backend_mode == "database":
            selected_backend = "database"
            backend_class = "MessagingDb"
            reason = "Database mode enabled via ConfigInfo WWPDB_MESSAGING_BACKEND=database"
        else:
            selected_backend = "cif"
            backend_class = "MessagingIo"
            reason = "CIF mode (default)" if backend_mode == "cif" else f"Unknown mode '{backend_mode}', defaulting to CIF"
        
        return {
            "selected_backend": selected_backend,
            "backend_class": backend_class,
            "config_source": "ConfigInfo",
            "current_value": backend_mode,
            "reason": reason
        }
