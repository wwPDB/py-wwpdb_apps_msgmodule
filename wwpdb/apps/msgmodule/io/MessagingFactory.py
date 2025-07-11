"""
MessagingFactory - Backend Selection and Creation

This module provides the factory pattern for creating the appropriate messaging backend
based on configuration. This is where the diversion between CIF and Database backends happens.

The factory now supports three modes based on feature flags:
1. CIF-only mode: Traditional file-based storage
2. Database-only mode: Pure database storage
3. Dual-mode: Write to both, configurable read priority (for migration)
"""

import logging
from typing import Union, Any

logger = logging.getLogger(__name__)

# Import backend classes - this makes them testable with proper mocking
def _import_messaging_io():
    """Lazy import MessagingIo to handle missing dependencies gracefully"""
    from wwpdb.apps.msgmodule.io.MessagingIo import MessagingIo
    return MessagingIo

def _import_messaging_db():
    """Lazy import MessagingDb to handle missing dependencies gracefully"""
    from wwpdb.apps.msgmodule.io.MessagingDb import MessagingDb
    return MessagingDb

def _import_messaging_dual_mode():
    """Lazy import MessagingDualMode to handle missing dependencies gracefully"""
    from wwpdb.apps.msgmodule.io.MessagingDualMode import MessagingDualMode
    return MessagingDualMode


class MessagingFactory:
    """Factory for creating the appropriate messaging backend implementation"""

    @staticmethod
    def create_messaging_backend(req_obj, verbose=False, log=None) -> Any:
        """
        Create the appropriate messaging backend based on configuration.
        
        This is THE CRITICAL DECISION POINT where we choose between:
        - MessagingDb (database-only storage)
        - MessagingIo (CIF file-only storage)  
        - MessagingDualMode (dual write with configurable read priority)
        
        Args:
            req_obj: Request object containing configuration
            verbose: Enable verbose logging
            log: Log output stream
            
        Returns:
            MessagingDb, MessagingIo, or MessagingDualMode instance with identical interfaces
        """
        try:
            # Import all feature flag checks
            from wwpdb.apps.msgmodule.db import (
                is_messaging_database_writes_enabled,
                is_messaging_database_reads_enabled,
                is_messaging_cif_writes_enabled,
                is_messaging_cif_reads_enabled
            )
            
            # Get site ID for configuration
            site_id = req_obj.getValue("WWPDB_SITE_ID") if req_obj else None
            
            # Check all feature flags
            db_writes = is_messaging_database_writes_enabled(site_id)
            db_reads = is_messaging_database_reads_enabled(site_id)
            cif_writes = is_messaging_cif_writes_enabled(site_id)
            cif_reads = is_messaging_cif_reads_enabled(site_id)
            
            if verbose:
                logger.info(f"🔧 Feature flags: DB(w:{db_writes}, r:{db_reads}) CIF(w:{cif_writes}, r:{cif_reads})")
            
            # THE CRITICAL DECISION POINT - determine which mode to use
            
            # Dual-mode: Both backends have at least one operation enabled
            if (db_writes or db_reads) and (cif_writes or cif_reads):
                if verbose:
                    logger.info("🔄 SELECTED: Dual-mode backend (MessagingDualMode)")
                
                MessagingDualMode = _import_messaging_dual_mode()
                MessagingIo = _import_messaging_io()
                MessagingDb = _import_messaging_db()
                
                # Create both backend instances
                cif_service = MessagingIo(req_obj, verbose=verbose, log=log) if (cif_writes or cif_reads) else None
                db_service = MessagingDb(req_obj, verbose=verbose, log=log) if (db_writes or db_reads) else None
                
                return MessagingDualMode(
                    cif_service=cif_service,
                    db_service=db_service,
                    db_writes_enabled=db_writes,
                    db_reads_enabled=db_reads,
                    cif_writes_enabled=cif_writes,
                    cif_reads_enabled=cif_reads,
                    verbose=verbose,
                    log=log
                )
            
            # Database-only mode
            elif db_writes or db_reads:
                if verbose:
                    logger.info("🗃️ SELECTED: Database-only backend (MessagingDb)")
                
                MessagingDb = _import_messaging_db()
                return MessagingDb(req_obj, verbose=verbose, log=log)
            
            # CIF-only mode (default/fallback)
            else:
                if verbose:
                    logger.info("📄 SELECTED: CIF file-only backend (MessagingIo)")
                
                MessagingIo = _import_messaging_io()
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
            from wwpdb.apps.msgmodule.db import (
                is_messaging_database_writes_enabled,
                is_messaging_database_reads_enabled,
                is_messaging_cif_writes_enabled,
                is_messaging_cif_reads_enabled,
                get_messaging_database_config
            )
            
            site_id = req_obj.getValue("WWPDB_SITE_ID") if req_obj else None
            
            # Check all feature flags
            db_writes = is_messaging_database_writes_enabled(site_id)
            db_reads = is_messaging_database_reads_enabled(site_id)
            cif_writes = is_messaging_cif_writes_enabled(site_id)
            cif_reads = is_messaging_cif_reads_enabled(site_id)
            
            # Determine mode
            if (db_writes or db_reads) and (cif_writes or cif_reads):
                selected_backend = "dual"
                backend_class = "MessagingDualMode"
                reason = "Both CIF and database operations enabled - using dual-mode"
            elif db_writes or db_reads:
                selected_backend = "database"
                backend_class = "MessagingDb"
                reason = "Only database operations enabled"
            else:
                selected_backend = "cif"
                backend_class = "MessagingIo"
                reason = "Only CIF operations enabled (or default)"
            
            info = {
                "selected_backend": selected_backend,
                "backend_class": backend_class,
                "site_id": site_id,
                "reason": reason,
                "feature_flags": {
                    "database_writes": db_writes,
                    "database_reads": db_reads,
                    "cif_writes": cif_writes,
                    "cif_reads": cif_reads
                }
            }
            
            # Add database config if any database operations are enabled
            if db_writes or db_reads:
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
