"""
Dual-mode messaging service for gradual migration from CIF to database.

This service allows writing to both CIF and database simultaneously,
while controlling read sources independently via feature flags.
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class MessagingDualMode:
    """
    Dual-mode messaging service that can write to both CIF and database
    while controlling read sources independently.
    
    This enables gradual migration scenarios:
    1. Write to both, read from CIF (validation phase)
    2. Write to both, read from database (testing phase)
    3. Write to database only, read from database (final phase)
    """

    def __init__(self, cif_service=None, db_service=None, 
                 db_writes_enabled=False, db_reads_enabled=False,
                 cif_writes_enabled=True, cif_reads_enabled=True,
                 verbose=False, log=None):
        """
        Initialize dual-mode messaging service.
        
        Args:
            cif_service: MessagingIo instance
            db_service: MessagingDb instance
            db_writes_enabled: Whether to write to database
            db_reads_enabled: Whether to read from database
            cif_writes_enabled: Whether to write to CIF
            cif_reads_enabled: Whether to read from CIF
            verbose: Enable verbose logging
            log: Log output stream
        """
        self.cif_service = cif_service
        self.db_service = db_service
        self.db_writes_enabled = db_writes_enabled
        self.db_reads_enabled = db_reads_enabled
        self.cif_writes_enabled = cif_writes_enabled
        self.cif_reads_enabled = cif_reads_enabled
        self.verbose = verbose
        self.log = log
        
        if self.verbose:
            logger.info(f"🔄 Dual-mode messaging initialized:")
            logger.info(f"   DB writes: {self.db_writes_enabled}")
            logger.info(f"   DB reads: {self.db_reads_enabled}")
            logger.info(f"   CIF writes: {self.cif_writes_enabled}")
            logger.info(f"   CIF reads: {self.cif_reads_enabled}")

    def set(self, *args, **kwargs) -> bool:
        """
        Write message to enabled backends.
        
        Returns True if at least one write succeeds.
        """
        success = False
        errors = []
        
        # Write to CIF if enabled
        if self.cif_writes_enabled and self.cif_service:
            try:
                if self.verbose:
                    logger.info("📄 Writing to CIF backend")
                result = self.cif_service.set(*args, **kwargs)
                success = success or result
            except Exception as e:
                error_msg = f"CIF write failed: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
        
        # Write to database if enabled
        if self.db_writes_enabled and self.db_service:
            try:
                if self.verbose:
                    logger.info("🗃️ Writing to database backend")
                result = self.db_service.set(*args, **kwargs)
                success = success or result
            except Exception as e:
                error_msg = f"Database write failed: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
        
        if not success and errors:
            logger.error(f"All write operations failed: {'; '.join(errors)}")
            
        return success

    def get(self, *args, **kwargs) -> Any:
        """
        Read message from enabled backend (priority: database -> CIF).
        """
        # Try database first if enabled
        if self.db_reads_enabled and self.db_service:
            try:
                if self.verbose:
                    logger.info("🗃️ Reading from database backend")
                return self.db_service.get(*args, **kwargs)
            except Exception as e:
                logger.error(f"Database read failed: {e}")
                if not self.cif_reads_enabled:
                    raise
        
        # Fall back to CIF if enabled
        if self.cif_reads_enabled and self.cif_service:
            try:
                if self.verbose:
                    logger.info("📄 Reading from CIF backend")
                return self.cif_service.get(*args, **kwargs)
            except Exception as e:
                logger.error(f"CIF read failed: {e}")
                raise
        
        raise RuntimeError("No read backends enabled or available")

    def getMessages(self, *args, **kwargs) -> List[Dict]:
        """
        Get messages from enabled backend (priority: database -> CIF).
        """
        # Try database first if enabled
        if self.db_reads_enabled and self.db_service:
            try:
                if self.verbose:
                    logger.info("🗃️ Getting messages from database backend")
                return self.db_service.getMessages(*args, **kwargs)
            except Exception as e:
                logger.error(f"Database getMessages failed: {e}")
                if not self.cif_reads_enabled:
                    raise
        
        # Fall back to CIF if enabled
        if self.cif_reads_enabled and self.cif_service:
            try:
                if self.verbose:
                    logger.info("📄 Getting messages from CIF backend")
                return self.cif_service.getMessages(*args, **kwargs)
            except Exception as e:
                logger.error(f"CIF getMessages failed: {e}")
                raise
        
        raise RuntimeError("No read backends enabled or available")

    def exists(self, *args, **kwargs) -> bool:
        """
        Check if message exists in enabled read backend.
        """
        # Try database first if enabled
        if self.db_reads_enabled and self.db_service:
            try:
                return self.db_service.exists(*args, **kwargs)
            except Exception as e:
                logger.error(f"Database exists check failed: {e}")
                if not self.cif_reads_enabled:
                    raise
        
        # Fall back to CIF if enabled
        if self.cif_reads_enabled and self.cif_service:
            try:
                return self.cif_service.exists(*args, **kwargs)
            except Exception as e:
                logger.error(f"CIF exists check failed: {e}")
                raise
        
        return False

    def delete(self, *args, **kwargs) -> bool:
        """
        Delete message from enabled backends.
        
        Returns True if at least one delete succeeds.
        """
        success = False
        errors = []
        
        # Delete from CIF if enabled
        if self.cif_writes_enabled and self.cif_service:
            try:
                if self.verbose:
                    logger.info("📄 Deleting from CIF backend")
                result = self.cif_service.delete(*args, **kwargs)
                success = success or result
            except Exception as e:
                error_msg = f"CIF delete failed: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
        
        # Delete from database if enabled
        if self.db_writes_enabled and self.db_service:
            try:
                if self.verbose:
                    logger.info("🗃️ Deleting from database backend")
                result = self.db_service.delete(*args, **kwargs)
                success = success or result
            except Exception as e:
                error_msg = f"Database delete failed: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
        
        if not success and errors:
            logger.error(f"All delete operations failed: {'; '.join(errors)}")
            
        return success

    def getBackendInfo(self) -> Dict:
        """
        Get information about current backend configuration.
        """
        return {
            "mode": "dual",
            "db_writes_enabled": self.db_writes_enabled,
            "db_reads_enabled": self.db_reads_enabled,
            "cif_writes_enabled": self.cif_writes_enabled,
            "cif_reads_enabled": self.cif_reads_enabled,
            "read_priority": "database" if self.db_reads_enabled else "cif",
            "write_targets": [
                backend for backend, enabled in [
                    ("database", self.db_writes_enabled),
                    ("cif", self.cif_writes_enabled)
                ] if enabled
            ]
        }

    # Delegate other methods to the appropriate backend
    def __getattr__(self, name):
        """
        Delegate unknown methods to the primary read backend.
        """
        if self.db_reads_enabled and self.db_service and hasattr(self.db_service, name):
            return getattr(self.db_service, name)
        elif self.cif_reads_enabled and self.cif_service and hasattr(self.cif_service, name):
            return getattr(self.cif_service, name)
        else:
            raise AttributeError(f"'MessagingDualMode' object has no attribute '{name}'")
