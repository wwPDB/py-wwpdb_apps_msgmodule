"""
Drop-in replacement for LockFile compatible with database adaptors.

This module provides a LockFile class that has the same interface as the original
mmcif_utils.persist.LockFile but is compatible with database adaptors that return
dummy file paths. It automatically detects dummy paths and uses no-op operations
for them while preserving real file locking for actual file paths.
"""

import os
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class LockFile:
    """
    Drop-in replacement for mmcif_utils.persist.LockFile.
    
    Automatically detects dummy paths returned by database adaptors and uses
    no-op operations for them, while preserving real file locking for actual paths.
    """
    
    def __init__(self, file_path, timeoutSeconds=60, retrySeconds=1, verbose=False, log=None):
        """
        Initialize LockFile with same interface as original.
        
        Args:
            file_path: Path to file to lock
            timeoutSeconds: Lock timeout in seconds (default 60)
            retrySeconds: Retry interval in seconds (default 1)
            verbose: Enable verbose logging (default False)
            log: Logger instance (default None)
        """
        self.file_path = file_path
        self.timeoutSeconds = timeoutSeconds
        self.retrySeconds = retrySeconds
        self.verbose = verbose
        self.log = log or logger
        self._is_dummy = self._is_dummy_path(file_path)
        self._original_lock = None
        
    def _is_dummy_path(self, file_path):
        """
        Check if a file path is a dummy path returned by database adaptors.
        """
        if not file_path:
            return False
        
        # Normalize path for comparison
        normalized = os.path.normpath(str(file_path))
        
        # Check for common dummy path patterns
        dummy_indicators = ['/dummy/', '\\dummy\\', 'dummy/messaging', 'dummy\\messaging']
        
        return any(indicator in normalized for indicator in dummy_indicators)
    
    def __enter__(self):
        """
        Enter the lock context.
        """
        if self._is_dummy:
            # No-op for dummy paths
            if self.verbose and self.log:
                self.log.debug(f"LockFile: Bypassing lock for dummy path: {self.file_path}")
            return self
        else:
            # Use real lock for actual file paths
            try:
                from mmcif_utils.persist.LockFile import LockFile as OriginalLockFile
                self._original_lock = OriginalLockFile(
                    self.file_path,
                    timeoutSeconds=self.timeoutSeconds,
                    retrySeconds=self.retrySeconds,
                    verbose=self.verbose,
                    log=self.log
                )
                return self._original_lock.__enter__()
            except ImportError:
                if self.verbose and self.log:
                    self.log.warning(f"Original LockFile not available, using no-op for: {self.file_path}")
                return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the lock context.
        """
        if self._is_dummy:
            # No-op for dummy paths
            if self.verbose and self.log:
                self.log.debug(f"LockFile: Bypassing unlock for dummy path: {self.file_path}")
            return False
        else:
            # Use real unlock for actual file paths
            if self._original_lock:
                return self._original_lock.__exit__(exc_type, exc_val, exc_tb)
            else:
                if self.verbose and self.log:
                    self.log.debug(f"LockFile: No-op unlock for: {self.file_path}")
                return False
