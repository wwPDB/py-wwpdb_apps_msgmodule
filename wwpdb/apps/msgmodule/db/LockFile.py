##
# Drop-in replacement for mmcif_utils.persist.LockFile compatible with database adaptors.
#
# This module provides a LockFile class that has the same interface as the original
# mmcif_utils.persist.LockFile but is compatible with database adaptors that return
# dummy file paths. It automatically detects dummy paths and uses no-op operations
# for them while preserving real file locking for actual file paths.
##

import os
import sys
import time
import errno


class LockFileTimeoutException(Exception):
    pass


class LockFile:
    """
    Drop-in replacement for mmcif_utils.persist.LockFile.
    
    Automatically detects dummy paths returned by database adaptors and uses
    no-op operations for them, while preserving real file locking for actual paths.
    """
    
    def __init__(self, filePath, timeoutSeconds=15, retrySeconds=.2, verbose=False, log=sys.stderr):
        """
        Initialize LockFile with same interface as original.
        
        Args:
            filePath: Path to file to lock
            timeoutSeconds: Lock timeout in seconds (default 15)
            retrySeconds: Retry interval in seconds (default .2)
            verbose: Enable verbose logging (default False)  
            log: File handle for logging (default sys.stderr)
        """
        self.__filePath = filePath
        self.__timeoutSeconds = timeoutSeconds
        self.__retrySeconds = retrySeconds
        self.__verbose = verbose
        self.__lfh = log
        self.__debug = False
        self.__isLocked = False
        self.__lockFilePath = os.path.join(filePath + ".lock")
        self.__fd = None
        self._is_dummy = self._is_dummy_path(filePath)
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

    def acquire(self):
        """
        Create the lock if no lock file exists.  If a lockfile exists then
        repeat the test every retrySeconds.

        If the lock cannot be acquired within  'timeoutSeconds' then
        throw an exception.
        """
        if self._is_dummy:
            # No-op for dummy paths
            if self.__debug and self.__lfh:
                self.__lfh.write(f"LockFile(acquire) bypassing lock for dummy path {self.__filePath}\n")
            self.__isLocked = True
            return
            
        # Use real locking for actual file paths
        try:
            from mmcif_utils.persist.LockFile import LockFile as OriginalLockFile
            self._original_lock = OriginalLockFile(
                self.__filePath,
                timeoutSeconds=self.__timeoutSeconds,
                retrySeconds=self.__retrySeconds,
                verbose=self.__verbose,
                log=self.__lfh
            )
            self._original_lock.acquire()
            self.__isLocked = True
        except ImportError:
            # Fallback to local implementation if original not available
            timeBegin = time.time()
            while True:
                try:
                    self.__fd = os.open(self.__lockFilePath, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                    if self.__debug and self.__lfh:
                        self.__lfh.write(f"LockFile(acquire) created lock file {self.__lockFilePath}\n")
                    break
                except OSError as myErr:
                    if myErr.errno != errno.EEXIST:
                        # pass on some unanticipated problem ---
                        raise
                    # handle timeout and retry -
                    if (time.time() - timeBegin) >= self.__timeoutSeconds:
                        raise LockFileTimeoutException("LockFile(acquire) Internal timeout of %d (seconds) exceeded for %s" %
                                                       (self.__timeoutSeconds, self.__filePath))
                    time.sleep(self.__retrySeconds)
            self.__isLocked = True

    def release(self):
        """
        Remove an existing lock file.
        """
        if not self.__isLocked:
            return
            
        if self._is_dummy:
            # No-op for dummy paths
            if self.__debug and self.__lfh:
                self.__lfh.write(f"LockFile(release) bypassing unlock for dummy path {self.__filePath}\n")
            self.__isLocked = False
            return
            
        # Use real unlocking for actual file paths
        if self._original_lock:
            self._original_lock.release()
        else:
            # Fallback to local implementation
            if self.__fd is not None:
                os.close(self.__fd)
                self.__fd = None
            if os.path.exists(self.__lockFilePath):
                os.unlink(self.__lockFilePath)
            if self.__debug and self.__lfh:
                self.__lfh.write(f"LockFile(release) removed lock file {self.__lockFilePath}\n")
        self.__isLocked = False

    def __enter__(self):
        """
        Internal method for Context-management support.  Invoked at the beginning of a 'with' clause.
        """
        if not self.__isLocked:
            self.acquire()
        return self

    def __exit__(self, type, value, traceback):
        """
        Internal method for Context-management support.  Invoked at the end of a 'with' clause.
        """
        if self.__isLocked:
            self.release()

    def __del__(self):
        """
        Internal method to cleanup any lingering lock file.
        """
        self.release()


if __name__ == "__main__":
    pass
