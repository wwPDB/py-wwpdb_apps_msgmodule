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


class FileSizeLogger:
    """Drop-in replacement for MessagingIo's FileSizeLogger with dummy path detection.

    Automatically detects dummy paths returned by database adaptors and uses no-op
    operations for them, while preserving real file size logging for actual file paths.

    Args:
        filePath: Path to file to log size for
        verbose: Enable verbose logging (default: False)
        log: File handle for logging output (default: sys.stderr)

    Note:
        Dummy paths (containing '/dummy/' or '\\dummy\\') are detected automatically
        and logging is skipped for compatibility with database backend.
    """

    def __init__(self, filePath, verbose=False, log=sys.stderr):  # pylint: disable=unused-argument
        """
        Initialize FileSizeLogger with same interface as original.

        Args:
            filePath: Path to file to log size for
            verbose: Enable verbose logging (default False)
            log: File handle for logging (default sys.stderr)
        """
        self.__filePath = filePath
        self.__verbose = verbose
        self.__debug = True
        self._is_dummy = self._is_dummy_path(filePath)

    def _is_dummy_path(self, file_path):
        """Check if a file path is a dummy path returned by database adaptors.

        Args:
            file_path: File path to check

        Returns:
            True if path contains dummy indicators ('/dummy/' or '\\dummy\\'), False otherwise
        """
        if not file_path:
            return False

        # Normalize path for comparison
        normalized = os.path.normpath(str(file_path))

        # Check for common dummy path patterns
        dummy_indicators = ['/dummy/', '\\dummy\\', 'dummy/messaging', 'dummy\\messaging']

        # Also treat message files as dummy when in database mode - they don't need filesystem locking
        # since the data is read from/written to database instead of filesystem
        message_file_patterns = ['_messages-', '_notes-']

        has_dummy_indicator = any(indicator in normalized for indicator in dummy_indicators)
        is_message_file = any(pattern in normalized for pattern in message_file_patterns)

        return has_dummy_indicator or is_message_file

    def __enter__(self):
        """Enter context manager - log file size if not a dummy path.

        Returns:
            self: This FileSizeLogger instance
        """
        if self._is_dummy:
            # No-op for dummy paths
            if self.__verbose and self.__debug:
                # Use a logger since that's what the original does
                import logging
                logger = logging.getLogger(__name__)
                logger.debug("+%s -- bypassing filesize logging for dummy path: %s",
                             self.__class__.__name__, self.__filePath)
            return self
        else:
            # Use real file size logging for actual paths
            try:
                filesize = os.stat(self.__filePath).st_size
                if self.__verbose and self.__debug:
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.debug("+%s -- filesize for %s before call: %s bytes.",
                                 self.__class__.__name__, self.__filePath, filesize)
            except (OSError, FileNotFoundError):
                # File doesn't exist yet, that's ok
                if self.__verbose and self.__debug:
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.debug("+%s -- file %s does not exist yet.",
                                 self.__class__.__name__, self.__filePath)
            return self

    def __exit__(self, exc_type, value, tb):
        """Exit context manager - log file size if not a dummy path.

        Args:
            exc_type: Exception type if an exception occurred
            value: Exception value if an exception occurred
            tb: Exception traceback if an exception occurred
        """
        if self._is_dummy:
            # No-op for dummy paths
            if self.__verbose and self.__debug:
                import logging
                logger = logging.getLogger(__name__)
                logger.debug("+%s -- bypassing filesize logging cleanup for dummy path: %s",
                             self.__class__.__name__, self.__filePath)
        else:
            # Use real file size logging for actual paths
            try:
                filesize = os.stat(self.__filePath).st_size
                if self.__verbose and self.__debug:
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.debug("+%s -- filesize for %s after call: %s bytes.",
                                 self.__class__.__name__, self.__filePath, filesize)
            except (OSError, FileNotFoundError):
                # File might have been deleted or never created
                if self.__verbose and self.__debug:
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.debug("+%s -- file %s not found after call.",
                                 self.__class__.__name__, self.__filePath)


class LockFile:
    """Drop-in replacement for mmcif_utils.persist.LockFile with dummy path detection.

    Automatically detects dummy paths returned by database adaptors and uses no-op
    locking operations for them, while preserving real file locking for actual file paths.

    Args:
        filePath: Path to file to lock
        timeoutSeconds: Lock timeout in seconds (default: 15)
        retrySeconds: Retry interval in seconds (default: 0.2)
        verbose: Enable verbose logging (default: False)
        log: File handle for logging output (default: sys.stderr)

    Example:
        >>> with LockFile("/path/to/file.txt") as lock:
        ...     # File is locked for exclusive access
        ...     pass
        >>> # Lock is automatically released

    Note:
        Dummy paths (containing '/dummy/' or '\\dummy\\') are detected automatically
        and locking is skipped for compatibility with database backend.
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
        """Check if a file path is a dummy path returned by database adaptors.

        Args:
            file_path: File path to check

        Returns:
            True if path contains dummy indicators ('/dummy/' or '\\dummy\\'), False otherwise
        """
        if not file_path:
            return False

        # Normalize path for comparison
        normalized = os.path.normpath(str(file_path))

        # Check for common dummy path patterns
        dummy_indicators = ['/dummy/', '\\dummy\\', 'dummy/messaging', 'dummy\\messaging']

        # Also treat message files as dummy when in database mode - they don't need filesystem locking
        # since the data is read from/written to database instead of filesystem
        message_file_patterns = ['_messages-', '_notes-']

        has_dummy_indicator = any(indicator in normalized for indicator in dummy_indicators)
        is_message_file = any(pattern in normalized for pattern in message_file_patterns)

        return has_dummy_indicator or is_message_file

    def acquire(self):
        """Acquire the file lock (no-op for dummy paths).

        Creates a lock file if none exists. If a lock file already exists, retries
        at retrySeconds intervals until the lock is acquired or timeoutSeconds expires.

        For dummy paths, this is a no-op that always succeeds immediately.

        Raises:
            LockFileTimeoutException: If lock cannot be acquired within timeoutSeconds

        Note:
            For real file paths, attempts to use mmcif_utils.persist.LockFile if available,
            otherwise falls back to local os.open() implementation.
        """
        if self._is_dummy:
            # No-op for dummy paths
            if self.__debug and self.__lfh:
                self.__lfh.write("LockFile(acquire) bypassing lock for dummy path %s\n" % self.__filePath)
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
                        self.__lfh.write("LockFile(acquire) created lock file %s\n" % self.__lockFilePath)
                    break
                except OSError as myErr:
                    if myErr.errno != errno.EEXIST:
                        # pass on some unanticipated problem ---
                        raise
                    # handle timeout and retry -
                    if (time.time() - timeBegin) >= self.__timeoutSeconds:
                        raise LockFileTimeoutException("LockFile(acquire) Internal timeout of %d (seconds) exceeded for %s" %  # pylint: disable=raise-missing-from
                                                       (self.__timeoutSeconds, self.__filePath))
                    time.sleep(self.__retrySeconds)
            self.__isLocked = True

    def release(self):
        """Release the file lock (no-op for dummy paths).

        Removes an existing lock file. For dummy paths, this is a no-op.
        """
        if not self.__isLocked:
            return

        if self._is_dummy:
            # No-op for dummy paths
            if self.__debug and self.__lfh:
                self.__lfh.write("LockFile(release) bypassing unlock for dummy path %s\n" % self.__filePath)
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
                self.__lfh.write("LockFile(release) removed lock file %s\n" % self.__lockFilePath)
        self.__isLocked = False

    def __enter__(self):
        """Enter context manager - acquire lock.

        Returns:
            self: This LockFile instance

        Note:
            Automatically called at the beginning of a 'with' clause.
        """
        if not self.__isLocked:
            self.acquire()
        return self

    def __exit__(self, type, value, traceback):  # pylint: disable=redefined-builtin
        """Exit context manager - release lock.

        Args:
            type: Exception type if an exception occurred
            value: Exception value if an exception occurred
            traceback: Exception traceback if an exception occurred

        Note:
            Automatically called at the end of a 'with' clause.
        """
        if self.__isLocked:
            self.release()

    def __del__(self):
        """Destructor - cleanup any lingering lock file."""
        self.release()


if __name__ == "__main__":
    pass
