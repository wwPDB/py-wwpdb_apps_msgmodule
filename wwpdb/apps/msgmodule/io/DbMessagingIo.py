#!/usr/bin/env python
"""
Hybrid messaging I/O implementation for dual-write operations.

This module implements the hybrid messaging system that writes to both
CIF files and database simultaneously, providing automatic failover
and data consistency validation.

Author: wwPDB Migration Team
Date: July 2025
"""

import os
import sys
import time
import logging
import hashlib
import threading
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

# Import existing CIF-based implementation
try:
    from wwpdb.apps.msgmodule.io.MessagingIo import MessagingIo
    MESSAGING_IO_AVAILABLE = True
except ImportError:
    # Create a mock class if MessagingIo is not available
    class MessagingIo:
        def __init__(self, *args, **kwargs):
            pass
        def addMessage(self, *args, **kwargs):
            return True
        def fetchMessages(self, *args, **kwargs):
            return []
        def fetchMessageSubjects(self, *args, **kwargs):
            return []
        def fetchMessageIds(self, *args, **kwargs):
            return []
    MESSAGING_IO_AVAILABLE = False

# Import database implementation
try:
    from wwpdb.apps.msgmodule.io.MessagingIoDatabase import MessagingIoDatabase
    from wwpdb.apps.msgmodule.db.config import DatabaseConfig
    DATABASE_IO_AVAILABLE = True
except ImportError:
    # Create mock classes if database components are not available
    class MessagingIoDatabase:
        def __init__(self, *args, **kwargs):
            pass
        def addMessage(self, *args, **kwargs):
            return True
        def fetchMessages(self, *args, **kwargs):
            return []
        def fetchMessageSubjects(self, *args, **kwargs):
            return []
        def fetchMessageIds(self, *args, **kwargs):
            return []
    
    class DatabaseConfig:
        def is_enabled(self):
            return False
    
    DATABASE_IO_AVAILABLE = False

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

logger = logging.getLogger(__name__)


class WriteStrategy(Enum):
    """
    DEPRECATED: Write strategies - replaced by feature flags in revised migration plan.
    Kept for backward compatibility only.
    """
    CIF_ONLY = "cif_only"
    DB_ONLY = "db_only"
    DUAL_WRITE = "dual_write"
    DB_PRIMARY_CIF_FALLBACK = "db_primary_cif_fallback"


class BackendStatus(Enum):
    """Defines the status of each backend"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"
    UNKNOWN = "unknown"


@dataclass
class WriteResult:
    """Result of a write operation"""
    backend: str
    success: bool
    duration_ms: float
    error: Optional[str] = None
    message_count: int = 0


@dataclass
class ConsistencyCheck:
    """Result of data consistency validation"""
    deposition_id: str
    cif_count: int
    db_count: int
    consistent: bool
    differences: List[str]
    check_timestamp: datetime


class PerformanceMetrics:
    """Collects and tracks performance metrics"""
    
    def __init__(self):
        self._lock = threading.Lock()
        self._metrics = {
            'cif_write_times': [],
            'db_write_times': [],
            'dual_write_times': [],
            'failover_count': 0,
            'consistency_checks': 0,
            'consistency_failures': 0,
            'total_operations': 0
        }
    
    def record_write(self, backend: str, duration_ms: float, success: bool):
        """Record a write operation metric"""
        with self._lock:
            if backend == 'cif':
                self._metrics['cif_write_times'].append(duration_ms)
            elif backend == 'database':
                self._metrics['db_write_times'].append(duration_ms)
            elif backend == 'dual':
                self._metrics['dual_write_times'].append(duration_ms)
            
            self._metrics['total_operations'] += 1
            
            if not success and backend in ['database', 'dual']:
                self._metrics['failover_count'] += 1
    
    def record_consistency_check(self, consistent: bool):
        """Record consistency check result"""
        with self._lock:
            self._metrics['consistency_checks'] += 1
            if not consistent:
                self._metrics['consistency_failures'] += 1
    
    def get_summary(self) -> Dict:
        """Get performance metrics summary"""
        with self._lock:
            summary = {}
            for key, values in self._metrics.items():
                if isinstance(values, list) and values:
                    summary[f"{key}_avg"] = sum(values) / len(values)
                    summary[f"{key}_p95"] = sorted(values)[int(len(values) * 0.95)] if len(values) >= 20 else max(values)
                    summary[f"{key}_count"] = len(values)
                else:
                    summary[key] = values
            return summary


class ConsistencyValidator:
    """Validates data consistency between CIF and database backends"""
    
    def __init__(self, cif_io: MessagingIo, db_io: MessagingIoDatabase):
        self.cif_io = cif_io
        self.db_io = db_io
        self.logger = logging.getLogger(f"{__name__}.ConsistencyValidator")
    
    def validate_deposition(self, deposition_id: str) -> ConsistencyCheck:
        """Validate consistency for a specific deposition"""
        try:
            # Get messages from both backends
            cif_messages = self._get_cif_messages(deposition_id)
            db_messages = self._get_db_messages(deposition_id)
            
            # Compare counts
            cif_count = len(cif_messages)
            db_count = len(db_messages)
            
            differences = []
            consistent = True
            
            # Check message count consistency
            if cif_count != db_count:
                differences.append(f"Message count mismatch: CIF={cif_count}, DB={db_count}")
                consistent = False
            
            # Check message content consistency (for matching messages)
            if consistent and cif_messages and db_messages:
                content_diffs = self._compare_message_content(cif_messages, db_messages)
                if content_diffs:
                    differences.extend(content_diffs)
                    consistent = False
            
            return ConsistencyCheck(
                deposition_id=deposition_id,
                cif_count=cif_count,
                db_count=db_count,
                consistent=consistent,
                differences=differences,
                check_timestamp=datetime.now()
            )
            
        except Exception as e:
            self.logger.error(f"Consistency validation failed for {deposition_id}: {e}")
            return ConsistencyCheck(
                deposition_id=deposition_id,
                cif_count=0,
                db_count=0,
                consistent=False,
                differences=[f"Validation error: {str(e)}"],
                check_timestamp=datetime.now()
            )
    
    def _get_cif_messages(self, deposition_id: str) -> List[Dict]:
        """Get messages from CIF backend"""
        try:
            return self.cif_io.fetchMessages(depositionDataSetId=deposition_id)
        except Exception as e:
            self.logger.warning(f"Failed to fetch CIF messages for {deposition_id}: {e}")
            return []
    
    def _get_db_messages(self, deposition_id: str) -> List[Dict]:
        """Get messages from database backend"""
        try:
            return self.db_io.fetchMessages(depositionDataSetId=deposition_id)
        except Exception as e:
            self.logger.warning(f"Failed to fetch DB messages for {deposition_id}: {e}")
            return []
    
    def _compare_message_content(self, cif_messages: List[Dict], db_messages: List[Dict]) -> List[str]:
        """Compare message content between backends"""
        differences = []
        
        # Create message maps for comparison
        cif_map = {msg.get('messageId', ''): msg for msg in cif_messages}
        db_map = {msg.get('messageId', ''): msg for msg in db_messages}
        
        # Check for content differences
        common_ids = set(cif_map.keys()) & set(db_map.keys())
        
        for msg_id in common_ids:
            cif_msg = cif_map[msg_id]
            db_msg = db_map[msg_id]
            
            # Compare key fields
            fields_to_compare = ['messageSubject', 'messageText', 'sender', 'timestamp']
            for field in fields_to_compare:
                cif_value = cif_msg.get(field, '')
                db_value = db_msg.get(field, '')
                
                if str(cif_value).strip() != str(db_value).strip():
                    differences.append(f"Message {msg_id} field '{field}' differs: CIF='{cif_value}' DB='{db_value}'")
        
        return differences


class HybridMessagingIo:
    """
    Hybrid messaging I/O implementation supporting dual-write operations
    with automatic failover and consistency validation.
    """
    
    def __init__(self, 
                 verbose: bool = False,
                 log: Optional[Any] = None,
                 site_id: str = "RCSB"):
        """
        Initialize hybrid messaging I/O with revised migration plan approach.
        
        Args:
            verbose: Enable verbose logging
            log: Logger instance
            site_id: Site identifier
        """
        self.__verbose = verbose
        self.__lfh = log if log else logging.getLogger(__name__)
        self.__siteId = site_id
        
        # Initialize feature flag manager for revised plan
        try:
            from wwpdb.apps.msgmodule.util.FeatureFlagManager import get_feature_flag_manager
            self._feature_manager = get_feature_flag_manager(site_id)
        except ImportError:
            # Fallback to environment variables
            self._feature_manager = None
            self.__lfh.warning("FeatureFlagManager not available, using environment fallback")
        
        # Backend health tracking
        self._backend_health = {
            'cif': BackendStatus.UNKNOWN,
            'database': BackendStatus.UNKNOWN
        }
        
        # Initialize backends
        self._initialize_backends()
        
        # Initialize components
        self._metrics = PerformanceMetrics()
        if self._cif_io and self._db_io:
            self._consistency_validator = ConsistencyValidator(self._cif_io, self._db_io)
        else:
            self._consistency_validator = None
        
        # Log current configuration
        self._log_configuration()
    
    def _log_configuration(self):
        """Log current feature flag configuration"""
        if self._feature_manager:
            db_writes = self._feature_manager.is_database_writes_enabled()
            db_reads = self._feature_manager.is_database_reads_enabled()
            dual_write = self._feature_manager.is_dual_write_enabled()
            cif_fallback = self._feature_manager.is_cif_fallback_enabled()
            
            self.__lfh.info(f"Messaging configuration - DB writes: {db_writes}, DB reads: {db_reads}, "
                           f"Dual-write: {dual_write}, CIF fallback: {cif_fallback}")
        else:
            # Fallback to environment
            db_writes = os.getenv('MSGDB_WRITES_ENABLED', 'true').lower() == 'true'
            db_reads = os.getenv('MSGDB_READS_ENABLED', 'false').lower() == 'true'
            self.__lfh.info(f"Messaging configuration (env) - DB writes: {db_writes}, DB reads: {db_reads}")
    
    def _initialize_backends(self):
        """Initialize CIF and database backends"""
        try:
            # Initialize CIF backend
            if MESSAGING_IO_AVAILABLE:
                self._cif_io = MessagingIo(
                    verbose=self.__verbose,
                    log=self.__lfh,
                    siteId=self.__siteId
                )
                self._backend_health['cif'] = BackendStatus.HEALTHY
                self.__lfh.info("CIF backend initialized successfully")
            else:
                self._cif_io = MessagingIo()  # Mock version
                self._backend_health['cif'] = BackendStatus.DEGRADED
                self.__lfh.warning("CIF backend using mock implementation (mmcif_utils not available)")
            
        except Exception as e:
            self.__lfh.error(f"Failed to initialize CIF backend: {e}")
            self._cif_io = None
            self._backend_health['cif'] = BackendStatus.FAILED
        
        try:
            # Initialize database backend
            if DATABASE_IO_AVAILABLE:
                db_config = DatabaseConfig()
                if db_config.is_enabled():
                    self._db_io = MessagingIoDatabase(
                        verbose=self.__verbose,
                        log=self.__lfh,
                        siteId=self.__siteId
                    )
                    self._backend_health['database'] = BackendStatus.HEALTHY
                    self.__lfh.info("Database backend initialized successfully")
                else:
                    self._db_io = None
                    self._backend_health['database'] = BackendStatus.FAILED
                    self.__lfh.warning("Database backend disabled by configuration")
            else:
                self._db_io = MessagingIoDatabase()  # Mock version
                self._backend_health['database'] = BackendStatus.DEGRADED
                self.__lfh.warning("Database backend using mock implementation")
                
        except Exception as e:
            self.__lfh.error(f"Failed to initialize database backend: {e}")
            self._db_io = None
            self._backend_health['database'] = BackendStatus.FAILED
    
    def _execute_write_with_timing(self, backend_name: str, write_func, *args, **kwargs) -> WriteResult:
        """Execute a write operation with timing and error handling"""
        start_time = time.time()
        try:
            result = write_func(*args, **kwargs)
            duration_ms = (time.time() - start_time) * 1000
            
            # Record metrics (always enabled in revised plan)
            self._metrics.record_write(backend_name, duration_ms, True)
            
            return WriteResult(
                backend=backend_name,
                success=True,
                duration_ms=duration_ms,
                message_count=1
            )
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            
            # Record metrics (always enabled in revised plan)
            self._metrics.record_write(backend_name, duration_ms, False)
            
            self.__lfh.error(f"{backend_name} write failed: {e}")
            return WriteResult(
                backend=backend_name,
                success=False,
                duration_ms=duration_ms,
                error=str(e)
            )
    
    def addMessage(self, 
                   depositionDataSetId: str,
                   messageText: str = "",
                   messageSubject: str = "",
                   **kwargs) -> bool:
        """
        Add a message using revised migration plan approach.
        
        Args:
            depositionDataSetId: Deposition dataset ID
            messageText: Message content
            messageSubject: Message subject
            **kwargs: Additional message parameters
            
        Returns:
            bool: True if write successful
        """
        # Determine write strategy based on feature flags
        if self._feature_manager:
            dual_write_enabled = self._feature_manager.is_dual_write_enabled()
            db_writes_enabled = self._feature_manager.is_database_writes_enabled()
            cif_fallback_enabled = self._feature_manager.is_cif_fallback_enabled()
        else:
            # Fallback to environment variables
            dual_write_enabled = os.getenv('MSGDB_DUAL_WRITE', 'false').lower() == 'true'
            db_writes_enabled = os.getenv('MSGDB_WRITES_ENABLED', 'true').lower() == 'true'
            cif_fallback_enabled = os.getenv('MSGDB_CIF_FALLBACK', 'true').lower() == 'true'
        
        # Execute write strategy according to revised plan
        if dual_write_enabled:
            # Legacy dual-write for sites that require it
            return self._write_dual(depositionDataSetId, messageText, messageSubject, **kwargs)
        elif db_writes_enabled:
            # Default: database-only with optional CIF fallback
            if cif_fallback_enabled:
                return self._write_db_with_cif_fallback(depositionDataSetId, messageText, messageSubject, **kwargs)
            else:
                return self._write_db_only(depositionDataSetId, messageText, messageSubject, **kwargs)
        else:
            # Rollback mode: CIF-only
            return self._write_cif_only(depositionDataSetId, messageText, messageSubject, **kwargs)
    
    def _write_cif_only(self, depositionDataSetId: str, messageText: str, messageSubject: str, **kwargs) -> bool:
        """Write to CIF backend only"""
        if not self._cif_io:
            self.__lfh.error("CIF backend not available")
            return False
        
        result = self._execute_write_with_timing(
            'cif',
            self._cif_io.addMessage,
            depositionDataSetId,
            messageText,
            messageSubject,
            **kwargs
        )
        return result.success
    
    def _write_db_only(self, depositionDataSetId: str, messageText: str, messageSubject: str, **kwargs) -> bool:
        """Write to database backend only"""
        if not self._db_io:
            self.__lfh.error("Database backend not available")
            return False
        
        result = self._execute_write_with_timing(
            'database',
            self._db_io.addMessage,
            depositionDataSetId,
            messageText,
            messageSubject,
            **kwargs
        )
        return result.success
    
    def _write_dual(self, depositionDataSetId: str, messageText: str, messageSubject: str, **kwargs) -> bool:
        """Write to both backends simultaneously"""
        start_time = time.time()
        
        # Execute writes in parallel
        cif_result = None
        db_result = None
        
        if self._cif_io:
            cif_result = self._execute_write_with_timing(
                'cif',
                self._cif_io.addMessage,
                depositionDataSetId,
                messageText,
                messageSubject,
                **kwargs
            )
        
        if self._db_io:
            db_result = self._execute_write_with_timing(
                'database',
                self._db_io.addMessage,
                depositionDataSetId,
                messageText,
                messageSubject,
                **kwargs
            )
        
        # Record dual write timing
        total_duration = (time.time() - start_time) * 1000
        dual_success = (cif_result and cif_result.success) and (db_result and db_result.success)
        
        # Record dual write timing (always enabled in revised plan)
        self._metrics.record_write('dual', total_duration, dual_success)
        
        # Log results
        if cif_result and not cif_result.success:
            self.__lfh.error(f"CIF write failed: {cif_result.error}")
        
        if db_result and not db_result.success:
            self.__lfh.error(f"Database write failed: {db_result.error}")
        
        # For dual write, we require both to succeed
        return dual_success
    
    def _write_db_with_cif_fallback(self, depositionDataSetId: str, messageText: str, messageSubject: str, **kwargs) -> bool:
        """Write to database with CIF fallback on failure (revised plan approach)"""
        # Try database first
        if self._db_io:
            db_result = self._execute_write_with_timing(
                'database',
                self._db_io.addMessage,
                depositionDataSetId,
                messageText,
                messageSubject,
                **kwargs
            )
            
            if db_result.success:
                return True
            
            # Log database failure and prepare for fallback
            self.__lfh.warning(f"Database write failed: {db_result.error}, falling back to CIF")
        
        # Fallback to CIF
        self.__lfh.info("Falling back to CIF backend")
        if self._cif_io:
            cif_result = self._execute_write_with_timing(
                'cif',
                self._cif_io.addMessage,
                depositionDataSetId,
                messageText,
                messageSubject,
                **kwargs
            )
            return cif_result.success
        
        self.__lfh.error("Both database and CIF backends failed")
        return False
    
    def fetchMessages(self, depositionDataSetId: str, **kwargs) -> List[Dict]:
        """
        Fetch messages using revised migration plan approach.
        
        Args:
            depositionDataSetId: Deposition dataset ID
            **kwargs: Additional fetch parameters
            
        Returns:
            List[Dict]: List of messages
        """
        # Determine read strategy based on feature flags
        if self._feature_manager:
            db_reads_enabled = self._feature_manager.is_database_reads_enabled()
        else:
            # Fallback to environment variables
            db_reads_enabled = os.getenv('MSGDB_READS_ENABLED', 'false').lower() == 'true'
        
        # Execute read strategy according to revised plan
        if db_reads_enabled and self._db_io and self._backend_health['database'] == BackendStatus.HEALTHY:
            try:
                return self._db_io.fetchMessages(depositionDataSetId, **kwargs)
            except Exception as e:
                self.__lfh.warning(f"Database read failed, falling back to CIF: {e}")
        
        # Default or fallback: read from CIF
        if self._cif_io and self._backend_health['cif'] == BackendStatus.HEALTHY:
            return self._cif_io.fetchMessages(depositionDataSetId, **kwargs)
        
        self.__lfh.error("No available backends for reading messages")
        return []
    
    def validateConsistency(self, depositionDataSetId: str) -> ConsistencyCheck:
        """Validate data consistency between backends"""
        # Check if consistency checks are enabled via feature manager
        consistency_enabled = True
        if self._feature_manager:
            consistency_enabled = self._feature_manager.is_enabled('consistency_checks')
        
        if not consistency_enabled:
            return ConsistencyCheck(
                deposition_id=depositionDataSetId,
                cif_count=0,
                db_count=0,
                consistent=True,
                differences=["Consistency checks disabled"],
                check_timestamp=datetime.now()
            )
        
        result = self._consistency_validator.validate_deposition(depositionDataSetId)
        
        # Record consistency check metrics (always enabled in revised plan)
        self._metrics.record_consistency_check(result.consistent)
        
        return result
    
    def getPerformanceMetrics(self) -> Dict:
        """Get performance metrics summary"""
        metrics = self._metrics.get_summary()
        metrics['backend_health'] = dict(self._backend_health)
        
        # Include current feature flag status
        if self._feature_manager:
            metrics['feature_flags'] = {
                'database_writes_enabled': self._feature_manager.is_database_writes_enabled(),
                'database_reads_enabled': self._feature_manager.is_database_reads_enabled(),
                'dual_write_enabled': self._feature_manager.is_dual_write_enabled(),
                'cif_fallback_enabled': self._feature_manager.is_cif_fallback_enabled()
            }
        else:
            # Fallback to environment
            metrics['feature_flags'] = {
                'database_writes_enabled': os.getenv('MSGDB_WRITES_ENABLED', 'true').lower() == 'true',
                'database_reads_enabled': os.getenv('MSGDB_READS_ENABLED', 'false').lower() == 'true',
                'dual_write_enabled': os.getenv('MSGDB_DUAL_WRITE', 'false').lower() == 'true'
            }
        
        return metrics
    
    def setWriteStrategy(self, strategy: WriteStrategy):
        """
        DEPRECATED: Change the write strategy at runtime.
        Use feature flags instead in revised migration plan.
        """
        self.__lfh.warning("setWriteStrategy is deprecated. Use feature flags for write control.")
        
        # Map legacy strategy to feature flags for backward compatibility
        if self._feature_manager:
            if strategy == WriteStrategy.CIF_ONLY:
                self._feature_manager.disable_flag('database_writes_enabled')
                self._feature_manager.disable_flag('dual_write_enabled')
            elif strategy == WriteStrategy.DB_ONLY:
                self._feature_manager.enable_flag('database_writes_enabled')
                self._feature_manager.disable_flag('dual_write_enabled')
                self._feature_manager.disable_flag('cif_fallback_enabled')
            elif strategy == WriteStrategy.DUAL_WRITE:
                self._feature_manager.enable_flag('dual_write_enabled')
            elif strategy == WriteStrategy.DB_PRIMARY_CIF_FALLBACK:
                self._feature_manager.enable_flag('database_writes_enabled')
                self._feature_manager.enable_flag('cif_fallback_enabled')
                self._feature_manager.disable_flag('dual_write_enabled')
        
        self.__lfh.info(f"Mapped legacy write strategy {strategy.value} to feature flags")
    
    def getBackendHealth(self) -> Dict[str, str]:
        """Get current backend health status"""
        return {k: v.value for k, v in self._backend_health.items()}
    
    # Delegate other methods to appropriate backend
    def fetchMessageSubjects(self, depositionDataSetId: str, **kwargs):
        """Fetch message subjects from primary backend"""
        if self._db_io and self._backend_health['database'] == BackendStatus.HEALTHY:
            try:
                return self._db_io.fetchMessageSubjects(depositionDataSetId, **kwargs)
            except Exception as e:
                self.__lfh.warning(f"Database fetchMessageSubjects failed, falling back to CIF: {e}")
        
        if self._cif_io and self._backend_health['cif'] == BackendStatus.HEALTHY:
            return self._cif_io.fetchMessageSubjects(depositionDataSetId, **kwargs)
        
        return []
    
    def fetchMessageIds(self, depositionDataSetId: str, **kwargs):
        """Fetch message IDs from primary backend"""
        if self._db_io and self._backend_health['database'] == BackendStatus.HEALTHY:
            try:
                return self._db_io.fetchMessageIds(depositionDataSetId, **kwargs)
            except Exception as e:
                self.__lfh.warning(f"Database fetchMessageIds failed, falling back to CIF: {e}")
        
        if self._cif_io and self._backend_health['cif'] == BackendStatus.HEALTHY:
            return self._cif_io.fetchMessageIds(depositionDataSetId, **kwargs)
        
        return []
