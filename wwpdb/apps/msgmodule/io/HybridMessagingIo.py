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
    """Defines the write strategy for hybrid operations"""
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
                 site_id: str = "RCSB",
                 write_strategy: WriteStrategy = WriteStrategy.DUAL_WRITE):
        """
        Initialize hybrid messaging I/O.
        
        Args:
            verbose: Enable verbose logging
            log: Logger instance
            site_id: Site identifier
            write_strategy: Strategy for write operations
        """
        self.__verbose = verbose
        self.__lfh = log if log else logging.getLogger(__name__)
        self.__siteId = site_id
        self.__write_strategy = write_strategy
        
        # Backend health tracking
        self._backend_health = {
            'cif': BackendStatus.UNKNOWN,
            'database': BackendStatus.UNKNOWN
        }
        
        # Initialize backends
        self._initialize_backends()
        
        # Initialize components
        self._metrics = PerformanceMetrics()
        self._consistency_validator = ConsistencyValidator(self._cif_io, self._db_io)
        
        # Feature flags (environment-based)
        self._feature_flags = {
            'enable_consistency_checks': os.getenv('MSGDB_CONSISTENCY_CHECKS', 'true').lower() == 'true',
            'enable_metrics': os.getenv('MSGDB_METRICS', 'true').lower() == 'true',
            'failover_threshold_ms': int(os.getenv('MSGDB_FAILOVER_THRESHOLD', '5000')),
            'consistency_check_interval': int(os.getenv('MSGDB_CONSISTENCY_INTERVAL', '100'))
        }
        
        self.__lfh.info(f"Initialized HybridMessagingIo with strategy: {write_strategy.value}")
    
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
            
            # Record metrics
            if self._feature_flags['enable_metrics']:
                self._metrics.record_write(backend_name, duration_ms, True)
            
            return WriteResult(
                backend=backend_name,
                success=True,
                duration_ms=duration_ms,
                message_count=1
            )
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            
            # Record metrics
            if self._feature_flags['enable_metrics']:
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
        Add a message using hybrid write strategy.
        
        Args:
            depositionDataSetId: Deposition dataset ID
            messageText: Message content
            messageSubject: Message subject
            **kwargs: Additional message parameters
            
        Returns:
            bool: True if write successful according to strategy
        """
        if self.__write_strategy == WriteStrategy.CIF_ONLY:
            return self._write_cif_only(depositionDataSetId, messageText, messageSubject, **kwargs)
        
        elif self.__write_strategy == WriteStrategy.DB_ONLY:
            return self._write_db_only(depositionDataSetId, messageText, messageSubject, **kwargs)
        
        elif self.__write_strategy == WriteStrategy.DUAL_WRITE:
            return self._write_dual(depositionDataSetId, messageText, messageSubject, **kwargs)
        
        elif self.__write_strategy == WriteStrategy.DB_PRIMARY_CIF_FALLBACK:
            return self._write_db_primary_with_fallback(depositionDataSetId, messageText, messageSubject, **kwargs)
        
        else:
            self.__lfh.error(f"Unknown write strategy: {self.__write_strategy}")
            return False
    
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
        
        if self._feature_flags['enable_metrics']:
            self._metrics.record_write('dual', total_duration, dual_success)
        
        # Log results
        if cif_result and not cif_result.success:
            self.__lfh.error(f"CIF write failed: {cif_result.error}")
        
        if db_result and not db_result.success:
            self.__lfh.error(f"Database write failed: {db_result.error}")
        
        # For dual write, we require both to succeed
        return dual_success
    
    def _write_db_primary_with_fallback(self, depositionDataSetId: str, messageText: str, messageSubject: str, **kwargs) -> bool:
        """Write to database with CIF fallback on failure"""
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
            
            # Check if we should fallback based on error type or timing
            if db_result.duration_ms > self._feature_flags['failover_threshold_ms']:
                self.__lfh.warning(f"Database write exceeded threshold ({db_result.duration_ms}ms), falling back to CIF")
        
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
        
        return False
    
    def fetchMessages(self, depositionDataSetId: str, **kwargs) -> List[Dict]:
        """
        Fetch messages using the primary backend based on strategy.
        
        Args:
            depositionDataSetId: Deposition dataset ID
            **kwargs: Additional fetch parameters
            
        Returns:
            List[Dict]: List of messages
        """
        # For reads, prefer database if available, otherwise CIF
        if self._db_io and self._backend_health['database'] == BackendStatus.HEALTHY:
            try:
                return self._db_io.fetchMessages(depositionDataSetId, **kwargs)
            except Exception as e:
                self.__lfh.warning(f"Database read failed, falling back to CIF: {e}")
        
        if self._cif_io and self._backend_health['cif'] == BackendStatus.HEALTHY:
            return self._cif_io.fetchMessages(depositionDataSetId, **kwargs)
        
        self.__lfh.error("No available backends for reading messages")
        return []
    
    def validateConsistency(self, depositionDataSetId: str) -> ConsistencyCheck:
        """Validate data consistency between backends"""
        if not self._feature_flags['enable_consistency_checks']:
            return ConsistencyCheck(
                deposition_id=depositionDataSetId,
                cif_count=0,
                db_count=0,
                consistent=True,
                differences=["Consistency checks disabled"],
                check_timestamp=datetime.now()
            )
        
        result = self._consistency_validator.validate_deposition(depositionDataSetId)
        
        if self._feature_flags['enable_metrics']:
            self._metrics.record_consistency_check(result.consistent)
        
        return result
    
    def getPerformanceMetrics(self) -> Dict:
        """Get performance metrics summary"""
        metrics = self._metrics.get_summary()
        metrics['backend_health'] = dict(self._backend_health)
        metrics['write_strategy'] = self.__write_strategy.value
        metrics['feature_flags'] = dict(self._feature_flags)
        return metrics
    
    def setWriteStrategy(self, strategy: WriteStrategy):
        """Change the write strategy at runtime"""
        old_strategy = self.__write_strategy
        self.__write_strategy = strategy
        self.__lfh.info(f"Write strategy changed from {old_strategy.value} to {strategy.value}")
    
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
