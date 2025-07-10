#!/usr/bin/env python
"""
Circuit breaker implementation for database operations.

This module provides circuit breaker functionality to protect against
database failures and enable automatic fallback to CIF storage.

Author: wwPDB Migration Team
Date: July 2025
"""

import time
import threading
import logging
from typing import Optional, Callable, Any, Dict
from enum import Enum
from dataclasses import dataclass
from datetime import datetime, timedelta


class CircuitBreakerState(Enum):
    """States of the circuit breaker"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject calls
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker"""
    failure_threshold: int = 5          # Number of failures before opening
    recovery_timeout: int = 60          # Seconds before trying half-open
    success_threshold: int = 3          # Successful calls to close from half-open
    timeout: float = 30.0              # Operation timeout in seconds
    expected_exception: type = Exception  # Exception type that counts as failure


@dataclass
class CircuitBreakerMetrics:
    """Metrics tracked by circuit breaker"""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    timeouts: int = 0
    circuit_opened_count: int = 0
    last_failure_time: Optional[datetime] = None
    last_success_time: Optional[datetime] = None
    state_changed_time: Optional[datetime] = None


class CircuitBreakerOpenException(Exception):
    """Exception raised when circuit breaker is open"""
    pass


class CircuitBreakerTimeoutException(Exception):
    """Exception raised when operation times out"""
    pass


class CircuitBreaker:
    """
    Circuit breaker implementation for protecting database operations.
    
    The circuit breaker monitors failures and automatically opens to prevent
    cascading failures, then attempts recovery after a timeout period.
    """
    
    def __init__(self, 
                 name: str,
                 config: Optional[CircuitBreakerConfig] = None):
        """
        Initialize circuit breaker.
        
        Args:
            name: Name identifier for this circuit breaker
            config: Configuration parameters
        """
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.logger = logging.getLogger(f"{__name__}.{name}")
        
        # Thread-safe state management
        self._lock = threading.RLock()
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None
        
        # Metrics tracking
        self.metrics = CircuitBreakerMetrics()
        
        self.logger.info(f"Circuit breaker '{name}' initialized with config: {self.config}")
    
    @property
    def state(self) -> CircuitBreakerState:
        """Get current circuit breaker state"""
        with self._lock:
            return self._state
    
    @property
    def is_closed(self) -> bool:
        """Check if circuit breaker is closed (normal operation)"""
        return self.state == CircuitBreakerState.CLOSED
    
    @property
    def is_open(self) -> bool:
        """Check if circuit breaker is open (failing)"""
        return self.state == CircuitBreakerState.OPEN
    
    @property
    def is_half_open(self) -> bool:
        """Check if circuit breaker is half-open (testing recovery)"""
        return self.state == CircuitBreakerState.HALF_OPEN
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute a function call protected by circuit breaker.
        
        Args:
            func: Function to execute
            *args: Positional arguments for function
            **kwargs: Keyword arguments for function
            
        Returns:
            Any: Result of function call
            
        Raises:
            CircuitBreakerOpenException: If circuit is open
            CircuitBreakerTimeoutException: If operation times out
        """
        with self._lock:
            self.metrics.total_calls += 1
            
            # Check if we should reject the call
            if self._should_reject_call():
                self.logger.warning(f"Circuit breaker '{self.name}' is open, rejecting call")
                raise CircuitBreakerOpenException(f"Circuit breaker '{self.name}' is open")
            
            # If half-open, only allow limited calls
            if self._state == CircuitBreakerState.HALF_OPEN:
                if self._success_count >= self.config.success_threshold:
                    self._close_circuit()
        
        # Execute the function call with timeout
        start_time = time.time()
        try:
            # Simple timeout implementation
            result = self._execute_with_timeout(func, *args, **kwargs)
            
            # Record success
            self._on_success()
            return result
            
        except Exception as e:
            # Record failure
            execution_time = time.time() - start_time
            if execution_time >= self.config.timeout:
                self._on_timeout()
                raise CircuitBreakerTimeoutException(f"Operation timed out after {execution_time:.2f}s")
            else:
                self._on_failure(e)
                raise
    
    def _execute_with_timeout(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with timeout (simplified implementation)"""
        # Note: This is a simplified timeout implementation
        # In production, you might want to use threading.Timer or asyncio
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            if execution_time >= self.config.timeout:
                raise CircuitBreakerTimeoutException(f"Operation exceeded timeout of {self.config.timeout}s")
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            if execution_time >= self.config.timeout:
                raise CircuitBreakerTimeoutException(f"Operation timed out after {execution_time:.2f}s")
            raise
    
    def _should_reject_call(self) -> bool:
        """Determine if the call should be rejected based on current state"""
        if self._state == CircuitBreakerState.CLOSED:
            return False
        
        if self._state == CircuitBreakerState.OPEN:
            # Check if we should transition to half-open
            if self._should_attempt_reset():
                self._attempt_reset()
                return False
            return True
        
        # Half-open state - allow limited calls
        return False
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset"""
        if self._last_failure_time is None:
            return True
        
        time_since_failure = time.time() - self._last_failure_time
        return time_since_failure >= self.config.recovery_timeout
    
    def _attempt_reset(self):
        """Attempt to reset circuit breaker to half-open state"""
        self.logger.info(f"Circuit breaker '{self.name}' attempting reset to half-open")
        self._state = CircuitBreakerState.HALF_OPEN
        self._success_count = 0
        self.metrics.state_changed_time = datetime.now()
    
    def _on_success(self):
        """Handle successful operation"""
        with self._lock:
            self.metrics.successful_calls += 1
            self.metrics.last_success_time = datetime.now()
            
            if self._state == CircuitBreakerState.HALF_OPEN:
                self._success_count += 1
                self.logger.debug(f"Circuit breaker '{self.name}' half-open success count: {self._success_count}")
                
                if self._success_count >= self.config.success_threshold:
                    self._close_circuit()
            
            # Reset failure count on success in closed state
            if self._state == CircuitBreakerState.CLOSED:
                self._failure_count = 0
    
    def _on_failure(self, exception: Exception):
        """Handle failed operation"""
        with self._lock:
            self.metrics.failed_calls += 1
            self.metrics.last_failure_time = datetime.now()
            self._last_failure_time = time.time()
            
            # Check if this exception should count as a failure
            if isinstance(exception, self.config.expected_exception):
                self._failure_count += 1
                self.logger.warning(f"Circuit breaker '{self.name}' recorded failure {self._failure_count}: {exception}")
                
                # Check if we should open the circuit
                if self._failure_count >= self.config.failure_threshold:
                    self._open_circuit()
            
            # If we're in half-open and get a failure, go back to open
            if self._state == CircuitBreakerState.HALF_OPEN:
                self.logger.warning(f"Circuit breaker '{self.name}' failed in half-open state, reopening")
                self._open_circuit()
    
    def _on_timeout(self):
        """Handle timeout"""
        with self._lock:
            self.metrics.timeouts += 1
            self.metrics.failed_calls += 1
            self.metrics.last_failure_time = datetime.now()
            self._last_failure_time = time.time()
            
            self._failure_count += 1
            self.logger.warning(f"Circuit breaker '{self.name}' recorded timeout {self._failure_count}")
            
            if self._failure_count >= self.config.failure_threshold:
                self._open_circuit()
    
    def _open_circuit(self):
        """Open the circuit breaker"""
        if self._state != CircuitBreakerState.OPEN:
            self.logger.error(f"Circuit breaker '{self.name}' opened due to failures")
            self._state = CircuitBreakerState.OPEN
            self.metrics.circuit_opened_count += 1
            self.metrics.state_changed_time = datetime.now()
    
    def _close_circuit(self):
        """Close the circuit breaker (normal operation)"""
        if self._state != CircuitBreakerState.CLOSED:
            self.logger.info(f"Circuit breaker '{self.name}' closed, resuming normal operation")
            self._state = CircuitBreakerState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self.metrics.state_changed_time = datetime.now()
    
    def force_open(self):
        """Manually force circuit breaker open"""
        with self._lock:
            self.logger.warning(f"Circuit breaker '{self.name}' manually forced open")
            self._open_circuit()
    
    def force_close(self):
        """Manually force circuit breaker closed"""
        with self._lock:
            self.logger.info(f"Circuit breaker '{self.name}' manually forced closed")
            self._close_circuit()
    
    def reset(self):
        """Reset circuit breaker to initial state"""
        with self._lock:
            self.logger.info(f"Circuit breaker '{self.name}' reset")
            self._state = CircuitBreakerState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = None
            
            # Reset metrics except totals
            self.metrics.circuit_opened_count = 0
            self.metrics.last_failure_time = None
            self.metrics.state_changed_time = datetime.now()
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current circuit breaker metrics"""
        with self._lock:
            return {
                'name': self.name,
                'state': self._state.value,
                'failure_count': self._failure_count,
                'success_count': self._success_count,
                'metrics': {
                    'total_calls': self.metrics.total_calls,
                    'successful_calls': self.metrics.successful_calls,
                    'failed_calls': self.metrics.failed_calls,
                    'timeouts': self.metrics.timeouts,
                    'circuit_opened_count': self.metrics.circuit_opened_count,
                    'success_rate': (self.metrics.successful_calls / max(self.metrics.total_calls, 1)) * 100,
                    'last_failure_time': self.metrics.last_failure_time.isoformat() if self.metrics.last_failure_time else None,
                    'last_success_time': self.metrics.last_success_time.isoformat() if self.metrics.last_success_time else None,
                    'state_changed_time': self.metrics.state_changed_time.isoformat() if self.metrics.state_changed_time else None
                },
                'config': {
                    'failure_threshold': self.config.failure_threshold,
                    'recovery_timeout': self.config.recovery_timeout,
                    'success_threshold': self.config.success_threshold,
                    'timeout': self.config.timeout
                }
            }


class DatabaseCircuitBreaker(CircuitBreaker):
    """
    Specialized circuit breaker for database operations.
    
    Configured with database-specific defaults and exception handling.
    """
    
    def __init__(self, name: str = "database"):
        """Initialize database circuit breaker with appropriate defaults"""
        config = CircuitBreakerConfig(
            failure_threshold=3,        # Open after 3 failures
            recovery_timeout=30,        # Try recovery after 30 seconds
            success_threshold=2,        # Close after 2 successes
            timeout=10.0,              # 10 second timeout for DB operations
            expected_exception=Exception  # Any exception counts as failure
        )
        
        super().__init__(name, config)


# Global circuit breaker instances
_circuit_breakers: Dict[str, CircuitBreaker] = {}
_circuit_breaker_lock = threading.Lock()


def get_circuit_breaker(name: str, config: Optional[CircuitBreakerConfig] = None) -> CircuitBreaker:
    """
    Get or create a circuit breaker instance.
    
    Args:
        name: Name of the circuit breaker
        config: Optional configuration (only used for new instances)
        
    Returns:
        CircuitBreaker: Circuit breaker instance
    """
    with _circuit_breaker_lock:
        if name not in _circuit_breakers:
            if name == "database":
                _circuit_breakers[name] = DatabaseCircuitBreaker(name)
            else:
                _circuit_breakers[name] = CircuitBreaker(name, config)
        
        return _circuit_breakers[name]


def get_database_circuit_breaker() -> CircuitBreaker:
    """Get the database circuit breaker instance"""
    return get_circuit_breaker("database")


def circuit_breaker(name: str, config: Optional[CircuitBreakerConfig] = None):
    """
    Decorator for protecting functions with circuit breaker.
    
    Args:
        name: Name of the circuit breaker
        config: Optional configuration
        
    Returns:
        Decorator function
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            breaker = get_circuit_breaker(name, config)
            return breaker.call(func, *args, **kwargs)
        return wrapper
    return decorator


def database_circuit_breaker(func):
    """Decorator for protecting database operations with circuit breaker"""
    def wrapper(*args, **kwargs):
        breaker = get_database_circuit_breaker()
        return breaker.call(func, *args, **kwargs)
    return wrapper
