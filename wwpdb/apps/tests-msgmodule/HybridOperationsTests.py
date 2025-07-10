#!/usr/bin/env python
"""
Test suite for Phase 2: Hybrid Operations

This module contains comprehensive tests for the hybrid messaging system,
including dual-write operations, failover logic, and consistency validation.

Author: wwPDB Migration Team
Date: July 2025
"""

import os
import sys
import unittest
import tempfile
import shutil
import time
import threading
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch, call

# Add the project root to Python path for testing
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from wwpdb.apps.msgmodule.io.HybridMessagingIo import (
    HybridMessagingIo, WriteStrategy, BackendStatus, WriteResult, 
    ConsistencyCheck, PerformanceMetrics, ConsistencyValidator
)
from wwpdb.apps.msgmodule.util.FeatureFlagManager import (
    FeatureFlagManager, FeatureFlag, FeatureFlagScope, FeatureFlagContext
)
from wwpdb.apps.msgmodule.util.CircuitBreaker import (
    CircuitBreaker, CircuitBreakerState, CircuitBreakerConfig,
    DatabaseCircuitBreaker, CircuitBreakerOpenException
)


class TestHybridMessagingIo(unittest.TestCase):
    """Test suite for HybridMessagingIo class"""
    
    def setUp(self):
        """Set up test environment"""
        # Mock the backend dependencies
        self.mock_cif_io = Mock()
        self.mock_db_io = Mock()
        
        # Create hybrid messaging IO with mocked backends
        with patch('wwpdb.apps.msgmodule.io.HybridMessagingIo.MessagingIo') as mock_cif_class:
            with patch('wwpdb.apps.msgmodule.io.HybridMessagingIo.MessagingIoDatabase') as mock_db_class:
                with patch('wwpdb.apps.msgmodule.io.HybridMessagingIo.DatabaseConfig') as mock_config:
                    # Configure mocks
                    mock_cif_class.return_value = self.mock_cif_io
                    mock_db_class.return_value = self.mock_db_io
                    mock_config.return_value.is_enabled.return_value = True
                    
                    self.hybrid_io = HybridMessagingIo(
                        verbose=True,
                        site_id="TEST",
                        write_strategy=WriteStrategy.DUAL_WRITE
                    )
    
    def test_initialization(self):
        """Test HybridMessagingIo initialization"""
        self.assertIsNotNone(self.hybrid_io)
        self.assertEqual(self.hybrid_io._HybridMessagingIo__write_strategy, WriteStrategy.DUAL_WRITE)
        self.assertEqual(self.hybrid_io._HybridMessagingIo__siteId, "TEST")
    
    def test_cif_only_write_strategy(self):
        """Test CIF-only write strategy"""
        self.hybrid_io.setWriteStrategy(WriteStrategy.CIF_ONLY)
        
        # Mock successful CIF write
        self.mock_cif_io.addMessage.return_value = True
        
        result = self.hybrid_io.addMessage(
            depositionDataSetId="D_000001",
            messageText="Test message",
            messageSubject="Test subject"
        )
        
        self.assertTrue(result)
        self.mock_cif_io.addMessage.assert_called_once()
        self.mock_db_io.addMessage.assert_not_called()
    
    def test_db_only_write_strategy(self):
        """Test database-only write strategy"""
        self.hybrid_io.setWriteStrategy(WriteStrategy.DB_ONLY)
        
        # Mock successful DB write
        self.mock_db_io.addMessage.return_value = True
        
        result = self.hybrid_io.addMessage(
            depositionDataSetId="D_000001",
            messageText="Test message",
            messageSubject="Test subject"
        )
        
        self.assertTrue(result)
        self.mock_db_io.addMessage.assert_called_once()
        self.mock_cif_io.addMessage.assert_not_called()
    
    def test_dual_write_strategy_success(self):
        """Test successful dual-write strategy"""
        self.hybrid_io.setWriteStrategy(WriteStrategy.DUAL_WRITE)
        
        # Mock successful writes to both backends
        self.mock_cif_io.addMessage.return_value = True
        self.mock_db_io.addMessage.return_value = True
        
        result = self.hybrid_io.addMessage(
            depositionDataSetId="D_000001",
            messageText="Test message",
            messageSubject="Test subject"
        )
        
        self.assertTrue(result)
        self.mock_cif_io.addMessage.assert_called_once()
        self.mock_db_io.addMessage.assert_called_once()
    
    def test_dual_write_strategy_failure(self):
        """Test dual-write strategy with one backend failing"""
        self.hybrid_io.setWriteStrategy(WriteStrategy.DUAL_WRITE)
        
        # Mock CIF success, DB failure
        self.mock_cif_io.addMessage.return_value = True
        self.mock_db_io.addMessage.side_effect = Exception("Database error")
        
        result = self.hybrid_io.addMessage(
            depositionDataSetId="D_000001",
            messageText="Test message",
            messageSubject="Test subject"
        )
        
        # Dual write requires both to succeed
        self.assertFalse(result)
        self.mock_cif_io.addMessage.assert_called_once()
        self.mock_db_io.addMessage.assert_called_once()
    
    def test_db_primary_with_fallback_success(self):
        """Test DB primary with CIF fallback - DB succeeds"""
        self.hybrid_io.setWriteStrategy(WriteStrategy.DB_PRIMARY_CIF_FALLBACK)
        
        # Mock successful DB write
        self.mock_db_io.addMessage.return_value = True
        
        result = self.hybrid_io.addMessage(
            depositionDataSetId="D_000001",
            messageText="Test message",
            messageSubject="Test subject"
        )
        
        self.assertTrue(result)
        self.mock_db_io.addMessage.assert_called_once()
        self.mock_cif_io.addMessage.assert_not_called()  # Should not fallback
    
    def test_db_primary_with_fallback_failure(self):
        """Test DB primary with CIF fallback - DB fails, CIF succeeds"""
        self.hybrid_io.setWriteStrategy(WriteStrategy.DB_PRIMARY_CIF_FALLBACK)
        
        # Mock DB failure, CIF success
        self.mock_db_io.addMessage.side_effect = Exception("Database error")
        self.mock_cif_io.addMessage.return_value = True
        
        result = self.hybrid_io.addMessage(
            depositionDataSetId="D_000001",
            messageText="Test message",
            messageSubject="Test subject"
        )
        
        self.assertTrue(result)
        self.mock_db_io.addMessage.assert_called_once()
        self.mock_cif_io.addMessage.assert_called_once()  # Should fallback
    
    def test_fetch_messages_db_primary(self):
        """Test message fetching with database primary"""
        # Mock database backend as healthy
        self.hybrid_io._backend_health['database'] = BackendStatus.HEALTHY
        
        # Mock successful DB fetch
        expected_messages = [
            {"messageId": "1", "messageText": "Test 1"},
            {"messageId": "2", "messageText": "Test 2"}
        ]
        self.mock_db_io.fetchMessages.return_value = expected_messages
        
        result = self.hybrid_io.fetchMessages(depositionDataSetId="D_000001")
        
        self.assertEqual(result, expected_messages)
        self.mock_db_io.fetchMessages.assert_called_once_with("D_000001")
        self.mock_cif_io.fetchMessages.assert_not_called()
    
    def test_fetch_messages_fallback_to_cif(self):
        """Test message fetching fallback to CIF on DB failure"""
        # Mock database backend as healthy initially
        self.hybrid_io._backend_health['database'] = BackendStatus.HEALTHY
        self.hybrid_io._backend_health['cif'] = BackendStatus.HEALTHY
        
        # Mock DB failure, CIF success
        self.mock_db_io.fetchMessages.side_effect = Exception("Database error")
        expected_messages = [{"messageId": "1", "messageText": "Test 1"}]
        self.mock_cif_io.fetchMessages.return_value = expected_messages
        
        result = self.hybrid_io.fetchMessages(depositionDataSetId="D_000001")
        
        self.assertEqual(result, expected_messages)
        self.mock_db_io.fetchMessages.assert_called_once()
        self.mock_cif_io.fetchMessages.assert_called_once()
    
    def test_performance_metrics_collection(self):
        """Test performance metrics collection"""
        self.hybrid_io.setWriteStrategy(WriteStrategy.DUAL_WRITE)
        
        # Mock successful writes
        self.mock_cif_io.addMessage.return_value = True
        self.mock_db_io.addMessage.return_value = True
        
        # Perform multiple operations
        for i in range(3):
            self.hybrid_io.addMessage(
                depositionDataSetId=f"D_00000{i}",
                messageText=f"Test message {i}",
                messageSubject=f"Test subject {i}"
            )
        
        # Get metrics
        metrics = self.hybrid_io.getPerformanceMetrics()
        
        # Verify metrics structure
        self.assertIn('backend_health', metrics)
        self.assertIn('write_strategy', metrics)
        self.assertIn('feature_flags', metrics)
        self.assertEqual(metrics['write_strategy'], 'dual_write')
    
    def test_backend_health_monitoring(self):
        """Test backend health status monitoring"""
        health = self.hybrid_io.getBackendHealth()
        
        self.assertIn('cif', health)
        self.assertIn('database', health)
        self.assertIsInstance(health['cif'], str)
        self.assertIsInstance(health['database'], str)


class TestConsistencyValidator(unittest.TestCase):
    """Test suite for ConsistencyValidator class"""
    
    def setUp(self):
        """Set up test environment"""
        self.mock_cif_io = Mock()
        self.mock_db_io = Mock()
        self.validator = ConsistencyValidator(self.mock_cif_io, self.mock_db_io)
    
    def test_consistency_validation_success(self):
        """Test successful consistency validation"""
        # Mock identical messages from both backends
        messages = [
            {
                "messageId": "1",
                "messageSubject": "Test Subject",
                "messageText": "Test message content",
                "sender": "test@example.com",
                "timestamp": "2025-07-10T12:00:00"
            }
        ]
        
        self.mock_cif_io.fetchMessages.return_value = messages
        self.mock_db_io.fetchMessages.return_value = messages
        
        result = self.validator.validate_deposition("D_000001")
        
        self.assertTrue(result.consistent)
        self.assertEqual(result.cif_count, 1)
        self.assertEqual(result.db_count, 1)
        self.assertEqual(len(result.differences), 0)
    
    def test_consistency_validation_count_mismatch(self):
        """Test consistency validation with count mismatch"""
        cif_messages = [{"messageId": "1"}, {"messageId": "2"}]
        db_messages = [{"messageId": "1"}]
        
        self.mock_cif_io.fetchMessages.return_value = cif_messages
        self.mock_db_io.fetchMessages.return_value = db_messages
        
        result = self.validator.validate_deposition("D_000001")
        
        self.assertFalse(result.consistent)
        self.assertEqual(result.cif_count, 2)
        self.assertEqual(result.db_count, 1)
        self.assertGreater(len(result.differences), 0)
        self.assertIn("Message count mismatch", result.differences[0])
    
    def test_consistency_validation_content_mismatch(self):
        """Test consistency validation with content differences"""
        cif_messages = [{
            "messageId": "1",
            "messageSubject": "Original Subject",
            "messageText": "Original text"
        }]
        db_messages = [{
            "messageId": "1", 
            "messageSubject": "Modified Subject",
            "messageText": "Original text"
        }]
        
        self.mock_cif_io.fetchMessages.return_value = cif_messages
        self.mock_db_io.fetchMessages.return_value = db_messages
        
        result = self.validator.validate_deposition("D_000001")
        
        self.assertFalse(result.consistent)
        self.assertGreater(len(result.differences), 0)
        self.assertTrue(any("messageSubject" in diff for diff in result.differences))


class TestFeatureFlagManager(unittest.TestCase):
    """Test suite for FeatureFlagManager class"""
    
    def setUp(self):
        """Set up test environment"""
        self.flag_manager = FeatureFlagManager(site_id="TEST")
    
    def test_default_flags_initialization(self):
        """Test that default flags are properly initialized"""
        flags = self.flag_manager.get_all_flags()
        
        # Check that key flags exist
        expected_flags = [
            'hybrid_dual_write',
            'hybrid_db_primary',
            'hybrid_db_only',
            'consistency_checks',
            'performance_metrics'
        ]
        
        for flag_name in expected_flags:
            self.assertIn(flag_name, flags)
            self.assertIsInstance(flags[flag_name]['enabled'], bool)
    
    def test_flag_enabling_and_disabling(self):
        """Test enabling and disabling flags"""
        flag_name = 'hybrid_dual_write'
        
        # Initially disabled
        self.assertFalse(self.flag_manager.is_enabled(flag_name))
        
        # Enable flag
        self.flag_manager.set_flag(flag_name, True)
        self.assertTrue(self.flag_manager.is_enabled(flag_name))
        
        # Disable flag
        self.flag_manager.set_flag(flag_name, False)
        self.assertFalse(self.flag_manager.is_enabled(flag_name))
    
    def test_rollout_percentage(self):
        """Test rollout percentage functionality"""
        flag_name = 'test_rollout'
        
        # Set flag with 50% rollout
        self.flag_manager.set_flag(flag_name, True, rollout_percentage=50.0)
        
        # Test with consistent context
        context = {'deposition_id': 'TEST_DEPOSITION'}
        
        # The result should be consistent for the same context
        result1 = self.flag_manager.is_enabled(flag_name, context)
        result2 = self.flag_manager.is_enabled(flag_name, context)
        self.assertEqual(result1, result2)
    
    def test_recommended_write_strategy(self):
        """Test recommended write strategy logic"""
        # Test default strategy (no flags enabled)
        strategy = self.flag_manager.get_recommended_write_strategy()
        self.assertEqual(strategy, 'cif_only')
        
        # Test dual write strategy
        self.flag_manager.set_flag('hybrid_dual_write', True)
        strategy = self.flag_manager.get_recommended_write_strategy()
        self.assertEqual(strategy, 'dual_write')
        
        # Test DB primary strategy (should override dual write)
        self.flag_manager.set_flag('hybrid_db_primary', True)
        strategy = self.flag_manager.get_recommended_write_strategy()
        self.assertEqual(strategy, 'db_primary_cif_fallback')
        
        # Test DB only strategy (should override all others)
        self.flag_manager.set_flag('hybrid_db_only', True)
        strategy = self.flag_manager.get_recommended_write_strategy()
        self.assertEqual(strategy, 'db_only')
    
    def test_feature_flag_context(self):
        """Test FeatureFlagContext helper"""
        context = (FeatureFlagContext()
                  .with_deposition('D_000001')
                  .with_user('test_user', ['admin', 'developer'])
                  .with_site('TEST')
                  .build())
        
        expected_context = {
            'deposition_id': 'D_000001',
            'user_id': 'test_user',
            'user_groups': ['admin', 'developer'],
            'site_id': 'TEST'
        }
        
        self.assertEqual(context, expected_context)
    
    @patch.dict(os.environ, {'MSGDB_FLAG_HYBRID_DUAL_WRITE': 'true'})
    def test_environment_variable_override(self):
        """Test environment variable override functionality"""
        # Create new manager to pick up environment variables
        flag_manager = FeatureFlagManager(site_id="TEST")
        
        # Check that environment variable overrode default
        self.assertTrue(flag_manager.is_enabled('hybrid_dual_write'))


class TestCircuitBreaker(unittest.TestCase):
    """Test suite for CircuitBreaker class"""
    
    def setUp(self):
        """Set up test environment"""
        config = CircuitBreakerConfig(
            failure_threshold=2,
            recovery_timeout=1,  # 1 second for fast testing
            success_threshold=1,
            timeout=0.5
        )
        self.circuit_breaker = CircuitBreaker("test", config)
    
    def test_circuit_breaker_closed_state(self):
        """Test circuit breaker in closed state"""
        self.assertTrue(self.circuit_breaker.is_closed)
        self.assertFalse(self.circuit_breaker.is_open)
        self.assertFalse(self.circuit_breaker.is_half_open)
    
    def test_successful_operation(self):
        """Test successful operation through circuit breaker"""
        def successful_operation():
            return "success"
        
        result = self.circuit_breaker.call(successful_operation)
        self.assertEqual(result, "success")
        self.assertTrue(self.circuit_breaker.is_closed)
    
    def test_circuit_breaker_opening_on_failures(self):
        """Test circuit breaker opening after failure threshold"""
        def failing_operation():
            raise Exception("Operation failed")
        
        # First failure
        with self.assertRaises(Exception):
            self.circuit_breaker.call(failing_operation)
        self.assertTrue(self.circuit_breaker.is_closed)
        
        # Second failure - should open circuit
        with self.assertRaises(Exception):
            self.circuit_breaker.call(failing_operation)
        self.assertTrue(self.circuit_breaker.is_open)
    
    def test_circuit_breaker_rejects_when_open(self):
        """Test circuit breaker rejects calls when open"""
        # Force circuit breaker open
        self.circuit_breaker.force_open()
        
        def any_operation():
            return "should not execute"
        
        with self.assertRaises(CircuitBreakerOpenException):
            self.circuit_breaker.call(any_operation)
    
    def test_circuit_breaker_half_open_recovery(self):
        """Test circuit breaker recovery through half-open state"""
        # Force circuit breaker open
        self.circuit_breaker.force_open()
        
        # Wait for recovery timeout
        time.sleep(1.1)
        
        def successful_operation():
            return "success"
        
        # Should transition to half-open and then closed
        result = self.circuit_breaker.call(successful_operation)
        self.assertEqual(result, "success")
        self.assertTrue(self.circuit_breaker.is_closed)
    
    def test_circuit_breaker_metrics(self):
        """Test circuit breaker metrics collection"""
        def successful_operation():
            return "success"
        
        def failing_operation():
            raise Exception("failure")
        
        # Perform some operations
        self.circuit_breaker.call(successful_operation)
        
        try:
            self.circuit_breaker.call(failing_operation)
        except Exception:
            pass
        
        metrics = self.circuit_breaker.get_metrics()
        
        self.assertIn('total_calls', metrics['metrics'])
        self.assertIn('successful_calls', metrics['metrics'])
        self.assertIn('failed_calls', metrics['metrics'])
        self.assertEqual(metrics['metrics']['total_calls'], 2)
        self.assertEqual(metrics['metrics']['successful_calls'], 1)
        self.assertEqual(metrics['metrics']['failed_calls'], 1)
    
    def test_database_circuit_breaker(self):
        """Test specialized database circuit breaker"""
        db_breaker = DatabaseCircuitBreaker()
        
        self.assertEqual(db_breaker.name, "database")
        self.assertTrue(db_breaker.is_closed)
        
        # Test with database-appropriate configuration
        self.assertEqual(db_breaker.config.failure_threshold, 3)
        self.assertEqual(db_breaker.config.recovery_timeout, 30)


class TestPerformanceMetrics(unittest.TestCase):
    """Test suite for PerformanceMetrics class"""
    
    def setUp(self):
        """Set up test environment"""
        self.metrics = PerformanceMetrics()
    
    def test_metrics_recording(self):
        """Test metrics recording functionality"""
        # Record some operations
        self.metrics.record_write('cif', 100.0, True)
        self.metrics.record_write('database', 50.0, True)
        self.metrics.record_write('database', 200.0, False)  # Failure
        
        summary = self.metrics.get_summary()
        
        # Check that metrics are recorded
        self.assertEqual(summary['total_operations'], 3)
        self.assertEqual(summary['failover_count'], 1)
        self.assertEqual(summary['cif_write_times_count'], 1)
        self.assertEqual(summary['db_write_times_count'], 2)
    
    def test_consistency_check_recording(self):
        """Test consistency check metrics"""
        # Record consistency checks
        self.metrics.record_consistency_check(True)   # Success
        self.metrics.record_consistency_check(False)  # Failure
        self.metrics.record_consistency_check(True)   # Success
        
        summary = self.metrics.get_summary()
        
        self.assertEqual(summary['consistency_checks'], 3)
        self.assertEqual(summary['consistency_failures'], 1)
    
    def test_performance_percentiles(self):
        """Test performance percentile calculations"""
        # Record enough data points for percentile calculation
        for i in range(25):
            self.metrics.record_write('database', float(i * 10), True)
        
        summary = self.metrics.get_summary()
        
        self.assertIn('db_write_times_avg', summary)
        self.assertIn('db_write_times_p95', summary)
        self.assertGreater(summary['db_write_times_p95'], summary['db_write_times_avg'])


if __name__ == '__main__':
    # Create test suite
    test_classes = [
        TestHybridMessagingIo,
        TestConsistencyValidator, 
        TestFeatureFlagManager,
        TestCircuitBreaker,
        TestPerformanceMetrics
    ]
    
    suite = unittest.TestSuite()
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)
