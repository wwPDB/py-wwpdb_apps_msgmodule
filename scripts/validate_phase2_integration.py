#!/usr/bin/env python
"""
Phase 2 Integration Validation Script

Comprehensive validation of the complete Phase 2 hybrid operations implementation.
This script validates all components working together as a complete system.

Author: wwPDB Migration Team
Date: July 2025
"""

import os
import sys
import unittest
import logging
from datetime import datetime

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

def setup_logging():
    """Set up logging for integration tests"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def test_hybrid_io_integration():
    """Test HybridMessagingIo integration with all strategies"""
    try:
        from wwpdb.apps.msgmodule.io.HybridMessagingIo import HybridMessagingIo, WriteStrategy
        
        # Test with all strategies
        strategies = [
            WriteStrategy.CIF_ONLY,
            WriteStrategy.DB_ONLY, 
            WriteStrategy.DUAL_WRITE,
            WriteStrategy.DB_PRIMARY_CIF_FALLBACK
        ]
        
        for strategy in strategies:
            hybrid_io = HybridMessagingIo(
                verbose=True,
                site_id="INTEGRATION_TEST",
                write_strategy=strategy
            )
            
            # Test basic operations
            result = hybrid_io.addMessage(
                depositionDataSetId="D_TEST_001",
                messageText="Integration test message",
                messageSubject="Integration Test"
            )
            
            # Test metrics
            metrics = hybrid_io.getPerformanceMetrics()
            assert 'backend_health' in metrics
            assert 'write_strategy' in metrics
            
            # Test health monitoring
            health = hybrid_io.getBackendHealth()
            assert 'cif' in health
            assert 'database' in health
            
            print(f"✅ Strategy {strategy.value}: Hybrid I/O integration working")
        
        return True
        
    except Exception as e:
        print(f"❌ Hybrid I/O integration failed: {e}")
        return False

def test_feature_flags_integration():
    """Test FeatureFlagManager integration"""
    try:
        from wwpdb.apps.msgmodule.util.FeatureFlagManager import FeatureFlagManager, FeatureFlagContext
        
        # Test flag manager
        manager = FeatureFlagManager(site_id="INTEGRATION_TEST")
        
        # Test default flags
        flags = manager.get_all_flags()
        assert len(flags) > 0
        
        # Test flag operations
        manager.set_flag('test_integration_flag', True, rollout_percentage=75.0)
        assert manager.is_enabled('test_integration_flag')
        
        # Test context
        context = (FeatureFlagContext()
                  .with_deposition('D_TEST_001')
                  .with_user('test_user')
                  .build())
        
        # Test strategy recommendation
        strategy = manager.get_recommended_write_strategy(context)
        assert strategy in ['cif_only', 'dual_write', 'db_primary_cif_fallback', 'db_only']
        
        print("✅ Feature flags integration working")
        return True
        
    except Exception as e:
        print(f"❌ Feature flags integration failed: {e}")
        return False

def test_circuit_breaker_integration():
    """Test CircuitBreaker integration"""
    try:
        from wwpdb.apps.msgmodule.util.CircuitBreaker import (
            CircuitBreaker, CircuitBreakerConfig, get_database_circuit_breaker
        )
        
        # Test basic circuit breaker
        config = CircuitBreakerConfig(failure_threshold=3, recovery_timeout=1)
        breaker = CircuitBreaker("integration_test", config)
        
        # Test successful operation
        def success_op():
            return "success"
        
        result = breaker.call(success_op)
        assert result == "success"
        assert breaker.is_closed
        
        # Test database circuit breaker
        db_breaker = get_database_circuit_breaker()
        assert db_breaker.name == "database"
        
        # Test metrics
        metrics = breaker.get_metrics()
        assert 'state' in metrics
        assert 'metrics' in metrics
        
        print("✅ Circuit breaker integration working")
        return True
        
    except Exception as e:
        print(f"❌ Circuit breaker integration failed: {e}")
        return False

def test_configuration_integration():
    """Test configuration system integration"""
    try:
        from wwpdb.apps.msgmodule.db.config import DatabaseConfig
        
        # Test database config
        config = DatabaseConfig()
        db_config = config.get_config()
        assert 'enabled' in db_config
        assert 'host' in db_config
        
        # Test validation
        is_valid, errors = config.validate()
        # Should be valid even if disabled
        
        print("✅ Configuration integration working")
        return True
        
    except Exception as e:
        print(f"❌ Configuration integration failed: {e}")
        return False

def run_test_suite():
    """Run the hybrid operations test suite"""
    try:
        # Import using sys.path since tests-msgmodule has hyphen
        sys.path.insert(0, os.path.join(project_root, 'wwpdb', 'apps', 'tests-msgmodule'))
        from HybridOperationsTests import (
            TestHybridMessagingIo, TestFeatureFlagManager, TestCircuitBreaker
        )
        
        # Create test suite
        suite = unittest.TestSuite()
        
        # Add key tests
        suite.addTest(TestHybridMessagingIo('test_initialization'))
        suite.addTest(TestHybridMessagingIo('test_dual_write_strategy_success'))
        suite.addTest(TestFeatureFlagManager('test_default_flags_initialization'))
        suite.addTest(TestCircuitBreaker('test_circuit_breaker_closed_state'))
        
        # Run tests
        runner = unittest.TextTestRunner(verbosity=1)
        result = runner.run(suite)
        
        if result.wasSuccessful():
            print("✅ Test suite passed")
            return True
        else:
            print(f"❌ Test suite failed: {len(result.failures)} failures, {len(result.errors)} errors")
            return False
            
    except Exception as e:
        print(f"❌ Test suite execution failed: {e}")
        return False

def main():
    """Main integration validation function"""
    setup_logging()
    
    print("=" * 80)
    print("Phase 2 Hybrid Operations - Integration Validation")
    print("=" * 80)
    print(f"Validation started at: {datetime.now()}")
    print()
    
    # Run integration tests
    tests = [
        ("Hybrid I/O Integration", test_hybrid_io_integration),
        ("Feature Flags Integration", test_feature_flags_integration), 
        ("Circuit Breaker Integration", test_circuit_breaker_integration),
        ("Configuration Integration", test_configuration_integration),
        ("Test Suite Execution", run_test_suite)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"Running {test_name}...")
        try:
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            results.append((test_name, False))
        print()
    
    # Summary
    print("=" * 80)
    print("Integration Validation Summary:")
    print("=" * 80)
    
    passed = 0
    total = len(results)
    
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} - {test_name}")
        if success:
            passed += 1
    
    print()
    print(f"Overall Result: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 Phase 2 Integration Validation: SUCCESS")
        print("All hybrid operations components are working correctly!")
        return 0
    else:
        print("⚠️  Phase 2 Integration Validation: ISSUES DETECTED")
        print("Some components need attention before production deployment.")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
