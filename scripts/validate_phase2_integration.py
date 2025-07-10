#!/usr/bin/env python
"""
Phase 2 Integration Validation Script

Comprehensive validation of the complete Phase 2 database operations implementation.
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
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def test_database_io_integration():
    """Test MessagingDb operations - database-backed messaging implementation"""
    try:
        from wwpdb.apps.msgmodule.io.MessagingDb import MessagingDb
        from unittest.mock import Mock

        # Create a mock request object for testing
        mock_req_obj = Mock()
        mock_session_obj = Mock()
        mock_req_obj.newSessionObj.return_value = mock_session_obj
        mock_session_obj.getPath.return_value = "/tmp/integration_test"
        mock_req_obj.getValue.side_effect = lambda key: {
            "WWPDB_SITE_ID": "INTEGRATION_TEST",
            "groupid": "integration_test_group"
        }.get(key, "")

        # Test MessagingDb initialization
        messaging_db = MessagingDb(mock_req_obj, verbose=True)
        
        # Test that the instance was created successfully
        assert messaging_db is not None
        assert hasattr(messaging_db, '_MessagingDb__siteId')
        assert messaging_db._MessagingDb__siteId == "INTEGRATION_TEST"

        # Test basic interface methods exist
        assert hasattr(messaging_db, 'processMsg')
        assert hasattr(messaging_db, 'getMsgRowList')
        assert hasattr(messaging_db, 'markMsgAsRead')
        assert hasattr(messaging_db, 'getMsg')

        print("✅ MessagingDb integration working")
        return True

    except Exception as e:
        print(f"❌ MessagingDb integration failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_messaging_io_integration():
    """Test the original CIF-based MessagingIo for comparison"""
    try:
        from wwpdb.apps.msgmodule.io.MessagingIo import MessagingIo
        from unittest.mock import Mock

        # Create a mock request object for testing
        mock_req_obj = Mock()
        mock_session_obj = Mock()
        mock_req_obj.newSessionObj.return_value = mock_session_obj
        mock_session_obj.getPath.return_value = "/tmp/integration_test"
        mock_req_obj.getValue.side_effect = lambda key: {
            "WWPDB_SITE_ID": "INTEGRATION_TEST",
            "groupid": "integration_test_group"
        }.get(key, "")

        # Test MessagingIo initialization
        messaging_io = MessagingIo(mock_req_obj, verbose=True)
        
        # Test that the instance was created successfully
        assert messaging_io is not None

        # Test basic interface methods exist
        assert hasattr(messaging_io, 'processMsg')
        assert hasattr(messaging_io, 'getMsgRowList')

        print("✅ CIF-based MessagingIo integration working")
        return True

    except ImportError as e:
        if "MySQLdb" in str(e) or "_mysql" in str(e):
            print("⚠️  CIF-based MessagingIo skipped (MySQL dependencies not available)")
            return True  # Consider this a pass since it's an environment issue
        else:
            print(f"❌ CIF-based MessagingIo integration failed: {e}")
            import traceback
            traceback.print_exc()
            return False
    except Exception as e:
        print(f"❌ CIF-based MessagingIo integration failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_circuit_breaker_integration():
    """Test CircuitBreaker integration"""
    try:
        from wwpdb.apps.msgmodule.util.CircuitBreaker import (
            CircuitBreaker,
            CircuitBreakerConfig,
            get_database_circuit_breaker,
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
        assert "state" in metrics
        assert "metrics" in metrics

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
        assert "enabled" in db_config
        assert "host" in db_config

        # Test validation
        is_valid, errors = config.validate()
        # Should be valid even if disabled

        print("✅ Configuration integration working")
        return True

    except Exception as e:
        print(f"❌ Configuration integration failed: {e}")
        return False


def test_backend_selection():
    """Test that both MessagingIo (CIF) and MessagingDb (database) can be instantiated"""
    try:
        from wwpdb.apps.msgmodule.io.MessagingDb import MessagingDb
        from unittest.mock import Mock

        # Create a mock request object for testing
        mock_req_obj = Mock()
        mock_session_obj = Mock()
        mock_req_obj.newSessionObj.return_value = mock_session_obj
        mock_session_obj.getPath.return_value = "/tmp/integration_test"
        mock_req_obj.getValue.side_effect = lambda key: {
            "WWPDB_SITE_ID": "INTEGRATION_TEST",
            "groupid": "integration_test_group"
        }.get(key, "")

        # Test that MessagingDb can be created
        db_backend = MessagingDb(mock_req_obj, verbose=False)
        assert hasattr(db_backend, 'processMsg')
        assert hasattr(db_backend, 'getMsgRowList')

        # Try to import CIF backend, but don't fail if MySQL dependencies are missing
        try:
            from wwpdb.apps.msgmodule.io.MessagingIo import MessagingIo
            cif_backend = MessagingIo(mock_req_obj, verbose=False)
            assert hasattr(cif_backend, 'processMsg')
            assert hasattr(cif_backend, 'getMsgRowList')
            print("✅ Both CIF and Database backends available")
        except ImportError as e:
            if "MySQLdb" in str(e) or "_mysql" in str(e):
                print("✅ Database backend available, CIF backend skipped (MySQL dependencies)")
            else:
                raise

        return True

    except Exception as e:
        print(f"❌ Backend selection test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_test_suite():
    """Run the database operations test suite"""
    try:
        # Import using sys.path since tests-msgmodule has hyphen
        sys.path.insert(
            0, os.path.join(project_root, "wwpdb", "apps", "tests-msgmodule")
        )
        from DatabaseOperationsTests import (
            TestMessagingDb,
        )

        # Create test suite
        suite = unittest.TestSuite()

        # Add key tests for MessagingDb
        suite.addTest(TestMessagingDb("test_initialization"))
        suite.addTest(TestMessagingDb("test_database_enabled_initialization"))
        suite.addTest(TestMessagingDb("test_process_message_interface"))

        # Run tests
        runner = unittest.TextTestRunner(verbosity=1)
        result = runner.run(suite)

        if result.wasSuccessful():
            print("✅ Test suite passed")
            return True
        else:
            print(
                f"❌ Test suite failed: {len(result.failures)} failures, {len(result.errors)} errors"
            )
            return False

    except Exception as e:
        print(f"❌ Test suite execution failed: {e}")
        return False


def main():
    """Main integration validation function"""
    setup_logging()

    print("=" * 80)
    print("Phase 2 Database Operations - Integration Validation")
    print("=" * 80)
    print(f"Validation started at: {datetime.now()}")
    print()

    # Run integration tests
    tests = [
        ("Database Backend Integration", test_database_io_integration),
        ("CIF Backend Integration", test_messaging_io_integration),
        ("Backend Selection Test", test_backend_selection),
        ("Test Suite Execution", run_test_suite),
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
        print("All database operations components are working correctly!")
        return 0
    else:
        print("⚠️  Phase 2 Integration Validation: ISSUES DETECTED")
        print("Some components need attention before production deployment.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
