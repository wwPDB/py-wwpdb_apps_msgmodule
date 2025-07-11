#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test cases for dual-mode messaging functionality

This file tests the new dual-mode messaging features including:
- Feature flag configuration logic
- MessagingFactory backend selection
- MessagingDualMode operations
- Migration scenario support
"""

import os
import sys
import unittest
from unittest.mock import Mock, patch, MagicMock

# Add the project root to the Python path
project_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..")
)
sys.path.insert(0, project_root)

# Import commonsetup for test configuration
sys.path.append(os.path.dirname(__file__))
import commonsetup


class TestDualModeFeatureFlags(unittest.TestCase):
    """Test suite for dual-mode feature flag configuration"""

    def setUp(self):
        """Set up test environment"""
        # Clear environment variables before each test
        self.env_vars = [
            "MSGDB_WRITES_ENABLED", "MSGDB_READS_ENABLED",
            "MSGCIF_WRITES_ENABLED", "MSGCIF_READS_ENABLED"
        ]
        for var in self.env_vars:
            if var in os.environ:
                del os.environ[var]

    def tearDown(self):
        """Clean up after each test"""
        # Clear environment variables after each test
        for var in self.env_vars:
            if var in os.environ:
                del os.environ[var]

    def test_cif_only_default(self):
        """Test CIF-only mode (default configuration)"""
        from wwpdb.apps.msgmodule.db.config import MessagingDatabaseConfig
        
        config = MessagingDatabaseConfig()
        
        self.assertFalse(config.is_database_writes_enabled())
        self.assertFalse(config.is_database_reads_enabled())
        self.assertTrue(config.is_cif_writes_enabled())
        self.assertTrue(config.is_cif_reads_enabled())

    def test_database_only_mode(self):
        """Test database-only mode configuration"""
        os.environ["MSGDB_WRITES_ENABLED"] = "true"
        os.environ["MSGDB_READS_ENABLED"] = "true"
        
        from wwpdb.apps.msgmodule.db.config import MessagingDatabaseConfig
        
        config = MessagingDatabaseConfig()
        
        self.assertTrue(config.is_database_writes_enabled())
        self.assertTrue(config.is_database_reads_enabled())
        self.assertFalse(config.is_cif_writes_enabled())
        self.assertFalse(config.is_cif_reads_enabled())

    def test_dual_write_cif_read_phase1(self):
        """Test Migration Phase 1: dual-write, CIF-read"""
        os.environ["MSGDB_WRITES_ENABLED"] = "true"
        os.environ["MSGCIF_WRITES_ENABLED"] = "true"
        os.environ["MSGCIF_READS_ENABLED"] = "true"
        
        from wwpdb.apps.msgmodule.db.config import MessagingDatabaseConfig
        
        config = MessagingDatabaseConfig()
        
        self.assertTrue(config.is_database_writes_enabled())
        self.assertFalse(config.is_database_reads_enabled())
        self.assertTrue(config.is_cif_writes_enabled())
        self.assertTrue(config.is_cif_reads_enabled())

    def test_dual_write_db_read_phase2(self):
        """Test Migration Phase 2: dual-write, DB-read"""
        os.environ["MSGDB_WRITES_ENABLED"] = "true"
        os.environ["MSGDB_READS_ENABLED"] = "true"
        os.environ["MSGCIF_WRITES_ENABLED"] = "true"
        
        from wwpdb.apps.msgmodule.db.config import MessagingDatabaseConfig
        
        config = MessagingDatabaseConfig()
        
        self.assertTrue(config.is_database_writes_enabled())
        self.assertTrue(config.is_database_reads_enabled())
        self.assertTrue(config.is_cif_writes_enabled())
        self.assertFalse(config.is_cif_reads_enabled())

    def test_explicit_false_flags(self):
        """Test explicitly setting flags to false"""
        os.environ["MSGDB_WRITES_ENABLED"] = "false"
        os.environ["MSGDB_READS_ENABLED"] = "false"
        os.environ["MSGCIF_WRITES_ENABLED"] = "false"
        os.environ["MSGCIF_READS_ENABLED"] = "false"
        
        from wwpdb.apps.msgmodule.db.config import MessagingDatabaseConfig
        
        config = MessagingDatabaseConfig()
        
        self.assertFalse(config.is_database_writes_enabled())
        self.assertFalse(config.is_database_reads_enabled())
        self.assertFalse(config.is_cif_writes_enabled())
        self.assertFalse(config.is_cif_reads_enabled())

    def test_convenience_functions(self):
        """Test convenience functions in db module"""
        os.environ["MSGDB_WRITES_ENABLED"] = "true"
        os.environ["MSGCIF_READS_ENABLED"] = "true"
        
        from wwpdb.apps.msgmodule.db import (
            is_messaging_database_writes_enabled,
            is_messaging_database_reads_enabled,
            is_messaging_cif_writes_enabled,
            is_messaging_cif_reads_enabled
        )
        
        self.assertTrue(is_messaging_database_writes_enabled())
        self.assertFalse(is_messaging_database_reads_enabled())
        self.assertFalse(is_messaging_cif_writes_enabled())
        self.assertTrue(is_messaging_cif_reads_enabled())


class TestMessagingFactory(unittest.TestCase):
    """Test suite for MessagingFactory backend selection"""

    def setUp(self):
        """Set up test environment"""
        # Clear environment variables before each test
        self.env_vars = [
            "MSGDB_WRITES_ENABLED", "MSGDB_READS_ENABLED",
            "MSGCIF_WRITES_ENABLED", "MSGCIF_READS_ENABLED"
        ]
        for var in self.env_vars:
            if var in os.environ:
                del os.environ[var]
                
        # Create mock request object
        self.mock_req_obj = Mock()
        self.mock_req_obj.getValue.return_value = "RCSB"

    def tearDown(self):
        """Clean up after each test"""
        for var in self.env_vars:
            if var in os.environ:
                del os.environ[var]

    def test_factory_backend_info_cif_only(self):
        """Test factory backend info for CIF-only mode"""
        from wwpdb.apps.msgmodule.io.MessagingFactory import MessagingFactory
        
        info = MessagingFactory.get_backend_info(self.mock_req_obj)
        
        self.assertEqual(info["selected_backend"], "cif")
        self.assertEqual(info["backend_class"], "MessagingIo")
        self.assertIn("feature_flags", info)
        
        flags = info["feature_flags"]
        self.assertFalse(flags["database_writes"])
        self.assertFalse(flags["database_reads"])
        self.assertTrue(flags["cif_writes"])
        self.assertTrue(flags["cif_reads"])

    def test_factory_backend_info_database_only(self):
        """Test factory backend info for database-only mode"""
        os.environ["MSGDB_WRITES_ENABLED"] = "true"
        os.environ["MSGDB_READS_ENABLED"] = "true"
        
        from wwpdb.apps.msgmodule.io.MessagingFactory import MessagingFactory
        
        info = MessagingFactory.get_backend_info(self.mock_req_obj)
        
        self.assertEqual(info["selected_backend"], "database")
        self.assertEqual(info["backend_class"], "MessagingDb")
        
        flags = info["feature_flags"]
        self.assertTrue(flags["database_writes"])
        self.assertTrue(flags["database_reads"])
        self.assertFalse(flags["cif_writes"])
        self.assertFalse(flags["cif_reads"])

    def test_factory_backend_info_dual_mode(self):
        """Test factory backend info for dual mode"""
        os.environ["MSGDB_WRITES_ENABLED"] = "true"
        os.environ["MSGCIF_WRITES_ENABLED"] = "true"
        os.environ["MSGCIF_READS_ENABLED"] = "true"
        
        from wwpdb.apps.msgmodule.io.MessagingFactory import MessagingFactory
        
        info = MessagingFactory.get_backend_info(self.mock_req_obj)
        
        self.assertEqual(info["selected_backend"], "dual")
        self.assertEqual(info["backend_class"], "MessagingDualMode")
        
        flags = info["feature_flags"]
        self.assertTrue(flags["database_writes"])
        self.assertFalse(flags["database_reads"])
        self.assertTrue(flags["cif_writes"])
        self.assertTrue(flags["cif_reads"])

    def test_factory_create_backend_selection_only(self):
        """Test factory backend selection logic (without full instantiation)"""
        from wwpdb.apps.msgmodule.io.MessagingFactory import MessagingFactory
        
        # Test 1: CIF-only mode (default)
        info = MessagingFactory.get_backend_info(self.mock_req_obj)
        self.assertEqual(info["selected_backend"], "cif")
        self.assertEqual(info["backend_class"], "MessagingIo")
        
        # Test 2: Database-only mode
        os.environ["MSGDB_WRITES_ENABLED"] = "true"
        os.environ["MSGDB_READS_ENABLED"] = "true"
        
        info = MessagingFactory.get_backend_info(self.mock_req_obj)
        self.assertEqual(info["selected_backend"], "database")
        self.assertEqual(info["backend_class"], "MessagingDb")
        
        # Test 3: Dual mode
        os.environ["MSGCIF_WRITES_ENABLED"] = "true"
        os.environ["MSGCIF_READS_ENABLED"] = "true"
        
        info = MessagingFactory.get_backend_info(self.mock_req_obj)
        self.assertEqual(info["selected_backend"], "dual")
        self.assertEqual(info["backend_class"], "MessagingDualMode")
        
        # The backend selection logic is working correctly
        # Full instantiation tests would require more complex mocking of session paths, etc.


class TestMessagingDualMode(unittest.TestCase):
    """Test suite for MessagingDualMode operations"""

    def setUp(self):
        """Set up test environment"""
        self.mock_cif_service = Mock()
        self.mock_db_service = Mock()
        
        from wwpdb.apps.msgmodule.io.MessagingDualMode import MessagingDualMode
        
        self.dual_mode = MessagingDualMode(
            cif_service=self.mock_cif_service,
            db_service=self.mock_db_service,
            db_writes_enabled=True,
            db_reads_enabled=False,
            cif_writes_enabled=True,
            cif_reads_enabled=True,
            verbose=False
        )

    def test_dual_write_operation(self):
        """Test dual-write operation"""
        self.mock_cif_service.set.return_value = True
        self.mock_db_service.set.return_value = True
        
        result = self.dual_mode.set("test_deposition", {"message": "test"})
        
        self.assertTrue(result)
        self.mock_cif_service.set.assert_called_once_with("test_deposition", {"message": "test"})
        self.mock_db_service.set.assert_called_once_with("test_deposition", {"message": "test"})

    def test_write_partial_success(self):
        """Test write operation with partial success"""
        self.mock_cif_service.set.return_value = True
        self.mock_db_service.set.side_effect = Exception("DB Error")
        
        result = self.dual_mode.set("test_deposition", {"message": "test"})
        
        # Should still return True if at least one write succeeds
        self.assertTrue(result)
        self.mock_cif_service.set.assert_called_once()
        self.mock_db_service.set.assert_called_once()

    def test_write_all_fail(self):
        """Test write operation when all backends fail"""
        self.mock_cif_service.set.side_effect = Exception("CIF Error")
        self.mock_db_service.set.side_effect = Exception("DB Error")
        
        result = self.dual_mode.set("test_deposition", {"message": "test"})
        
        self.assertFalse(result)

    def test_read_from_cif_priority(self):
        """Test read operation with CIF priority"""
        expected_data = {"messages": ["test message"]}
        self.mock_cif_service.get.return_value = expected_data
        
        result = self.dual_mode.get("test_deposition")
        
        self.assertEqual(result, expected_data)
        self.mock_cif_service.get.assert_called_once_with("test_deposition")
        self.mock_db_service.get.assert_not_called()

    def test_read_db_priority_when_enabled(self):
        """Test read operation with database priority when DB reads enabled"""
        # Create dual mode with DB reads enabled
        from wwpdb.apps.msgmodule.io.MessagingDualMode import MessagingDualMode
        
        dual_mode_db_reads = MessagingDualMode(
            cif_service=self.mock_cif_service,
            db_service=self.mock_db_service,
            db_writes_enabled=True,
            db_reads_enabled=True,  # Enable DB reads
            cif_writes_enabled=True,
            cif_reads_enabled=False,  # Disable CIF reads
            verbose=False
        )
        
        expected_data = {"messages": ["test message"]}
        self.mock_db_service.get.return_value = expected_data
        
        result = dual_mode_db_reads.get("test_deposition")
        
        self.assertEqual(result, expected_data)
        self.mock_db_service.get.assert_called_once_with("test_deposition")
        self.mock_cif_service.get.assert_not_called()

    def test_read_fallback_behavior(self):
        """Test read fallback from DB to CIF when DB fails"""
        from wwpdb.apps.msgmodule.io.MessagingDualMode import MessagingDualMode
        
        dual_mode_with_fallback = MessagingDualMode(
            cif_service=self.mock_cif_service,
            db_service=self.mock_db_service,
            db_writes_enabled=True,
            db_reads_enabled=True,
            cif_writes_enabled=True,
            cif_reads_enabled=True,  # Enable both for fallback
            verbose=False
        )
        
        expected_data = {"messages": ["fallback message"]}
        self.mock_db_service.get.side_effect = Exception("DB Error")
        self.mock_cif_service.get.return_value = expected_data
        
        result = dual_mode_with_fallback.get("test_deposition")
        
        self.assertEqual(result, expected_data)
        self.mock_db_service.get.assert_called_once()
        self.mock_cif_service.get.assert_called_once()

    def test_backend_info(self):
        """Test getBackendInfo method"""
        info = self.dual_mode.getBackendInfo()
        
        expected_info = {
            "mode": "dual",
            "db_writes_enabled": True,
            "db_reads_enabled": False,
            "cif_writes_enabled": True,
            "cif_reads_enabled": True,
            "read_priority": "cif",
            "write_targets": ["database", "cif"]
        }
        
        self.assertEqual(info, expected_info)

    def test_exists_method(self):
        """Test exists method delegation"""
        self.mock_cif_service.exists.return_value = True
        
        result = self.dual_mode.exists("test_deposition")
        
        self.assertTrue(result)
        self.mock_cif_service.exists.assert_called_once_with("test_deposition")

    def test_delete_dual_operation(self):
        """Test delete operation on both backends"""
        self.mock_cif_service.delete.return_value = True
        self.mock_db_service.delete.return_value = True
        
        result = self.dual_mode.delete("test_deposition")
        
        self.assertTrue(result)
        self.mock_cif_service.delete.assert_called_once_with("test_deposition")
        self.mock_db_service.delete.assert_called_once_with("test_deposition")


class TestMigrationScenarios(unittest.TestCase):
    """Test suite for migration scenarios end-to-end"""

    def setUp(self):
        """Set up test environment"""
        self.env_vars = [
            "MSGDB_WRITES_ENABLED", "MSGDB_READS_ENABLED",
            "MSGCIF_WRITES_ENABLED", "MSGCIF_READS_ENABLED"
        ]
        for var in self.env_vars:
            if var in os.environ:
                del os.environ[var]
                
        self.mock_req_obj = Mock()
        self.mock_req_obj.getValue.return_value = "RCSB"

    def tearDown(self):
        """Clean up after each test"""
        for var in self.env_vars:
            if var in os.environ:
                del os.environ[var]

    def test_migration_phase_progression(self):
        """Test progression through migration phases"""
        from wwpdb.apps.msgmodule.io.MessagingFactory import MessagingFactory
        
        # Phase 1: Dual-write, CIF-read
        os.environ["MSGDB_WRITES_ENABLED"] = "true"
        os.environ["MSGCIF_WRITES_ENABLED"] = "true"
        os.environ["MSGCIF_READS_ENABLED"] = "true"
        
        info = MessagingFactory.get_backend_info(self.mock_req_obj)
        self.assertEqual(info["selected_backend"], "dual")
        
        flags = info["feature_flags"]
        self.assertTrue(flags["database_writes"])
        self.assertFalse(flags["database_reads"])
        self.assertTrue(flags["cif_writes"])
        self.assertTrue(flags["cif_reads"])
        
        # Phase 2: Dual-write, DB-read
        os.environ["MSGDB_READS_ENABLED"] = "true"
        del os.environ["MSGCIF_READS_ENABLED"]
        
        info = MessagingFactory.get_backend_info(self.mock_req_obj)
        self.assertEqual(info["selected_backend"], "dual")
        
        flags = info["feature_flags"]
        self.assertTrue(flags["database_writes"])
        self.assertTrue(flags["database_reads"])
        self.assertTrue(flags["cif_writes"])
        self.assertFalse(flags["cif_reads"])
        
        # Phase 3: Database-only
        del os.environ["MSGCIF_WRITES_ENABLED"]
        
        info = MessagingFactory.get_backend_info(self.mock_req_obj)
        self.assertEqual(info["selected_backend"], "database")
        
        flags = info["feature_flags"]
        self.assertTrue(flags["database_writes"])
        self.assertTrue(flags["database_reads"])
        self.assertFalse(flags["cif_writes"])
        self.assertFalse(flags["cif_reads"])


if __name__ == "__main__":
    unittest.main()
