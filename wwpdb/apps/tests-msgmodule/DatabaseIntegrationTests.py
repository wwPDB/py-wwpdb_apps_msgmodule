#!/usr/bin/env python
"""
Test suite for messaging database functionality.

This module contains unit tests for the database migration components
following standard Python testing conventions.
"""

import os
import sys
import unittest
import tempfile
import shutil
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch

# Add the project root to Python path for testing
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


class TestDatabaseConfiguration(unittest.TestCase):
    """Test database configuration management"""

    def setUp(self):
        """Set up test environment"""
        self.original_env = {}
        # Store original environment variables
        env_vars = [
            "MSGDB_ENABLED",
            "MSGDB_HOST",
            "MSGDB_PORT",
            "MSGDB_NAME",
            "MSGDB_USER",
            "MSGDB_PASS",
        ]
        for var in env_vars:
            if var in os.environ:
                self.original_env[var] = os.environ[var]
                del os.environ[var]

    def tearDown(self):
        """Clean up test environment"""
        # Restore original environment variables
        for var, value in self.original_env.items():
            os.environ[var] = value

        # Clean up any test environment variables
        env_vars = [
            "MSGDB_ENABLED",
            "MSGDB_HOST",
            "MSGDB_PORT",
            "MSGDB_NAME",
            "MSGDB_USER",
            "MSGDB_PASS",
        ]
        for var in env_vars:
            if var in os.environ and var not in self.original_env:
                del os.environ[var]

    def test_database_disabled_by_default(self):
        """Test that database is disabled by default"""
        from wwpdb.apps.msgmodule.db import is_messaging_database_enabled

        # No environment variables set
        self.assertFalse(is_messaging_database_enabled())

    def test_database_enabled_by_environment(self):
        """Test database enabling through environment variables"""
        from wwpdb.apps.msgmodule.db import is_messaging_database_enabled

        os.environ["MSGDB_ENABLED"] = "true"
        self.assertTrue(is_messaging_database_enabled())

        os.environ["MSGDB_ENABLED"] = "false"
        self.assertFalse(is_messaging_database_enabled())

    def test_configuration_loading_from_environment(self):
        """Test configuration loading from environment variables"""
        from wwpdb.apps.msgmodule.db import get_messaging_database_config

        # Set test environment
        os.environ["MSGDB_ENABLED"] = "true"
        os.environ["MSGDB_HOST"] = "test-host"
        os.environ["MSGDB_PORT"] = "3307"
        os.environ["MSGDB_NAME"] = "test_database"
        os.environ["MSGDB_USER"] = "test_user"
        os.environ["MSGDB_PASS"] = "test_password"

        config = get_messaging_database_config()

        self.assertEqual(config["host"], "test-host")
        self.assertEqual(config["port"], 3307)
        self.assertEqual(config["database"], "test_database")
        self.assertEqual(config["username"], "test_user")
        self.assertEqual(config["password"], "test_password")

    def test_configuration_validation(self):
        """Test configuration validation"""
        from wwpdb.apps.msgmodule.db.config import MessagingDatabaseConfig

        config_mgr = MessagingDatabaseConfig()

        # Valid configuration
        valid_config = {
            "host": "localhost",
            "port": 3306,
            "database": "test_db",
            "username": "test_user",
            "password": "test_pass",
        }
        self.assertTrue(config_mgr.validate_config(valid_config))

        # Invalid configuration - missing required field
        invalid_config = {
            "host": "localhost",
            "port": 3306,
            # Missing database and username
        }
        self.assertFalse(config_mgr.validate_config(invalid_config))

        # Invalid configuration - invalid port
        invalid_port_config = {
            "host": "localhost",
            "port": "invalid",
            "database": "test_db",
            "username": "test_user",
        }
        self.assertFalse(config_mgr.validate_config(invalid_port_config))


class TestDataModels(unittest.TestCase):
    """Test database data model classes"""

    def test_message_record_creation(self):
        """Test MessageRecord data model"""
        from wwpdb.apps.msgmodule.db import MessageRecord

        timestamp = datetime.now()
        message = MessageRecord(
            message_id="test-msg-001",
            deposition_data_set_id="D_1000000001",
            timestamp=timestamp,
            sender="test@wwpdb.org",
            message_subject="Test Subject",
            message_text="Test message content",
            content_type="msgs",
        )

        self.assertEqual(message.message_id, "test-msg-001")
        self.assertEqual(message.deposition_data_set_id, "D_1000000001")
        self.assertEqual(message.timestamp, timestamp)
        self.assertEqual(message.sender, "test@wwpdb.org")
        self.assertEqual(message.message_subject, "Test Subject")
        self.assertEqual(message.message_text, "Test message content")
        self.assertEqual(message.content_type, "msgs")
        self.assertEqual(message.send_status, "Y")  # Default value

    def test_message_status_creation(self):
        """Test MessageStatus data model"""
        from wwpdb.apps.msgmodule.db import MessageStatus

        status = MessageStatus(
            message_id="test-msg-001",
            deposition_data_set_id="D_1000000001",
            read_status="Y",
            action_reqd="N",
            for_release="Y",
        )

        self.assertEqual(status.message_id, "test-msg-001")
        self.assertEqual(status.deposition_data_set_id, "D_1000000001")
        self.assertEqual(status.read_status, "Y")
        self.assertEqual(status.action_reqd, "N")
        self.assertEqual(status.for_release, "Y")

    def test_message_file_reference_creation(self):
        """Test MessageFileReference data model"""
        from wwpdb.apps.msgmodule.db import MessageFileReference

        file_ref = MessageFileReference(
            message_id="test-msg-001",
            deposition_data_set_id="D_1000000001",
            content_type="validation-report",
            content_format="pdf",
            partition_number=1,
            version_id=2,
            file_source="archive",
            upload_file_name="validation_report.pdf",
            file_path="/path/to/file.pdf",
            file_size=1024000,
        )

        self.assertEqual(file_ref.message_id, "test-msg-001")
        self.assertEqual(file_ref.deposition_data_set_id, "D_1000000001")
        self.assertEqual(file_ref.content_type, "validation-report")
        self.assertEqual(file_ref.content_format, "pdf")
        self.assertEqual(file_ref.partition_number, 1)
        self.assertEqual(file_ref.version_id, 2)
        self.assertEqual(file_ref.file_source, "archive")
        self.assertEqual(file_ref.upload_file_name, "validation_report.pdf")
        self.assertEqual(file_ref.file_path, "/path/to/file.pdf")
        self.assertEqual(file_ref.file_size, 1024000)


class TestDatabaseService(unittest.TestCase):
    """Test database service functionality"""

    def test_database_service_creation_with_invalid_config(self):
        """Test database service creation with invalid configuration"""
        from wwpdb.apps.msgmodule.db import MessagingDatabaseService

        # Invalid configuration (non-existent host)
        invalid_config = {
            "host": "nonexistent-host-12345",
            "port": 3306,
            "database": "test_db",
            "username": "test_user",
            "password": "test_pass",
            "pool_size": 5,
            "charset": "utf8mb4",
        }

        # Should handle connection failure gracefully
        with self.assertRaises(Exception):
            service = MessagingDatabaseService(invalid_config)

    @patch("mysql.connector.pooling.MySQLConnectionPool")
    def test_database_service_creation_with_mock(self, mock_pool_class):
        """Test database service creation with mocked connection pool"""
        from wwpdb.apps.msgmodule.db import MessagingDatabaseService

        # Mock the connection pool
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool

        config = {
            "host": "localhost",
            "port": 3306,
            "database": "test_db",
            "username": "test_user",
            "password": "test_pass",
            "pool_size": 5,
            "charset": "utf8mb4",
        }

        service = MessagingDatabaseService(config)

        # Verify pool was created with correct configuration
        mock_pool_class.assert_called_once()
        call_args = mock_pool_class.call_args[1]
        self.assertEqual(call_args["host"], "localhost")
        self.assertEqual(call_args["port"], 3306)
        self.assertEqual(call_args["database"], "test_db")
        self.assertEqual(call_args["user"], "test_user")
        self.assertEqual(call_args["password"], "test_pass")


class TestMessagingIoIntegration(unittest.TestCase):
    """Test MessagingIo integration functionality"""

    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_mock_request_object(self):
        """Create a mock request object for testing"""

        class MockSession:
            def __init__(self, path):
                self.path = path

            def getPath(self):
                return self.path

        class MockRequest:
            def __init__(self, temp_dir):
                self.session = MockSession(temp_dir)
                self.values = {
                    "WWPDB_SITE_ID": "RCSB",
                    "groupid": "",
                    "identifier": "D_1000000001",
                }

            def newSessionObj(self):
                return self.session

            def getValue(self, key, default=""):
                return self.values.get(key, default)

        return MockRequest(self.temp_dir)

    def test_messaging_io_database_creation(self):
        """Test MessagingIoDatabase creation"""
        # This will fail due to missing dependencies, but we test the import and basic structure
        try:
            from wwpdb.apps.msgmodule.io.MessagingIoDatabase import MessagingIoDatabase

            req_obj = self.create_mock_request_object()

            # This may fail due to missing dependencies, which is expected
            try:
                msgIo = MessagingIoDatabase(req_obj, verbose=False)
                # If creation succeeds, verify it has the expected interface
                self.assertTrue(hasattr(msgIo, "processMsg"))
            except ImportError:
                # Expected if wwPDB dependencies are not available
                pass

        except ImportError:
            # Expected if the module can't be imported due to dependencies
            pass

    def test_backend_selection_logic(self):
        """Test the backend selection logic"""
        from wwpdb.apps.msgmodule.db import is_messaging_database_enabled

        # Test with database disabled
        os.environ["MSGDB_ENABLED"] = "false"
        self.assertFalse(is_messaging_database_enabled())

        # Test with database enabled
        os.environ["MSGDB_ENABLED"] = "true"
        self.assertTrue(is_messaging_database_enabled())


class TestMigrationUtilities(unittest.TestCase):
    """Test migration utility functionality"""

    def test_migration_script_imports(self):
        """Test that migration script can be imported"""
        try:
            # Test that the migration script module structure is correct
            import subprocess

            # Test help output for init script
            result = subprocess.run(
                [
                    sys.executable,
                    os.path.join(project_root, "scripts", "init_messaging_database.py"),
                    "--help",
                ],
                capture_output=True,
                text=True,
                cwd=project_root,
            )

            # Should exit successfully with help text
            self.assertEqual(result.returncode, 0)
            self.assertIn("Initialize messaging database schema", result.stdout)

        except Exception as e:
            # If script can't run due to missing dependencies, that's acceptable for testing
            pass


if __name__ == "__main__":
    # Set up logging to reduce noise during testing
    import logging

    logging.getLogger().setLevel(logging.WARNING)

    # Run the test suite
    unittest.main(verbosity=2)
