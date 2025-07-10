#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test cases for MessagingDb class - database-backed messaging implementation

This file tests the MessagingDb class which implements database-backed 
message storage as an alternative to the CIF-based MessagingIo class.
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

from wwpdb.apps.msgmodule.io.MessagingDb import MessagingDb


class TestMessagingDb(unittest.TestCase):
    """Test suite for MessagingDb class"""

    def setUp(self):
        """Set up test environment for MessagingDb"""
        # Create a mock request object
        self.mock_req_obj = Mock()
        self.mock_session_obj = Mock()
        self.mock_req_obj.newSessionObj.return_value = self.mock_session_obj
        self.mock_session_obj.getPath.return_value = "/tmp/test"
        self.mock_req_obj.getValue.side_effect = lambda key: {
            "WWPDB_SITE_ID": "TEST",
            "groupid": "test_group"
        }.get(key, "")

        # Create MessagingDb instance with mocked database dependencies
        with patch("wwpdb.apps.msgmodule.io.MessagingDb.is_messaging_database_enabled") as mock_enabled:
            with patch("wwpdb.apps.msgmodule.io.MessagingDb.get_messaging_database_config") as mock_config:
                with patch("wwpdb.apps.msgmodule.io.MessagingDb.MessagingDatabaseService") as mock_service:
                    with patch("wwpdb.apps.msgmodule.io.MessagingDb.ConfigInfo") as mock_config_info:
                        mock_enabled.return_value = True
                        mock_config.return_value = {"host": "test", "database": "test"}
                        self.mock_db_service = Mock()
                        mock_service.return_value = self.mock_db_service
                        
                        self.messaging_db = MessagingDb(self.mock_req_obj, verbose=True)

    def test_initialization(self):
        """Test MessagingDb initialization"""
        self.assertIsNotNone(self.messaging_db)
        self.assertTrue(hasattr(self.messaging_db, '_MessagingDb__siteId'))
        self.assertEqual(self.messaging_db._MessagingDb__siteId, "TEST")

    def test_database_enabled_initialization(self):
        """Test initialization when database is enabled"""
        with patch("wwpdb.apps.msgmodule.io.MessagingDb.is_messaging_database_enabled", return_value=True):
            with patch("wwpdb.apps.msgmodule.io.MessagingDb.get_messaging_database_config") as mock_config:
                with patch("wwpdb.apps.msgmodule.io.MessagingDb.MessagingDatabaseService") as mock_service:
                    with patch("wwpdb.apps.msgmodule.io.MessagingDb.ConfigInfo"):
                        mock_config.return_value = {"host": "testhost", "database": "testdb"}
                        
                        messaging_db = MessagingDb(self.mock_req_obj, verbose=True)
                        
                        # Verify database service was created
                        mock_service.assert_called_once()
                        self.assertIsNotNone(messaging_db._MessagingDb__db_service)

    def test_database_disabled_initialization(self):
        """Test initialization when database is disabled"""
        with patch("wwpdb.apps.msgmodule.io.MessagingDb.is_messaging_database_enabled", return_value=False):
            with patch("wwpdb.apps.msgmodule.io.MessagingDb.ConfigInfo"):
                messaging_db = MessagingDb(self.mock_req_obj, verbose=True)
                
                # Verify database service is None when disabled
                self.assertIsNone(messaging_db._MessagingDb__db_service)

    def test_process_message_with_database(self):
        """Test message processing with database enabled"""
        # Mock message object
        mock_msg_obj = Mock()
        mock_msg_obj.getDepositionDataSetId.return_value = "D_000001"
        mock_msg_obj.getMessageType.return_value = "to-depositor"
        
        # Mock database service methods
        self.mock_db_service.store_message.return_value = True
        
        # Process the message
        result = self.messaging_db.processMsg(mock_msg_obj)
        
        # Verify the result and that database service was called
        self.assertTrue(result)
        self.mock_db_service.store_message.assert_called_once()

    def test_add_message_interface(self):
        """Test the addMessage interface method"""
        # Mock the underlying database service
        self.mock_db_service.create_message.return_value = {"message_id": "test-id"}
        
        result = self.messaging_db.addMessage(
            depositionDataSetId="D_000001",
            messageText="Test message",
            messageSubject="Test subject"
        )
        
        self.assertTrue(result)

    def test_fetch_messages_interface(self):
        """Test the fetchMessages interface method"""
        # Mock the database service response
        mock_messages = [
            {
                "message_id": "msg-1",
                "message_text": "Test message 1",
                "message_subject": "Subject 1",
                "created_timestamp": "2024-01-01T00:00:00"
            }
        ]
        self.mock_db_service.get_messages.return_value = mock_messages
        
        result = self.messaging_db.fetchMessages("D_000001", "to-depositor")
        
        self.assertIsNotNone(result)
        self.mock_db_service.get_messages.assert_called_once()

    def test_error_handling_database_failure(self):
        """Test error handling when database operations fail"""
        # Mock database service to raise an exception
        self.mock_db_service.store_message.side_effect = Exception("Database error")
        
        mock_msg_obj = Mock()
        mock_msg_obj.getDepositionDataSetId.return_value = "D_000001"
        mock_msg_obj.getMessageType.return_value = "to-depositor"
        
        # Should handle the exception gracefully
        result = self.messaging_db.processMsg(mock_msg_obj)
        
        # The result should indicate failure
        self.assertFalse(result)


class TestMessagingDbIntegration(unittest.TestCase):
    """Integration tests for MessagingDb without mocking core dependencies"""

    def setUp(self):
        """Set up test environment for integration tests"""
        # Create a minimal mock request object
        self.mock_req_obj = Mock()
        self.mock_session_obj = Mock()
        self.mock_req_obj.newSessionObj.return_value = self.mock_session_obj
        self.mock_session_obj.getPath.return_value = "/tmp/test"
        self.mock_req_obj.getValue.side_effect = lambda key: {
            "WWPDB_SITE_ID": "TEST",
            "groupid": "test_group"
        }.get(key, "")

    def test_creation_without_database(self):
        """Test that MessagingDb can be created even when database is not available"""
        # This test doesn't mock the database dependencies to test real behavior
        try:
            messaging_db = MessagingDb(self.mock_req_obj, verbose=False)
            self.assertIsNotNone(messaging_db)
            # Should fallback gracefully when database is not available
        except Exception as e:
            self.fail(f"MessagingDb creation should not fail: {e}")


if __name__ == "__main__":
    unittest.main()
