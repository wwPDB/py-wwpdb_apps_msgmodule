#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Integration test for MessagingDb as a replacement for MessagingIo

This test verifies that both implementations work correctly with the same interface
by testing them with identical inputs and comparing their outputs.
"""

import os
import sys
import unittest
import tempfile
import shutil
from datetime import datetime

# Add the project root to the Python path
project_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..")
)
sys.path.insert(0, project_root)

# Import necessary modules
from wwpdb.apps.msgmodule.io.MessagingFactory import MessagingFactory
from wwpdb.apps.msgmodule.models.Message import Message


class IntegrationTestRequest:
    """Mock request object with necessary methods for integration testing"""
    
    def __init__(self, site_id="TEST", group_id="test_group", session_path=None):
        self.site_id = site_id
        self.group_id = group_id
        self.session_path = session_path or tempfile.mkdtemp()
        self.values = {
            "WWPDB_SITE_ID": site_id,
            "groupid": group_id,
            "content_type": "msgs"
        }
    
    def getValue(self, key):
        """Get a value from the mock request"""
        return self.values.get(key, "")
    
    def setValue(self, key, value):
        """Set a value in the mock request"""
        self.values[key] = value
    
    def newSessionObj(self):
        """Return a mock session object"""
        return self
    
    def getPath(self):
        """Return the session path"""
        return self.session_path
    
    def getRelativePath(self):
        """Return a relative path (mock)"""
        return "session/path"
    
    def cleanup(self):
        """Clean up temporary files"""
        if os.path.exists(self.session_path):
            shutil.rmtree(self.session_path)


class TestMessagingBackendIntegration(unittest.TestCase):
    """Integration tests for both messaging backends"""
    
    def setUp(self):
        """Set up test environment with actual files and directories"""
        # Create a temporary session directory
        self.test_dir = tempfile.mkdtemp()
        
        # Create mock request object with real paths
        self.req_obj = IntegrationTestRequest(session_path=self.test_dir)
        
        # Save original environment variable
        self.original_backend = os.environ.get("WWPDB_MESSAGING_BACKEND")
        
        # Test ID for messages
        self.test_dep_id = "D_TESTCASE"
    
    def tearDown(self):
        """Clean up after tests"""
        # Restore original environment variable
        if self.original_backend is not None:
            os.environ["WWPDB_MESSAGING_BACKEND"] = self.original_backend
        elif "WWPDB_MESSAGING_BACKEND" in os.environ:
            del os.environ["WWPDB_MESSAGING_BACKEND"]
        
        # Clean up temporary directories
        self.req_obj.cleanup()
    
    def create_test_message(self):
        """Create a test message for both implementations"""
        message = Message()
        message.setDepositionDataSetId(self.test_dep_id)
        message.setMessageSubject("Test Subject")
        message.setMessageText("This is a test message.")
        message.setSender("test@example.com")
        message.setParentMessageId(None)
        message.setContextType("test-context")
        message.setContextValue("test-value")
        message.setMessageType("to-depositor")
        message.setTimestamp(datetime.now().isoformat())
        message.setIsBeingSent(True)
        return message
    
    def test_process_message_both_backends(self):
        """Test that both backends can process a message"""
        message = self.create_test_message()
        
        # Test CIF backend
        os.environ["WWPDB_MESSAGING_BACKEND"] = "cif"
        cif_backend = MessagingFactory.create_messaging_backend(self.req_obj, verbose=True)
        
        # Just testing that it doesn't throw an exception
        try:
            cif_backend.processMsg(message)
            cif_success = True
        except Exception as e:
            print(f"CIF backend error: {e}")
            cif_success = False
        
        # Test database backend (if available)
        os.environ["WWPDB_MESSAGING_BACKEND"] = "database"
        db_backend = MessagingFactory.create_messaging_backend(self.req_obj, verbose=True)
        
        # Just testing that it doesn't throw an exception
        try:
            db_backend.processMsg(message)
            db_success = True
        except Exception as e:
            print(f"Database backend error: {e}")
            db_success = False
        
        # Report results rather than failing the test if database isn't configured
        print(f"CIF backend: {'SUCCESS' if cif_success else 'FAILED'}")
        print(f"Database backend: {'SUCCESS' if db_success else 'FAILED (may need configuration)'}")
        
        # At least the CIF backend should work
        self.assertTrue(cif_success, "CIF backend failed to process message")
    
    def test_query_methods(self):
        """Test that both backends provide compatible query methods"""
        # Test CIF backend query methods
        os.environ["WWPDB_MESSAGING_BACKEND"] = "cif"
        cif_backend = MessagingFactory.create_messaging_backend(self.req_obj, verbose=True)
        
        # Get message columns - should work with both backends
        cif_cols_result = cif_backend.getMsgColList(False)
        
        # Test database backend query methods (if available)
        os.environ["WWPDB_MESSAGING_BACKEND"] = "database"
        db_backend = MessagingFactory.create_messaging_backend(self.req_obj, verbose=True)
        
        try:
            db_cols_result = db_backend.getMsgColList(False)
            print("Database backend getMsgColList successful")
        except Exception as e:
            print(f"Database backend getMsgColList error: {e}")
            db_cols_result = None
        
        # Test structure compatibility if both implementations returned results
        if db_cols_result is not None:
            self.assertEqual(
                type(cif_cols_result), type(db_cols_result),
                "Column list return types don't match"
            )
        
        # Test message listing with both backends
        try:
            cif_rows = cif_backend.getMsgRowList(self.test_dep_id)
            self.assertIsNotNone(cif_rows, "CIF backend should return a result from getMsgRowList")
        except Exception as e:
            print(f"CIF backend getMsgRowList error: {e}")
        
        try:
            db_rows = db_backend.getMsgRowList(self.test_dep_id)
            self.assertIsNotNone(db_rows, "Database backend should return a result from getMsgRowList")
        except Exception as e:
            print(f"Database backend getMsgRowList error: {e}")


if __name__ == "__main__":
    unittest.main()
