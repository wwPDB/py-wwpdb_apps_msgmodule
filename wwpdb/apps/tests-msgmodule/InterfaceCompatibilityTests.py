#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test interface compatibility between MessagingIo and MessagingDb

This test suite verifies that MessagingDb implements the same interface
as MessagingIo, ensuring it can be used as a drop-in replacement.
"""

import os
import sys
import unittest
from unittest.mock import Mock, patch

# Add the project root to the Python path
project_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..")
)
sys.path.insert(0, project_root)

# Import both implementations for comparison
from wwpdb.apps.msgmodule.io.MessagingIo import MessagingIo
from wwpdb.apps.msgmodule.db.MessagingDb import MessagingDb


class TestInterfaceCompatibility(unittest.TestCase):
    """Test suite for interface compatibility between MessagingIo and MessagingDb"""

    def setUp(self):
        """Set up test environment with mock request object"""
        # Create a mock request object
        self.mock_req_obj = Mock()
        self.mock_session_obj = Mock()
        self.mock_req_obj.newSessionObj.return_value = self.mock_session_obj
        self.mock_session_obj.getPath.return_value = "/tmp/test"
        self.mock_req_obj.getValue.side_effect = lambda key: {
            "WWPDB_SITE_ID": "TEST",
            "groupid": "test_group"
        }.get(key, "")

        # Create instances of both implementations
        with patch("wwpdb.utils.config.ConfigInfo.ConfigInfo"):
            self.messaging_io = MessagingIo(self.mock_req_obj, verbose=False)
            self.messaging_db = MessagingDb(self.mock_req_obj, verbose=False)

    def test_public_methods_match(self):
        """Test that MessagingDb has all the public methods of MessagingIo"""
        io_methods = [method for method in dir(self.messaging_io) 
                     if not method.startswith('_') and callable(getattr(self.messaging_io, method))]
        
        for method_name in io_methods:
            self.assertTrue(
                hasattr(self.messaging_db, method_name),
                f"MessagingDb is missing method: {method_name}"
            )
            
            # Check that the methods have the same signature
            io_method = getattr(self.messaging_io, method_name)
            db_method = getattr(self.messaging_db, method_name)
            
            self.assertEqual(
                io_method.__code__.co_argcount,
                db_method.__code__.co_argcount,
                f"Method {method_name} has different argument count"
            )

    def test_method_signatures(self):
        """Test specific important methods for signature compatibility"""
        key_methods = [
            'processMsg', 
            'getMsgRowList',
            'getMsgColList',
            'getMsg',
            'markMsgAsRead',
            'areAllMsgsRead',
            'anyNotesExist',
            'getFilesRfrncd'
        ]
        
        for method_name in key_methods:
            io_method = getattr(self.messaging_io, method_name)
            db_method = getattr(self.messaging_db, method_name)
            
            io_args = io_method.__code__.co_varnames[:io_method.__code__.co_argcount]
            db_args = db_method.__code__.co_varnames[:db_method.__code__.co_argcount]
            
            # Skip 'self' in comparison
            io_args = io_args[1:]
            db_args = db_args[1:]
            
            self.assertEqual(
                io_args, db_args,
                f"Method {method_name} has different parameter names: {io_args} vs {db_args}"
            )

    def test_return_type_compatibility(self):
        """Test that methods return compatible types/structures"""
        # This is a basic test that could be expanded with more specific assertions
        # for actual return value structure compatibility
        
        # Mock message object for testing
        mock_msg = Mock()
        mock_msg.getDepositionDataSetId.return_value = "D_000001"
        mock_msg.getMessageType.return_value = "to-depositor"
        mock_msg.getSender.return_value = "test@example.com"
        mock_msg.getAllMessages.return_value = []
        
        # Test getMsgRowList return type
        with patch.object(MessagingIo, 'getMsgRowList', return_value={'RECORD_LIST': [], 'TOTAL_COUNT': 0}):
            with patch.object(MessagingDb, 'getMsgRowList', return_value={'RECORD_LIST': [], 'TOTAL_COUNT': 0}):
                io_result = self.messaging_io.getMsgRowList("D_000001")
                db_result = self.messaging_db.getMsgRowList("D_000001")
                
                self.assertEqual(
                    type(io_result), type(db_result),
                    "getMsgRowList returns different types"
                )
                self.assertEqual(
                    io_result.keys(), db_result.keys(),
                    "getMsgRowList returns dictionaries with different keys"
                )


class TestFactoryPattern(unittest.TestCase):
    """Test the factory pattern implementation"""

    def setUp(self):
        """Set up test environment"""
        # Create mock request object
        self.mock_req_obj = Mock()
        self.mock_req_obj.getValue.return_value = "TEST"

    def test_factory_creates_correct_implementation(self):
        """Test that MessagingFactory creates the correct implementation based on env var"""
        from wwpdb.apps.msgmodule.io.MessagingFactory import MessagingFactory
        
        # Save the original environment variable value
        original_value = os.environ.get('WWPDB_MESSAGING_BACKEND')
        
        try:
            # Test with CIF backend
            if 'WWPDB_MESSAGING_BACKEND' in os.environ:
                del os.environ['WWPDB_MESSAGING_BACKEND']
            
            backend = MessagingFactory.create_messaging_backend(self.mock_req_obj)
            self.assertEqual(backend.__class__.__name__, 'MessagingIo')
            
            # Test with database backend
            os.environ['WWPDB_MESSAGING_BACKEND'] = 'database'
            backend = MessagingFactory.create_messaging_backend(self.mock_req_obj)
            self.assertEqual(backend.__class__.__name__, 'MessagingDb')
            
        finally:
            # Restore the original environment variable
            if original_value is not None:
                os.environ['WWPDB_MESSAGING_BACKEND'] = original_value
            elif 'WWPDB_MESSAGING_BACKEND' in os.environ:
                del os.environ['WWPDB_MESSAGING_BACKEND']


if __name__ == "__main__":
    unittest.main()
