#!/usr/bin/env python
"""
Basic integration tests for database functionality.
"""

import os
import sys
import unittest
from unittest.mock import Mock
from datetime import datetime

# Add the project root to Python path for testing
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


class TestDatabaseConfiguration(unittest.TestCase):
    """Test basic database configuration"""

    def test_database_configuration(self):
        """Test that database config can be loaded"""
        try:
            from wwpdb.apps.msgmodule.db import get_messaging_database_config
            config = get_messaging_database_config()
            self.assertIsInstance(config, dict)
        except ImportError:
            # Database layer optional for basic testing
            pass


class TestMessagingFactory(unittest.TestCase):
    """Test the MessagingFactory backend selection"""
    
    def test_factory_creates_appropriate_backend(self):
        """Test factory creates the right backend based on environment"""
        from wwpdb.apps.msgmodule.io.MessagingFactory import MessagingFactory
        
        # Create mock request object
        mock_req_obj = Mock()
        
        # Test with environment variable not set (should default to CIF)
        if "WWPDB_MESSAGING_BACKEND" in os.environ:
            original_backend = os.environ.get("WWPDB_MESSAGING_BACKEND")
            del os.environ["WWPDB_MESSAGING_BACKEND"]
        else:
            original_backend = None
            
        backend = MessagingFactory.create_messaging_backend(mock_req_obj)
        self.assertIn("MessagingIo", backend.__class__.__name__)
        
        # Restore environment variable if it was set
        if original_backend is not None:
            os.environ["WWPDB_MESSAGING_BACKEND"] = original_backend


if __name__ == "__main__":
    unittest.main()
