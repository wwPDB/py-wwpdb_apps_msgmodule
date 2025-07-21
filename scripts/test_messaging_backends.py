#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Manual test script for testing MessagingIo vs MessagingDb functionality

This script allows manual testing and comparison of both backend implementations.
It can be run directly to see how each backend handles the same operations.
"""

import os
import sys
import argparse
from datetime import datetime

# Make sure we can find the module
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(0, project_root)

# Import required modules
from wwpdb.apps.msgmodule.io.MessagingFactory import MessagingFactory
from wwpdb.apps.msgmodule.models.Message import Message


class MockRequestObj:
    """Simple mock request object for testing"""
    
    def __init__(self, site_id="TEST", session_path="/tmp/test_session"):
        self.site_id = site_id
        self.session_path = session_path
        self.values = {
            "WWPDB_SITE_ID": site_id,
            "groupid": "test_group",
            "content_type": "msgs"
        }
    
    def getValue(self, key):
        """Get a value from the request"""
        return self.values.get(key, "")
    
    def setValue(self, key, value):
        """Set a value in the request"""
        self.values[key] = value
    
    def newSessionObj(self):
        """Return a mock session object"""
        return self
    
    def getPath(self):
        """Return the session path"""
        return self.session_path
    
    def getRelativePath(self):
        """Return a relative path"""
        return "session/path"


def create_test_message(dep_id):
    """Create a test message for backend testing"""
    message = Message()
    message.setDepositionDataSetId(dep_id)
    message.setMessageSubject("Test Subject")
    message.setMessageText("This is a test message created at " + datetime.now().isoformat())
    message.setSender("test@example.com")
    message.setParentMessageId(None)
    message.setContextType("test-context")
    message.setContextValue("test-value")
    message.setMessageType("to-depositor")
    message.setTimestamp(datetime.now().isoformat())
    message.setIsBeingSent(True)
    return message


def test_backend(backend_name, dep_id):
    """Test a specific backend with basic operations"""
    print(f"\n=== Testing {backend_name.upper()} Backend ===")
    
    # Set environment variable for backend selection
    os.environ["WWPDB_MESSAGING_BACKEND"] = backend_name
    
    # Create request object
    req_obj = MockRequestObj()
    
    # Create backend instance
    print(f"Creating {backend_name} backend instance...")
    backend = MessagingFactory.create_messaging_backend(req_obj, verbose=True)
    print(f"Created backend: {backend.__class__.__name__}")
    
    # Test 1: Process a message
    print("\nTest 1: Process a message")
    message = create_test_message(dep_id)
    try:
        result = backend.processMsg(message)
        print(f"Result: {result}")
    except Exception as e:
        print(f"Error processing message: {e}")
    
    # Test 2: Get message list
    print("\nTest 2: Get message list")
    try:
        result = backend.getMsgRowList(dep_id)
        print(f"Found {result.get('TOTAL_COUNT', 0)} messages")
        for idx, msg in enumerate(result.get('RECORD_LIST', [])[:3]):
            print(f"Message {idx + 1}: {msg.get('message_subject', 'N/A')}")
        if len(result.get('RECORD_LIST', [])) > 3:
            print(f"...and {len(result.get('RECORD_LIST', [])) - 3} more")
    except Exception as e:
        print(f"Error getting message list: {e}")
    
    # Test 3: Test status methods
    print("\nTest 3: Status methods")
    try:
        print(f"All messages read: {backend.areAllMsgsRead()}")
        print(f"Any notes exist: {backend.anyNotesExist()}")
    except Exception as e:
        print(f"Error checking statuses: {e}")
    
    print("\n=== Completed Testing ===")


def main():
    """Main function for manual testing"""
    parser = argparse.ArgumentParser(description="Test MessagingIo vs MessagingDb backends")
    parser.add_argument("--backend", choices=["cif", "database"], default="cif",
                        help="Backend to test (cif or database)")
    parser.add_argument("--depid", default="D_TEST123",
                        help="Deposition ID to use for testing")
    parser.add_argument("--compare", action="store_true",
                        help="Compare both backends")
    
    args = parser.parse_args()
    
    if args.compare:
        test_backend("cif", args.depid)
        test_backend("database", args.depid)
    else:
        test_backend(args.backend, args.depid)


if __name__ == "__main__":
    main()
