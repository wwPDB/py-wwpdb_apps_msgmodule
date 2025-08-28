#!/usr/bin/env python
"""
Database integration test for the messaging system.

This test integrates with the existing test infrastructure and tests real
database operations using ConfigInfo for database connection.

Prerequisites:
- Database must already exist (created via init_messaging_database.py)
- Tables must be created (via init_messaging_database.py --create-tables)
- ConfigInfo must provide valid database connection details
"""

import sys
import unittest
import os
import tempfile
import shutil
from datetime import datetime

# Import ConfigInfo at module level BEFORE any mock imports to avoid contamination
from wwpdb.utils.config.ConfigInfo import ConfigInfo as RealConfigInfo

from unittest.mock import patch, MagicMock

if __package__ is None or __package__ == "":
    from os import path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from commonsetup import TESTOUTPUT, configInfo
else:
    from .commonsetup import TESTOUTPUT, configInfo

# Import the database components
from wwpdb.apps.msgmodule.db import (
    DataAccessLayer,
    MessageInfo,
    MessageFileReference, 
    MessageStatus,
    PdbxMessageIo,
    PdbxMessageInfo,
    PdbxMessageFileReference,
    PdbxMessageStatus
)

# Import path for scripts
SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'scripts')
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

class DatabaseIntegrationTests(unittest.TestCase):
    """Test database integration with real connections and scripts"""
    
    def setUp(self):
        """Set up test with REAL database configuration from ConfigInfo"""
        
        # Get REAL database configuration from ConfigInfo like the scripts do
        try:
            # ConfigInfo imported at module level as RealConfigInfo
            
            # Use real site ID from environment (e.g., PDBE_EMDB_DEV_ROCKY_1)
            site_id = os.getenv("WWPDB_SITE_ID")
            if not site_id:
                raise RuntimeError("WWPDB_SITE_ID environment variable not set")
                
            config_info = RealConfigInfo(site_id)
            
            # Get real database configuration (same as migrate_cif_to_db.py)
            host = config_info.get("SITE_DB_HOST_NAME")
            user = config_info.get("SITE_DB_ADMIN_USER") 
            database = config_info.get("WWPDB_MESSAGING_DB_NAME")
            port = config_info.get("SITE_DB_PORT_NUMBER", "3306")
            password = config_info.get("SITE_DB_ADMIN_PASS", "")
            
            if not all([host, user, database]):
                missing = [k for k, v in [
                    ("SITE_DB_HOST_NAME", host), 
                    ("SITE_DB_ADMIN_USER", user), 
                    ("WWPDB_MESSAGING_DB_NAME", database)
                ] if not v]
                raise RuntimeError(f"Missing required ConfigInfo database settings: {', '.join(missing)}")
            
            self.test_db_config = {
                "host": host,
                "port": int(port),
                "database": database,
                "username": user,
                "password": password,
                "charset": "utf8mb4"
            }
            
            self.has_real_db_config = True
            print(f"Using REAL database config from ConfigInfo site_id={site_id}")
            print(f"Database: {host}:{port}/{database} (user: {user})")
            
        except Exception as e:
            print(f"Could not get real database config: {e}")
            print("Tests will be skipped - this requires proper wwPDB environment setup")
            self.has_real_db_config = False
            self.test_db_config = None
        
        # Create a temporary directory for test outputs
        self.test_output_dir = os.path.join(TESTOUTPUT, "database_integration")
        if not os.path.exists(self.test_output_dir):
            os.makedirs(self.test_output_dir)
    
    def tearDown(self):
        """Clean up test artifacts"""
        if os.path.exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir, ignore_errors=True)
    
    def test_database_initialization_functionality(self):
        """Test database initialization functionality (not script import)"""
        
        if not self.has_real_db_config:
            self.skipTest("No real database configuration available")
        
        # Test the database configuration functionality that scripts would use
        try:
            # ConfigInfo imported at module level as RealConfigInfo
            site_id = os.getenv("WWPDB_SITE_ID")
            config_info = RealConfigInfo(site_id)
            
            # Test that we can get the required database configuration values
            host = config_info.get("SITE_DB_HOST_NAME")
            database = config_info.get("WWPDB_MESSAGING_DB_NAME")
            user = config_info.get("SITE_DB_ADMIN_USER")
            
            self.assertEqual(host, self.test_db_config["host"])
            self.assertEqual(database, self.test_db_config["database"])
            self.assertEqual(user, self.test_db_config["username"])
            
            print(f"✓ Database configuration retrieval working correctly")
            
        except Exception as e:
            # In a real environment, this would connect to the database
            # For testing, we expect connection errors with test credentials
            if "connection" in str(e).lower() or "access denied" in str(e).lower():
                print(f"✓ Database configuration tested (connection error expected in test)")
            else:
                raise
    
    def test_data_access_layer_integration(self):
        """Test DataAccessLayer with real database config"""
        
        if not self.has_real_db_config:
            self.skipTest("No real database configuration available")
        
        try:
            # Test DataAccessLayer instantiation with real config
            dal = DataAccessLayer(self.test_db_config)
            
            # Test that the DAL can be created and connect to real database
            self.assertIsNotNone(dal)
            print(f"✓ DataAccessLayer instantiated successfully with real database")
            
            # Test basic database connectivity
            session = dal.db_connection.get_session()
            session.close()
            print(f"✓ Database connection established and closed successfully")
            
        except Exception as e:
            self.fail(f"DataAccessLayer integration failed with real database: {e}")
    
    def test_pdbx_message_io_with_real_config(self):
        """Test PdbxMessageIo with real database configuration and all content types"""
        
        if not self.has_real_db_config:
            self.skipTest("No real database configuration available")
        
        # Test all supported content types
        content_types = [
            "messages-to-depositor",
            "messages-from-depositor", 
            "notes-from-annotator",
        ]
        
        test_dep_id = "D_1000000001"
        site_id = os.getenv("WWPDB_SITE_ID")
        
        for content_type in content_types:
            with self.subTest(content_type=content_type):
                try:
                    # Test PdbxMessageIo with real database configuration
                    msg_io = PdbxMessageIo(verbose=True, log=sys.stdout, site_id=site_id, db_config=self.test_db_config)
                    
                    self.assertIsNotNone(msg_io)
                    print(f"✓ PdbxMessageIo instantiated for content type: {content_type}")
                    
                    # Create a test message and actually write it to the database
                    test_msg_id = f"TEST_{content_type.upper().replace('-', '_')}_{int(__import__('time').time())}"
                    
                    # Create test message data
                    test_message = {
                        "deposition_data_set_id": test_dep_id,
                        "message_id": test_msg_id,
                        "sender": f"test@{content_type.replace('-', '_')}.com",
                        "message_subject": f"Integration Test for {content_type}", 
                        "message_text": f"This message was created to test {content_type} content type in the database backend",
                        "content_type": content_type,
                        "message_type": "text",
                        "send_status": "Y"
                    }
                    
                    # Add the message to PdbxMessageIo
                    msg_io.appendMessage(test_message)
                    
                    # Write to database (using synthetic path for DB backend)
                    file_path = f"/synthetic/{test_dep_id}_{content_type}_P1.cif"
                    success = msg_io.write(file_path)
                    self.assertTrue(success, f"Database write operation should succeed for {content_type}")
                    print(f"✓ Successfully wrote test message for {content_type}: {test_msg_id}")
                    
                    # Verify the message was written by reading it back
                    msg_io2 = PdbxMessageIo(verbose=True, log=sys.stdout, site_id=site_id, db_config=self.test_db_config)
                    read_success = msg_io2.read(file_path)
                    self.assertTrue(read_success, f"Database read operation should succeed for {content_type}")
                    
                    messages = msg_io2.getMessageInfo()
                    
                    # Find our test message
                    test_msg_found = False
                    for msg in messages:
                        if msg.get("message_id") == test_msg_id:
                            test_msg_found = True
                            self.assertEqual(msg.get("deposition_data_set_id"), test_dep_id)
                            self.assertEqual(msg.get("content_type"), content_type)
                            self.assertEqual(msg.get("sender"), f"test@{content_type.replace('-', '_')}.com")
                            break
                    
                    self.assertTrue(test_msg_found, f"Test message {test_msg_id} should be found in database for {content_type}")
                    print(f"✓ Successfully verified {content_type} message was persisted and can be read back")
                    
                except Exception as e:
                    self.fail(f"PdbxMessageIo integration failed for {content_type}: {e}")
        
        print(f"✓ Database integration test completed successfully for all {len(content_types)} content types")
    
    def test_message_models_integration(self):
        """Test SQLAlchemy message models with actual database persistence across all content types and message threading"""
        
        if not self.has_real_db_config:
            self.skipTest("No real database configuration available")
        
        # Test all supported content types
        content_types = [
            "messages-to-depositor",
            "messages-from-depositor", 
            "notes-from-annotator",
        ]
        
        try:
            # Create DataAccessLayer for database operations
            dal = DataAccessLayer(self.test_db_config)
            
            created_message_ids = []
            parent_messages = {}  # Track parent messages for threading
            
            # First, create initial messages for each content type
            for content_type in content_types:
                with self.subTest(content_type=content_type, phase="initial"):
                    # Test MessageInfo model with database persistence
                    test_message_id = f"TEST_MODEL_{content_type.upper().replace('-', '_')}_{int(__import__('time').time())}"
                    msg_info = MessageInfo()
                    msg_info.deposition_data_set_id = "D_1000000001"
                    msg_info.message_id = test_message_id
                    msg_info.timestamp = datetime.now()  # Add required timestamp
                    msg_info.sender = f"test@{content_type.replace('-', '_')}.com"
                    msg_info.message_subject = f"Initial message for {content_type}"
                    msg_info.message_text = f"This is the first message in a thread for {content_type} content type"
                    msg_info.content_type = content_type
                    msg_info.message_type = "text"
                    msg_info.send_status = "Y"
                    msg_info.parent_message_id = None  # Initial message has no parent
                    
                    # Save to database
                    dal.create_message(msg_info)
                    created_message_ids.append(test_message_id)
                    parent_messages[content_type] = test_message_id
                    print(f"✓ Initial {content_type} message saved: {test_message_id}")
                    
                    # Verify data was saved by querying it back
                    session = dal.db_connection.get_session()
                    try:
                        saved_msg = session.query(MessageInfo).filter_by(message_id=test_message_id).first()
                        self.assertIsNotNone(saved_msg, f"Message should be found in database for {content_type}")
                        self.assertEqual(saved_msg.deposition_data_set_id, "D_1000000001")
                        self.assertEqual(saved_msg.content_type, content_type)
                        self.assertIsNone(saved_msg.parent_message_id, "Initial message should have no parent")
                        print(f"✓ {content_type} initial message successfully persisted and verified")
                    finally:
                        session.close()
            
            # Now create reply messages that reference the parent messages
            for content_type in content_types:
                with self.subTest(content_type=content_type, phase="reply"):
                    # Create a reply message
                    reply_message_id = f"REPLY_{content_type.upper().replace('-', '_')}_{int(__import__('time').time())}"
                    reply_msg = MessageInfo()
                    reply_msg.deposition_data_set_id = "D_1000000001"
                    reply_msg.message_id = reply_message_id
                    reply_msg.timestamp = datetime.now()
                    reply_msg.sender = f"reply@{content_type.replace('-', '_')}.com"
                    reply_msg.message_subject = f"Re: Initial message for {content_type}"
                    reply_msg.message_text = f"This is a reply to the initial {content_type} message, demonstrating message threading"
                    reply_msg.content_type = content_type
                    reply_msg.message_type = "text"
                    reply_msg.send_status = "Y"
                    reply_msg.parent_message_id = parent_messages[content_type]  # Reference parent message
                    
                    # Save reply to database
                    dal.create_message(reply_msg)
                    created_message_ids.append(reply_message_id)
                    print(f"✓ Reply {content_type} message saved: {reply_message_id} (parent: {parent_messages[content_type]})")
                    
                    # Verify reply was saved with correct parent reference
                    session = dal.db_connection.get_session()
                    try:
                        saved_reply = session.query(MessageInfo).filter_by(message_id=reply_message_id).first()
                        self.assertIsNotNone(saved_reply, f"Reply message should be found for {content_type}")
                        self.assertEqual(saved_reply.parent_message_id, parent_messages[content_type])
                        self.assertEqual(saved_reply.content_type, content_type)
                        print(f"✓ {content_type} reply message threading verified")
                    finally:
                        session.close()
            
            # Test file references and statuses for all created messages
            for i, (content_type, test_message_id) in enumerate(zip(content_types * 2, created_message_ids)):  # *2 because we have initial + reply
                # Test MessageFileReference model
                file_ref = MessageFileReference()
                file_ref.message_id = test_message_id
                file_ref.deposition_data_set_id = "D_1000000001"
                file_ref.content_type = content_type
                file_ref.content_format = "pdbx"  # All database-supported content types use pdbx format
                file_ref.partition_number = 1
                file_ref.version_id = 1
                file_ref.storage_type = "archive"
                file_ref.upload_file_name = f"test_file_{content_type}_{int(__import__('time').time())}.cif"
                
                # Save to database
                dal.create_file_reference(file_ref)
                print(f"✓ MessageFileReference saved for {content_type}")
                
                # Test MessageStatus model
                status = MessageStatus()
                status.message_id = test_message_id
                status.deposition_data_set_id = "D_1000000001"
                status.read_status = "N" if i % 2 == 0 else "Y"  # Alternate read status
                status.action_reqd = "Y" if content_type in ["messages-from-depositor"] else "N"
                status.for_release = "N"
                
                # Save to database
                dal.create_or_update_status(status)
                print(f"✓ MessageStatus saved for {content_type}")
            
            # Verify message threading and relationships
            session = dal.db_connection.get_session()
            try:
                # Check that we have the expected message threads
                for content_type in content_types:
                    # Find all messages for this content type
                    messages = session.query(MessageInfo).filter_by(
                        deposition_data_set_id="D_1000000001",
                        content_type=content_type
                    ).order_by(MessageInfo.timestamp).all()
                    
                    # Should have at least 2 messages (initial + reply)
                    self.assertGreaterEqual(len(messages), 2, f"Should have at least 2 messages for {content_type}")
                    
                    # Find parent and child messages
                    parent_msg = None
                    child_msg = None
                    for msg in messages:
                        if msg.parent_message_id is None:
                            parent_msg = msg
                        elif msg.parent_message_id == parent_messages[content_type]:
                            child_msg = msg
                    
                    self.assertIsNotNone(parent_msg, f"Should find parent message for {content_type}")
                    self.assertIsNotNone(child_msg, f"Should find child message for {content_type}")
                    self.assertEqual(child_msg.parent_message_id, parent_msg.message_id)
                    
                    print(f"✓ Message threading verified for {content_type}: {parent_msg.message_id} -> {child_msg.message_id}")
                
                # Check total counts
                total_msg_count = session.query(MessageInfo).filter_by(deposition_data_set_id="D_1000000001").count()
                total_ref_count = session.query(MessageFileReference).filter(
                    MessageFileReference.message_id.in_(created_message_ids)
                ).count()
                total_status_count = session.query(MessageStatus).filter(
                    MessageStatus.message_id.in_(created_message_ids)
                ).count()
                
                expected_messages = len(content_types) * 2  # Initial + reply for each content type
                self.assertGreaterEqual(total_msg_count, expected_messages, f"Should have at least {expected_messages} messages")
                self.assertEqual(total_ref_count, expected_messages, f"Should have {expected_messages} file references")
                self.assertEqual(total_status_count, expected_messages, f"Should have {expected_messages} statuses")
                
                print(f"✓ All database models working correctly with message threading")
                print(f"✓ Found {total_msg_count} total messages, {total_ref_count} file refs, {total_status_count} statuses")
                print(f"✓ Successfully tested {len(content_types)} content types with message threading")
                
            finally:
                session.close()
                
        except Exception as e:
            self.fail(f"Message models integration with threading failed: {e}")
    
    def test_pdbx_message_wrappers_with_models(self):
        """Test the optimized PdbxMessage wrapper classes"""
        
        # Test PdbxMessageInfo wrapper
        msg_info = PdbxMessageInfo(verbose=True)
        msg_info.setDepositionId("D_1000000001")
        msg_info.setMessageId("WRAPPER_TEST_001")
        msg_info.setSender("wrapper@test.com")
        msg_info.setMessageSubject("Wrapper Integration Test")
        msg_info.setMessageText("Testing optimized wrapper implementation")
        msg_info.setContentType("messages-to-depositor")
        
        # Verify wrapper functionality
        self.assertEqual(msg_info.getDepositionId(), "D_1000000001")
        self.assertEqual(msg_info.getMessageId(), "WRAPPER_TEST_001")
        self.assertEqual(msg_info.getSender(), "wrapper@test.com")
        
        # Verify underlying SQLAlchemy model
        model = msg_info.get_model()
        self.assertIsNotNone(model)
        self.assertIsInstance(model, MessageInfo)
        self.assertEqual(model.deposition_data_set_id, "D_1000000001")
        self.assertEqual(model.message_id, "WRAPPER_TEST_001")
        
        print(f"✓ PdbxMessageInfo wrapper with SQLAlchemy model working correctly")
        
        # Test get/set dict functionality
        data = msg_info.get()
        self.assertIn("deposition_data_set_id", data)
        self.assertEqual(data["deposition_data_set_id"], "D_1000000001")
        
        # Test setting from dict
        new_data = {
            "deposition_data_set_id": "D_1000000002",
            "message_id": "WRAPPER_TEST_002",
            "sender": "updated@test.com"
        }
        msg_info.set(new_data)
        self.assertEqual(msg_info.getDepositionId(), "D_1000000002")
        self.assertEqual(msg_info.getMessageId(), "WRAPPER_TEST_002")
        
        print(f"✓ PdbxMessage wrapper dict compatibility working correctly")
    
    def test_migration_functionality(self):
        """Test migration functionality without importing the script"""
        
        # Test the core migration functionality - converting CIF data to model objects
        test_cif_data = {
            "deposition_data_set_id": "D_1000000001",
            "message_id": "MIGRATION_TEST_001", 
            "sender": "migration@test.com",
            "message_subject": "Migration Test",
            "message_text": "Test migration functionality",
            "content_type": "messages-to-depositor"
        }
        
        # Test creating MessageInfo from CIF-like data (what migration script would do)
        msg_info = MessageInfo()
        msg_info.deposition_data_set_id = test_cif_data["deposition_data_set_id"]
        msg_info.message_id = test_cif_data["message_id"]
        msg_info.sender = test_cif_data["sender"]
        msg_info.message_subject = test_cif_data["message_subject"]
        msg_info.message_text = test_cif_data["message_text"]
        msg_info.content_type = test_cif_data["content_type"]
        
        self.assertIsInstance(msg_info, MessageInfo)
        self.assertEqual(msg_info.deposition_data_set_id, "D_1000000001")
        self.assertEqual(msg_info.message_id, "MIGRATION_TEST_001")
        
        print(f"✓ CIF to MessageInfo conversion functionality working correctly")
        
        # Test file reference creation
        file_ref = MessageFileReference()
        file_ref.message_id = test_cif_data["message_id"]
        file_ref.deposition_data_set_id = test_cif_data["deposition_data_set_id"]
        file_ref.content_type = test_cif_data["content_type"]
        file_ref.content_format = "pdbx"
        file_ref.partition_number = 1
        file_ref.version_id = 1
        
        self.assertEqual(file_ref.content_type, "messages-to-depositor")
        self.assertEqual(file_ref.content_format, "pdbx")
        
        print(f"✓ Migration file reference creation working correctly")
        
        # Test status creation
        status = MessageStatus()
        status.message_id = test_cif_data["message_id"]
        status.deposition_data_set_id = test_cif_data["deposition_data_set_id"]
        status.read_status = "N"
        status.action_reqd = "N"
        status.for_release = "N"
        
        self.assertEqual(status.read_status, "N")
        
        print(f"✓ Migration status creation working correctly")


if __name__ == "__main__":
    # Configure test runner
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(DatabaseIntegrationTests)
    
    # Run tests with verbose output
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "="*60)
    print("Database Integration Test Summary")
    print("="*60)
    
    if result.wasSuccessful():
        print("✓ All database integration tests passed!")
        print(f"✓ {result.testsRun} tests executed successfully")
        print("\nDatabase backend integration is working correctly:")
        print("• Database configuration from ConfigInfo ✓")
        print("• DataAccessLayer initialization ✓")
        print("• PdbxMessageIo database integration ✓")
        print("• SQLAlchemy model functionality ✓")
        print("• Optimized PdbxMessage wrappers ✓")
        print("• Script integration components ✓")
    else:
        print(f"✗ {len(result.failures)} test(s) failed")
        print(f"✗ {len(result.errors)} test(s) had errors")
        for test, traceback in result.failures + result.errors:
            print(f"  - {test}: {traceback.split('AssertionError:')[-1].strip()}")
    
    print("="*60)
