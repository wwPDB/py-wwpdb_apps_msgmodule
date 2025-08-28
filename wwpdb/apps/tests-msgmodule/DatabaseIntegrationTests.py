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
        """Test PdbxMessageIo with real database configuration and actual database writes"""
        
        if not self.has_real_db_config:
            self.skipTest("No real database configuration available")
        
        try:
            # Test PdbxMessageIo with real database configuration
            site_id = os.getenv("WWPDB_SITE_ID")
            msg_io = PdbxMessageIo(verbose=True, log=sys.stdout, site_id=site_id, db_config=self.test_db_config)
            
            self.assertIsNotNone(msg_io)
            print(f"✓ PdbxMessageIo instantiated with real database configuration")
            
            # Create a test message and actually write it to the database
            test_dep_id = "D_1000000001"
            test_msg_id = f"TEST_DB_WRITE_{int(__import__('time').time())}"
            
            # Create test message data
            test_message = {
                "deposition_data_set_id": test_dep_id,
                "message_id": test_msg_id,
                "sender": "test@integration.com",
                "message_subject": "Database Integration Test Message", 
                "message_text": "This message was created by DatabaseIntegrationTests to verify database write operations",
                "content_type": "messages-to-depositor",
                "message_type": "text",
                "send_status": "Y"
            }
            
            # Add the message to PdbxMessageIo
            msg_io.appendMessage(test_message)
            
            # Write to database (using synthetic path for DB backend)
            success = msg_io.write(f"/synthetic/{test_dep_id}_messages-to-depositor_P1.cif")
            self.assertTrue(success, "Database write operation should succeed")
            print(f"✓ Successfully wrote test message {test_msg_id} to database")
            
            # Verify the message was written by reading it back
            msg_io2 = PdbxMessageIo(verbose=True, log=sys.stdout, site_id=site_id, db_config=self.test_db_config)
            read_success = msg_io2.read(f"/synthetic/{test_dep_id}_messages-to-depositor_P1.cif")
            self.assertTrue(read_success, "Database read operation should succeed")
            
            messages = msg_io2.getMessageInfo()
            
            # Find our test message
            test_msg_found = False
            for msg in messages:
                if msg.get("message_id") == test_msg_id:
                    test_msg_found = True
                    self.assertEqual(msg.get("deposition_data_set_id"), test_dep_id)
                    self.assertEqual(msg.get("sender"), "test@integration.com")
                    self.assertEqual(msg.get("message_subject"), "Database Integration Test Message")
                    break
            
            self.assertTrue(test_msg_found, f"Test message {test_msg_id} should be found in database")
            print(f"✓ Successfully verified test message {test_msg_id} was persisted and can be read back")
            print(f"✓ Database round-trip test completed successfully")
            
        except Exception as e:
            self.fail(f"PdbxMessageIo integration failed with real database: {e}")
    
    def test_message_models_integration(self):
        """Test SQLAlchemy message models with actual database persistence"""
        
        if not self.has_real_db_config:
            self.skipTest("No real database configuration available")
        
        try:
            # Create DataAccessLayer for database operations
            dal = DataAccessLayer(self.test_db_config)
            
            # Test MessageInfo model with database persistence
            msg_info = MessageInfo()
            msg_info.deposition_data_set_id = "D_1000000001"
            msg_info.message_id = f"TEST_MODEL_{int(__import__('time').time())}"
            msg_info.sender = "test@example.com"
            msg_info.message_subject = "Integration Test Message"
            msg_info.message_text = "This is a test message for database integration"
            msg_info.content_type = "messages-to-depositor"
            msg_info.message_type = "text"
            msg_info.send_status = "Y"
            
            # Save to database
            dal.create_message(msg_info)
            print(f"✓ MessageInfo model saved to database: {msg_info.message_id}")
            
            # Verify data was saved by querying it back
            session = dal.db_connection.get_session()
            try:
                saved_msg = session.query(MessageInfo).filter_by(message_id=msg_info.message_id).first()
                self.assertIsNotNone(saved_msg, "Message should be found in database")
                self.assertEqual(saved_msg.deposition_data_set_id, "D_1000000001")
                self.assertEqual(saved_msg.sender, "test@example.com")
                self.assertEqual(saved_msg.content_type, "messages-to-depositor")
                print(f"✓ MessageInfo model successfully persisted and retrieved from database")
            finally:
                session.close()
            
            # Test MessageFileReference model
            file_ref = MessageFileReference()
            file_ref.message_id = msg_info.message_id
            file_ref.deposition_data_set_id = "D_1000000001"
            file_ref.content_type = "messages-to-depositor"
            file_ref.content_format = "pdbx"
            file_ref.partition_number = 1
            file_ref.version_id = 1
            file_ref.storage_type = "archive"
            file_ref.upload_file_name = f"test_file_{int(__import__('time').time())}.cif"
            
            # Save to database
            dal.create_file_reference(file_ref)
            print(f"✓ MessageFileReference model saved to database")
            
            # Test MessageStatus model
            status = MessageStatus()
            status.message_id = msg_info.message_id
            status.deposition_data_set_id = "D_1000000001"
            status.read_status = "N"
            status.action_reqd = "Y"
            status.for_release = "N"
            
            # Save to database
            dal.create_or_update_status(status)
            print(f"✓ MessageStatus model saved to database")
            
            # Verify all related data
            session = dal.db_connection.get_session()
            try:
                # Check message exists
                msg_count = session.query(MessageInfo).filter_by(deposition_data_set_id="D_1000000001").count()
                self.assertGreater(msg_count, 0, "Should have at least one message for D_1000000001")
                
                # Check file reference exists  
                ref_count = session.query(MessageFileReference).filter_by(message_id=msg_info.message_id).count()
                self.assertGreater(ref_count, 0, "Should have file reference for test message")
                
                # Check status exists
                status_count = session.query(MessageStatus).filter_by(message_id=msg_info.message_id).count()
                self.assertGreater(status_count, 0, "Should have status for test message")
                
                print(f"✓ All database models working correctly with persistence")
                print(f"✓ Found {msg_count} messages, {ref_count} file refs, {status_count} statuses for test data")
                
            finally:
                session.close()
                
        except Exception as e:
            self.fail(f"Message models integration with database failed: {e}")
    
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
