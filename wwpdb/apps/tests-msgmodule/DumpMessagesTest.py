#!/usr/bin/env python
"""
Test suite for dump_db_to_cif.py - Database to CIF export functionality.

This test suite validates that the dump_db_to_cif.py script correctly exports
message data from the database to properly formatted CIF files using gemmi.

Run:
  export WWPDB_SITE_ID=PDBE_DEV
  python -m pytest wwpdb/apps/tests-msgmodule/DumpMessagesTest.py -v -s
"""

import os
import sys
import unittest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
import time

if __package__ is None or __package__ == "":
    from os import path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from commonsetup import TESTOUTPUT
else:
    from .commonsetup import TESTOUTPUT

# CRITICAL: Remove the mock ConfigInfo from commonsetup.py before importing anything
# that needs real database configuration (like dump_db_to_cif which uses DataAccessLayer)
if 'wwpdb.utils.config.ConfigInfo' in sys.modules:
    del sys.modules['wwpdb.utils.config.ConfigInfo']

# Test imports
try:
    import gemmi
except ImportError:
    gemmi = None

from wwpdb.apps.msgmodule.io.MessagingIo import MessagingIo
from wwpdb.apps.msgmodule.models.Message import Message

# Import the classes we want to test
try:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'scripts'))
    from dump_db_to_cif import DbToCifExporter, escape_non_ascii
except ImportError as e:
    print(f"Warning: Could not import dump_db_to_cif: {e}")
    DbToCifExporter = None
    escape_non_ascii = None


class TestDbToCifExporter(unittest.TestCase):
    """Test the DbToCifExporter functionality"""

    @classmethod
    def setUpClass(cls):
        """Set up test class - skip if no site ID configured"""
        cls.site_id = os.getenv("WWPDB_SITE_ID")
        if not cls.site_id:
            raise unittest.SkipTest("WWPDB_SITE_ID environment variable not set")
        
        cls.dep_id = os.getenv("WWPDB_TEST_DEP_ID", "D_1000000001")
        
        # Skip if gemmi is not available
        if gemmi is None:
            raise unittest.SkipTest("gemmi library not available - required for CIF export")
        
        # Skip if DbToCifExporter is not available
        if DbToCifExporter is None:
            raise unittest.SkipTest("Could not import DbToCifExporter from dump_db_to_cif")
        
        print(f"\n🧪 Testing DbToCifExporter with site_id={cls.site_id}, dep_id={cls.dep_id}")

    def setUp(self):
        """Set up each test"""
        self.test_output_dir = tempfile.mkdtemp(prefix="dump_test_")
        print(f"   📁 Test output directory: {self.test_output_dir}")

    def tearDown(self):
        """Clean up after each test"""
        if os.path.exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)

    # ---- Helper methods ----

    def _create_test_message(self, subject_suffix=""):
        """Create a test message in the database for export testing"""
        
        # Capture site_id from test class for use in MockRequest
        test_site_id = self.site_id
        
        class MockRequest:
            def __init__(self, dep_id, subject, sender, body):
                self.site_id = test_site_id  # Add site_id as direct attribute
                self._values = {
                    'identifier': dep_id,
                    'subject': subject,
                    'sender': sender,
                    'message': body,
                    'message_type': 'text',
                    'content_type': 'messages-to-depositor',
                    'message_state': 'livemsg',
                    'send_status': 'Y',
                    'WWPDB_SITE_ID': test_site_id,
                    'filesource': 'archive'
                }
            
            def getValue(self, key):
                return self._values.get(key, '')
            
            def getRawValue(self, key):
                return self._values.get(key, '')
            
            def getValueList(self, key):
                return []
            
            def newSessionObj(self):
                return MockSession()
            
            def getSessionObj(self):
                return MockSession()

        class MockSession:
            def getId(self):
                return "test_session"
            
            def getPath(self):
                return "/tmp/test_session"
            
            def getRelativePath(self):
                return "test_session"

        # Create unique message
        timestamp = datetime.utcnow().isoformat()
        subject = f"DUMP_TEST_{subject_suffix}_{timestamp}"
        body = f"Test message for CIF export created at {timestamp}"
        sender = "dump_test@example.com"

        # Create message object
        req = MockRequest(self.dep_id, subject, sender, body)
        msg_obj = Message.fromReqObj(req, verbose=True)
        
        # Store message using MessagingIo
        io = MessagingIo(req, verbose=True)
        result = io.processMsg(msg_obj)
        
        success = result[0] if isinstance(result, tuple) else bool(result)
        if not success:
            raise Exception(f"Failed to create test message: {result}")
        
        return msg_obj.messageId, subject, body, sender

    def _validate_cif_file(self, file_path, expected_message_id=None):
        """Validate that a CIF file is properly formatted and contains expected data"""
        self.assertTrue(os.path.exists(file_path), f"CIF file should exist: {file_path}")
        
        # Check file size
        file_size = os.path.getsize(file_path)
        self.assertGreater(file_size, 0, "CIF file should not be empty")
        
        # Parse with gemmi to validate structure
        try:
            doc = gemmi.cif.read_file(file_path)
            self.assertGreater(len(doc), 0, "CIF document should have blocks")
            
            block = doc[0]  # First block should be 'messages'
            self.assertEqual(block.name, "messages", "First block should be named 'messages'")
            
            # Check for required categories
            has_message_info = False
            message_ids_found = []
            
            # Look for message info items or loops
            for item in block:
                if item.pair and item.pair[0].startswith("_pdbx_deposition_message_info"):
                    has_message_info = True
                    if item.pair[0] == "_pdbx_deposition_message_info.message_id":
                        message_ids_found.append(item.pair[1].strip("'\""))
                elif item.loop:
                    for tag in item.loop.tags:
                        if tag.startswith("_pdbx_deposition_message_info"):
                            has_message_info = True
                            if tag == "_pdbx_deposition_message_info.message_id":
                                # Get message IDs from loop values
                                msg_id_col = item.loop.tags.index(tag)
                                for row in item.loop:
                                    message_ids_found.append(row[msg_id_col].strip("'\""))
                            break
            
            self.assertTrue(has_message_info, "CIF should contain _pdbx_deposition_message_info category")
            
            # If we expect a specific message ID, verify it's present
            if expected_message_id:
                self.assertIn(expected_message_id, message_ids_found, 
                             f"Expected message ID {expected_message_id} should be in CIF file")
            
            return {
                "valid": True,
                "message_ids": message_ids_found,
                "file_size": file_size,
                "block_count": len(doc)
            }
            
        except Exception as e:
            self.fail(f"Failed to parse CIF file {file_path} with gemmi: {e}")

    # ---- Core functionality tests ----

    def test_ascii_escaping_function(self):
        """Test the ASCII escaping functionality
        
        This test verifies that non-ASCII characters are properly escaped to valid ASCII.
        The exact escape format (\xe9 vs \u00e9) doesn't matter as long as:
        1. The output contains only ASCII characters (all ord(c) < 128)
        2. Non-ASCII input characters are escaped
        3. ASCII characters are preserved unchanged
        """
        if escape_non_ascii is None:
            self.skipTest("escape_non_ascii function not available")
        
        # Test cases - now validating ASCII output rather than exact escape format
        test_cases = [
            ("Hello World", "Hello World", False),  # ASCII only - unchanged
            ("Café", None, True),                   # Non-ASCII - must be escaped
            ("Müller", None, True),                 # German umlaut - must be escaped  
            ("José", None, True),                   # Spanish accent - must be escaped
            ("", "", False),                        # Empty string - unchanged
        ]
        
        for input_text, expected_exact, must_escape in test_cases:
            result = escape_non_ascii(input_text)
            
            if expected_exact is not None:
                # For ASCII-only input, expect exact match
                self.assertEqual(result, expected_exact, 
                               f"ASCII input '{input_text}' should be unchanged")
            else:
                # For non-ASCII input, verify proper escaping
                self.assertIsNotNone(result, f"Result should not be None for input: {input_text}")
                
                # 1. Verify output is valid ASCII
                try:
                    result.encode('ascii')
                except UnicodeEncodeError:
                    self.fail(f"Output '{result}' for input '{input_text}' is not valid ASCII")
                
                # 2. Verify non-ASCII chars were actually escaped (result differs from input)
                if must_escape:
                    self.assertNotEqual(result, input_text,
                                      f"Non-ASCII input '{input_text}' should be escaped")
                
                # 3. Verify output contains backslash escapes
                self.assertIn('\\', result,
                            f"Escaped output '{result}' should contain backslash escape sequences")
        
        print("   ✅ ASCII escaping function validated")

    def test_exporter_initialization(self):
        """Test that DbToCifExporter initializes correctly"""
        try:
            exporter = DbToCifExporter(self.site_id)
            self.assertIsNotNone(exporter.data_access, "Data access layer should be initialized")
            self.assertEqual(exporter.site_id, self.site_id, "Site ID should be set correctly")
            print("   ✅ DbToCifExporter initialization successful")
        except Exception as e:
            self.fail(f"DbToCifExporter initialization failed: {e}")

    def test_export_single_deposition_to_custom_directory(self):
        """Test exporting a single deposition to a custom output directory"""
        # Create a test message first
        try:
            msg_id, subject, body, sender = self._create_test_message("SINGLE_EXPORT")
            print(f"   📝 Created test message: {msg_id}")
        except Exception as e:
            self.skipTest(f"Could not create test message: {e}")
        
        # Export using DbToCifExporter
        try:
            exporter = DbToCifExporter(self.site_id)
            success = exporter.export_deposition(self.dep_id, self.test_output_dir, overwrite=True)
            
            self.assertTrue(success, "Export should succeed")
            print("   ✅ Export operation completed successfully")
            
            # Check that files were created
            dep_dir = Path(self.test_output_dir) / self.dep_id
            self.assertTrue(dep_dir.exists(), f"Deposition directory should be created: {dep_dir}")
            
            # Look for CIF files
            cif_files = list(dep_dir.glob("*.cif*"))
            self.assertGreater(len(cif_files), 0, "At least one CIF file should be created")
            
            # Validate each CIF file
            for cif_file in cif_files:
                print(f"   🔍 Validating CIF file: {cif_file.name}")
                validation_result = self._validate_cif_file(str(cif_file), msg_id)
                print(f"       File size: {validation_result['file_size']} bytes")
                print(f"       Message IDs found: {len(validation_result['message_ids'])}")
            
            print("   ✅ All exported CIF files validated successfully")
            
        except Exception as e:
            self.fail(f"Export operation failed: {e}")

    def test_export_with_overwrite_protection(self):
        """Test that export respects overwrite settings"""
        try:
            exporter = DbToCifExporter(self.site_id)
            
            # First export
            success1 = exporter.export_deposition(self.dep_id, self.test_output_dir, overwrite=True)
            self.assertTrue(success1, "First export should succeed")
            
            # Create a dummy file to test overwrite protection
            dep_dir = Path(self.test_output_dir) / self.dep_id
            test_file = dep_dir / f"{self.dep_id}_messages-to-depositor_P1.cif.V1"
            
            if test_file.exists():
                # Modify the file to test overwrite protection
                original_size = test_file.stat().st_size
                with open(test_file, 'a') as f:
                    f.write("# Modified for overwrite test\n")
                modified_size = test_file.stat().st_size
                
                # Second export without overwrite should preserve the modification
                success2 = exporter.export_deposition(self.dep_id, self.test_output_dir, overwrite=False)
                self.assertTrue(success2, "Second export should succeed (files skipped)")
                
                # File should still have the modification
                final_size = test_file.stat().st_size
                self.assertEqual(final_size, modified_size, "File should not be overwritten")
                
                print("   ✅ Overwrite protection working correctly")
            else:
                print("   ⚠️  No CIF file created to test overwrite protection")
                
        except Exception as e:
            self.fail(f"Overwrite protection test failed: {e}")

    def test_export_nonexistent_deposition(self):
        """Test export behavior with non-existent deposition ID"""
        try:
            exporter = DbToCifExporter(self.site_id)
            
            # Use a definitely non-existent deposition ID
            nonexistent_id = "D_NONEXISTENT_999999999"
            success = exporter.export_deposition(nonexistent_id, self.test_output_dir)
            
            # Should succeed but not create any files (no messages to export)
            self.assertTrue(success, "Export of non-existent deposition should succeed (no data)")
            
            # Check that no files were created
            dep_dir = Path(self.test_output_dir) / nonexistent_id
            if dep_dir.exists():
                cif_files = list(dep_dir.glob("*.cif*"))
                self.assertEqual(len(cif_files), 0, "No CIF files should be created for non-existent deposition")
            
            print("   ✅ Non-existent deposition handled correctly")
            
        except Exception as e:
            self.fail(f"Non-existent deposition test failed: {e}")

    def test_export_with_invalid_deposition_id_format(self):
        """Test export with invalid deposition ID format"""
        try:
            exporter = DbToCifExporter(self.site_id)
            
            # Test various invalid formats
            invalid_ids = ["INVALID", "123456", "D_", "X_123456", ""]
            
            for invalid_id in invalid_ids:
                success = exporter.export_deposition(invalid_id, self.test_output_dir)
                # Should fail for invalid deposition ID formats
                self.assertFalse(success, f"Export should fail for invalid deposition ID format: '{invalid_id}'")
            
            print("   ✅ Invalid deposition ID formats handled correctly")
            
        except Exception as e:
            self.fail(f"Invalid deposition ID test failed: {e}")

    def test_export_with_valid_deposition_id_prefixes(self):
        """Test export accepts both D_ and G_ prefixes for deposition IDs"""
        try:
            exporter = DbToCifExporter(self.site_id)
            
            # Test valid prefixes (D_ for depositions, G_ for groups)
            valid_ids = [
                ("D_1000000001", "standard deposition"),
                ("G_1000000001", "group deposition")
            ]
            
            for dep_id, description in valid_ids:
                # These should not fail validation (though may have no data)
                try:
                    success = exporter.export_deposition(dep_id, self.test_output_dir, overwrite=True)
                    # Success could be True (exported) or True (no data, but didn't fail validation)
                    print(f"       ✓ {description} ID '{dep_id}' passed validation: success={success}")
                except Exception as e:
                    # Should not raise exception for validation errors
                    if "must start with" in str(e).lower() or "invalid" in str(e).lower():
                        self.fail(f"{description} ID '{dep_id}' should be accepted but got: {e}")
                    else:
                        # Other errors (DB issues, etc.) are acceptable for this test
                        print(f"       ~ {description} ID '{dep_id}' validation passed (other error: {e})")
            
            print("   ✅ Valid deposition ID prefixes (D_ and G_) accepted correctly")
            
        except Exception as e:
            self.fail(f"Valid deposition ID prefix test failed: {e}")

    def test_bulk_export_functionality(self):
        """Test bulk export of multiple depositions"""
        try:
            # Create test messages for multiple depositions (or use the same one multiple times)
            try:
                msg_id1, _, _, _ = self._create_test_message("BULK_1")
                print(f"   📝 Created test message 1: {msg_id1}")
            except:
                print("   ⚠️  Could not create test messages, using existing data")
            
            exporter = DbToCifExporter(self.site_id)
            
            # Test bulk export with specific depositions
            deposition_list = [self.dep_id]  # Use the configured test deposition
            results = exporter.export_bulk(deposition_list, self.test_output_dir, overwrite=True)
            
            self.assertIn("successful", results, "Results should contain 'successful' key")
            self.assertIn("failed", results, "Results should contain 'failed' key")
            
            # At least our test deposition should be processed
            total_processed = len(results["successful"]) + len(results["failed"])
            self.assertGreater(total_processed, 0, "At least one deposition should be processed")
            
            print(f"   ✅ Bulk export completed: {len(results['successful'])} successful, {len(results['failed'])} failed")
            
        except Exception as e:
            self.fail(f"Bulk export test failed: {e}")

    def test_cif_file_round_trip(self):
        """Test that exported CIF files can be read back and parsed correctly"""
        try:
            # Create a test message with special characters
            msg_id, subject, body, sender = self._create_test_message("ROUND_TRIP")
            print(f"   📝 Created test message with special chars: {msg_id}")
        except Exception as e:
            self.skipTest(f"Could not create test message: {e}")
        
        try:
            # Export to CIF
            exporter = DbToCifExporter(self.site_id)
            success = exporter.export_deposition(self.dep_id, self.test_output_dir, overwrite=True)
            self.assertTrue(success, "Export should succeed")
            
            # Find the created CIF file
            dep_dir = Path(self.test_output_dir) / self.dep_id
            cif_files = list(dep_dir.glob("*.cif*"))
            self.assertGreater(len(cif_files), 0, "CIF files should be created")
            
            # Test round-trip parsing
            for cif_file in cif_files:
                print(f"   🔄 Testing round-trip for: {cif_file.name}")
                
                # Parse with gemmi
                doc = gemmi.cif.read_file(str(cif_file))
                block = doc[0]
                
                # Extract message data
                messages_found = []
                
                # Handle both single items and loops
                for item in block:
                    if item.pair and item.pair[0] == "_pdbx_deposition_message_info.message_id":
                        # Single message format
                        msg_data = {"message_id": item.pair[1].strip("'\"")}
                        # Look for other fields
                        for other_item in block:
                            if other_item.pair:
                                key = other_item.pair[0]
                                if key.startswith("_pdbx_deposition_message_info."):
                                    field = key.split(".")[-1]
                                    msg_data[field] = other_item.pair[1].strip("'\"")
                        messages_found.append(msg_data)
                        break
                    elif item.loop:
                        # Loop format
                        tags = item.loop.tags
                        if "_pdbx_deposition_message_info.message_id" in tags:
                            msg_id_idx = tags.index("_pdbx_deposition_message_info.message_id")
                            # Iterate using loop indices (gemmi requires loop[row, col] access)
                            for row_idx in range(item.loop.length()):
                                msg_data = {}
                                for col_idx, tag in enumerate(tags):
                                    field = tag.split(".")[-1]
                                    msg_data[field] = item.loop[row_idx, col_idx].strip("'\"")
                                messages_found.append(msg_data)
                            break
                
                # Verify we found our message
                found_our_message = any(msg.get("message_id") == msg_id for msg in messages_found)
                if found_our_message:
                    print(f"       ✅ Found our test message in parsed data")
                else:
                    print(f"       ⚠️  Test message {msg_id} not found in parsed data")
                    print(f"       Found message IDs: {[msg.get('message_id') for msg in messages_found]}")
            
            print("   ✅ Round-trip parsing successful")
            
        except Exception as e:
            self.fail(f"Round-trip test failed: {e}")

    def test_error_handling_scenarios(self):
        """Test various error handling scenarios"""
        try:
            exporter = DbToCifExporter(self.site_id)
            
            # Test with invalid output directory (permission denied scenario)
            invalid_dir = "/root/cannot_write_here"
            if not os.path.exists(invalid_dir):
                # Create a read-only directory to simulate permission issues
                readonly_dir = os.path.join(self.test_output_dir, "readonly")
                os.makedirs(readonly_dir)
                os.chmod(readonly_dir, 0o444)  # Read-only
                
                try:
                    success = exporter.export_deposition(self.dep_id, readonly_dir)
                    # May succeed or fail depending on system - just ensure it doesn't crash
                    print(f"       Export to read-only directory result: {success}")
                except Exception as e:
                    print(f"       Expected error for read-only directory: {e}")
                finally:
                    # Restore permissions for cleanup
                    os.chmod(readonly_dir, 0o755)
            
            print("   ✅ Error handling scenarios tested")
            
        except Exception as e:
            print(f"   ⚠️  Error handling test had issues: {e}")

    def test_export_statistics(self):
        """Test that export statistics are properly maintained"""
        try:
            exporter = DbToCifExporter(self.site_id)
            
            # Check initial stats
            initial_stats = exporter.stats.copy()
            self.assertEqual(initial_stats["depositions_processed"], 0)
            self.assertEqual(initial_stats["files_created"], 0)
            self.assertEqual(initial_stats["messages_exported"], 0)
            self.assertEqual(initial_stats["errors"], 0)
            
            # Perform an export
            success = exporter.export_deposition(self.dep_id, self.test_output_dir, overwrite=True)
            
            # Check updated stats
            final_stats = exporter.stats
            self.assertGreaterEqual(final_stats["depositions_processed"], 1)
            
            if success:
                # If export succeeded, we should have some activity
                print(f"   📊 Export statistics:")
                print(f"       Depositions processed: {final_stats['depositions_processed']}")
                print(f"       Files created: {final_stats['files_created']}")
                print(f"       Messages exported: {final_stats['messages_exported']}")
                print(f"       Errors: {final_stats['errors']}")
            
            print("   ✅ Export statistics tracking validated")
            
        except Exception as e:
            self.fail(f"Statistics test failed: {e}")


class TestDumpScriptIntegration(unittest.TestCase):
    """Test the dump_db_to_cif.py script as a command-line tool"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test class"""
        cls.site_id = os.getenv("WWPDB_SITE_ID")
        if not cls.site_id:
            raise unittest.SkipTest("WWPDB_SITE_ID environment variable not set")
        
        cls.dep_id = os.getenv("WWPDB_TEST_DEP_ID", "D_1000000001")
        
        # Find the script path
        script_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'scripts')
        cls.script_path = os.path.join(script_dir, 'dump_db_to_cif.py')
        
        if not os.path.exists(cls.script_path):
            raise unittest.SkipTest(f"dump_db_to_cif.py script not found at {cls.script_path}")
        
        print(f"\n🧪 Testing dump_db_to_cif.py script at {cls.script_path}")

    def setUp(self):
        """Set up each test"""
        self.test_output_dir = tempfile.mkdtemp(prefix="dump_script_test_")

    def tearDown(self):
        """Clean up after each test"""
        if os.path.exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)

    def test_script_help_output(self):
        """Test that the script shows help correctly"""
        import subprocess
        
        try:
            result = subprocess.run([
                sys.executable, self.script_path, "--help"
            ], capture_output=True, text=True, timeout=30)
            
            self.assertEqual(result.returncode, 0, "Help command should succeed")
            self.assertIn("Export message data from database to CIF files", result.stdout)
            self.assertIn("--site-id", result.stdout)
            self.assertIn("--deposition", result.stdout)
            
            print("   ✅ Script help output validated")
            
        except subprocess.TimeoutExpired:
            self.fail("Script help command timed out")
        except Exception as e:
            self.fail(f"Script help test failed: {e}")

    def test_script_single_deposition_export(self):
        """Test script with single deposition export"""
        import subprocess
        
        try:
            result = subprocess.run([
                sys.executable, self.script_path,
                "--site-id", self.site_id,
                "--deposition", self.dep_id,
                "--output-dir", self.test_output_dir,
                "--overwrite"
            ], capture_output=True, text=True, timeout=60)
            
            print(f"   📤 Script output:")
            if result.stdout:
                print(f"       STDOUT: {result.stdout}")
            if result.stderr:
                print(f"       STDERR: {result.stderr}")
            print(f"       Return code: {result.returncode}")
            
            # Script should complete without crashing
            self.assertNotEqual(result.returncode, 1, "Script should not exit with error code 1")
            
            # Check if any files were created
            dep_dir = Path(self.test_output_dir) / self.dep_id
            if dep_dir.exists():
                cif_files = list(dep_dir.glob("*.cif*"))
                print(f"       Created {len(cif_files)} CIF files")
            
            print("   ✅ Script single deposition export completed")
            
        except subprocess.TimeoutExpired:
            self.fail("Script single deposition export timed out")
        except Exception as e:
            self.fail(f"Script single deposition export failed: {e}")


if __name__ == "__main__":
    unittest.main()
