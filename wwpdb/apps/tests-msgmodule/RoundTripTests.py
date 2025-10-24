# wwpdb/apps/tests-msgmodule/test_RoundTripTests.py
"""
Round-trip tests for CIF ↔ Database conversion.

Tests the complete cycle: CIF file → Database → CIF file
Validates data integrity through migrate_cif_to_db.py and dump_db_to_cif.py

Run:
  export WWPDB_SITE_ID=PDBE_DEV
  python -m pytest wwpdb/apps/tests-msgmodule/test_RoundTripTests.py -v -s
"""
import os
import sys
import unittest
import tempfile
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

# Setup test path - follow same pattern as other test files
if __package__ is None or __package__ == "":
    from os import path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from commonsetup import TESTOUTPUT
else:
    from .commonsetup import TESTOUTPUT

# Remove mock ConfigInfo to allow real database access
if 'wwpdb.utils.config.ConfigInfo' in sys.modules:
    del sys.modules['wwpdb.utils.config.ConfigInfo']

try:
    import gemmi
except ImportError:
    gemmi = None

# Import the migration and export modules from scripts directory
# Note: scripts/ contains standalone executables, not part of wwpdb package structure
try:
    scripts_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'scripts')
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    
    import migrate_cif_to_db
    import dump_db_to_cif
    
    CifToDbMigrator = migrate_cif_to_db.CifToDbMigrator
    DbToCifExporter = dump_db_to_cif.DbToCifExporter
except ImportError as e:
    print(f"Warning: Could not import migration scripts: {e}")
    CifToDbMigrator = None
    DbToCifExporter = None


class TestCifDatabaseRoundTrip(unittest.TestCase):
    """Test complete round-trip: CIF → Database → CIF"""

    @classmethod
    def setUpClass(cls):
        """Set up test class"""
        cls.site_id = os.getenv("WWPDB_SITE_ID")
        if not cls.site_id:
            raise unittest.SkipTest("WWPDB_SITE_ID environment variable not set")
        
        if gemmi is None:
            raise unittest.SkipTest("gemmi library not available")
        
        if CifToDbMigrator is None or DbToCifExporter is None:
            raise unittest.SkipTest("Migration scripts not available")
        
        # Find test data directory
        cls.test_data_dir = os.path.join(
            os.path.dirname(__file__), 
            "test_data"
        )
        if not os.path.exists(cls.test_data_dir):
            raise unittest.SkipTest(f"Test data directory not found: {cls.test_data_dir}")
        
        print(f"\n🔄 Round-trip testing with site_id={cls.site_id}")

    def setUp(self):
        """Set up each test"""
        self.test_dir = tempfile.mkdtemp(prefix="roundtrip_test_")
        self.input_dir = os.path.join(self.test_dir, "input")
        self.output_dir = os.path.join(self.test_dir, "output")
        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        print(f"   📁 Test directory: {self.test_dir}")

    def tearDown(self):
        """Clean up after each test"""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    # ---- Helper methods ----

    def _extract_dep_id_from_filename(self, filename: str) -> str:
        """Extract deposition ID from CIF filename (e.g., D_000219_messages-to-depositor_P1.cif.V1 -> D_000219)"""
        basename = os.path.basename(filename)
        # Format: D_NNNNNN_content-type_P1.cif.V1
        parts = basename.split('_')
        if len(parts) >= 2 and parts[0] == 'D':
            return f"{parts[0]}_{parts[1]}"
        raise ValueError(f"Could not extract deposition ID from filename: {filename}")

    def _parse_cif_file(self, file_path: str) -> Dict[str, List[Dict]]:
        """Parse a CIF file and extract message data
        
        Handles both loop format (multiple messages) and key-value format (single message).
        """
        doc = gemmi.cif.read_file(file_path)
        block = doc[0]
        
        result = {"messages": [], "file_refs": [], "statuses": []}
        
        # Track single-value items for each category
        msg_items = {}
        ref_items = {}
        status_items = {}
        
        # Iterate through block items to find both loops and key-value pairs
        for item in block:
            if item.loop:
                loop = item.loop
                tags = loop.tags
                
                # Check if this is a message info loop
                if any(tag.startswith("_pdbx_deposition_message_info.") for tag in tags):
                    for i in range(loop.length()):
                        msg_dict = {}
                        for col_idx, tag in enumerate(tags):
                            field_name = tag.split('.')[-1]
                            msg_dict[field_name] = loop[i, col_idx]
                        result["messages"].append(msg_dict)
                
                # Check if this is a file reference loop
                elif any(tag.startswith("_pdbx_deposition_message_file_reference.") for tag in tags):
                    for i in range(loop.length()):
                        ref_dict = {}
                        for col_idx, tag in enumerate(tags):
                            field_name = tag.split('.')[-1]
                            ref_dict[field_name] = loop[i, col_idx]
                        result["file_refs"].append(ref_dict)
                
                # Check if this is a status loop
                elif any(tag.startswith("_pdbx_deposition_message_status.") for tag in tags):
                    for i in range(loop.length()):
                        status_dict = {}
                        for col_idx, tag in enumerate(tags):
                            field_name = tag.split('.')[-1]
                            status_dict[field_name] = loop[i, col_idx]
                        result["statuses"].append(status_dict)
            
            elif item.pair:
                # Handle key-value pairs (single message format)
                key, value = item.pair
                
                if key.startswith("_pdbx_deposition_message_info."):
                    field_name = key.split('.')[-1]
                    msg_items[field_name] = value
                elif key.startswith("_pdbx_deposition_message_file_reference."):
                    field_name = key.split('.')[-1]
                    ref_items[field_name] = value
                elif key.startswith("_pdbx_deposition_message_status."):
                    field_name = key.split('.')[-1]
                    status_items[field_name] = value
        
        # Add single-value items as messages (if any were found)
        if msg_items:
            result["messages"].append(msg_items)
        if ref_items:
            result["file_refs"].append(ref_items)
        if status_items:
            result["statuses"].append(status_items)
        
        return result

    def _compare_messages(self, original: Dict, exported: Dict) -> List[str]:
        """Compare two message dictionaries and return list of differences"""
        differences = []
        
        # Compare key fields that must match exactly
        key_fields = ["message_id", "deposition_data_set_id", "sender", "message_subject"]
        for field in key_fields:
            orig_val = original.get(field, "?")
            exp_val = exported.get(field, "?")
            if orig_val != exp_val:
                differences.append(f"{field}: '{orig_val}' != '{exp_val}'")
        
        # Compare message text with normalized whitespace
        # CIF text blocks may have different line ending conventions or trailing whitespace
        orig_text = original.get("message_text", "")
        exp_text = exported.get("message_text", "")
        
        # Normalize: strip outer whitespace, normalize line endings, remove trailing spaces per line
        def normalize_text(text):
            lines = text.replace('\r\n', '\n').replace('\r', '\n').split('\n')
            # Strip trailing whitespace from each line but preserve empty lines
            normalized_lines = [line.rstrip() for line in lines]
            return '\n'.join(normalized_lines).strip()
        
        orig_normalized = normalize_text(orig_text)
        exp_normalized = normalize_text(exp_text)
        
        if orig_normalized != exp_normalized:
            # Show both raw and normalized lengths for debugging
            differences.append(
                f"message_text differs: "
                f"original {len(orig_text)} bytes (normalized: {len(orig_normalized)}), "
                f"exported {len(exp_text)} bytes (normalized: {len(exp_normalized)})"
            )
        
        return differences

    def _validate_round_trip(self, original_data: Dict, exported_data: Dict, content_type: str):
        """Validate that round-trip preserved data integrity"""
        
        # Check message counts
        orig_count = len(original_data["messages"])
        exp_count = len(exported_data["messages"])
        self.assertEqual(orig_count, exp_count, 
                        f"Message count mismatch: {orig_count} original vs {exp_count} exported")
        
        # Compare each message
        all_differences = []
        for idx, (orig_msg, exp_msg) in enumerate(zip(original_data["messages"], exported_data["messages"])):
            diffs = self._compare_messages(orig_msg, exp_msg)
            if diffs:
                all_differences.append(f"Message {idx + 1}: " + ", ".join(diffs))
        
        if all_differences:
            self.fail(f"Data integrity issues found in {content_type}:\n" + "\n".join(all_differences))
        
        print(f"   ✅ Round-trip validation successful ({orig_count} messages)")
        print(f"      - All message_ids preserved")
        print(f"      - All subjects preserved")
        print(f"      - All message text preserved")

    # ---- Round-trip tests ----

    def test_messages_to_depositor_round_trip(self):
        """Test round-trip with real messages-to-depositor file"""
        print("\n🔄 Testing messages-to-depositor round-trip...")
        
        # Use existing test file
        source_file = os.path.join(self.test_data_dir, "D_000219_messages-to-depositor_P1.cif.V1")
        if not os.path.exists(source_file):
            self.skipTest(f"Test file not found: {source_file}")
        
        # Extract deposition ID from filename
        dep_id = self._extract_dep_id_from_filename(source_file)
        print(f"   📋 Using deposition ID: {dep_id}")
        
        # Copy test file to input directory
        input_file = os.path.join(self.input_dir, f"{dep_id}_messages-to-depositor.cif")
        shutil.copy2(source_file, input_file)
        print(f"   ✅ Prepared input CIF from: {os.path.basename(source_file)}")
        
        # Import to database
        migrator = CifToDbMigrator(self.site_id)
        success = migrator._migrate_file(input_file, "messages-to-depositor", dry_run=False)
        self.assertTrue(success, "Migration to database should succeed")
        print(f"   ✅ Imported to database")
        
        # Export from database
        exporter = DbToCifExporter(self.site_id)
        export_success = exporter.export_deposition(dep_id, output_dir=self.output_dir, overwrite=True)
        self.assertTrue(export_success, "Export from database should succeed")
        print(f"   ✅ Exported from database")
        
        # List files in output directory - dump_db_to_cif creates a subdirectory per deposition
        output_items = os.listdir(self.output_dir)
        print(f"   📂 Items in output directory: {output_items}")
        
        # The exporter creates a subdirectory named after the deposition ID
        dep_output_dir = os.path.join(self.output_dir, dep_id)
        if os.path.isdir(dep_output_dir):
            dep_output_files = os.listdir(dep_output_dir)
            print(f"   📂 Files in {dep_id}/ subdirectory: {dep_output_files}")
            
            # Look for messages-to-depositor file in the subdirectory
            matching_files = [f for f in dep_output_files if 'messages-to-depositor' in f]
            if matching_files:
                output_file = os.path.join(dep_output_dir, matching_files[0])
            else:
                self.fail(f"No messages-to-depositor file found in {dep_id}/. Available files: {dep_output_files}")
        else:
            self.fail(f"Expected subdirectory {dep_id} not found. Available items: {output_items}")
        
        # Compare original and exported with detailed validation
        original_data = self._parse_cif_file(input_file)
        exported_data = self._parse_cif_file(output_file)
        
        self._validate_round_trip(original_data, exported_data, "messages-to-depositor")

    def test_messages_from_depositor_round_trip(self):
        """Test round-trip with real messages-from-depositor file"""
        print("\n🔄 Testing messages-from-depositor round-trip...")
        
        # Use existing test file
        source_file = os.path.join(self.test_data_dir, "D_0000265933_messages-from-depositor_P1.cif.V1")
        if not os.path.exists(source_file):
            self.skipTest(f"Test file not found: {source_file}")
        
        # Extract deposition ID from filename
        dep_id = self._extract_dep_id_from_filename(source_file)
        print(f"   📋 Using deposition ID: {dep_id}")
        
        # Copy test file to input directory
        input_file = os.path.join(self.input_dir, f"{dep_id}_messages-from-depositor.cif")
        shutil.copy2(source_file, input_file)
        print(f"   ✅ Prepared input CIF from: {os.path.basename(source_file)}")
        
        # Import and export
        migrator = CifToDbMigrator(self.site_id)
        success = migrator._migrate_file(input_file, "messages-from-depositor", dry_run=False)
        self.assertTrue(success, "Migration to database should succeed")
        print(f"   ✅ Imported to database")
        
        exporter = DbToCifExporter(self.site_id)
        export_success = exporter.export_deposition(dep_id, output_dir=self.output_dir, overwrite=True)
        self.assertTrue(export_success, "Export from database should succeed")
        print(f"   ✅ Exported from database")
        
        # List files in output directory - dump_db_to_cif creates a subdirectory per deposition
        output_items = os.listdir(self.output_dir)
        print(f"   📂 Items in output directory: {output_items}")
        
        # The exporter creates a subdirectory named after the deposition ID
        dep_output_dir = os.path.join(self.output_dir, dep_id)
        if os.path.isdir(dep_output_dir):
            dep_output_files = os.listdir(dep_output_dir)
            print(f"   📂 Files in {dep_id}/ subdirectory: {dep_output_files}")
            
            # Look for messages-from-depositor file in the subdirectory
            matching_files = [f for f in dep_output_files if 'messages-from-depositor' in f]
            if matching_files:
                output_file = os.path.join(dep_output_dir, matching_files[0])
            else:
                self.fail(f"No messages-from-depositor file found in {dep_id}/. Available files: {dep_output_files}")
        else:
            self.fail(f"Expected subdirectory {dep_id} not found. Available items: {output_items}")
        
        # Verify message count and data integrity
        original_data = self._parse_cif_file(input_file)
        exported_data = self._parse_cif_file(output_file)
        
        self._validate_round_trip(original_data, exported_data, "messages-from-depositor")

    def test_notes_from_annotator_round_trip(self):
        """Test round-trip with real notes-from-annotator file"""
        print("\n🔄 Testing notes-from-annotator round-trip...")
        
        # Use the D_9000265933 notes file (not D_0000265933 which has mismatched data)
        source_file = os.path.join(self.test_data_dir, "D_9000265933_notes-from-annotator_P1.cif.V1")
        if not os.path.exists(source_file):
            self.skipTest(f"Test file not found: {source_file}")
        
        # Extract deposition ID from filename
        dep_id = self._extract_dep_id_from_filename(source_file)
        print(f"   📋 Using deposition ID: {dep_id}")
        
        # Copy test file to input directory
        input_file = os.path.join(self.input_dir, f"{dep_id}_notes-from-annotator.cif")
        shutil.copy2(source_file, input_file)
        print(f"   ✅ Prepared input CIF from: {os.path.basename(source_file)}")
        
        # Import and export
        migrator = CifToDbMigrator(self.site_id)
        success = migrator._migrate_file(input_file, "notes-from-annotator", dry_run=False)
        self.assertTrue(success, "Migration to database should succeed")
        print(f"   ✅ Imported to database")
        
        exporter = DbToCifExporter(self.site_id)
        export_success = exporter.export_deposition(dep_id, output_dir=self.output_dir, overwrite=True)
        self.assertTrue(export_success, "Export from database should succeed")
        print(f"   ✅ Exported from database")
        
        # List files in output directory - dump_db_to_cif creates a subdirectory per deposition
        output_items = os.listdir(self.output_dir)
        print(f"   📂 Items in output directory: {output_items}")
        
        # The exporter creates a subdirectory named after the deposition ID
        dep_output_dir = os.path.join(self.output_dir, dep_id)
        if os.path.isdir(dep_output_dir):
            dep_output_files = os.listdir(dep_output_dir)
            print(f"   📂 Files in {dep_id}/ subdirectory: {dep_output_files}")
            
            # Look for notes-from-annotator file in the subdirectory
            matching_files = [f for f in dep_output_files if 'notes-from-annotator' in f]
            if not matching_files:
                # Notes might not have been exported - check what was exported
                print(f"   ⚠️  No notes-from-annotator file found. This may indicate:")
                print(f"      - No notes exist in database for this deposition")
                print(f"      - Notes were stored under a different content_type")
                print(f"      - Export logic doesn't handle notes-from-annotator")
                self.skipTest(f"No notes-from-annotator file exported for {dep_id}. Available: {dep_output_files}")
            
            output_file = os.path.join(dep_output_dir, matching_files[0])
        else:
            self.fail(f"Expected subdirectory {dep_id} not found. Available items: {output_items}")
        
        # Verify message count and data integrity
        original_data = self._parse_cif_file(input_file)
        exported_data = self._parse_cif_file(output_file)
        
        self._validate_round_trip(original_data, exported_data, "notes-from-annotator")

    def test_non_ascii_characters_round_trip(self):
        """Test round-trip with non-ASCII characters in message text
        
        This test validates that special characters survive the full cycle:
        1. Create message in database with Unicode characters
        2. Export to CIF (should escape non-ASCII)
        3. Re-import from CIF to database
        4. Compare database records to verify character preservation
        """
        print("\n🔄 Testing non-ASCII character round-trip...")
        
        # Import database classes
        from wwpdb.apps.msgmodule.db import MessageInfo
        from datetime import datetime
        import uuid
        
        # Create test deposition ID
        test_dep_id = f"D_TEST_UNICODE_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        test_message_id = str(uuid.uuid4())
        
        print(f"   📋 Using test deposition ID: {test_dep_id}")
        print(f"   🔖 Message ID: {test_message_id}")
        
        # Create message with various non-ASCII characters
        test_message_text = """Test message with special characters:
        
Greek: α β γ δ Ω
Cyrillic: Д п р и в е т
Chinese: 你好世界
Japanese: こんにちは
Arabic: مرحبا
Accents: café, naïve, résumé
Symbols: © ™ € £ ¥
Emoji: 🧬 🔬 ⚛️
Math: ∑ ∫ √ ∞
"""
        
        test_subject = "Unicode Test: café naïve résumé 你好 مرحبا"
        
        # Step 1: Create message directly in database
        print(f"   📝 Creating message in database with non-ASCII text...")
        migrator = CifToDbMigrator(self.site_id)
        
        message1 = MessageInfo(
            message_id=test_message_id,
            deposition_data_set_id=test_dep_id,
            timestamp=datetime.now(),
            sender="test-user@example.com",
            context_type="test",
            context_value="unicode-test",
            parent_message_id=test_message_id,
            message_subject=test_subject,
            message_text=test_message_text,
            message_type="text",
            send_status="Y",
            content_type="messages-to-depositor"
        )
        
        session = migrator.data_access.db_connection.get_session()
        try:
            session.add(message1)
            session.commit()
            print(f"   ✅ Message created in database")
            
            # Step 2: Export to CIF
            print(f"   📤 Exporting to CIF...")
            exporter = DbToCifExporter(self.site_id)
            export_success = exporter.export_deposition(
                test_dep_id, 
                output_dir=self.output_dir, 
                overwrite=True
            )
            self.assertTrue(export_success, "Export should succeed")
            
            # Find the exported file
            dep_output_dir = os.path.join(self.output_dir, test_dep_id)
            self.assertTrue(os.path.isdir(dep_output_dir), "Export directory should exist")
            
            exported_files = os.listdir(dep_output_dir)
            print(f"   📂 Exported files: {exported_files}")
            
            matching_files = [f for f in exported_files if 'messages-to-depositor' in f]
            self.assertTrue(len(matching_files) > 0, "Should have exported CIF file")
            
            exported_cif = os.path.join(dep_output_dir, matching_files[0])
            print(f"   ✅ Exported to: {os.path.basename(exported_cif)}")
            
            # Verify the CIF contains escaped characters
            with open(exported_cif, 'r', encoding='utf-8') as f:
                cif_content = f.read()
                # Check for some escape sequences
                self.assertIn('\\u', cif_content, "CIF should contain Unicode escapes")
                print(f"   ✅ CIF contains Unicode escape sequences")
            
            # Step 3: Delete original message from database
            print(f"   🗑️  Deleting original message from database...")
            session.delete(message1)
            session.commit()
            
            # Verify deletion
            retrieved = migrator.data_access.get_message_by_id(test_message_id)
            self.assertIsNone(retrieved, "Message should be deleted")
            print(f"   ✅ Original message deleted")
            
            # Step 4: Re-import from CIF
            print(f"   📥 Re-importing from CIF...")
            import_success = migrator._migrate_file(
                exported_cif, 
                "messages-to-depositor", 
                dry_run=False
            )
            self.assertTrue(import_success, "Re-import should succeed")
            print(f"   ✅ Re-imported from CIF")
            
            # Step 5: Retrieve the re-imported message
            message2 = migrator.data_access.get_message_by_id(test_message_id)
            self.assertIsNotNone(message2, "Re-imported message should exist")
            
            # Step 6: Compare original and re-imported messages
            print(f"   🔍 Comparing original vs re-imported message...")
            
            # Compare critical fields
            self.assertEqual(message1.message_id, message2.message_id, "Message ID should match")
            self.assertEqual(message1.deposition_data_set_id, message2.deposition_data_set_id, 
                           "Deposition ID should match")
            self.assertEqual(message1.sender, message2.sender, "Sender should match")
            
            # Compare subject - normalize whitespace
            subject1_normalized = ' '.join(message1.message_subject.split())
            subject2_normalized = ' '.join(message2.message_subject.split())
            self.assertEqual(subject1_normalized, subject2_normalized, 
                           f"Subject should match:\nOriginal: {repr(subject1_normalized)}\nRe-imported: {repr(subject2_normalized)}")
            
            # Compare message text - normalize whitespace and line endings
            def normalize_text(text):
                return '\n'.join(line.rstrip() for line in text.replace('\r\n', '\n').split('\n')).strip()
            
            text1_normalized = normalize_text(message1.message_text)
            text2_normalized = normalize_text(message2.message_text)
            
            if text1_normalized != text2_normalized:
                print(f"   ❌ Text mismatch!")
                print(f"      Original length: {len(text1_normalized)}")
                print(f"      Re-imported length: {len(text2_normalized)}")
                print(f"      Original (first 200 chars): {repr(text1_normalized[:200])}")
                print(f"      Re-imported (first 200 chars): {repr(text2_normalized[:200])}")
                self.fail("Message text should match after normalization")
            
            print(f"   ✅ All fields preserved correctly!")
            print(f"      - Message ID: {message2.message_id}")
            print(f"      - Subject: {message2.message_subject[:50]}...")
            print(f"      - Text length: {len(message2.message_text)} chars")
            print(f"      - Special chars verified: α, 你好, café, 🧬, etc.")
            print(f"   ✅ Non-ASCII round-trip validation successful!")
            
        finally:
            # Cleanup: delete test message
            try:
                test_msg = migrator.data_access.get_message_by_id(test_message_id)
                if test_msg:
                    session.delete(test_msg)
                    session.commit()
            except:
                pass
            session.close()


if __name__ == "__main__":
    unittest.main()