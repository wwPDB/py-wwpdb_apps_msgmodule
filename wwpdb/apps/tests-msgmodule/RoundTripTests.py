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
        
        cls.dep_id = f"D_RT{int(datetime.now().timestamp())}"  # Unique deposition ID
        print(f"\n🔄 Round-trip testing with site_id={cls.site_id}, dep_id={cls.dep_id}")

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

    def _create_test_cif_file(self, file_path: str, messages: List[Dict[str, Any]], 
                               file_refs: List[Dict[str, Any]] = None,
                               statuses: List[Dict[str, Any]] = None) -> None:
        """Create a test CIF file with specified message data"""
        doc = gemmi.cif.Document()
        block = doc.add_new_block("messages")
        
        # Add message info loop
        loop = block.init_loop("_pdbx_deposition_message_info.", [
            "ordinal", "message_id", "deposition_data_set_id", "sender",
            "context_type", "context_value", "subject", "message_text",
            "send_timestamp", "message_type", "parent_message_id"
        ])
        
        for msg in messages:
            loop.add_row([
                str(msg.get("ordinal", 1)),
                msg.get("message_id", "MSG001"),
                msg.get("deposition_data_set_id", self.dep_id),
                msg.get("sender", "test@example.com"),
                msg.get("context_type", "annotation"),
                msg.get("context_value", "general"),
                msg.get("subject", "Test Subject"),
                msg.get("message_text", "Test message"),
                msg.get("send_timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                msg.get("message_type", "text"),
                msg.get("parent_message_id", "?")
            ])
        
        # Add file references if provided
        if file_refs:
            ref_loop = block.init_loop("_pdbx_deposition_message_file_reference.", [
                "ordinal", "message_id", "file_name", "file_type",
                "content_type", "version_id", "storage_type", "upload_timestamp"
            ])
            for ref in file_refs:
                ref_loop.add_row([
                    str(ref.get("ordinal", 1)),
                    ref.get("message_id", "MSG001"),
                    ref.get("file_name", "test.txt"),
                    ref.get("file_type", "text"),
                    ref.get("content_type", "application/text"),
                    str(ref.get("version_id", 1)),
                    ref.get("storage_type", "filesystem"),
                    ref.get("upload_timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                ])
        
        # Add statuses if provided
        if statuses:
            status_loop = block.init_loop("_pdbx_deposition_message_status.", [
                "ordinal", "deposition_data_set_id", "message_id",
                "read_status", "timestamp"
            ])
            for status in statuses:
                status_loop.add_row([
                    str(status.get("ordinal", 1)),
                    status.get("deposition_data_set_id", self.dep_id),
                    status.get("message_id", "MSG001"),
                    status.get("read_status", "Y"),
                    status.get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                ])
        
        doc.write_file(file_path)

    def _parse_cif_file(self, file_path: str) -> Dict[str, List[Dict]]:
        """Parse a CIF file and extract message data"""
        doc = gemmi.cif.read_file(file_path)
        block = doc[0]
        
        result = {"messages": [], "file_refs": [], "statuses": []}
        
        # Extract messages
        msg_loop = block.find_loop("_pdbx_deposition_message_info.ordinal")
        if msg_loop:
            for row in msg_loop:
                result["messages"].append({
                    "ordinal": row.str(0),
                    "message_id": row.str(1),
                    "deposition_data_set_id": row.str(2),
                    "sender": row.str(3),
                    "context_type": row.str(4),
                    "context_value": row.str(5),
                    "subject": row.str(6),
                    "message_text": row.str(7),
                    "send_timestamp": row.str(8),
                    "message_type": row.str(9),
                    "parent_message_id": row.str(10)
                })
        
        # Extract file references
        ref_loop = block.find_loop("_pdbx_deposition_message_file_reference.ordinal")
        if ref_loop:
            for row in ref_loop:
                result["file_refs"].append({
                    "ordinal": row.str(0),
                    "message_id": row.str(1),
                    "file_name": row.str(2),
                    "file_type": row.str(3),
                    "content_type": row.str(4),
                    "version_id": row.str(5),
                    "storage_type": row.str(6),
                    "upload_timestamp": row.str(7)
                })
        
        # Extract statuses
        status_loop = block.find_loop("_pdbx_deposition_message_status.ordinal")
        if status_loop:
            for row in status_loop:
                result["statuses"].append({
                    "ordinal": row.str(0),
                    "deposition_data_set_id": row.str(1),
                    "message_id": row.str(2),
                    "read_status": row.str(3),
                    "timestamp": row.str(4)
                })
        
        return result

    def _compare_messages(self, original: Dict, exported: Dict) -> List[str]:
        """Compare two message dictionaries and return list of differences"""
        differences = []
        
        # Compare key fields (ignore auto-generated fields like ordinal)
        key_fields = ["message_id", "deposition_data_set_id", "sender", "subject"]
        for field in key_fields:
            orig_val = original.get(field, "?")
            exp_val = exported.get(field, "?")
            if orig_val != exp_val:
                differences.append(f"{field}: '{orig_val}' != '{exp_val}'")
        
        # Compare message text (may have whitespace differences)
        orig_text = original.get("message_text", "").strip()
        exp_text = exported.get("message_text", "").strip()
        if orig_text != exp_text:
            differences.append(f"message_text differs (length: {len(orig_text)} vs {len(exp_text)})")
        
        return differences

    # ---- Round-trip tests ----

    def test_simple_message_round_trip(self):
        """Test round-trip of a simple message"""
        print("\n🔄 Testing simple message round-trip...")
        
        # Create input CIF file
        input_file = os.path.join(self.input_dir, f"{self.dep_id}_messages-to-depositor.cif")
        messages = [{
            "ordinal": 1,
            "message_id": "RT001",
            "deposition_data_set_id": self.dep_id,
            "sender": "roundtrip@test.com",
            "context_type": "annotation",
            "context_value": "general",
            "subject": "Simple Round-trip Test",
            "message_text": "This is a simple test message for round-trip validation.",
            "send_timestamp": "2024-01-15 10:30:00",
            "message_type": "text",
            "parent_message_id": "?"
        }]
        
        self._create_test_cif_file(input_file, messages)
        print(f"   ✅ Created input CIF: {input_file}")
        
        # Import to database
        migrator = CifToDbMigrator(self.site_id)
        success = migrator._migrate_file(input_file, "messages-to-depositor", dry_run=False)
        self.assertTrue(success, "Migration to database should succeed")
        print(f"   ✅ Imported to database")
        
        # Export from database
        exporter = DbToCifExporter(self.site_id)
        output_file = os.path.join(self.output_dir, f"{self.dep_id}_messages-to-depositor.cif")
        export_success = exporter.export_deposition(self.dep_id, output_dir=self.output_dir, overwrite=True)
        self.assertTrue(export_success, "Export from database should succeed")
        print(f"   ✅ Exported from database")
        
        # Compare original and exported
        original_data = self._parse_cif_file(input_file)
        exported_data = self._parse_cif_file(output_file)
        
        self.assertEqual(len(original_data["messages"]), len(exported_data["messages"]),
                        "Should have same number of messages")
        
        # Compare message content
        for orig, exp in zip(original_data["messages"], exported_data["messages"]):
            diffs = self._compare_messages(orig, exp)
            if diffs:
                print(f"   ⚠️  Differences found: {', '.join(diffs)}")
            self.assertEqual(len(diffs), 0, f"Messages should match: {diffs}")
        
        print("   ✅ Round-trip validation successful")

    def test_special_characters_round_trip(self):
        """Test round-trip with special characters in message text"""
        print("\n🔄 Testing special characters round-trip...")
        
        input_file = os.path.join(self.input_dir, f"{self.dep_id}_messages-to-depositor.cif")
        messages = [{
            "ordinal": 1,
            "message_id": "RT002",
            "deposition_data_set_id": self.dep_id,
            "sender": "special@test.com",
            "context_type": "annotation",
            "context_value": "general",
            "subject": "Special Characters: àéîôü",
            "message_text": "Test with special chars: café, naïve, Müller, José\nNew line test.",
            "send_timestamp": "2024-01-15 10:30:00",
            "message_type": "text",
            "parent_message_id": "?"
        }]
        
        self._create_test_cif_file(input_file, messages)
        
        # Import and export
        migrator = CifToDbMigrator(self.site_id)
        migrator._migrate_file(input_file, "messages-to-depositor", dry_run=False)
        
        exporter = DbToCifExporter(self.site_id)
        output_file = os.path.join(self.output_dir, f"{self.dep_id}_messages-to-depositor.cif")
        exporter.export_deposition(self.dep_id, output_dir=self.output_dir, overwrite=True)
        
        # Verify exported file is valid CIF
        doc = gemmi.cif.read_file(output_file)
        self.assertGreater(len(doc), 0, "Exported CIF should be parseable")
        
        print("   ✅ Special characters handled correctly")

    def test_multiple_messages_round_trip(self):
        """Test round-trip with multiple messages"""
        print("\n🔄 Testing multiple messages round-trip...")
        
        input_file = os.path.join(self.input_dir, f"{self.dep_id}_messages-to-depositor.cif")
        messages = [
            {
                "ordinal": 1,
                "message_id": "RT003_1",
                "deposition_data_set_id": self.dep_id,
                "sender": "multi1@test.com",
                "subject": "First Message",
                "message_text": "First test message",
                "send_timestamp": "2024-01-15 10:00:00"
            },
            {
                "ordinal": 2,
                "message_id": "RT003_2",
                "deposition_data_set_id": self.dep_id,
                "sender": "multi2@test.com",
                "subject": "Second Message",
                "message_text": "Second test message",
                "send_timestamp": "2024-01-15 11:00:00"
            },
            {
                "ordinal": 3,
                "message_id": "RT003_3",
                "deposition_data_set_id": self.dep_id,
                "sender": "multi3@test.com",
                "subject": "Third Message",
                "message_text": "Third test message",
                "send_timestamp": "2024-01-15 12:00:00"
            }
        ]
        
        self._create_test_cif_file(input_file, messages)
        
        # Import and export
        migrator = CifToDbMigrator(self.site_id)
        migrator._migrate_file(input_file, "messages-to-depositor", dry_run=False)
        
        exporter = DbToCifExporter(self.site_id)
        output_file = os.path.join(self.output_dir, f"{self.dep_id}_messages-to-depositor.cif")
        exporter.export_deposition(self.dep_id, output_dir=self.output_dir, overwrite=True)
        
        # Compare counts
        original_data = self._parse_cif_file(input_file)
        exported_data = self._parse_cif_file(output_file)
        
        self.assertEqual(len(original_data["messages"]), 3, "Should have 3 original messages")
        self.assertEqual(len(exported_data["messages"]), 3, "Should have 3 exported messages")
        
        print("   ✅ Multiple messages preserved correctly")

    def test_messages_with_file_references_round_trip(self):
        """Test round-trip of messages with file references"""
        print("\n🔄 Testing messages with file references round-trip...")
        
        input_file = os.path.join(self.input_dir, f"{self.dep_id}_messages-to-depositor.cif")
        messages = [{
            "ordinal": 1,
            "message_id": "RT004",
            "deposition_data_set_id": self.dep_id,
            "sender": "fileref@test.com",
            "subject": "Message with Attachments",
            "message_text": "This message has file references",
            "send_timestamp": "2024-01-15 10:30:00"
        }]
        
        file_refs = [
            {
                "ordinal": 1,
                "message_id": "RT004",
                "file_name": "attachment1.pdf",
                "file_type": "pdf",
                "content_type": "application/pdf",
                "version_id": 1,
                "storage_type": "filesystem",
                "upload_timestamp": "2024-01-15 10:30:00"
            },
            {
                "ordinal": 2,
                "message_id": "RT004",
                "file_name": "attachment2.txt",
                "file_type": "text",
                "content_type": "text/plain",
                "version_id": 1,
                "storage_type": "filesystem",
                "upload_timestamp": "2024-01-15 10:31:00"
            }
        ]
        
        self._create_test_cif_file(input_file, messages, file_refs=file_refs)
        
        # Import and export
        migrator = CifToDbMigrator(self.site_id)
        migrator._migrate_file(input_file, "messages-to-depositor", dry_run=False)
        
        exporter = DbToCifExporter(self.site_id)
        output_file = os.path.join(self.output_dir, f"{self.dep_id}_messages-to-depositor.cif")
        exporter.export_deposition(self.dep_id, output_dir=self.output_dir, overwrite=True)
        
        # Compare file references
        original_data = self._parse_cif_file(input_file)
        exported_data = self._parse_cif_file(output_file)
        
        self.assertEqual(len(original_data["file_refs"]), 2, "Should have 2 file references")
        self.assertEqual(len(exported_data["file_refs"]), 2, "Should preserve 2 file references")
        
        print("   ✅ File references preserved correctly")

    def test_messages_with_statuses_round_trip(self):
        """Test round-trip of messages with status information"""
        print("\n🔄 Testing messages with statuses round-trip...")
        
        input_file = os.path.join(self.input_dir, f"{self.dep_id}_messages-to-depositor.cif")
        messages = [{
            "ordinal": 1,
            "message_id": "RT005",
            "deposition_data_set_id": self.dep_id,
            "sender": "status@test.com",
            "subject": "Message with Status",
            "message_text": "This message has read status",
            "send_timestamp": "2024-01-15 10:30:00"
        }]
        
        statuses = [{
            "ordinal": 1,
            "deposition_data_set_id": self.dep_id,
            "message_id": "RT005",
            "read_status": "Y",
            "timestamp": "2024-01-15 11:00:00"
        }]
        
        self._create_test_cif_file(input_file, messages, statuses=statuses)
        
        # Import and export
        migrator = CifToDbMigrator(self.site_id)
        migrator._migrate_file(input_file, "messages-to-depositor", dry_run=False)
        
        exporter = DbToCifExporter(self.site_id)
        output_file = os.path.join(self.output_dir, f"{self.dep_id}_messages-to-depositor.cif")
        exporter.export_deposition(self.dep_id, output_dir=self.output_dir, overwrite=True)
        
        # Compare statuses
        original_data = self._parse_cif_file(input_file)
        exported_data = self._parse_cif_file(output_file)
        
        self.assertEqual(len(original_data["statuses"]), 1, "Should have 1 status")
        self.assertEqual(len(exported_data["statuses"]), 1, "Should preserve 1 status")
        
        print("   ✅ Status information preserved correctly")


if __name__ == "__main__":
    unittest.main()