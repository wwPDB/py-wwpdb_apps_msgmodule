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
        
        # List files in output directory to see what was actually created
        output_files = os.listdir(self.output_dir)
        print(f"   📂 Files in output directory: {output_files}")
        
        # Compare original and exported
        output_file = os.path.join(self.output_dir, f"{dep_id}_messages-to-depositor.cif")
        if not os.path.exists(output_file):
            # Try to find any messages-to-depositor file
            matching_files = [f for f in output_files if 'messages-to-depositor' in f]
            if matching_files:
                print(f"   ⚠️  Expected {os.path.basename(output_file)}, found: {matching_files}")
                output_file = os.path.join(self.output_dir, matching_files[0])
            else:
                self.fail(f"No messages-to-depositor file found. Available files: {output_files}")
        
        original_data = self._parse_cif_file(input_file)
        exported_data = self._parse_cif_file(output_file)
        
        self.assertEqual(len(original_data["messages"]), len(exported_data["messages"]),
                        "Should have same number of messages")
        
        print(f"   ✅ Round-trip validation successful ({len(original_data['messages'])} messages)")

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
        
        # List files in output directory to see what was actually created
        output_files = os.listdir(self.output_dir)
        print(f"   📂 Files in output directory: {output_files}")
        
        # Verify message count
        output_file = os.path.join(self.output_dir, f"{dep_id}_messages-from-depositor.cif")
        if not os.path.exists(output_file):
            # Try to find any messages-from-depositor file
            matching_files = [f for f in output_files if 'messages-from-depositor' in f]
            if matching_files:
                print(f"   ⚠️  Expected {os.path.basename(output_file)}, found: {matching_files}")
                output_file = os.path.join(self.output_dir, matching_files[0])
            else:
                self.fail(f"No messages-from-depositor file found. Available files: {output_files}")
        
        original_data = self._parse_cif_file(input_file)
        exported_data = self._parse_cif_file(output_file)
        
        self.assertEqual(len(original_data["messages"]), len(exported_data["messages"]),
                        "Should have same number of messages")
        
        print(f"   ✅ Round-trip validation successful ({len(original_data['messages'])} messages)")

    def test_notes_from_annotator_round_trip(self):
        """Test round-trip with real notes-from-annotator file"""
        print("\n🔄 Testing notes-from-annotator round-trip...")
        
        # Use existing test file
        source_file = os.path.join(self.test_data_dir, "D_0000265933_notes-from-annotator_P1.cif.V1")
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
        
        # List files in output directory to see what was actually created
        output_files = os.listdir(self.output_dir)
        print(f"   📂 Files in output directory: {output_files}")
        
        # Verify message count
        output_file = os.path.join(self.output_dir, f"{dep_id}_notes-from-annotator.cif")
        if not os.path.exists(output_file):
            # Try to find any notes-from-annotator file
            matching_files = [f for f in output_files if 'notes-from-annotator' in f]
            if matching_files:
                print(f"   ⚠️  Expected {os.path.basename(output_file)}, found: {matching_files}")
                output_file = os.path.join(self.output_dir, matching_files[0])
            else:
                self.fail(f"No notes-from-annotator file found. Available files: {output_files}")
        
        original_data = self._parse_cif_file(input_file)
        exported_data = self._parse_cif_file(output_file)
        
        self.assertEqual(len(original_data["messages"]), len(exported_data["messages"]),
                        "Should have same number of messages")
        
        print(f"   ✅ Round-trip validation successful ({len(original_data['messages'])} messages)")


if __name__ == "__main__":
    unittest.main()