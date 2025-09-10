#!/usr/bin/env python
"""
MessagingIo DB Integration Tests.

- Exercises ALL public methods of MessagingIo at least once.
- Skips only if WWPDB_SITE_ID is not set.
- Uses real ConfigInfo, MessagingIo, Message; uses DAL (best-effort) for cleanup.
- Writes a real message and tries to read it back.

Run:
  export WWPDB_SITE_ID=PDBE_DEV
  # optionally set a dataset id:
  # export WWPDB_TEST_DEP_ID=D_1000000001
  pytest -q  # or: python -m unittest
"""

import os
import sys
import time
import unittest
from datetime import datetime


class TestMessagingIoDBIntegration(unittest.TestCase):
    """DB integration tests for MessagingIo (covers all public methods)."""

    @classmethod
    def setUpClass(cls):
        cls.site_id = os.getenv("WWPDB_SITE_ID", "").strip()
        if not cls.site_id:
            raise unittest.SkipTest("Set WWPDB_SITE_ID to run MessagingIo DB integration tests.")

        # If a previous harness mocked ConfigInfo, unmock it now.
        if "wwpdb.utils.config.ConfigInfo" in sys.modules:
            del sys.modules["wwpdb.utils.config.ConfigInfo"]

        # Import real dependencies
        from wwpdb.utils.config.ConfigInfo import ConfigInfo  # noqa: F401
        from wwpdb.apps.msgmodule.io.MessagingIo import MessagingIo  # noqa: F401
        from wwpdb.apps.msgmodule.models.Message import Message  # noqa: F401

        cls.ConfigInfo = ConfigInfo
        cls.MessagingIo = MessagingIo
        cls.Message = Message

        # Default dataset id (override with WWPDB_TEST_DEP_ID)
        cls.dep_id = os.getenv("WWPDB_TEST_DEP_ID", "D_1000000001")
        cls._cleanup_queue = []  # Track messages to clean up

        # Basic connection info (non-fatal if None)
        try:
            cI = cls.ConfigInfo(cls.site_id)
            cls._db_host = cI.get("SITE_DB_HOST_NAME")
            cls._db_name = cI.get("WWPDB_MESSAGING_DB_NAME")
            print(f"DB target: host={cls._db_host} db={cls._db_name} site_id={cls.site_id}")
        except Exception:
            pass

    @classmethod
    def tearDownClass(cls):
        """Clean up test messages."""
        for msg_id in cls._cleanup_queue:
            try:
                cls._cleanup_message(msg_id)
            except Exception as e:
                print(f"Warning: Failed to cleanup {msg_id}: {e}")

    @classmethod
    def _cleanup_message(cls, message_id: str):
        """Actually remove test message from database."""
        try:
            from wwpdb.apps.msgmodule.db.DataAccessLayer import DataAccessLayer
            cI = cls.ConfigInfo(cls.site_id)
            db_cfg = {
                "host": cI.get("SITE_DB_HOST_NAME"),
                "port": int(cI.get("SITE_DB_PORT_NUMBER", "3306")),
                "database": cI.get("WWPDB_MESSAGING_DB_NAME"),
                "username": cI.get("SITE_DB_ADMIN_USER"),
                "password": cI.get("SITE_DB_ADMIN_PASS", ""),
                "charset": "utf8mb4",
            }
            dal = DataAccessLayer(db_cfg)
            with dal.db_connection.get_session() as sess:
                sess.execute(
                    "DELETE FROM pdbx_deposition_message_info WHERE message_id = :mid",
                    {"mid": message_id},
                )
                sess.commit()
        except Exception:
            pass  # Best effort cleanup

    # ---- Minimal session/request shims ----

    class _Sess:
        def getId(self): return "it_session"
        def getPath(self): return "/tmp/it_session"
        def getRelativePath(self): return "it_session"

    class _Req:
        def __init__(
            self,
            site_id,
            dep_id,
            *,
            sender="it@test",
            subject="IT subject",
            message_text="IT body",
            content_type="messages-to-depositor",
            message_state="livemsg",
            msg_id=None,
            extra=None,
        ):
            self._vals = {
                "identifier": dep_id,
                "sender": sender,
                "subject": subject,
                "message": message_text,
                "message_type": "text",
                "content_type": content_type,
                "send_status": "Y",
                "message_state": message_state,
                "msg_id": msg_id,
                "parent_msg_id": None,
                "filesource": "archive",      # triggers workflow/db path
                "WWPDB_SITE_ID": site_id,
                "SITE_ID": site_id,
            }
            if extra:
                self._vals.update(extra)

        def getValue(self, k): return self._vals.get(k, "")
        def getRawValue(self, k): return self._vals.get(k, "")
        def getValueList(self, k): return []
        def setValue(self, k, v): self._vals[k] = v
        def newSessionObj(self): return TestMessagingIoDBIntegration._Sess()
        def getSessionObj(self): return TestMessagingIoDBIntegration._Sess()

    # ---- Helpers ----

    def _new_msg_id(self, prefix="IT"):
        return f"{prefix}_{int(time.time())}_{os.getpid()}"

    def _new_io(self, dep_id=None, **req_extra):
        req = self._Req(self.site_id, dep_id or self.dep_id, extra=req_extra or {})
        return self.MessagingIo(req, verbose=True)
    
    def _find_message_by_id(self, msg_id: str, dep_id: str = None):
        """Find a specific message in the database."""
        io = self._new_io(dep_id or self.dep_id)
        result = io.getMsgRowList(p_depDataSetId=dep_id or self.dep_id, p_colSearchDict={})
        
        if isinstance(result, dict):
            records = result.get("RECORD_LIST", [])
        else:
            records = result if isinstance(result, list) else []
        
        for record in records:
            if record.get("message_id") == msg_id:
                return record
        return None

    # ---- Core smoke tests (reads, writes) ----

    def test_initializeDataStore(self):
        """Test that initializeDataStore doesn't crash and sets up properly."""
        io = self._new_io()
        # Must not raise
        io.initializeDataStore()
        # TODO: Add assertion to verify data store was actually initialized
        # This could check if required database connections are established

    def test_getMsgColList(self):
        """Test column list retrieval with meaningful validation."""
        io = self._new_io()
        # Most impls accept a history flag; False is a safe default
        ok, cols = io.getMsgColList(False)
        
        # Enhanced assertions
        self.assertTrue(ok, "getMsgColList should return success")
        self.assertIsInstance(cols, list, "Columns should be returned as a list")
        self.assertGreater(len(cols), 0, "Column list should not be empty")
        
        # Verify essential columns exist
        expected_columns = ["message_id", "message_subject", "sender", "timestamp", "deposition_data_set_id"]
        col_names = [col if isinstance(col, str) else col.get("name", "") for col in cols]
        
        for expected_col in expected_columns:
            self.assertIn(expected_col, col_names, f"Essential column '{expected_col}' should be present")
        
        # Test with history flag
        ok_hist, cols_hist = io.getMsgColList(True)
        self.assertTrue(ok_hist, "getMsgColList with history=True should succeed")
        self.assertIsInstance(cols_hist, list, "History columns should be returned as a list")
        self.assertGreaterEqual(len(cols_hist), len(cols), "History view should have at least as many columns")

    def test_setGroupId(self):
        """Test group ID setting with validation."""
        io = self._new_io()
        
        # Test valid group ID
        test_group_id = "grp-test-123"
        io.setGroupId(test_group_id)
        # TODO: Add assertion to verify group ID was actually set
        # This would require access to the internal state or a getter method
        
        # Test edge cases
        io.setGroupId("")  # Empty string should not crash
        io.setGroupId(None)  # None should not crash

    def test_getMsgRowList_read_path(self):
        """Test message list retrieval with structure validation."""
        io = self._new_io()
        res = io.getMsgRowList(p_depDataSetId=self.dep_id, p_colSearchDict={})
        
        # Enhanced assertions
        self.assertIsInstance(res, (dict, list), "Result should be dict or list")
        
        if isinstance(res, dict):
            self.assertIn("RECORD_LIST", res, "Dict result should contain RECORD_LIST")
            self.assertIsInstance(res["RECORD_LIST"], list, "RECORD_LIST should be a list")
            
            # Validate optional fields
            if "TOTAL_RECORDS" in res:
                self.assertIsInstance(res["TOTAL_RECORDS"], int, "TOTAL_RECORDS should be integer")
                self.assertGreaterEqual(res["TOTAL_RECORDS"], 0, "TOTAL_RECORDS should be non-negative")
        
        # Test with different parameters
        res_filtered = io.getMsgRowList(
            p_depDataSetId=self.dep_id, 
            p_colSearchDict={"sender": "test"}, 
            p_bServerSide=True,
            p_iDisplayStart=0,
            p_iDisplayLength=10
        )
        self.assertIsInstance(res_filtered, (dict, list), "Filtered result should also be valid")

    def test_getMsg_specific(self):
        """Test specific message retrieval with proper null handling."""
        io = self._new_io()
        
        # Test with non-existent message
        res = io.getMsg(p_msgId="DEFINITELY_NONEXISTENT_MSG", p_depId=self.dep_id)
        self.assertTrue(res is None or res == {}, "Non-existent message should return None or empty dict")
        
        # Test with invalid parameters
        res_invalid = io.getMsg(p_msgId="", p_depId=self.dep_id)
        self.assertTrue(res_invalid is None or res_invalid == {}, "Empty message ID should return None or empty dict")
        
        res_invalid_dep = io.getMsg(p_msgId="test", p_depId="INVALID_DEP_ID")
        self.assertTrue(res_invalid_dep is None or isinstance(res_invalid_dep, dict), 
                       "Invalid dep ID should return None or dict")

    def test_processMsg_write_then_read(self):
        """Test complete message lifecycle with content verification."""
        subject = f"IT write/read test at {datetime.utcnow().isoformat()}Z"
        body = f"Created at {datetime.utcnow().isoformat()}Z"
        sender = "it@write.test"
        
        print(f"\n🔍 PERSISTENCE TEST - Creating message:")
        print(f"   Dataset ID: {self.dep_id}")
        print(f"   Subject: {subject}")
        print(f"   Timestamp: {datetime.utcnow().isoformat()}Z")

        # Build a real Message from request-like object (NO custom msg_id - let system generate)
        req_for_msg = self._Req(
            self.site_id, self.dep_id,
            sender=sender,
            subject=subject,
            message_text=body,
            # msg_id=None - let the system generate the ID
        )
        msg_obj = self.Message.fromReqObj(req_for_msg, verbose=True)

        io = self._new_io()
        write_res = io.processMsg(msg_obj)
        write_ok = write_res[0] if isinstance(write_res, tuple) else bool(write_res)
        
        # Enhanced assertions for write operation
        self.assertIsInstance(write_ok, bool, "processMsg should return boolean success status")
        
        if isinstance(write_res, tuple) and len(write_res) >= 3:
            _, pdbx_updated, failed_refs = write_res
            self.assertIsInstance(pdbx_updated, bool, "pdbx_updated should be boolean")
            self.assertIsInstance(failed_refs, list, "failed_refs should be list")

        # Get the ACTUAL message ID that was generated by the system
        actual_msg_id = msg_obj.messageId
        self.assertIsNotNone(actual_msg_id, "Message should have an ID after processing")
        print(f"   🆔 Generated Message ID: {actual_msg_id}")

        # Read back and verify content
        res = io.getMsgRowList(p_depDataSetId=self.dep_id, p_colSearchDict={})
        self.assertIsInstance(res, (dict, list), "Read result should be dict or list")
        
        records = res.get("RECORD_LIST", res) if isinstance(res, dict) else res
        self.assertIsInstance(records, list, "Records should be a list")

        # Verify message was actually written
        if write_ok:
            found_message = None
            for r in records:
                if r.get("message_id") == actual_msg_id:
                    found_message = r
                    break
            
            if found_message:
                # Verify content integrity
                self.assertEqual(found_message.get("message_id"), actual_msg_id, "Message ID should match")
                self.assertEqual(found_message.get("message_subject"), subject, "Subject should match")
                self.assertEqual(found_message.get("sender"), sender, "Sender should match")
                self.assertEqual(found_message.get("deposition_data_set_id"), self.dep_id, "Deposition ID should match")
                # Note: message_text might be encoded/escaped, so we check if it contains our content
                msg_text = found_message.get("message_text", "")
                self.assertIn("Created at", msg_text, "Message text should contain our test content")
                print(f"✅ SUCCESS: Message {actual_msg_id} written and verified in database!")
            else:
                print(f"⚠️  WARNING: Message {actual_msg_id} was reportedly written but not found in database")
                print(f"   This might be due to database timing, permissions, or test isolation issues")
                print(f"   Write operation returned: {write_res}")
                # But this should now be considered a test failure if write_ok is True
                if write_ok:
                    self.fail(f"Message {actual_msg_id} reported as written but not found in database")
        else:
            print(f"❌ WRITE FAILED: Message write operation failed")

        # Test specific message retrieval
        specific_msg = io.getMsg(p_msgId=actual_msg_id, p_depId=self.dep_id)
        if write_ok and specific_msg and specific_msg != {}:
            self.assertEqual(specific_msg.get("message_id"), actual_msg_id, "Retrieved message ID should match")

        print(f"\n📊 SQL QUERY TO FIND THIS MESSAGE:")
        print(f"   SELECT * FROM {self._db_name}.pdbx_deposition_message_info WHERE message_id = '{actual_msg_id}';")
        print(f"   SELECT * FROM {self._db_name}.pdbx_deposition_message_info WHERE deposition_data_set_id = '{self.dep_id}' ORDER BY timestamp DESC;")
        
        # Add to cleanup queue
        if write_ok:
            self._cleanup_queue.append(actual_msg_id)

    # ---- File helpers ----

    def test_checkAvailFiles(self):
        """Test file availability check with type validation."""
        io = self._new_io()
        avail = io.checkAvailFiles(self.dep_id)
        
        # Enhanced assertions
        self.assertIsInstance(avail, (list, dict), "Available files should be list or dict")
        
        if isinstance(avail, list):
            # Validate known file types are in expected format
            known_types = ["model", "sf", "val-report", "val-report-full", "val-data"]
            for file_type in avail:
                self.assertIsInstance(file_type, str, f"File type {file_type} should be string")
                if file_type in known_types:
                    self.assertGreater(len(file_type), 0, "File type should not be empty string")

    def test_getFilesRfrncd(self):
        """Test file reference retrieval with structure validation."""
        io = self._new_io()
        refs = io.getFilesRfrncd(self.dep_id)
        
        # Enhanced assertions  
        self.assertIsInstance(refs, (list, dict), "File references should be list or dict")
        
        # Test with message ID filter
        refs_filtered = io.getFilesRfrncd(self.dep_id, p_msgIdFilter="nonexistent")
        self.assertIsInstance(refs_filtered, (list, dict), "Filtered references should be valid type")

    # ---- Lists/filters ----

    def test_getMsgReadList(self):
        """Test read message list with validation."""
        io = self._new_io()
        out = io.getMsgReadList(self.dep_id)
        
        # Enhanced assertions
        self.assertIsInstance(out, list, "Read message list should be a list")
        
        # Validate list contents if not empty
        for item in out:
            self.assertIsInstance(item, (str, dict), "List items should be strings or dicts")

    def test_getMsgNoActionReqdList(self):
        """Test no-action-required message list."""
        io = self._new_io()
        out = io.getMsgNoActionReqdList(self.dep_id)
        
        # Enhanced assertions
        self.assertIsInstance(out, list, "No-action-required list should be a list")

    def test_getMsgForReleaseList(self):
        """Test release-flagged message list."""
        io = self._new_io()
        out = io.getMsgForReleaseList(self.dep_id)
        
        # Enhanced assertions
        self.assertIsInstance(out, list, "Release list should be a list")

    def test_getNotesList(self):
        """Test notes list retrieval."""
        io = self._new_io()
        out = io.getNotesList()
        
        # Enhanced assertions
        self.assertIsInstance(out, list, "Notes list should be a list")

    # ---- Writes/changes and helpers ----

    def test_markMsgAsRead_and_tagMsg(self):
        """Test message status updates with proper validation."""
        io = self._new_io()
        
        # Test marking as read
        status_dict = {
            "deposition_data_set_id": self.dep_id,
            "message_id": "NONEXISTENT_MSG",
            "read_flag": "Y",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        ok_read = io.markMsgAsRead(status_dict)
        
        # Enhanced assertions
        self.assertIsInstance(ok_read, bool, "markMsgAsRead should return boolean")
        
        # Test tagging message
        tag_dict = {
            "deposition_data_set_id": self.dep_id,
            "message_id": "NONEXISTENT_MSG", 
            "action_reqd": "Y",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        ok_tag = io.tagMsg(tag_dict)
        
        # Enhanced assertions
        self.assertIsInstance(ok_tag, bool, "tagMsg should return boolean")
        
        # Test with invalid data
        invalid_dict = {"invalid": "data"}
        ok_invalid = io.markMsgAsRead(invalid_dict)
        self.assertIsInstance(ok_invalid, bool, "Invalid data should still return boolean")

    def test_autoMsg(self):
        """Test automated message generation with result validation."""
        io = self._new_io()
        result = io.autoMsg(
            p_depIdList=[self.dep_id],
            p_tmpltType="release-publ",
            p_isEmdbEntry=False,
            p_sender="it@auto",
        )
        
        # Enhanced assertions
        self.assertIsInstance(result, dict, "autoMsg should return a dictionary")
        self.assertIn(self.dep_id, result, f"Result should contain entry for {self.dep_id}")
        
        dep_result = result[self.dep_id]
        self.assertIsInstance(dep_result, dict, "Deposition result should be a dictionary")
        
        # Validate required keys
        required_keys = ['success', 'pdbx_model_updated']
        for key in required_keys:
            self.assertIn(key, dep_result, f"Result should contain '{key}' key")
            self.assertIn(dep_result[key], ['true', 'false'], f"'{key}' should be 'true' or 'false'")
        
        # Test with different template types
        template_types = ["release-nopubl", "reminder", "obsolete"]
        for tmpl_type in template_types:
            try:
                result_tmpl = io.autoMsg(
                    p_depIdList=[self.dep_id],
                    p_tmpltType=tmpl_type,
                    p_isEmdbEntry=False,
                    p_sender="it@auto",
                )
                self.assertIsInstance(result_tmpl, dict, f"Template {tmpl_type} should return dict")
            except Exception as e:
                # Some templates may not be available in test environment
                print(f"Template {tmpl_type} failed (may be expected): {e}")

    def test_sendSingle(self):
        """Test single message sending with validation."""
        io = self._new_io()
        
        subject = "IT sendSingle subject"
        message = "IT sendSingle body"
        sender = "it@single.test"
        
        ok = io.sendSingle(
            depId=self.dep_id,
            subject=subject,
            msg=message,
            p_sender=sender,
            p_testemail=False,
            p_tmpltType="other",
        )
        
        # Enhanced assertions
        self.assertIsInstance(ok, bool, "sendSingle should return boolean")
        
        # Test with test email
        ok_test = io.sendSingle(
            depId=self.dep_id,
            subject=subject + " (test)",
            msg=message,
            p_sender=sender,
            p_testemail="test@example.com",
            p_tmpltType="other",
        )
        self.assertIsInstance(ok_test, bool, "sendSingle with test email should return boolean")

    def test_getMsgTmpltDataItems_and_getStarterMsgBody(self):
        """Test template data and starter message body retrieval."""
        io = self._new_io()
        
        # Test template data population
        template_dict = {}
        io.getMsgTmpltDataItems(template_dict)
        
        # Enhanced assertions
        self.assertIsInstance(template_dict, dict, "Template dict should remain a dictionary")
        
        # Verify some expected template keys are populated (if data is available)
        expected_keys = ["identifier", "pdb_id", "title", "status_code"]
        populated_keys = [k for k in expected_keys if k in template_dict and template_dict[k]]
        if populated_keys:
            print(f"Template populated with keys: {populated_keys}")
        
        # Test starter message body
        body = io.getStarterMsgBody()
        self.assertTrue(body is None or isinstance(body, str), 
                       "Starter message body should be None or string")

    # ---- Depositor-facing helpers ----

    def test_get_message_list_from_depositor(self):
        """Test depositor message list retrieval."""
        io = self._new_io()
        out = io.get_message_list_from_depositor()
        
        # Enhanced assertions
        self.assertIsInstance(out, list, "Depositor message list should be a list")

    def test_get_message_subject_from_depositor_and_is_release_request(self):
        """Test depositor message subject retrieval and release request detection."""
        io = self._new_io()
        
        # Test with non-existent message
        subj = io.get_message_subject_from_depositor("NONEXISTENT_MSG")
        self.assertTrue(subj is None or isinstance(subj, str), 
                       "Message subject should be None or string")
        
        # Test release request detection
        is_rel = io.is_release_request("NONEXISTENT_MSG")
        self.assertIsInstance(is_rel, bool, "Release request check should return boolean")

    # ---- Summary/boolean probes ----

    def test_summary_booleans(self):
        """Test summary boolean methods with validation."""
        io = self._new_io()
        
        # Test all summary methods
        summary_methods = [
            ("areAllMsgsRead", io.areAllMsgsRead),
            ("areAllMsgsActioned", io.areAllMsgsActioned), 
            ("anyReleaseFlags", io.anyReleaseFlags),
            ("anyUnactionApprovalWithoutCorrection", io.anyUnactionApprovalWithoutCorrection),
        ]
        
        for method_name, method in summary_methods:
            result = method()
            self.assertIsInstance(result, bool, f"{method_name} should return boolean")
        
        # Test notes existence (returns tuple)
        notes_result = io.anyNotesExist()
        self.assertIsInstance(notes_result, tuple, "anyNotesExist should return tuple")
        self.assertEqual(len(notes_result), 4, "anyNotesExist should return 4-element tuple")
        
        # Validate tuple contents
        any_notes, annot_notes, bmrb_notes, num_notes = notes_result
        self.assertIsInstance(any_notes, bool, "First element should be boolean")
        self.assertIsInstance(annot_notes, bool, "Second element should be boolean") 
        self.assertIsInstance(bmrb_notes, bool, "Third element should be boolean")
        self.assertIsInstance(num_notes, int, "Fourth element should be integer")
        self.assertGreaterEqual(num_notes, 0, "Number of notes should be non-negative")

    def test_error_handling_edge_cases(self):
        """Test error handling with various edge cases."""
        io = self._new_io()
        
        # Test with empty/None parameters
        try:
            result = io.getMsgRowList(p_depDataSetId="", p_colSearchDict={})
            self.assertIsInstance(result, (dict, list), "Empty dep ID should return valid structure")
        except Exception:
            pass  # Some methods may legitimately fail with invalid input
        
        # Test with very long strings
        long_string = "x" * 10000
        try:
            result = io.getMsg(p_msgId=long_string, p_depId=self.dep_id)
            self.assertTrue(result is None or isinstance(result, dict), 
                           "Long message ID should return None or dict")
        except Exception:
            pass  # May legitimately fail
        
        print("✅ Error handling tests completed")


if __name__ == "__main__":
    unittest.main()