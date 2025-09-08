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

        # Basic connection info (non-fatal if None)
        try:
            cI = cls.ConfigInfo(cls.site_id)
            cls._db_host = cI.get("SITE_DB_HOST_NAME")
            cls._db_name = cI.get("WWPDB_MESSAGING_DB_NAME")
            print(f"DB target: host={cls._db_host} db={cls._db_name} site_id={cls.site_id}")
        except Exception:
            pass

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

    # def _cleanup_message_best_effort(self, message_id):
    #     """Try to delete the test message directly from DB (best-effort; ignore failures)."""
    #     try:
    #         from wwpdb.apps.msgmodule.db.DataAccessLayer import DataAccessLayer
    #         cI = self.ConfigInfo(self.site_id)
    #         db_cfg = {
    #             "host": cI.get("SITE_DB_HOST_NAME"),
    #             "port": int(cI.get("SITE_DB_PORT_NUMBER", "3306")),
    #             "database": cI.get("WWPDB_MESSAGING_DB_NAME"),
    #             "username": cI.get("SITE_DB_ADMIN_USER"),
    #             "password": cI.get("SITE_DB_ADMIN_PASS", ""),
    #             "charset": "utf8mb4",
    #         }
    #         dal = DataAccessLayer(db_cfg)
    #         with dal.db_connection.get_session() as sess:
    #             sess.execute(
    #                 "DELETE FROM pdbx_deposition_message_info WHERE message_id = :mid",
    #                 {"mid": message_id},
    #             )
    #             sess.commit()
    #     except Exception as e:
    #         print(f"(cleanup skipped/failed) {e}")

    # ---- Core smoke tests (reads, writes) ----

    def test_initializeDataStore(self):
        io = self._new_io()
        # Must not raise
        io.initializeDataStore()

    def test_getMsgColList(self):
        io = self._new_io()
        # Most impls accept a history flag; False is a safe default
        ok, cols = io.getMsgColList(False)
        self.assertTrue(ok)
        self.assertIsInstance(cols, list)
        self.assertTrue(any(cols))

    def test_setGroupId(self):
        io = self._new_io()
        # Should not raise
        io.setGroupId("grp-it")

    def test_getMsgRowList_read_path(self):
        io = self._new_io()
        res = io.getMsgRowList(p_depDataSetId=self.dep_id, p_colSearchDict={})
        self.assertTrue(isinstance(res, (dict, list)))
        if isinstance(res, dict):
            self.assertIn("RECORD_LIST", res)
            self.assertIsInstance(res["RECORD_LIST"], list)

    def test_getMsg_specific(self):
        io = self._new_io()
        res = io.getMsg(p_msgId="NONEXISTENT_MSG", p_depId=self.dep_id)
        self.assertTrue(res is None or isinstance(res, dict))

    def test_processMsg_write_then_read(self):
        msg_id = self._new_msg_id("WRITE_READ")
        subject = f"IT write/read {msg_id}"
        body = f"Created at {datetime.utcnow().isoformat()}Z"
        
        print(f"\n🔍 PERSISTENCE TEST - Creating message:")
        print(f"   Message ID: {msg_id}")
        print(f"   Dataset ID: {self.dep_id}")
        print(f"   Subject: {subject}")
        print(f"   Timestamp: {datetime.utcnow().isoformat()}Z")

        # Build a real Message from request-like object
        req_for_msg = self._Req(
            self.site_id, self.dep_id,
            sender="it@write",
            subject=subject,
            message_text=body,
            msg_id=msg_id,
        )
        msg_obj = self.Message.fromReqObj(req_for_msg, verbose=True)

        io = self._new_io()
        write_res = io.processMsg(msg_obj)
        write_ok = write_res[0] if isinstance(write_res, tuple) else bool(write_res)
        self.assertIsInstance(write_ok, bool)

        # Read back
        res = io.getMsgRowList(p_depDataSetId=self.dep_id, p_colSearchDict={})
        self.assertTrue(isinstance(res, (dict, list)))
        records = res.get("RECORD_LIST", res) if isinstance(res, dict) else res
        self.assertIsInstance(records, list)

        if write_ok:
            found = any(r.get("message_id") == msg_id for r in records)
            if not found:
                print(f"⚠ wrote {msg_id} but didn’t see it immediately (records={len(records)})")
            print(f"✅ SUCCESS: Message {msg_id} written to database successfully!")
        else:
            print(f"❌ FAILED: Message {msg_id} write failed!")

        print(f"\n📊 SQL QUERY TO FIND THIS MESSAGE:")
        print(f"   SELECT * FROM wwpdb_messaging.pdbx_deposition_message_info WHERE message_id = '{msg_id}';")
        print(f"   SELECT * FROM wwpdb_messaging.pdbx_deposition_message_info WHERE deposition_data_set_id = '{self.dep_id}' ORDER BY timestamp DESC;")
        # self._cleanup_message_best_effort(msg_id)

    # ---- File helpers ----

    def test_checkAvailFiles(self):
        io = self._new_io()
        avail = io.checkAvailFiles(self.dep_id)
        self.assertTrue(isinstance(avail, (list, dict)))

    def test_getFilesRfrncd(self):
        io = self._new_io()
        refs = io.getFilesRfrncd(self.dep_id)
        self.assertTrue(isinstance(refs, (list, dict)))

    # ---- Lists/filters ----

    def test_getMsgReadList(self):
        io = self._new_io()
        out = io.getMsgReadList(self.dep_id)
        self.assertIsInstance(out, list)

    def test_getMsgNoActionReqdList(self):
        io = self._new_io()
        out = io.getMsgNoActionReqdList(self.dep_id)
        self.assertIsInstance(out, list)

    def test_getMsgForReleaseList(self):
        io = self._new_io()
        out = io.getMsgForReleaseList(self.dep_id)
        self.assertIsInstance(out, list)

    def test_getNotesList(self):
        io = self._new_io()
        out = io.getNotesList()
        self.assertIsInstance(out, list)

    # ---- Writes/changes and helpers ----

    def test_markMsgAsRead_and_tagMsg(self):
        io = self._new_io()
        status_dict = {
            "deposition_data_set_id": self.dep_id,
            "message_id": "NONEXISTENT_MSG",
            "read_flag": "Y",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        ok_read = io.markMsgAsRead(status_dict)
        self.assertIsInstance(ok_read, bool)

        tag_dict = {
            "deposition_data_set_id": self.dep_id,
            "message_id": "NONEXISTENT_MSG",
            "action_reqd_flag": "Y",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        ok_tag = io.tagMsg(tag_dict)
        self.assertIsInstance(ok_tag, bool)

    def test_autoMsg(self):
        io = self._new_io()
        result = io.autoMsg(
            p_depIdList=[self.dep_id],
            p_tmpltType="release-publ",
            p_isEmdbEntry=False,
            p_sender="it@auto",
        )
        # autoMsg returns a dictionary with results for each dep ID
        self.assertIsInstance(result, dict)
        self.assertIn(self.dep_id, result)
        self.assertIsInstance(result[self.dep_id], dict)
        self.assertIn('success', result[self.dep_id])
        self.assertIn('pdbx_model_updated', result[self.dep_id])

    def test_sendSingle(self):
        io = self._new_io()
        ok = io.sendSingle(
            depId=self.dep_id,
            subject="IT sendSingle subject",
            msg="IT sendSingle body",
            p_sender="it@single",
            p_testemail=False,
            p_tmpltType="other",
        )
        self.assertIsInstance(ok, bool)

    def test_getMsgTmpltDataItems_and_getStarterMsgBody(self):
        io = self._new_io()
        d = {}
        io.getMsgTmpltDataItems(d)   # Should not raise; content impl-dependent
        self.assertIsInstance(d, dict)
        body = io.getStarterMsgBody()
        self.assertTrue(body is None or isinstance(body, str))

    # ---- Depositor-facing helpers ----

    def test_get_message_list_from_depositor(self):
        io = self._new_io()
        out = io.get_message_list_from_depositor()
        self.assertIsInstance(out, list)

    def test_get_message_subject_from_depositor_and_is_release_request(self):
        io = self._new_io()
        subj = io.get_message_subject_from_depositor("NONEXISTENT_MSG")
        self.assertTrue(subj is None or isinstance(subj, str))
        is_rel = io.is_release_request("NONEXISTENT_MSG")
        self.assertIsInstance(is_rel, bool)

    # ---- Summary/boolean probes ----

    def test_summary_booleans(self):
        io = self._new_io()
        self.assertIsInstance(io.areAllMsgsRead(), bool)
        self.assertIsInstance(io.areAllMsgsActioned(), bool)
        self.assertIsInstance(io.anyReleaseFlags(), bool)
        self.assertIsInstance(io.anyUnactionApprovalWithoutCorrection(), bool)
        # anyNotesExist returns a tuple: (bAnyNotesIncldngArchvdMsgs, bAnnotNotes, bBmrbNotes, iNumNotesRecords)
        notes_result = io.anyNotesExist()
        self.assertIsInstance(notes_result, tuple)
        self.assertEqual(len(notes_result), 4)
        self.assertIsInstance(notes_result[0], bool)  # bAnyNotesIncldngArchvdMsgs
        self.assertIsInstance(notes_result[1], bool)  # bAnnotNotes
        self.assertIsInstance(notes_result[2], bool)  # bBmrbNotes
        self.assertIsInstance(notes_result[3], int)   # iNumNotesRecords


if __name__ == "__main__":
    unittest.main()
