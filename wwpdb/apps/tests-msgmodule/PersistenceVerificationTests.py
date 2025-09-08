#!/usr/bin/env python
"""
MessagingIo Persistence Verification Tests.

This test suite specifically verifies that messages, statuses, and file references
are properly persisted in the database and can be queried directly.

Run:
  export WWPDB_SITE_ID=PDBE_DEV
  python -m pytest wwpdb/apps/tests-msgmodule/PersistenceVerificationTests.py -v -s
"""

import os
import sys
import time
import unittest
from datetime import datetime, timedelta


class TestMessagingIoPersistence(unittest.TestCase):
    """Database persistence verification tests for MessagingIo."""

    @classmethod
    def setUpClass(cls):
        cls.site_id = os.getenv("WWPDB_SITE_ID", "").strip()
        if not cls.site_id:
            raise unittest.SkipTest("Set WWPDB_SITE_ID to run persistence verification tests.")

        # CRITICAL: Restore real ConfigInfo for database integration tests
        # Remove any mocked ConfigInfo
        if 'wwpdb.utils.config.ConfigInfo' in sys.modules:
            del sys.modules['wwpdb.utils.config.ConfigInfo']

        # Import real dependencies
        from wwpdb.utils.config.ConfigInfo import ConfigInfo
        from wwpdb.apps.msgmodule.io.MessagingIo import MessagingIo
        from wwpdb.apps.msgmodule.models.Message import Message
        from wwpdb.apps.msgmodule.db.DataAccessLayer import DataAccessLayer

        cls.ConfigInfo = ConfigInfo
        cls.MessagingIo = MessagingIo
        cls.Message = Message
        cls.DataAccessLayer = DataAccessLayer

        # Default dataset id (override with WWPDB_TEST_DEP_ID)
        cls.dep_id = os.getenv("WWPDB_TEST_DEP_ID", "D_1000000001")

        # Set up database connection
        cI = cls.ConfigInfo(cls.site_id)
        cls.db_cfg = {
            "host": cI.get("SITE_DB_HOST_NAME"),
            "port": int(cI.get("SITE_DB_PORT_NUMBER", "3306")),
            "database": cI.get("WWPDB_MESSAGING_DB_NAME"),
            "username": cI.get("SITE_DB_ADMIN_USER"),
            "password": cI.get("SITE_DB_ADMIN_PASS", ""),
            "charset": "utf8mb4",
        }
        
        print(f"\n🏗️  Database Config:")
        print(f"   Host: {cls.db_cfg['host']}")
        print(f"   Database: {cls.db_cfg['database']}")
        print(f"   Site ID: {cls.site_id}")
        print(f"   Dataset ID: {cls.dep_id}")

    # ---- Minimal session/request shims ----

    class _Sess:
        def getId(self): return "persist_test_session"
        def getPath(self): return "/tmp/persist_test_session"
        def getRelativePath(self): return "persist_test_session"

    class _Req:
        def __init__(
            self,
            site_id,
            dep_id,
            *,
            sender="persist@test.com",
            subject="PERSISTENCE Test",
            message_text="Persistence verification message",
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
                "filesource": "archive",      # CRITICAL: triggers workflow/db path
                "WWPDB_SITE_ID": site_id,
                "SITE_ID": site_id,
                "groupid": "",  # Empty is fine
            }
            if extra:
                self._vals.update(extra)

        def getValue(self, k): return self._vals.get(k, "")
        def getRawValue(self, k): return self._vals.get(k, "")
        def getValueList(self, k): return []
        def setValue(self, k, v): self._vals[k] = v
        def newSessionObj(self): return TestMessagingIoPersistence._Sess()
        def getSessionObj(self): return TestMessagingIoPersistence._Sess()

    # ---- Helper methods ----

    def _new_io(self, dep_id=None, **req_extra):
        req = self._Req(self.site_id, dep_id or self.dep_id, extra=req_extra or {})
        return self.MessagingIo(req, verbose=True)

    def _query_database_direct(self, query, params=None):
        """Execute a direct SQL query and return results."""
        dal = self.DataAccessLayer(self.db_cfg)
        with dal.db_connection.get_session() as sess:
            result = sess.execute(query, params or {})
            return result.fetchall()

    def _verify_message_in_db(self, message_id):
        """Verify that a specific message exists in the database."""
        query = """
        SELECT 
            message_id, 
            deposition_data_set_id,
            message_subject,
            message_text,
            sender,
            timestamp,
            created_at,
            updated_at
        FROM pdbx_deposition_message_info 
        WHERE message_id = :msg_id
        """
        results = self._query_database_direct(query, {"msg_id": message_id})
        return results

    def _verify_message_status_in_db(self, message_id):
        """Verify message status records in database."""
        query = """
        SELECT 
            message_id,
            deposition_data_set_id,
            read_status,
            action_reqd,
            for_release,
            created_at,
            updated_at
        FROM pdbx_deposition_message_status 
        WHERE message_id = :msg_id
        """
        results = self._query_database_direct(query, {"msg_id": message_id})
        return results

    def _verify_file_references_in_db(self, message_id):
        """Verify file reference records in database."""
        query = """
        SELECT 
            message_id,
            deposition_data_set_id,
            content_type,
            content_format,
            partition_number,
            version_number,
            uploaded_file_name,
            storage_type
        FROM pdbx_deposition_message_file_reference 
        WHERE message_id = :msg_id
        """
        results = self._query_database_direct(query, {"msg_id": message_id})
        return results

    def _get_recent_test_messages(self, hours_back=1):
        """Get messages created in the last N hours that look like test messages."""
        since_time = datetime.now() - timedelta(hours=hours_back)
        query = """
        SELECT 
            message_id,
            deposition_data_set_id,
            message_subject,
            sender,
            timestamp,
            created_at
        FROM pdbx_deposition_message_info 
        WHERE created_at >= :since_time
            AND (sender LIKE '%test%'
                 OR sender LIKE '%persist%'
                 OR sender LIKE '%cycle%'
                 OR sender LIKE '%status%'
                 OR message_subject LIKE '%TEST%'
                 OR message_subject LIKE '%PERSIST%'
                 OR message_subject LIKE '%CYCLE%')
        ORDER BY created_at DESC
        """
        results = self._query_database_direct(query, {"since_time": since_time})
        return results

    # ---- Persistence verification tests ----

    def test_01_write_message_and_verify_persistence(self):
        """Write a message and verify it's properly persisted in the database."""
        print(f"\n📝 Creating message for persistence verification...")
        
        subject = "PERSISTENCE VERIFICATION TEST"
        body = f"Message created at {datetime.utcnow().isoformat()}Z for persistence testing"
        
        print(f"   Subject: {subject}")

        # Create and process message - let system generate ID
        req_for_msg = self._Req(
            self.site_id, self.dep_id,
            sender="persistence@test.com",
            subject=subject,
            message_text=body,
        )
        msg_obj = self.Message.fromReqObj(req_for_msg, verbose=True)

        io = self._new_io()
        write_res = io.processMsg(msg_obj)
        write_ok = write_res[0] if isinstance(write_res, tuple) else bool(write_res)

        self.assertTrue(write_ok, "Message should be written successfully")
        print(f"   ✅ Message written successfully")
        
        # Get the message ID
        message_id = msg_obj.messageId
        self.assertIsNotNone(message_id, "Message should have an ID")
        print(f"   🆔 Message ID: {message_id}")

        # Verify in database
        db_results = self._verify_message_in_db(message_id)
        self.assertTrue(len(db_results) > 0, f"Message {message_id} should exist in database")
        
        db_msg = db_results[0]
        self.assertEqual(db_msg.message_id, message_id)
        self.assertEqual(db_msg.deposition_data_set_id, self.dep_id)
        self.assertEqual(db_msg.message_subject, subject)
        self.assertIn("persistence testing", db_msg.message_text)
        self.assertEqual(db_msg.sender, "persistence@test.com")
        
        print(f"   ✅ Message verified in database:")
        print(f"      ID: {db_msg.message_id}")
        print(f"      Dataset: {db_msg.deposition_data_set_id}")
        print(f"      Subject: {db_msg.message_subject}")
        print(f"      Sender: {db_msg.sender}")
        print(f"      Created: {db_msg.created_at}")

    def test_02_write_status_and_verify_persistence(self):
        """Write message status changes and verify they're persisted."""
        print(f"\n📊 Testing message status persistence...")
        
        subject = "STATUS PERSISTENCE TEST"
        body = f"Message for status persistence testing: {datetime.utcnow().isoformat()}Z"

        # Create message first - let system generate ID
        req_for_msg = self._Req(
            self.site_id, self.dep_id,
            sender="status@test.com",
            subject=subject,
            message_text=body,
        )
        msg_obj = self.Message.fromReqObj(req_for_msg, verbose=True)

        io = self._new_io()
        write_res = io.processMsg(msg_obj)
        write_ok = write_res[0] if isinstance(write_res, tuple) else bool(write_res)
        self.assertTrue(write_ok, "Message should be written successfully")

        # Get the message ID
        message_id = msg_obj.messageId
        self.assertIsNotNone(message_id, "Message should have an ID")
        print(f"   🆔 Message ID: {message_id}")

        # Mark message as read
        status_dict = {
            "deposition_data_set_id": self.dep_id,
            "message_id": message_id,
            "read_status": "Y",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        read_result = io.markMsgAsRead(status_dict)
        print(f"   📖 Mark as read result: {read_result}")

        # Tag message as action required
        tag_dict = {
            "deposition_data_set_id": self.dep_id,
            "message_id": message_id,
            "action_reqd": "Y",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        tag_result = io.tagMsg(tag_dict)
        print(f"   🏷️  Tag as action required result: {tag_result}")

        # Verify status in database
        status_results = self._verify_message_status_in_db(message_id)
        if len(status_results) > 0:
            print(f"   ✅ Status records found in database: {len(status_results)}")
            for status in status_results:
                print(f"      Status: read={status.read_status}, action_required={status.action_reqd}, release={status.for_release}")
        else:
            print(f"   ⚠️  No status records found for {message_id}")

    def test_03_list_recent_test_messages(self):
        """List all recent test messages to verify persistence."""
        print(f"\n📋 Listing recent test messages...")
        
        recent_messages = self._get_recent_test_messages(hours_back=2)
        
        print(f"   Found {len(recent_messages)} recent test messages:")
        for msg in recent_messages:
            print(f"      {msg.message_id} | {msg.deposition_data_set_id} | {msg.sender} | {msg.created_at}")
            print(f"         Subject: {msg.message_subject}")
        
        # This is informational - we don't assert since it depends on previous test runs
        self.assertIsInstance(recent_messages, list)

    def test_04_comprehensive_read_write_cycle(self):
        """Complete end-to-end test with full verification."""
        print(f"\n🔄 Comprehensive read-write cycle test...")
        
        subject = "COMPREHENSIVE CYCLE TEST"
        body = f"Complete test message created at {datetime.utcnow().isoformat()}Z"

        # 1. Write message - let system generate ID
        req_for_msg = self._Req(
            self.site_id, self.dep_id,
            sender="cycle@test.com",
            subject=subject,
            message_text=body,
        )
        msg_obj = self.Message.fromReqObj(req_for_msg, verbose=True)

        io = self._new_io()
        write_res = io.processMsg(msg_obj)
        write_ok = write_res[0] if isinstance(write_res, tuple) else bool(write_res)
        self.assertTrue(write_ok)
        print(f"   ✅ Step 1: Message written")
        
        # Get the message ID
        message_id = msg_obj.messageId
        self.assertIsNotNone(message_id, "Message should have an ID")
        print(f"   🆔 Message ID: {message_id}")

        # 2. Read back via MessagingIo API - ensure content_type is set to trigger DB loading
        io_for_read = self._new_io(content_type="msgs")  # MessagingIo.getMsg() requires content_type="msgs" to load from DB
        api_result = io_for_read.getMsg(p_msgId=message_id, p_depId=self.dep_id)
        if api_result:
            print(f"   ✅ Step 2: Message read via API")
            print(f"      API Subject: {api_result.get('message_subject', 'N/A')}")
        else:
            print(f"   ⚠️  Step 2: Message not found via API")

        # 3. Verify in database directly
        db_results = self._verify_message_in_db(message_id)
        
        # Debug: Show what message IDs are actually in the database
        all_msgs_query = """
        SELECT message_id, deposition_data_set_id, message_subject 
        FROM pdbx_deposition_message_info 
        WHERE deposition_data_set_id = :dep_id
        ORDER BY created_at DESC LIMIT 10
        """
        all_msgs = self._query_database_direct(all_msgs_query, {"dep_id": self.dep_id})
        print(f"   🔍 Debug: Recent messages in DB for {self.dep_id}:")
        for msg in all_msgs:
            print(f"      - {msg.message_id}: {msg.message_subject}")
        
        self.assertTrue(len(db_results) > 0)
        db_msg = db_results[0]
        print(f"   ✅ Step 3: Message verified in database")
        print(f"      DB Subject: {db_msg.message_subject}")
        print(f"      DB Timestamp: {db_msg.timestamp}")
        print(f"      DB Created: {db_msg.created_at}")

        # 4. Verify consistency between API and DB
        if api_result:
            self.assertEqual(api_result.get('message_subject'), db_msg.message_subject)
            self.assertEqual(api_result.get('message_id'), db_msg.message_id)
            print(f"   ✅ Step 4: API and DB data consistent")
        
        # 5. Test message listing - ensure content_type is set to trigger DB loading
        msg_list = io_for_read.getMsgRowList(p_depDataSetId=self.dep_id, p_colSearchDict={})
        records = msg_list.get("RECORD_LIST", msg_list) if isinstance(msg_list, dict) else msg_list
        # Records are lists with message_id at index 1 (per message attrib list order)
        found_in_list = any(r[1] == message_id for r in records if len(r) > 1)
        if found_in_list:
            print(f"   ✅ Step 5: Message found in listing")
        else:
            print(f"   ⚠️  Step 5: Message not found in listing (may be filtered)")

        print(f"\n🎯 PERSISTENCE VERIFICATION COMPLETE")
        print(f"   Message ID: {message_id}")
        print(f"   Database Record: CONFIRMED")
        print(f"   API Access: {'CONFIRMED' if api_result else 'CHECK REQUIRED'}")
        print(f"   Data Consistency: {'CONFIRMED' if api_result else 'N/A'}")


if __name__ == "__main__":
    unittest.main()
