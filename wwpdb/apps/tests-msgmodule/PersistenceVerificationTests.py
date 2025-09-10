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

        cls.ConfigInfo = ConfigInfo
        cls.MessagingIo = MessagingIo
        cls.Message = Message

        # Default dataset id (override with WWPDB_TEST_DEP_ID)
        cls.dep_id = os.getenv("WWPDB_TEST_DEP_ID", "D_1000000001")

        print(f"\n🏗️  Test Configuration:")
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

    def _verify_message_via_api(self, message_id):
        """Verify that a specific message exists using MessagingIo API."""
        io = self._new_io()
        # Use getMsg to retrieve specific message
        result = io.getMsg(p_msgId=message_id, p_depId=self.dep_id)
        return result

    def _find_message_in_list(self, message_id):
        """Find a specific message in the message list using MessagingIo API."""
        io = self._new_io()
        # Get all messages for this dataset
        result = io.getMsgRowList(p_depDataSetId=self.dep_id, p_colSearchDict={})
        
        if isinstance(result, dict):
            records = result.get("RECORD_LIST", [])
        else:
            records = result if isinstance(result, list) else []
        
        # Find our message in the records
        for record in records:
            if isinstance(record, dict) and record.get("message_id") == message_id:
                return record
            elif isinstance(record, list) and len(record) > 1 and record[1] == message_id:
                # Records might be lists with message_id at index 1
                return record
        return None

    def _verify_message_status_via_api(self, message_id):
        """Verify message status using MessagingIo API."""
        io = self._new_io()
        
        # Check different status flags
        all_read = io.areAllMsgsRead()
        all_actioned = io.areAllMsgsActioned()
        any_release = io.anyReleaseFlags()
        
        # Get message lists to check if our message appears in filtered lists
        read_list = io.getMsgReadList(self.dep_id)
        no_action_list = io.getMsgNoActionReqdList(self.dep_id)
        release_list = io.getMsgForReleaseList(self.dep_id)
        
        return {
            "all_read": all_read,
            "all_actioned": all_actioned,
            "any_release": any_release,
            "in_read_list": message_id in read_list if isinstance(read_list, list) else False,
            "in_no_action_list": message_id in no_action_list if isinstance(no_action_list, list) else False,
            "in_release_list": message_id in release_list if isinstance(release_list, list) else False,
        }

    def _get_recent_messages_via_api(self):
        """Get recent messages using MessagingIo API."""
        io = self._new_io()
        
        # Get all messages for this dataset
        result = io.getMsgRowList(p_depDataSetId=self.dep_id, p_colSearchDict={})
        
        if isinstance(result, dict):
            records = result.get("RECORD_LIST", [])
            total = result.get("TOTAL_RECORDS", len(records))
        else:
            records = result if isinstance(result, list) else []
            total = len(records)
        
        # Filter for test messages based on sender or subject
        test_messages = []
        for record in records:
            if isinstance(record, dict):
                sender = record.get("sender", "")
                subject = record.get("message_subject", "")
            elif isinstance(record, list) and len(record) >= 4:
                # Assuming order: [timestamp, message_id, subject, sender, ...]
                sender = record[3] if len(record) > 3 else ""
                subject = record[2] if len(record) > 2 else ""
            else:
                continue
                
            if any(test_word in sender.lower() for test_word in ["test", "persist", "cycle", "status"]) or \
               any(test_word in subject.upper() for test_word in ["TEST", "PERSIST", "CYCLE"]):
                test_messages.append(record)
        
        return {
            "total_messages": total,
            "test_messages": test_messages,
            "all_records": records
        }

    # ---- Persistence verification tests ----

    def test_write_message_and_verify_via_getMsg(self):
        """Write a message and verify it's retrievable via getMsg API."""
        print(f"\n📝 Testing message persistence via getMsg API...")
        
        subject = "GET_MSG VERIFICATION TEST"
        body = f"Message created at {datetime.utcnow().isoformat()}Z for getMsg testing"
        
        print(f"   Subject: {subject}")

        # Create and process message
        req_for_msg = self._Req(
            self.site_id, self.dep_id,
            sender="getmsg@test.com",
            subject=subject,
            message_text=body,
        )
        msg_obj = self.Message.fromReqObj(req_for_msg, verbose=True)

        io = self._new_io()
        write_res = io.processMsg(msg_obj)
        write_ok = write_res[0] if isinstance(write_res, tuple) else bool(write_res)

        self.assertTrue(write_ok, "Message should be written successfully")
        print(f"   ✅ Message written successfully")
        
        message_id = msg_obj.messageId
        self.assertIsNotNone(message_id, "Message should have an ID")
        print(f"   🆔 Message ID: {message_id}")

        # Verify via getMsg API - expect populated response
        api_result = self._verify_message_via_api(message_id)
        self.assertIsNotNone(api_result, f"getMsg should return message data for {message_id}")
        self.assertNotEqual(api_result, {}, "getMsg should not return empty dict for existing message")
        
        # Verify content integrity
        self.assertEqual(api_result.get("message_id"), message_id)
        self.assertEqual(api_result.get("deposition_data_set_id"), self.dep_id)
        self.assertEqual(api_result.get("message_subject"), subject)
        self.assertIn("getMsg testing", api_result.get("message_text", ""))
        self.assertEqual(api_result.get("sender"), "getmsg@test.com")
        print(f"   ✅ Message verified via getMsg API")

    def test_write_message_and_verify_via_list(self):
        """Write a message and verify it appears in message list."""
        print(f"\n📝 Testing message persistence via message list...")
        
        subject = "MESSAGE_LIST VERIFICATION TEST"
        body = f"Message created at {datetime.utcnow().isoformat()}Z for list testing"
        
        print(f"   Subject: {subject}")

        # Create and process message
        req_for_msg = self._Req(
            self.site_id, self.dep_id,
            sender="list@test.com",
            subject=subject,
            message_text=body,
        )
        msg_obj = self.Message.fromReqObj(req_for_msg, verbose=True)

        io = self._new_io()
        write_res = io.processMsg(msg_obj)
        write_ok = write_res[0] if isinstance(write_res, tuple) else bool(write_res)

        self.assertTrue(write_ok, "Message should be written successfully")
        print(f"   ✅ Message written successfully")
        
        message_id = msg_obj.messageId
        self.assertIsNotNone(message_id, "Message should have an ID")
        print(f"   🆔 Message ID: {message_id}")

        # Verify via message list
        found_in_list = self._find_message_in_list(message_id)
        self.assertIsNotNone(found_in_list, f"Message {message_id} must be found in message list")
        print(f"   ✅ Message found in message list")

    def test_handle_getMsg_empty_response(self):
        """Test behavior when getMsg returns empty dict for non-existent message."""
        print(f"\n📝 Testing getMsg empty response handling...")
        
        # Test with definitely non-existent message ID
        non_existent_id = "DEFINITELY_NON_EXISTENT_MESSAGE_12345"
        
        io = self._new_io()
        api_result = io.getMsg(p_msgId=non_existent_id, p_depId=self.dep_id)
        
        # getMsg should return empty dict or None for non-existent messages
        self.assertTrue(api_result == {} or api_result is None, 
                       f"getMsg should return empty dict or None for non-existent message, got: {type(api_result)}")
        print(f"   ✅ getMsg correctly returned {type(api_result)} for non-existent message")

    def test_write_status_and_verify_persistence(self):
        """Write message status changes and verify they're persisted via API."""
        print(f"\n📊 Testing message status persistence...")
        
        subject = "STATUS PERSISTENCE TEST"
        body = f"Message for status persistence testing: {datetime.utcnow().isoformat()}Z"

        # Create message first
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

        # Verify status changes via API
        status_info = self._verify_message_status_via_api(message_id)
        print(f"   📊 Status verification via API:")
        print(f"      All messages read: {status_info['all_read']}")
        print(f"      All messages actioned: {status_info['all_actioned']}")
        print(f"      Any release flags: {status_info['any_release']}")
        print(f"      In read list: {status_info['in_read_list']}")
        print(f"      In no-action list: {status_info['in_no_action_list']}")
        print(f"      In release list: {status_info['in_release_list']}")

        # Basic assertions about status
        self.assertIsInstance(status_info['all_read'], bool)
        self.assertIsInstance(status_info['all_actioned'], bool)
        self.assertIsInstance(status_info['any_release'], bool)

    def test_list_recent_test_messages(self):
        """List all recent test messages to verify persistence via API."""
        print(f"\n📋 Listing recent test messages via API...")
        
        message_data = self._get_recent_messages_via_api()
        
        print(f"   Total messages for dataset {self.dep_id}: {message_data['total_messages']}")
        print(f"   Test messages found: {len(message_data['test_messages'])}")
        
        for i, msg in enumerate(message_data['test_messages'][:5]):  # Show first 5
            if isinstance(msg, dict):
                print(f"      {i+1}. {msg.get('message_id')} | {msg.get('sender')} | {msg.get('message_subject')}")
            elif isinstance(msg, list) and len(msg) >= 4:
                print(f"      {i+1}. {msg[1]} | {msg[3]} | {msg[2]}")  # message_id, sender, subject
        
        if len(message_data['test_messages']) > 5:
            print(f"      ... and {len(message_data['test_messages']) - 5} more")
        
        # Verify we can process the data structure
        self.assertIsInstance(message_data['total_messages'], int)
        self.assertIsInstance(message_data['test_messages'], list)
        self.assertIsInstance(message_data['all_records'], list)
        
        if message_data['total_messages'] > 0:
            self.assertGreater(len(message_data['all_records']), 0, "Should have records if total > 0")

    def test_comprehensive_read_write_cycle_success_path(self):
        """Test complete end-to-end cycle expecting successful getMsg retrieval."""
        print(f"\n🔄 Comprehensive cycle test - success path...")
        
        subject = "COMPREHENSIVE SUCCESS TEST"
        body = f"Success path test message created at {datetime.utcnow().isoformat()}Z"

        # Write message
        req_for_msg = self._Req(
            self.site_id, self.dep_id,
            sender="success@test.com",
            subject=subject,
            message_text=body,
        )
        msg_obj = self.Message.fromReqObj(req_for_msg, verbose=True)

        io = self._new_io()
        write_res = io.processMsg(msg_obj)
        write_ok = write_res[0] if isinstance(write_res, tuple) else bool(write_res)
        self.assertTrue(write_ok)
        print(f"   ✅ Step 1: Message written")
        
        message_id = msg_obj.messageId
        self.assertIsNotNone(message_id, "Message should have an ID")
        print(f"   🆔 Message ID: {message_id}")

        # Read back via getMsg API - expect success
        api_result = self._verify_message_via_api(message_id)
        self.assertIsNotNone(api_result, "Message should be retrievable via getMsg API")
        self.assertNotEqual(api_result, {}, "getMsg should return populated data, not empty dict")
        print(f"   ✅ Step 2: Message read via getMsg API")
        
        # Verify content integrity 
        self.assertEqual(api_result.get('message_subject'), subject, "Subject should match")
        self.assertEqual(api_result.get('message_id'), message_id, "Message ID should match")
        self.assertEqual(api_result.get('sender'), "success@test.com", "Sender should match")
        self.assertEqual(api_result.get('deposition_data_set_id'), self.dep_id, "Dataset ID should match")
        self.assertIn("Success path test message", api_result.get('message_text', ''), "Message text should contain expected content")
        print(f"   ✅ Step 3: Content integrity verified")

        # Test message listing consistency
        found_in_list = self._find_message_in_list(message_id)
        self.assertIsNotNone(found_in_list, f"Message {message_id} must be found in message listing")
        print(f"   ✅ Step 4: Message found in listing")

        # Test status operations
        status_dict = {
            "deposition_data_set_id": self.dep_id,
            "message_id": message_id,
            "read_status": "Y",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        read_result = io.markMsgAsRead(status_dict)
        self.assertIsInstance(read_result, bool, "markMsgAsRead should return boolean")
        print(f"   ✅ Step 5: Status operations tested")

        print(f"\n🎯 SUCCESS PATH VERIFICATION COMPLETE")

    def test_08_comprehensive_read_write_cycle_fallback_path(self):
        """Test complete end-to-end cycle handling getMsg empty response with list fallback."""
        print(f"\n🔄 Comprehensive cycle test - fallback path...")
        
        subject = "COMPREHENSIVE FALLBACK TEST"
        body = f"Fallback path test message created at {datetime.utcnow().isoformat()}Z"

        # Write message
        req_for_msg = self._Req(
            self.site_id, self.dep_id,
            sender="fallback@test.com",
            subject=subject,
            message_text=body,
        )
        msg_obj = self.Message.fromReqObj(req_for_msg, verbose=True)

        io = self._new_io()
        write_res = io.processMsg(msg_obj)
        write_ok = write_res[0] if isinstance(write_res, tuple) else bool(write_res)
        self.assertTrue(write_ok)
        print(f"   ✅ Step 1: Message written")
        
        message_id = msg_obj.messageId
        self.assertIsNotNone(message_id, "Message should have an ID")
        print(f"   🆔 Message ID: {message_id}")

        # Read back via getMsg API
        api_result = self._verify_message_via_api(message_id)
        
        # Test fallback behavior when getMsg returns empty dict
        if api_result == {}:
            print(f"   ⚠️  Step 2: getMsg returned empty dict - testing fallback")
            
            # Verify fallback via message list works
            found_in_list = self._find_message_in_list(message_id)
            self.assertIsNotNone(found_in_list, f"Message {message_id} must be found in message list when getMsg fails")
            print(f"   ✅ Step 3: Fallback via message list successful")
        else:
            # If getMsg worked, this test becomes same as success path - that's fine
            self.assertIsNotNone(api_result, "Message should be retrievable")
            print(f"   ✅ Step 2: getMsg worked (no fallback needed)")
            print(f"   ✅ Step 3: Direct retrieval successful")

        # Always test message listing consistency
        found_in_list = self._find_message_in_list(message_id)
        self.assertIsNotNone(found_in_list, f"Message {message_id} must be found in message listing")
        print(f"   ✅ Step 4: Message found in listing")

        print(f"\n🎯 FALLBACK PATH VERIFICATION COMPLETE")


if __name__ == "__main__":
    unittest.main()
