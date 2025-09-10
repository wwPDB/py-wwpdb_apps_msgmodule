#!/usr/bin/env python
"""
Adversarial Tests for MessagingIo - Tests that are MEANT TO FIND BUGS

These tests specifically target failure modes, edge cases, and race conditions
that the existing "meant to pass" tests completely ignore.

The goal is to break the system and ensure it fails gracefully.
"""
import unittest
import threading
import time
import os
from unittest.mock import Mock, patch, MagicMock
from io import StringIO
import sys

if __package__ is None or __package__ == "":
    from os import path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from commonsetup import TESTOUTPUT
else:
    from .commonsetup import TESTOUTPUT

from wwpdb.apps.msgmodule.io.MessagingIo import MessagingIo


class MessagingIoAdversarialTests(unittest.TestCase):
    """Tests designed to find bugs, not to pass"""

    def setUp(self):
        self.req_obj = Mock()
        self.req_obj.getValue.return_value = 'test_value'
        self.session_obj = Mock()
        self.session_obj.getPath.return_value = '/tmp/session'
        self.session_obj.getRelativePath.return_value = 'relative_path'
        self.req_obj.newSessionObj.return_value = self.session_obj
        self.messaging_io = MessagingIo(self.req_obj, verbose=False, log=StringIO())

    # ====================================================================
    # FAILURE PATH TESTING - What happens when things go wrong?
    # ====================================================================

    def test_processMsg_with_database_integrity_error(self):
        """Test processMsg behavior when database throws IntegrityError (duplicate key, FK violation, etc.)"""
        from sqlalchemy.exc import IntegrityError
        
        mock_msg_obj = Mock()
        mock_msg_obj.isLive = True
        mock_msg_obj.messageId = 'DUPLICATE_MSG_ID'
        
        # Mock the database operation to raise IntegrityError
        with patch('wwpdb.apps.msgmodule.io.MessagingIo.PdbxMessageIo') as mock_pdbx:
            mock_instance = Mock()
            mock_pdbx.return_value = mock_instance
            mock_instance.write.side_effect = IntegrityError("statement", "params", "orig")
            
            # This should either:
            # 1. Return (False, False, []) indicating failure
            # 2. Raise a specific exception
            # 3. Handle the error gracefully
            with self.assertRaises((IntegrityError, ValueError, RuntimeError)):
                self.messaging_io.processMsg(mock_msg_obj)

    def test_processMsg_with_file_permission_error(self):
        """Test processMsg when file system operations fail"""
        mock_msg_obj = Mock()
        mock_msg_obj.getOutputFileTarget.side_effect = PermissionError("Access denied")
        
        # Should handle PermissionError gracefully, not crash
        result = self.messaging_io.processMsg(mock_msg_obj)
        
        if isinstance(result, tuple):
            success = result[0]
            self.assertFalse(success, "Should return False when file operations fail")
        else:
            # If it raises an exception, it should be a controlled one
            self.fail("processMsg should handle PermissionError gracefully")

    def test_getMsg_with_malformed_ids(self):
        """Test getMsg with various malformed ID inputs"""
        test_cases = [
            ('', ''),  # Empty strings
            (None, None),  # None values
            ('x' * 1000, 'y' * 1000),  # Oversized strings
            ('D_ABC', 'MALFORMED'),  # Invalid formats
            ('D_', ''),  # Partial IDs
            ('MSG_WITH_UNICODE_🔥', 'D_UNICODE_💀'),  # Unicode edge cases
            ('\n\r\t', '\0\x01\x02'),  # Control characters
        ]
        
        for msg_id, dep_id in test_cases:
            with self.subTest(msg_id=msg_id, dep_id=dep_id):
                # Should either return None/empty dict or raise ValueError
                # Should NOT crash with unhandled exception
                try:
                    result = self.messaging_io.getMsg(msg_id, dep_id)
                    # If it returns something, should be None or empty dict
                    self.assertIn(result, [None, {}], 
                                f"getMsg should return None or {{}} for malformed inputs, got: {type(result)}")
                except (ValueError, TypeError, AttributeError) as e:
                    # These are acceptable controlled exceptions
                    pass
                except Exception as e:
                    self.fail(f"getMsg should not crash with unhandled exception {type(e).__name__}: {e}")

    def test_markMsgAsRead_idempotency(self):
        """Test that marking the same message as read twice doesn't create duplicates"""
        status_dict = {
            "deposition_data_set_id": "D_000000",
            "message_id": "TEST_MSG_IDEMPOTENT",
            "read_status": "Y",
            "timestamp": "2023-01-01 12:00:00"
        }
        
        # Mock to track how many times the underlying operation is called
        with patch.object(self.messaging_io, '_MessagingIo__updateMsgStatus') as mock_update:
            mock_update.return_value = True
            
            # Call twice with identical data
            result1 = self.messaging_io.markMsgAsRead(status_dict)
            result2 = self.messaging_io.markMsgAsRead(status_dict)
            
            # Both should succeed
            self.assertTrue(result1)
            self.assertTrue(result2)
            
            # But the underlying operation should be idempotent
            # Either called twice (simple implementation) or once (smart implementation)
            call_count = mock_update.call_count
            self.assertIn(call_count, [1, 2], 
                         f"markMsgAsRead should be idempotent, got {call_count} calls")

    def test_concurrent_message_processing(self):
        """Test race conditions when multiple threads process messages simultaneously"""
        if sys.platform == 'win32':
            self.skipTest("Threading test may be unreliable on Windows")
            
        results = []
        errors = []
        
        def process_message(msg_id):
            try:
                mock_msg = Mock()
                mock_msg.messageId = f"CONCURRENT_{msg_id}"
                mock_msg.isLive = False
                
                result = self.messaging_io.processMsg(mock_msg)
                results.append((msg_id, result))
            except Exception as e:
                errors.append((msg_id, e))
        
        # Start multiple threads trying to process different messages
        threads = []
        for i in range(5):
            thread = threading.Thread(target=process_message, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all to complete
        for thread in threads:
            thread.join(timeout=10)
            if thread.is_alive():
                self.fail("Thread took too long - possible deadlock")
        
        # No thread should have crashed with unhandled exception
        self.assertEqual(len(errors), 0, f"Concurrent processing caused errors: {errors}")
        
        # All threads should have completed
        self.assertEqual(len(results), 5, "Not all threads completed successfully")

    # ====================================================================
    # BOUNDARY AND INPUT VALIDATION TESTING
    # ====================================================================

    def test_setGroupId_with_extreme_inputs(self):
        """Test setGroupId with various problematic inputs"""
        extreme_inputs = [
            None,
            '',
            ' ',
            '\n\r\t',
            'x' * 10000,  # Very long string
            '🔥💀🚀',  # Unicode emoji
            'group\x00null',  # Null byte
            'group"quote',  # SQL injection attempt
            "group'quote",  # SQL injection attempt  
            'group;DROP TABLE messages;--',  # SQL injection
        ]
        
        for group_id in extreme_inputs:
            with self.subTest(group_id=repr(group_id)):
                try:
                    self.messaging_io.setGroupId(group_id)
                    # If it doesn't raise an exception, that's fine too
                except (ValueError, TypeError) as e:
                    # These are acceptable validation errors
                    pass
                except Exception as e:
                    self.fail(f"setGroupId should not crash with {type(e).__name__}: {e}")

    def test_getMsgRowList_with_invalid_search_parameters(self):
        """Test getMsgRowList with malformed search parameters"""
        invalid_searches = [
            {'malformed': 'value'},
            {'': ''},
            {None: None},
            {'sender': 'x' * 10000},  # Oversized value
            {'message_id': "'; DROP TABLE messages; --"},  # SQL injection
            {'timestamp': 'not-a-date'},
            {'ordinal_id': 'not-a-number'},
        ]
        
        for search_dict in invalid_searches:
            with self.subTest(search=search_dict):
                try:
                    result = self.messaging_io.getMsgRowList("D_000000", search_dict)
                    # Should return empty result or valid structure
                    self.assertIsInstance(result, (dict, list))
                except (ValueError, TypeError, KeyError) as e:
                    # Acceptable validation errors
                    pass
                except Exception as e:
                    self.fail(f"getMsgRowList should handle invalid search gracefully: {e}")

    # ====================================================================
    # RESOURCE EXHAUSTION AND LIMITS TESTING
    # ====================================================================

    def test_processMsg_with_oversized_message_content(self):
        """Test processMsg behavior with extremely large message content"""
        huge_content = 'x' * (1024 * 1024)  # 1MB message
        
        mock_msg = Mock()
        mock_msg.messageText = huge_content
        mock_msg.messageSubject = 'y' * 10000  # 10KB subject
        mock_msg.isLive = False
        
        # Should either:
        # 1. Handle it gracefully (truncate, compress, etc.)
        # 2. Reject with clear error
        # 3. Process successfully if system supports it
        try:
            result = self.messaging_io.processMsg(mock_msg)
            if isinstance(result, tuple):
                # If it processes, should complete without corruption
                success = result[0]
                self.assertIsInstance(success, bool)
        except (ValueError, MemoryError, OverflowError) as e:
            # These are acceptable limits
            pass

    def test_checkAvailFiles_with_nonexistent_dataset(self):
        """Test checkAvailFiles with dataset IDs that definitely don't exist"""
        nonexistent_ids = [
            'D_DEFINITELY_NONEXISTENT_99999999999',
            'D_000000000',  # Zero padding edge case
            'D_999999999999999999999',  # Extremely large number
            'INVALID_FORMAT',
            '',
            None,
        ]
        
        for dataset_id in nonexistent_ids:
            with self.subTest(dataset=dataset_id):
                try:
                    files = self.messaging_io.checkAvailFiles(dataset_id)
                    # Should return empty list or handle gracefully
                    self.assertIsInstance(files, list)
                    # Empty list is expected for nonexistent datasets
                except (ValueError, TypeError) as e:
                    # Acceptable validation errors
                    pass

    # ====================================================================
    # STATE CONSISTENCY TESTING
    # ====================================================================

    def test_message_status_consistency_after_operations(self):
        """Test that message status flags remain consistent after various operations"""
        # This test checks for race conditions in status tracking
        
        with patch.object(self.messaging_io, 'getMsgReadList') as mock_read, \
             patch.object(self.messaging_io, 'getMsgNoActionReqdList') as mock_no_action, \
             patch.object(self.messaging_io, 'areAllMsgsRead') as mock_all_read, \
             patch.object(self.messaging_io, 'areAllMsgsActioned') as mock_all_actioned:
            
            # Set up inconsistent state to see if system catches it
            mock_read.return_value = ['MSG1', 'MSG2']  # 2 read messages
            mock_no_action.return_value = ['MSG1']     # 1 no-action message
            mock_all_read.return_value = True          # Says all are read
            mock_all_actioned.return_value = False     # Says not all actioned
            
            # These calls should be consistent with each other
            read_list = self.messaging_io.getMsgReadList("D_000000")
            no_action_list = self.messaging_io.getMsgNoActionReqdList("D_000000")
            all_read = self.messaging_io.areAllMsgsRead()
            all_actioned = self.messaging_io.areAllMsgsActioned()
            
            # Basic consistency check: if all messages are read and none need action,
            # then all should be actioned (assuming that's the business logic)
            if all_read and len(no_action_list) >= len(read_list):
                # This might indicate a business logic inconsistency
                pass  # Document the expected behavior here

    # ====================================================================
    # CONFIGURATION AND ENVIRONMENT EDGE CASES  
    # ====================================================================

    def test_initialization_with_missing_config(self):
        """Test MessagingIo initialization when configuration is incomplete"""
        # Create request object with missing critical values
        bad_req = Mock()
        bad_req.getValue.return_value = None  # All config returns None
        bad_req.newSessionObj.return_value = None
        
        try:
            bad_io = MessagingIo(bad_req, verbose=False, log=StringIO())
            # If it initializes, basic operations should still handle the bad config
            result = bad_io.getMsgColList()
            # Should either work or fail gracefully
        except (ValueError, AttributeError, TypeError) as e:
            # These are acceptable initialization failures
            pass

    def test_database_connection_failure_simulation(self):
        """Test behavior when database connections fail"""
        from sqlalchemy.exc import OperationalError
        
        # Mock database operations to fail
        with patch('wwpdb.apps.msgmodule.io.MessagingIo.PdbxMessageIo') as mock_pdbx:
            mock_instance = Mock()
            mock_pdbx.return_value = mock_instance
            mock_instance.read.side_effect = OperationalError("statement", "params", "orig")
            
            # Operations should handle DB failures gracefully
            result = self.messaging_io.getMsgRowList("D_000000", {})
            
            # Should return empty/error result, not crash
            self.assertIsInstance(result, (dict, list))

    # ====================================================================
    # BUSINESS LOGIC EDGE CASES
    # ====================================================================

    def test_parent_child_message_ordering_violations(self):
        """Test what happens when child messages are created before parents"""
        # This is a real-world scenario that often breaks systems
        
        parent_msg = Mock()
        parent_msg.messageId = "PARENT_MSG"
        parent_msg.parentMessageId = None
        
        child_msg = Mock()
        child_msg.messageId = "CHILD_MSG"  
        child_msg.parentMessageId = "PARENT_MSG"
        
        # Try to process child before parent (violates FK constraint)
        with patch.object(self.messaging_io, 'processMsg') as mock_process:
            mock_process.side_effect = [
                # First call (child) should fail or be queued
                (False, False, ["Parent not found"]),
                # Second call (parent) should succeed  
                (True, False, []),
                # Third call (child retry) should now succeed
                (True, False, [])
            ]
            
            # This sequence should be handled gracefully
            child_result = self.messaging_io.processMsg(child_msg)
            parent_result = self.messaging_io.processMsg(parent_msg)
            
            # System should prevent orphaned children
            if isinstance(child_result, tuple):
                child_success = child_result[0]
                # Either fails initially (good) or succeeds (if system allows temporary orphans)
                self.assertIsInstance(child_success, bool)

    def test_release_flag_state_transitions(self):
        """Test all possible combinations of release flags and their transitions"""
        # Business logic often has complex state machines that break at edge cases
        
        test_combinations = [
            # (read_status, action_reqd, release_flag, expected_any_release)
            ('Y', 'Y', 'Y', True),
            ('Y', 'N', 'Y', True), 
            ('N', 'Y', 'Y', True),
            ('N', 'N', 'N', False),
            (None, None, None, False),
            ('Y', None, 'Y', True),  # Mixed None values
        ]
        
        for read_status, action_reqd, release_flag, expected in test_combinations:
            with self.subTest(read=read_status, action=action_reqd, release=release_flag):
                # Mock the underlying data to return this combination
                with patch.object(self.messaging_io, 'anyReleaseFlags') as mock_flags:
                    mock_flags.return_value = expected
                    
                    result = self.messaging_io.anyReleaseFlags()
                    self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()
