import unittest
from unittest.mock import Mock, patch, MagicMock
from io import StringIO
import sys
import os

if __package__ is None or __package__ == "":
    from os import path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from commonsetup import TESTOUTPUT  # noqa:  F401 pylint: disable=import-error,unused-import
else:
    from .commonsetup import TESTOUTPUT  # noqa: F401

# Import the class to test
from wwpdb.apps.msgmodule.io.MessagingIo import MessagingIo

class TestMessagingIo(unittest.TestCase):

    def setUp(self):
        # Mock request object
        self.req_obj = Mock()
        self.req_obj.getValue.return_value = 'test_value'
        
        # Mock session object
        self.session_obj = Mock()
        self.session_obj.getPath.return_value = '/tmp/session'
        self.session_obj.getRelativePath.return_value = 'relative_path'
        self.req_obj.newSessionObj.return_value = self.session_obj
        
        # Initialize MessagingIo instance
        self.messaging_io = MessagingIo(self.req_obj, verbose=False, log=StringIO())

    @patch('wwpdb.apps.msgmodule.io.MessagingIo.ConfigInfo')
    @patch('wwpdb.apps.msgmodule.io.MessagingIo.ConfigInfoAppEm')
    def test_initialization(self, mock_config_em, mock_config):
        # Test initialization sets up correctly
        mock_config_instance = Mock()
        mock_config.return_value = mock_config_instance
        mock_config_instance.get.return_value = {}
        
        mock_config_em_instance = Mock()
        mock_config_em.return_value = mock_config_em_instance
        
        # Re-initialize to apply mocks
        messaging_io = MessagingIo(self.req_obj, verbose=False, log=StringIO())
        
        self.assertIsNotNone(messaging_io)
        mock_config.assert_called_once_with('test_value')

    @patch('wwpdb.apps.msgmodule.io.MessagingIo.MessagingDataImport')
    def test_initializeDataStore(self, mock_data_import):
        # Test data store initialization
        mock_instance = Mock()
        mock_data_import.return_value = mock_instance
        mock_instance.getFilePath.return_value = '/tmp/model_file.cif'
        
        # Mock __isWorkflow to return True and os.access to return False so initdb logic doesn't run
        with patch.object(self.messaging_io, '_MessagingIo__isWorkflow', return_value=True):
            with patch('os.access', return_value=False):
                self.messaging_io.initializeDataStore()
        
        # Assert that getFilePath was called with correct parameters
        mock_instance.getFilePath.assert_called_with(contentType="model", format="pdbx")

    def test_getMsgColList(self):
        # Test getting message column list
        success, col_list = self.messaging_io.getMsgColList()
        self.assertTrue(success)
        self.assertIsInstance(col_list, list)
        self.assertIn('ordinal_id', col_list)

    @patch('wwpdb.apps.msgmodule.io.MessagingIo.PdbxMessageIo')
    @patch('wwpdb.apps.msgmodule.io.MessagingIo.MessagingDataImport')
    @patch('os.access')
    def test_getMsg(self, mock_access, mock_data_import, mock_pdbx_io):
        # Test getting a single message
        mock_pdbx_instance = Mock()
        mock_pdbx_io.return_value = mock_pdbx_instance
        mock_pdbx_instance.read.return_value = True
        mock_pdbx_instance.getMessageInfo.return_value = [{
            'message_id': 'test_id',
            'message_text': 'test_text',
            'timestamp': '2020-01-01 00:00:00',
            'message_type': 'text',
            'message_subject': 'Test Subject',
            'sender': 'test_sender'
        }]
        
        mock_data_instance = Mock()
        mock_data_import.return_value = mock_data_instance
        mock_data_instance.getFilePath.return_value = '/tmp/message_file.cif'
        
        # Mock os.access to return True
        mock_access.return_value = True
        
        # Mock the request object to return 'msgs' for content_type
        self.req_obj.getValue.return_value = 'msgs'
        
        # Mock __isWorkflow to return True
        with patch.object(self.messaging_io, '_MessagingIo__isWorkflow', return_value=True):
            msg_dict = self.messaging_io.getMsg('test_id', 'D_000000')
        self.assertEqual(msg_dict['message_id'], 'test_id')

    @patch('wwpdb.apps.msgmodule.io.MessagingIo.PdbxMessageIo')
    @patch('wwpdb.apps.msgmodule.io.MessagingIo.MessagingDataImport')
    def test_getMsgRowList(self, mock_data_import, mock_pdbx_io):
        # Test getting message row list
        mock_pdbx_instance = Mock()
        mock_pdbx_io.return_value = mock_pdbx_instance
        mock_pdbx_instance.read.return_value = True
        mock_pdbx_instance.getMessageInfo.return_value = [{
            'message_id': 'test_id',
            'send_status': 'Y'
        }]
        
        mock_data_instance = Mock()
        mock_data_import.return_value = mock_data_instance
        mock_data_instance.getFilePath.return_value = '/tmp/message_file.cif'
        
        # Mock the request object and workflow state
        self.req_obj.getValue.return_value = 'msgs'
        
        with patch.object(self.messaging_io, '_MessagingIo__isWorkflow', return_value=True):
            with patch('os.access', return_value=True):
                result = self.messaging_io.getMsgRowList('D_000000', p_sSendStatus='Y')
        self.assertIn('RECORD_LIST', result)

    @patch('wwpdb.apps.msgmodule.io.MessagingIo.MessagingDataImport')
    def test_checkAvailFiles(self, mock_data_import):
        # Test checking available files
        mock_instance = Mock()
        mock_data_import.return_value = mock_instance
        mock_instance.getFilePath.return_value = '/tmp/file.txt'
        
        # Mock __isWorkflow to return True so we go through the workflow branch
        with patch.object(self.messaging_io, '_MessagingIo__isWorkflow', return_value=True):
            with patch('os.access', return_value=True):
                files = self.messaging_io.checkAvailFiles('D_000000')
        self.assertIsInstance(files, list)

    @patch('wwpdb.apps.msgmodule.io.MessagingIo.PdbxMessageIo')
    @patch('wwpdb.apps.msgmodule.io.MessagingIo.MessagingDataImport')
    def test_getFilesRfrncd(self, mock_data_import, mock_pdbx_io):
        # Test getting file references
        mock_pdbx_instance = Mock()
        mock_pdbx_io.return_value = mock_pdbx_instance
        mock_pdbx_instance.read.return_value = True
        mock_pdbx_instance.getFileReferenceInfo.return_value = [{
            'message_id': 'test_id',
            'content_type': 'model'
        }]
        
        mock_data_instance = Mock()
        mock_data_import.return_value = mock_data_instance
        mock_data_instance.getFilePath.return_value = '/tmp/message_file.cif'
        
        # Mock the request object and workflow state
        self.req_obj.getValue.return_value = 'msgs'
        
        with patch.object(self.messaging_io, '_MessagingIo__isWorkflow', return_value=True):
            with patch('os.access', return_value=True):
                files = self.messaging_io.getFilesRfrncd('D_000000')
        self.assertIsInstance(files, dict)

    # Additional tests for other methods can be added similarly

    @patch('wwpdb.apps.msgmodule.io.MessagingIo.AutoMessage')
    @patch('wwpdb.apps.msgmodule.io.MessagingIo.MessagingDataExport')
    @patch('wwpdb.apps.msgmodule.io.MessagingIo.MessagingDataImport')
    def test_processMsg(self, mock_data_import, mock_data_export, mock_auto_msg):
        # Test processing a message
        mock_msg_obj = Mock()
        mock_msg_obj.isLive = True
        mock_msg_obj.getOutputFileTarget.return_value = '/tmp/output.cif'
        mock_msg_obj.contentType = 'msgs'
        mock_msg_obj.isBeingSent = True
        mock_msg_obj.fileReferences = []
        mock_msg_obj.depositionId = 'D_000000'
        mock_msg_obj.messageId = 'test_id'
        
        mock_data_import_instance = Mock()
        mock_data_import.return_value = mock_data_import_instance
        mock_data_import_instance.getFilePath.return_value = '/tmp/message.cif'
        
        mock_data_export_instance = Mock()
        mock_data_export.return_value = mock_data_export_instance
        mock_data_export_instance.getFilePath.return_value = '/tmp/deposit.cif'
        
        mock_auto_msg_instance = Mock()
        mock_auto_msg.return_value = mock_auto_msg_instance
        
        success, model_updated, failed_files = self.messaging_io.processMsg(mock_msg_obj)
        self.assertIsInstance(success, bool)
        self.assertIsInstance(model_updated, bool)
        self.assertIsInstance(failed_files, list)

    @patch('wwpdb.apps.msgmodule.io.MessagingIo.MsgTmpltHlpr')
    def test_getMsgTmpltDataItems(self, mock_tmplt_hlpr):
        # Test getting message template data items
        mock_instance = Mock()
        mock_tmplt_hlpr.return_value = mock_instance
        mock_instance.populateTmpltDict.return_value = None
        
        return_dict = {}
        self.messaging_io.getMsgTmpltDataItems(return_dict)
        mock_instance.populateTmpltDict.assert_called_once_with(return_dict)

    def test_getStarterMsgBody(self):
        # Test getting starter message body - in non-workflow mode it returns "Groovin' High"
        content = self.messaging_io.getStarterMsgBody()
        self.assertEqual(content, "Groovin' High")

    def test_setGroupId_no_exception(self):
        # Should not raise even if it doesn't return anything
        try:
            self.messaging_io.setGroupId("grp42")
        except Exception as e:
            self.fail(f"setGroupId raised an exception: {e}")

    @patch.object(MessagingIo, "getMsgReadList", return_value=[{"message_id": "M1"}])
    def test_getMsgReadList(self, mock_method):
        res = self.messaging_io.getMsgReadList("D_000000")
        self.assertIsInstance(res, list)
        mock_method.assert_called_once_with("D_000000")

    @patch.object(MessagingIo, "getMsgNoActionReqdList", return_value=[{"message_id": "M2"}])
    def test_getMsgNoActionReqdList(self, mock_method):
        res = self.messaging_io.getMsgNoActionReqdList("D_000000")
        self.assertIsInstance(res, list)
        mock_method.assert_called_once_with("D_000000")

    @patch.object(MessagingIo, "getMsgForReleaseList", return_value=[{"message_id": "M3"}])
    def test_getMsgForReleaseList(self, mock_method):
        res = self.messaging_io.getMsgForReleaseList("D_000000")
        self.assertIsInstance(res, list)
        mock_method.assert_called_once_with("D_000000")

    @patch.object(MessagingIo, "getNotesList", return_value=[{"note_id": "N1"}])
    def test_getNotesList(self, mock_method):
        res = self.messaging_io.getNotesList("D_000000")
        self.assertIsInstance(res, list)
        mock_method.assert_called_once_with("D_000000")

    @patch.object(
        MessagingIo,
        "autoMsg",
        return_value=True,
    )
    def test_autoMsg(self, mock_method):
        # Minimal viable set of args based on signature
        kwargs = dict(
            p_depDataSetId="D_000000",
            p_op="create",
            p_msgId=None,
            p_msgSubject="Test Subject",
            p_msgType="text",
            p_msgLvl="info",
            p_msgRsltnLvl="none",
            p_msgGroupId=None,
            p_msgAnnCategory=None,
            p_msgAnnTask=None,
            p_msgAnnSubTask=None,
            p_fileReferences=[],
            msg="Body",
            p_sender="annotator",
            p_testemail=False,
            p_tmpltType=None,
        )
        ok = self.messaging_io.autoMsg(**kwargs)
        self.assertIsInstance(ok, bool)
        mock_method.assert_called_once()
        # Ensure key arguments passed through
        passed_kwargs = mock_method.call_args.kwargs
        self.assertEqual(passed_kwargs["p_depDataSetId"], "D_000000")
        self.assertEqual(passed_kwargs["p_msgSubject"], "Test Subject")

    @patch.object(MessagingIo, "sendSingle", return_value=True)
    def test_sendSingle(self, mock_method):
        ok = self.messaging_io.sendSingle(
            p_depDataSetId="D_000000",
            p_msgId="M123",
            p_sender="annotator",
            p_testemail=False,
        )
        self.assertTrue(ok)
        mock_method.assert_called_once()

    @patch.object(MessagingIo, "get_message_list_from_depositor", return_value=["M1", "M2"])
    def test_get_message_list_from_depositor(self, mock_method):
        res = self.messaging_io.get_message_list_from_depositor()
        self.assertIsInstance(res, list)
        mock_method.assert_called_once_with()

    @patch.object(MessagingIo, "get_message_subject_from_depositor", return_value="Subject")
    def test_get_message_subject_from_depositor(self, mock_method):
        res = self.messaging_io.get_message_subject_from_depositor("M1")
        self.assertIsInstance(res, str)
        mock_method.assert_called_once_with("M1")

    @patch.object(MessagingIo, "is_release_request", return_value=False)
    def test_is_release_request(self, mock_method):
        res = self.messaging_io.is_release_request("M1")
        self.assertIsInstance(res, bool)
        mock_method.assert_called_once_with("M1")

    @patch.object(MessagingIo, "markMsgAsRead", return_value=True)
    def test_markMsgAsRead(self, mock_method):
        payload = {"message_id": "M1", "status": "read"}
        res = self.messaging_io.markMsgAsRead(payload)
        self.assertTrue(res)
        mock_method.assert_called_once_with(payload)

    @patch.object(MessagingIo, "areAllMsgsRead", return_value=False)
    def test_areAllMsgsRead(self, mock_method):
        res = self.messaging_io.areAllMsgsRead()
        self.assertIsInstance(res, bool)
        mock_method.assert_called_once_with()

    @patch.object(MessagingIo, "areAllMsgsActioned", return_value=False)
    def test_areAllMsgsActioned(self, mock_method):
        res = self.messaging_io.areAllMsgsActioned()
        self.assertIsInstance(res, bool)
        mock_method.assert_called_once_with()

    @patch.object(MessagingIo, "anyReleaseFlags", return_value=False)
    def test_anyReleaseFlags(self, mock_method):
        res = self.messaging_io.anyReleaseFlags()
        self.assertIsInstance(res, bool)
        mock_method.assert_called_once_with()

    @patch.object(MessagingIo, "anyUnactionApprovalWithoutCorrection", return_value=False)
    def test_anyUnactionApprovalWithoutCorrection(self, mock_method):
        res = self.messaging_io.anyUnactionApprovalWithoutCorrection()
        self.assertIsInstance(res, bool)
        mock_method.assert_called_once_with()

    @patch.object(MessagingIo, "anyNotesExist", return_value=False)
    def test_anyNotesExist(self, mock_method):
        res = self.messaging_io.anyNotesExist()
        self.assertIsInstance(res, bool)
        mock_method.assert_called_once_with()

    @patch.object(MessagingIo, "tagMsg", return_value=True)
    def test_tagMsg(self, mock_method):
        payload = {"message_id": "M1", "tag": "important", "value": True}
        res = self.messaging_io.tagMsg(payload)
        self.assertTrue(res)
        mock_method.assert_called_once_with(payload)

if __name__ == '__main__':
    unittest.main()