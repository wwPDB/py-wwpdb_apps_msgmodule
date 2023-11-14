##
# File:    test_ExtractMessage.py
# Author:  Chenghua Shao
# Date:    2023-11-08
# Updates:
#    2023-11-08    CS    Created.
##

"""
Test wwpdb.apps.msgmodule.util.ExtractMessage
If new test data is to be added, make sure the confidential information is alterted.
"""
import unittest
import os
import sys
import logging

if __package__ is None or __package__ == "":
    from os import path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from commonsetup import TESTOUTPUT  # noqa:  F401 pylint: disable=import-error,unused-import
else:
    from .commonsetup import TESTOUTPUT  # noqa: F401

DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(DIR, 'test_data')
SRC_DIR = os.path.dirname(os.path.dirname(os.path.dirname(DIR)))
sys.path.append(SRC_DIR)
from wwpdb.apps.msgmodule.util.ExtractMessage import ExtractMessage

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.ERROR)

log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(funcName)s:%(lineno)d - %(message)s')
c_handler = logging.StreamHandler()
c_handler.setFormatter(log_format)
logger = logging.getLogger()
logger.addHandler(c_handler)
logger.setLevel(logging.INFO)

class TestExtractMessage(unittest.TestCase):
    def setUp(self):
        self.exmsg = ExtractMessage()

    def tearDown(self):
        pass

    def test_getLastAutoReminderDatetime(self):
        logger.info("test getLastAutoReminderDatetime")
        rt1 = self.exmsg.getLastAutoReminderDatetime("D_9000265933", test_folder=DATA_DIR)
        logger.info("last auto reminder for %s is %s" % ("D_9000265933", rt1))
        dt_ref1 = self.exmsg.convertStrToDatetime('2023-11-12 14:53:21')
        self.assertEqual(rt1, dt_ref1)
        
        rt2 = self.exmsg.getLastAutoReminderDatetime("D_9000277853", test_folder=DATA_DIR)
        logger.info("last auto reminder for %s is %s" % ("D_9000277853", rt2))
        self.assertFalse(rt2)
        
        rt3 = self.exmsg.getLastAutoReminderDatetime("D_800219", test_folder=DATA_DIR)
        logger.info("last auto reminder for %s is %s" % ("D_800219", rt3))
        self.assertFalse(rt3)
    
    def test_getLastMsgDatetime(self):
        logger.info("test getLastMsgDatetime")
        rt1 = self.exmsg.getLastMsgDatetime("D_9000265933", test_folder=DATA_DIR)
        logger.info("last message date for %s is %s" % ("D_9000265933", rt1))
        dt_ref1 = self.exmsg.convertStrToDatetime('2023-07-03 14:30:21')
        self.assertEqual(rt1, dt_ref1)
        
        rt2 = self.exmsg.getLastMsgDatetime("D_9000277853", test_folder=DATA_DIR)
        logger.info("last message date for %s is %s" % ("D_9000277853", rt2))
        dt_ref2= self.exmsg.convertStrToDatetime('2023-11-07 00:24:22')
        self.assertEqual(rt2, dt_ref2)
        
    def test_getLastSentMsgDatetime(self):
        logger.info("test getLastSentMsgDatetime")
        rt1 = self.exmsg.getLastSentMsgDatetime("D_9000265933", test_folder=DATA_DIR)
        logger.info("last message to depositor date for %s is %s" % ("D_9000265933", rt1))
        dt_ref1 = self.exmsg.convertStrToDatetime('2023-07-03 14:30:21')
        self.assertEqual(rt1, dt_ref1)
        
        rt2 = self.exmsg.getLastSentMsgDatetime("D_9000277853", test_folder=DATA_DIR)
        logger.info("last message to depositor date for %s is %s" % ("D_9000277853", rt2))
        dt_ref2= self.exmsg.convertStrToDatetime('2023-11-06 21:28:38')
        self.assertEqual(rt2, dt_ref2)
        
    def test_getLastReceivedMsgDatetime(self):
        logger.info("test getLastReceivedMsgDatetime")
        rt1 = self.exmsg.getLastReceivedMsgDatetime("D_9000265933", test_folder=DATA_DIR)
        logger.info("last message from depositor date for %s is %s" % ("D_9000265933", rt1))
        dt_ref1 = self.exmsg.convertStrToDatetime("2022-12-18 07:30:00")
        self.assertEqual(rt1, dt_ref1)
        
        rt2 = self.exmsg.getLastReceivedMsgDatetime("D_9000277853", test_folder=DATA_DIR)
        logger.info("last message from depositor date for %s is %s" % ("D_9000277853", rt2))
        dt_ref2 = self.exmsg.convertStrToDatetime("2023-11-07 00:24:22")
        self.assertEqual(rt2, dt_ref2)

    def test_getLastUnlockDatetime(self):
        logger.info("test getLastUnlockDatetime")
        rt1 = self.exmsg.getLastUnlockDatetime("D_9000265933", test_folder=DATA_DIR)
        logger.info("last unlocked date for %s is %s" % ("D_9000265933", rt1))
        dt_ref1 = self.exmsg.convertStrToDatetime("2022-12-19 13:33:27")
        self.assertEqual(rt1, dt_ref1)
        
        rt2 = self.exmsg.getLastUnlockDatetime("D_9000277853", test_folder=DATA_DIR)
        logger.info("last unlocked date for %s is %s" % ("D_9000277853", rt2))
        self.assertFalse(rt2)
    
    def test_getLastValidation(self):
        logger.info("test getLastValidation")
        rt1 = self.exmsg.getLastValidation("D_9000265933", test_folder=DATA_DIR)
        logger.info("last validation for %s was sent on %s with major issue status %s" % ("D_9000265933", rt1[0], rt1[1]))
        dt_ref1 = self.exmsg.convertStrToDatetime('2022-12-22 16:50:33')
        self.assertEqual(rt1[0], dt_ref1)
        self.assertFalse(rt1[1])
        
        rt2 = self.exmsg.getLastValidation("D_9000277853", test_folder=DATA_DIR)
        logger.info("last validation for %s was sent on %s with major issue status %s" % ("D_9000277853", rt2[0], rt2[1]))
        dt_ref2 = self.exmsg.convertStrToDatetime('2023-10-09 20:16:32')
        self.assertEqual(rt2[0], dt_ref2)
        self.assertTrue(rt2[1])
        
        rt3 = self.exmsg.getLastValidation("D_800219", test_folder=DATA_DIR)
        logger.info("last validation for %s was sent on %s with major issue status %s" % ("D_800219", rt3[0], rt3[1]))
        dt_ref3 = self.exmsg.convertStrToDatetime('2021-09-30 21:09:27')
        self.assertEqual(rt3[0], dt_ref3)
        self.assertTrue(rt3[1])

    def test_getLastManualReminderDatetime(self):
        logger.info("test getLastManualReminderDatetime")
        rt1 = self.exmsg.getLastManualReminderDatetime("D_9000265933", test_folder=DATA_DIR)
        logger.info("last reminder for %s was sent on %s" % ("D_9000265933", rt1))
        self.assertFalse(rt1)
        
        logger.info("test getLastReminderDatetime")
        rt2 = self.exmsg.getLastManualReminderDatetime("D_9000277853", test_folder=DATA_DIR)
        logger.info("last reminder for %s was sent on %s" % ("D_9000277853", rt2))
        dt_ref2 = self.exmsg.convertStrToDatetime('2023-10-25 21:51:35')
        self.assertEqual(rt2, dt_ref2)
        
        
if __name__ == "__main__":
    unittest.main()
