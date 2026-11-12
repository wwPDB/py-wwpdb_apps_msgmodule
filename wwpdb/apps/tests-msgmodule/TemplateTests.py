"""Test cases for backwartds compatibility"""

import unittest
from wwpdb.apps.msgmodule.io.MessagingIo import getBackupDoi

class TemplateTests(unittest.TestCase):
    def testBackupDOI(self):
        """Tests backup DOI generation"""
        self.assertEqual("10.2210/pdb1o08/pdb", getBackupDoi("1o08"))
        self.assertEqual("10.2210/pdb1o08/pdb", getBackupDoi("1O08"))
        self.assertEqual("10.2210/pdb1o08/pdb", getBackupDoi("pdb_00001o08"))
        self.assertEqual("10.2210/pdb_1000axyz/pdb", getBackupDoi("pdb_1000axyz"))


        
if __name__ == "__main__":
    unittest.main()
