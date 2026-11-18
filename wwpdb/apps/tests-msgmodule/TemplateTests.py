"""Test cases for backwartds compatibility"""

import unittest
from wwpdb.apps.msgmodule.depict.MessagingDepict import extension_letter_date
from wwpdb.apps.msgmodule.io.MessagingIo import getBackupDoi


class TemplateTests(unittest.TestCase):
    def testBackupDOI(self):
        """Tests backup DOI generation"""
        self.assertEqual("10.2210/pdb1o08/pdb", getBackupDoi("1o08"))
        self.assertEqual("10.2210/pdb1o08/pdb", getBackupDoi("1O08"))
        self.assertEqual("10.2210/pdb1o08/pdb", getBackupDoi("pdb_00001o08"))
        self.assertEqual("10.2210/pdb_1000axyz/pdb", getBackupDoi("pdb_1000axyz"))


class ExtensionLetterDateTests(unittest.TestCase):
    """The extension letter quotes the hold date saved by the status module."""

    def test_pdb_uses_saved_hold_date_not_one_year_expire(self):
        params = {
            "hold_date": "15 April 2027",
            "expire_date": "15 January 2027",
        }
        self.assertEqual(extension_letter_date(params, False), "15 April 2027")

    def test_em_uses_saved_map_hold_date(self):
        params = {
            "hold_date": "15 April 2027",
            "hold_date_em_map": "15 July 2027",
            "expire_date": "15 January 2027",
            "expire_date_em_map": "15 January 2027",
            "modelonly": "false",
        }
        self.assertEqual(extension_letter_date(params, True), "15 July 2027")

    def test_em_model_only_uses_pdb_hold_when_map_hold_is_absent(self):
        params = {
            "hold_date": "15 April 2027",
            "hold_date_em_map": "",
            "expire_date_em_map": "15 January 2027",
            "modelonly": "true",
        }
        self.assertEqual(extension_letter_date(params, True), "15 April 2027")

    def test_em_without_a_saved_map_hold_keeps_the_one_year_date(self):
        params = {
            "hold_date": "15 April 2027",
            "hold_date_em_map": "[none listed]",
            "expire_date_em_map": "15 January 2027",
            "modelonly": "false",
        }
        self.assertEqual(extension_letter_date(params, True), "15 January 2027")

    def test_missing_hold_date_falls_back_to_expire_date(self):
        params = {
            "hold_date": "[none listed]",
            "expire_date": "15 January 2027",
        }
        self.assertEqual(extension_letter_date(params, False), "15 January 2027")


if __name__ == "__main__":
    unittest.main()
