#
# File: DateUtilTest.py
# Date:  11-18-2018  E. Peisach
#
# Updates:
##
"""Test cases for DateUtil class"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "peisach@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import datetime
import unittest

from wwpdb.apps.msgmodule.io.DateUtil import DateUtil


class DateUtilTests(unittest.TestCase):
    def setUp(self):
        pass

    def _testsame(self, din, dexpect):
        du = DateUtil()
        self.assertEqual(du.date_to_display(din), dexpect)

    def _testsamedatetime(self, din, dexpect):
        du = DateUtil()
        self.assertEqual(du.datetime_to_display(din), dexpect)

    def testDates(self):
        """Tests date conversion"""
        self._testsame("2018-10-01", "1 October 2018")
        self._testsame("2018-10", "2018-10")
        self._testsame(".", "[UNKNOWN]")
        self._testsame("?", "[UNKNOWN]")
        # 2018 is not a leap year - no conversion
        self._testsame("2018-02-29", "2018-02-29")

    def testSelectObsoleteDate(self):
        """Tests choice of obsolete date for the obsolete notification"""
        sel = DateUtil.select_obsolete_date
        rel = "18 November 2026"
        # Map-only obsoletion: PDB placeholder must not block EM dates or fallback
        self.assertEqual(sel(["EMDB"], "[none listed]", "[none listed]", None, rel), rel)
        self.assertEqual(sel(["EMDB"], "[none listed]", "[none listed]", "4 March 2026", rel), "4 March 2026")
        self.assertEqual(sel(["EMDB"], "1 May 2025", "2 June 2025", None, rel), "2 June 2025")
        self.assertEqual(sel(["EMDB"], None, "[UNKNOWN]", "", rel), rel)
        # Model obsoletion prefers PDB date
        self.assertEqual(sel(["PDB"], "1 May 2025", "2 June 2025", None, rel), "1 May 2025")
        self.assertEqual(sel(["PDB", "EMDB"], "[none listed]", "2 June 2025", None, rel), "2 June 2025")
        self.assertEqual(sel([], None, None, None, rel), rel)
        self._testsame("2016-02-29", "29 February 2016")

    def testDateTime(self):
        dt = datetime.date(2018, 10, 3)
        self._testsamedatetime(dt, "3 October 2018")


if __name__ == "__main__":
    unittest.main()
