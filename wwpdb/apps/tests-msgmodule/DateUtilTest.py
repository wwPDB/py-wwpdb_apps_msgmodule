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

import unittest

from wwpdb.apps.msgmodule.io.DateUtil import DateUtil


class DateUtilTests(unittest.TestCase):
    def setUp(self):
        pass

    def _testsame(self, din, dexpect):
        du = DateUtil()
        self.assertEqual(du.date_to_display(din), dexpect)

    def testDates(self):
        """Tests date conversion"""
        self._testsame('2018-10-01', '1 October 2018')
        self._testsame('2018-10', '2018-10')
        self._testsame('.', '[UNKNOWN]')
        self._testsame('?', '[UNKNOWN]')
        # 2018 is not a leap year - no conversion
        self._testsame('2018-02-29', '2018-02-29')
        self._testsame('2016-02-29', '29 February 2016')

if __name__ == '__main__':
    unittest.main()
