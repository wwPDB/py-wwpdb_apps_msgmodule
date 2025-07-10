##
# File: ImportTests.py
# Date:  06-Oct-2018  E. Peisach
#
# Updates:
##
"""Test cases for msgmodule"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "peisach@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import sys
import unittest

if __package__ is None or __package__ == "":
    from os import path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from commonsetup import (
        TESTOUTPUT,
    )  # noqa:  F401 pylint: disable=import-error,unused-import
else:
    from .commonsetup import TESTOUTPUT  # noqa: F401

from wwpdb.apps.msgmodule.webapp.MessagingWebApp import (
    MessagingWebApp,
)  # noqa: E402,F401 pylint: disable=unused-import
from wwpdb.apps.msgmodule.util.AutoMessage import (
    AutoMessage,
)  # noqa: F401,E402  pylint: disable=unused-import


class ImportTests(unittest.TestCase):
    def testInstantiate(self):
        """Tests simple instantiation"""
        _am = AutoMessage()  # noqa: F841
        _mwa = MessagingWebApp()  # noqa: F841


if __name__ == "__main__":
    unittest.main()
