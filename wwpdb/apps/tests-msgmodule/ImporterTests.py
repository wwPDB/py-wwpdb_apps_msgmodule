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

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

from wwpdb.utils.config.ConfigInfo import ConfigInfo
from wwpdb.apps.msgmodule.webapp.MessagingWebApp import MessagingWebApp  # noqa: E402,F401 pylint: disable=unused-import
from wwpdb.apps.msgmodule.util.AutoMessage import AutoMessage  # noqa: F401,E402  pylint: disable=unused-import


class MyConfigInfo(ConfigInfo):
    """A class to bypass setting of refdata"""

    def __init__(self, siteId=None, verbose=True, log=sys.stderr):
        super(MyConfigInfo, self).__init__(siteId=siteId, verbose=verbose, log=log)

    def get(self, keyWord, default=None):
        if keyWord == "SITE_WEB_APPS_TOP_PATH":
            return "foo"

        return super(MyConfigInfo, self).get(keyWord, default)


class ImportTests(unittest.TestCase):

    @patch("wwpdb.apps.msgmodule.webapp.MessagingWebApp.ConfigInfo", side_effect=MyConfigInfo)
    def testInstantiate(self, mock1):  # pylint:  disable=unused-argument
        """Tests simple instantiation"""
        _am = AutoMessage()  # noqa: F841
        _mwa = MessagingWebApp()  # noqa: F841
        pass
