##
# File: test_NmrStarToCif.py
# Date:  02-Apr-2020  M. Yokochi
#
# Updates:
# 20-Oct-2021  M. Yokochi - add unit test for case-sensitive saveframe name (DAOTHER-7398, 7407)
#
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

from wwpdb.apps.msgmodule.io.MessagingIo import MessagingIo
from wwpdb.utils.session.WebRequest import InputRequest

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.ERROR)
l2 = logging.getLogger("wwpdb.apps.msgmodule.io.MessagingDataImport")
l2.setLevel(logging.INFO)
l2 = logging.getLogger("wwpdb.apps.msgmodule.io.MessagingDataExport")
l2.setLevel(logging.INFO)
l2 = logging.getLogger("mmcif.io")
l2.setLevel(logging.INFO)


class TestSignatures(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def __createTemplate(self, siteid, fName):
        x = f"""data_{siteid}
        _pdbx_database_status.statue_code  REL
        _pdbx_database_status.status_code                     REL
        _pdbx_database_status.entry_id                        0XXX
        _pdbx_database_status.recvd_initial_deposition_date   2003-02-20
        _pdbx_database_status.deposit_site                    {siteid}
        _pdbx_database_status.process_site                    {siteid}
        _pdbx_database_status.author_release_status_code      REL
        _pdbx_database_status.author_approval_type            ?
        _pdbx_database_status.date_hold_coordinates           ?
        _pdbx_database_status.date_of_ndb_release             ?

        loop_
        _pdbx_contact_author.id
        _pdbx_contact_author.email
        _pdbx_contact_author.name_first
        _pdbx_contact_author.name_last
        _pdbx_contact_author.role
        1 schmo@unknown.com     Joe  Unknown   'principal investigator/group leader'
        """

        with open(fName, "w") as fout:
            fout.writelines(x)

    def test_signatures(self):
        """Test that a signature appears for each of the expected siteids"""

        for subid, siteid in enumerate(["RCSB", "PDBE", "PDBJ"]):

            # Create a simple template file
            depid = "D_10000%s" % subid
            #
            fPath = os.path.join(TESTOUTPUT, "data", "archive", depid)
            if not os.path.exists(fPath):
                os.makedirs(fPath)

            fName = os.path.join(fPath, "%s_model_P1.cif.V1" % depid)
            self.__createTemplate(siteid, fName)

            paramDict = {"filesource": ["archive"], "TopSessionPath": [TESTOUTPUT], "identifier": [depid]}
            wr = InputRequest(paramDict)
            mio = MessagingIo(wr)

            rdict = {}
            mio.initializeDataStore()
            mio.getMsgTmpltDataItems(rdict)
            procsite = rdict["processing_site"]
            signoff = rdict["site_contact_details"]

            self.assertEqual(procsite, siteid)
            self.assertIn(siteid, signoff.upper())


if __name__ == "__main__":
    unittest.main()
