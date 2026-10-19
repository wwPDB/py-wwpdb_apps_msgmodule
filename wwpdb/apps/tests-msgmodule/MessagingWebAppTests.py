##
# File: MessagingWebAppTests.py
#
# Tests for deposition id verification used when archiving or forwarding messages.
#
# pylint: disable=protected-access
import unittest

try:
    from unittest.mock import Mock, patch
except ImportError:
    from mock import Mock, patch

from wwpdb.apps.msgmodule.webapp.MessagingWebApp import MessagingWebAppWorker

MODULE = "wwpdb.apps.msgmodule.webapp.MessagingWebApp"
SITE_ID = "WWPDB_DEPLOY_TEST"


def make_da_internal(known_dep_ids=(), pdb_ids=None, emdb_ids=None, ext_pdb_ids=None):
    """Mocked DA_INTERNAL helper. The id mappings convert a known PDB / EMDB / extended PDB id to a deposition id."""
    pdb_ids = pdb_ids or {}
    emdb_ids = emdb_ids or {}
    ext_pdb_ids = ext_pdb_ids or {}
    da = Mock()
    da.verifyDepId.side_effect = lambda dep_id: dep_id in known_dep_ids
    da.verifyPdbId.side_effect = lambda pdb_id: pdb_id in pdb_ids
    da.convertPdbIdToDepId.side_effect = lambda pdb_id: pdb_ids[pdb_id]
    da.verifyEmdbId.side_effect = lambda emdb_id: emdb_id in emdb_ids
    da.convertEmdbIdToDepId.side_effect = lambda emdb_id: emdb_ids[emdb_id]
    da.verifyExtendedPdbId.side_effect = lambda ext_id: ext_id in ext_pdb_ids
    da.convertExtendedPdbIdToDepId.side_effect = lambda ext_id: ext_pdb_ids[ext_id]
    return da


def make_workflow_db(known_dep_ids=()):
    """Mocked workflow DB api whose exist() recognises only the given deposition ids."""
    wf = Mock()
    wf.exist.side_effect = lambda data_obj: data_obj.get("DEP_SET_ID") in known_dep_ids
    return wf


def make_worker():
    """Worker without the constructor, which would open database and configuration connections."""
    return MessagingWebAppWorker.__new__(MessagingWebAppWorker)


class VerifyOrConvertIdTests(unittest.TestCase):
    def setUp(self):
        self.worker = make_worker()

    def test_submitted_deposition_found_in_da_internal(self):
        da = make_da_internal(known_dep_ids=["D_1000000001"])
        wf = make_workflow_db()
        self.assertEqual(self.worker._verifyOrConvertId("D_1000000001", da, wf), "D_1000000001")
        da.verifyDepId.assert_called_once_with("D_1000000001")
        wf.exist.assert_not_called()

    def test_unsubmitted_deposition_found_in_workflow_database(self):
        da = make_da_internal()
        wf = make_workflow_db(known_dep_ids=["D_1000000002"])
        self.assertEqual(self.worker._verifyOrConvertId("D_1000000002", da, wf), "D_1000000002")
        da.verifyDepId.assert_called_once_with("D_1000000002")
        wf.exist.assert_called_once_with({"DEP_SET_ID": "D_1000000002"})

    def test_deposition_absent_from_both_databases(self):
        da = make_da_internal(known_dep_ids=["D_1000000001"])
        wf = make_workflow_db(known_dep_ids=["D_1000000002"])
        self.assertIsNone(self.worker._verifyOrConvertId("D_1000000003", da, wf))
        da.verifyDepId.assert_called_once_with("D_1000000003")
        wf.exist.assert_called_once_with({"DEP_SET_ID": "D_1000000003"})

    def test_lowercase_and_whitespace_are_normalised(self):
        da = make_da_internal()
        wf = make_workflow_db(known_dep_ids=["D_1000000002"])
        self.assertEqual(self.worker._verifyOrConvertId("  d_1000000002 \n", da, wf), "D_1000000002")
        wf.exist.assert_called_once_with({"DEP_SET_ID": "D_1000000002"})

    def test_supported_deposition_id_lengths(self):
        for dep_id in ["D_800946", "D_1000000001", "D_1234567890"]:
            wf = make_workflow_db(known_dep_ids=[dep_id])
            self.assertEqual(self.worker._verifyOrConvertId(dep_id, make_da_internal(), wf), dep_id)

    def test_malformed_ids_are_rejected_without_queries(self):
        for bad_id in [
            "",
            "   ",
            "D_",
            "D_12345",
            "D_12345678901",
            "D_ABC123",
            "D_100000000A",
            "D_1000 00001",
            "D_\u0661\u0662\u0663\u0664\u0665\u0666\u0667",
            "X_1234567",
            "G_1000001",
            "not an id",
        ]:
            da = make_da_internal(known_dep_ids=[bad_id.strip().upper()])
            wf = make_workflow_db(known_dep_ids=[bad_id.strip().upper()])
            with self.subTest(bad_id=bad_id):
                self.assertIsNone(self.worker._verifyOrConvertId(bad_id, da, wf))
                self.assertEqual(da.method_calls, [])
                self.assertEqual(wf.method_calls, [])

    def test_pdb_id_is_converted(self):
        da = make_da_internal(pdb_ids={"1ABC": "D_1000000001"})
        wf = make_workflow_db()
        self.assertEqual(self.worker._verifyOrConvertId(" 1abc ", da, wf), "D_1000000001")
        da.verifyPdbId.assert_called_once_with("1ABC")
        wf.exist.assert_not_called()

    def test_unknown_pdb_id_is_rejected(self):
        da = make_da_internal(pdb_ids={"1ABC": "D_1000000001"})
        wf = make_workflow_db(known_dep_ids=["2XYZ"])
        self.assertIsNone(self.worker._verifyOrConvertId("2XYZ", da, wf))
        wf.exist.assert_not_called()

    def test_extended_pdb_id_is_converted(self):
        da = make_da_internal(ext_pdb_ids={"PDB_00001ABC": "D_1000000001"})
        wf = make_workflow_db()
        self.assertEqual(self.worker._verifyOrConvertId("pdb_00001abc", da, wf), "D_1000000001")
        da.verifyExtendedPdbId.assert_called_once_with("PDB_00001ABC")
        wf.exist.assert_not_called()

    def test_unknown_extended_pdb_id_is_rejected(self):
        da = make_da_internal()
        wf = make_workflow_db()
        self.assertIsNone(self.worker._verifyOrConvertId("PDB_00001ABC", da, wf))
        wf.exist.assert_not_called()

    def test_emdb_id_is_converted(self):
        da = make_da_internal(emdb_ids={"EMD-12345": "D_1000000001"})
        wf = make_workflow_db()
        self.assertEqual(self.worker._verifyOrConvertId("emd-12345", da, wf), "D_1000000001")
        da.verifyEmdbId.assert_called_once_with("EMD-12345")
        wf.exist.assert_not_called()

    def test_unknown_emdb_id_is_rejected(self):
        da = make_da_internal()
        wf = make_workflow_db()
        self.assertIsNone(self.worker._verifyOrConvertId("EMD-12345", da, wf))
        wf.exist.assert_not_called()

    def test_workflow_lookup_failure_is_logged_and_reraised(self):
        da = make_da_internal()
        wf = make_workflow_db()
        db_error = RuntimeError("workflow database unavailable")
        wf.exist.side_effect = db_error
        with self.assertLogs(MODULE, level="ERROR") as captured:
            with self.assertRaises(RuntimeError) as raised:
                self.worker._verifyOrConvertId("D_1000000002", da, wf)
        self.assertIs(raised.exception, db_error)
        self.assertTrue(any(record.exc_info for record in captured.records))
        self.assertTrue(any("D_1000000002" in record.getMessage() for record in captured.records))
        da.verifyDepId.assert_called_once_with("D_1000000002")


class FakeRequest(object):
    """Dictionary backed stand-in for the web request object."""

    def __init__(self, values):
        self.values = dict(values)
        self.setReturnFormat = Mock()

    def getValue(self, key):
        return self.values.get(key, "")

    def getRawValue(self, key):
        return self.values.get(key, "")

    def setValue(self, key, value):
        self.values[key] = value


class PropagateMsgTests(unittest.TestCase):
    def test_mixed_targets_are_verified_independently_with_shared_helpers(self):
        worker = make_worker()
        worker._MessagingWebAppWorker__reqObj = FakeRequest({"identifier": "d_1000000001,D_1000000002,D_1000000003", "subject": "A subject"})
        worker._MessagingWebAppWorker__siteId = SITE_ID
        worker._MessagingWebAppWorker__verbose = False
        log = Mock()
        worker._MessagingWebAppWorker__lfh = log
        worker.statusApi = Mock()
        worker.statusApi.getGroupId.return_value = ""

        da = make_da_internal(known_dep_ids=["D_1000000001"])
        wf = make_workflow_db(known_dep_ids=["D_1000000002"])
        msging_io = Mock()
        msging_io.processMsg.return_value = (True, False, [])

        with patch.object(worker, "_MessagingWebAppWorker__getSession"), patch.object(worker, "_getNotesFlaggingStatus", return_value="true"), patch(
            MODULE + ".DaInternalDb", return_value=da
        ) as da_ctor, patch(MODULE + ".WfDbApi", return_value=wf) as wf_ctor, patch(MODULE + ".MessagingIo", return_value=msging_io), patch(MODULE + ".Message"), patch(
            MODULE + ".ResponseContent"
        ) as response_ctor:
            worker._archiveMsg()

        da_ctor.assert_called_once_with(siteId=SITE_ID)
        wf_ctor.assert_called_once_with(siteId=SITE_ID, verbose=False, log=log)
        self.assertEqual(msging_io.processMsg.call_count, 2)
        response_ctor.return_value.addDictionaryItems.assert_called_once_with({"success": {"D_1000000001": "true", "D_1000000002": "true", "D_1000000003": "false", "job": "ok"}})


if __name__ == "__main__":
    unittest.main()
