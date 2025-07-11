import sys
import os
import platform

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
TESTOUTPUT = os.path.join(HERE, "test-output", platform.python_version())
if not os.path.exists(TESTOUTPUT):
    os.makedirs(TESTOUTPUT)

# We do this here - as unittest loads all at once - need to insure common

try:
    from unittest.mock import Mock, MagicMock
except ImportError:
    from mock import Mock, MagicMock

configInfo = {
    "SITE_REFDATA_PROJ_NAME_CC": "ligand-dict-v3",
    "REFERENCE_PATH": os.path.join(HERE, "data"),
    "RO_RESOURCE_PATH": os.path.join(HERE, os.pardir, os.pardir, "mock-data", "da_top", "resources_ro"),
    "SITE_ARCHIVE_STORAGE_PATH": os.path.join(TESTOUTPUT, "data"),
    "SITE_WEB_APPS_TOP_PATH": TESTOUTPUT,
    "FILE_FORMAT_EXTENSION_DICTIONARY": {"pdbx": "cif", "pdb": "pdb", "nmr-star": "str", "txt": "txt"},
    "CONTENT_TYPE_DICTIONARY": {
        "model": (["pdbx", "pdb", "pdbml", "cifeps"], "model"),
        "messages-from-depositor": (["pdbx"], "messages-from-depositor"),
        "messages-to-depositor": (["pdbx"], "messages-to-depositor"),
        "notes-from-annotator": (["pdbx"], "notes-from-annotator"),
        "correspondence-to-depositor": (["txt"], "correspondence-to-depositor"),
        "correspondence-legacy-rcsb": (["pdbx"], "correspondence-legacy-rcsb"),
    },
}

configInfoMockConfig = {
    "return_value": configInfo,
}

configMock = MagicMock(**configInfoMockConfig)

# Returns a dictionary by default - which has a get operator
sys.modules["wwpdb.utils.config.ConfigInfo"] = Mock(ConfigInfo=configMock)


#  Need to stub out from wwpdb.utils.wf.dbapi.dbAPI import dbAPI
#  brainPageContactList = ss.runSelectNQ(table="user_data", select=["email", "role", "last_name"], where={"dep_set_id": self.__depId, "role": roleFilter})
# The only place that dbAPI is used is to get the contact author
dbAPIMock = MagicMock()
dbAPIMock.dbAPI.runSelectNQ.return_value = ["someone@unknown.com", "principal investigator/group leader", "One"]
sys.modules["wwpdb.utils.wf.dbapi.dbAPI"] = dbAPIMock
