# =============================================================================
# Author:  Chenghua Shao
# Date:    2021-12-07
# Updates:
#
# =============================================================================
"""
Query da_internal database
"""

from wwpdb.utils.db.MyConnectionBase import MyConnectionBase
import logging

logger = logging.getLogger(__name__)


class DaInternalDb(object):
    def __init__(self, siteId=None):
        self.__mydb = None
        self.__siteId = siteId
        self.__open()

    def __del__(self):
        self.__close()

    def __open(self, resource="DA_INTERNAL"):
        """Opens up DB connection
        """
        self.__mydb = MyConnectionBase(siteId=self.__siteId)
        self.__mydb.setResource(resourceName=resource)
        ok = self.__mydb.openConnection()
        if not ok:
            logger.error("Could not open resourve %s", resource)
            self.__mydb = None
            return False

        return True

    def __close(self):
        if self.__mydb:
            self.__mydb.closeConnection()
            self.__mydb = None

    def run(self, query):
        cur = self.__mydb.getCursor()
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        return rows

    def verifyDepId(self, dep_id):
        query = "select structure_id from rcsb_status where structure_id = '%s'" % dep_id
        rows = self.run(query)
        if rows:
            return True
        else:
            return False

    def verifyPdbId(self, pdb_id):
        query = "select structure_id from rcsb_status where pdb_id = '%s'" % pdb_id
        rows = self.run(query)
        if rows:
            return True
        else:
            return False

    def verifyEmdbId(self, emdb_id):
        query = "select structure_id from database_2 where database_code = '%s'" % emdb_id
        rows = self.run(query)
        if rows:
            return True
        else:
            return False

    def convertPdbIdToDepId(self, pdb_id):
        query = "select structure_id from rcsb_status where pdb_id = '%s'" % pdb_id
        rows = self.run(query)
        if rows:
            return rows[0][0]
        else:
            return None

    def convertEmdbIdToDepId(self, emdb_id):
        query = "select structure_id from database_2 where database_id = 'EMDB' and database_code = '%s'" % emdb_id
        rows = self.run(query)
        if rows:
            return rows[0][0]
        else:
            return None


if __name__ == "__main__":
    db_da_internal = DaInternalDb()
    print(db_da_internal.verifyDepId("D_1000272951"))
    print(db_da_internal.verifyPdbId("8GI8"))
    print(db_da_internal.verifyEmdbId("EMD-40062"))
    print(db_da_internal.convertPdbIdToDepId("8GI8"))
    print(db_da_internal.convertEmdbIdToDepId("EMD-40062"))
