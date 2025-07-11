# =============================================================================
# Author:  Chenghua Shao
# Date:    2024-08-30
# Updates:
# 2024-12-23    CS     Add support on extended PDB ID
#
# =============================================================================
"""
DA_INTERNAL database utility
"""

from wwpdb.utils.db.MyConnectionBase import MyConnectionBase
import logging

logger = logging.getLogger(__name__)


class DaInternalDb(object):
    """DA_INTERNAL DB class for data lookup

    Args:
        object (obj): object
    """
    def __init__(self, siteId=None):
        """Initiator

        Args:
            siteId (str, optional): SITE ID. Defaults to None that will use the SITE ID of the current server.
        """
        self.__mydb = None
        self.__siteId = siteId
        self.__open()

    def __del__(self):
        """Finalizer. Close DB connection when all references to the object have been deleted.
        """
        self.__close()

    def __open(self, resource="DA_INTERNAL"):
        """Open DB connection

        Args:
            resource (str, optional): DB name. Defaults to "DA_INTERNAL".

        Returns:
            bool: True/False for DB connection
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
        """Proper DB closure
        """
        if self.__mydb:
            self.__mydb.closeConnection()
            self.__mydb = None

    def run(self, query):
        """Simplified query runner

        Args:
            query (str): Full text of a query

        Returns:
            tuple: raw query results as tuple of tuples, e.g. ((1,2),(3,4))
        """
        cur = self.__mydb.getCursor()
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        return rows

    def verifyDepId(self, dep_id):
        """Verify if an id is deposition id

        Args:
            dep_id (str): presumed dep id input

        Returns:
            bool: True/False
        """
        query = "select structure_id from rcsb_status where structure_id = '%s'" % dep_id
        rows = self.run(query)
        if rows:
            return True
        else:
            return False

    def verifyPdbId(self, pdb_id):
        """Verify if an id is PDB ID

        Args:
            pdb_id (str): presumed PDB id input

        Returns:
            bool: True/False
        """
        query = "select structure_id from rcsb_status where pdb_id = '%s'" % pdb_id
        rows = self.run(query)
        if rows:
            return True
        else:
            return False

    def verifyExtendedPdbId(self, pdb_ext_id):
        """Verify if an id is extended PDB ID

        Args:
            pdb_ext_id (str): presumed extended PDB id input

        Returns:
            bool: True/False
        """
        query = "select structure_id from database_2 where database_id = 'PDB' and pdbx_database_accession = '%s'" % pdb_ext_id
        rows = self.run(query)
        if rows:
            return True
        else:
            return False

    def verifyEmdbId(self, emdb_id):
        """Verify if an id is EMDB ID

        Args:
            emdb_id (str): presumed EMDB ID input

        Returns:
            bool: True/False
        """
        query = "select structure_id from database_2 where database_id = 'EMDB' and database_code = '%s'" % emdb_id
        rows = self.run(query)
        if rows:
            return True
        else:
            return False

    def convertPdbIdToDepId(self, pdb_id):
        """Convert PDB ID to deposition id

        Args:
            pdb_id (str): presumed PDB ID

        Returns:
            str: valid deposition id at this site, or None
        """
        query = "select structure_id from rcsb_status where pdb_id = '%s'" % pdb_id
        rows = self.run(query)
        if rows:
            return rows[0][0]
        else:
            return None

    def convertExtendedPdbIdToDepId(self, pdb_ext_id):
        """Convert extended PDB ID to deposition id

        Args:
            pdb_id (str): presumed PDB ID

        Returns:
            str: valid deposition id at this site, or None
        """
        query = "select structure_id from database_2 where database_id = 'PDB' and pdbx_database_accession = '%s'" % pdb_ext_id
        rows = self.run(query)
        if rows:
            return rows[0][0]
        else:
            return None

    def convertEmdbIdToDepId(self, emdb_id):
        """Convert EMDB ID to deposition id

        Args:
            pdb_id (str): presumed EMDB ID

        Returns:
            str: valid deposition id at this site, or None
        """
        query = "select structure_id from database_2 where database_id = 'EMDB' and database_code = '%s'" % emdb_id
        rows = self.run(query)
        if rows:
            return rows[0][0]
        else:
            return None


if __name__ == "__main__":
    # Minimal temporary testing, cannot develop unit test because the IDs at the test sites are cleaned up regularly.
    db_da_internal = DaInternalDb()
    print(db_da_internal.verifyDepId("D_1000272951"))
    print(db_da_internal.verifyPdbId("8GI8"))
    print(db_da_internal.verifyEmdbId("EMD-40062"))
    print(db_da_internal.convertPdbIdToDepId("8GI8"))
    print(db_da_internal.convertEmdbIdToDepId("EMD-40062"))
