# File:  DaInternalDb.py
# Date:  16-Feb-2020 E. Peisach
#
# Update:
#
##
"""
Retrives data from da_internal pertaining lookup data
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
        """Opens up DB connection"""
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

    def getDatabase2(self, structure_id):
        query = """select database_id, database_code from database_2 where Structure_id = %s"""

        curs = self.__mydb.getCursor()

        data = {}
        curs.execute(query, (structure_id,))

        row = curs.fetchone()
        while row is not None:
            data[row[0]] = row[1]
            row = curs.fetchone()

        curs.close()

        return data


if __name__ == "__main__":
    da = DaInternalDb()
    dt = da.getDatabase2("D_800410")
    print(dt)
    dt = da.getDatabase2("D_800411")
    print(dt)
