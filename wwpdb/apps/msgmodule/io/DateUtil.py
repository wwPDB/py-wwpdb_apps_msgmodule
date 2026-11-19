##
#
# File:    DateUtil.py
# Author:  E. Peisach
# Date:    17-Nov-2018
# Version: 0.001
# Updates:
"""
Class to manipulate dates for display purposes
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "ezra.peisach@rcsb.org"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import datetime


class DateUtil(object):
    def __init__(self):
        pass

    @staticmethod
    def select_obsolete_date(obs_id_types, pdb_obs_date=None, em_obs_date=None, em_admin_obs_date=None, fallback_date=None):
        """Pick the display date for an obsolete notification.

        Candidates that are empty or display placeholders ("[none listed]", "[UNKNOWN]") are skipped.
        EM dates are preferred when only the EMDB entry is obsoleted (map-only obsoletion);
        otherwise the PDB date is preferred. fallback_date is used when no candidate is usable.
        """

        def usable(d):
            return d is not None and len(str(d).strip()) > 0 and d not in ("[none listed]", "[UNKNOWN]")

        if "EMDB" in obs_id_types and "PDB" not in obs_id_types:
            candidates = [em_admin_obs_date, em_obs_date, pdb_obs_date]
        else:
            candidates = [pdb_obs_date, em_admin_obs_date, em_obs_date]

        for d in candidates:
            if usable(d):
                return d
        return fallback_date

    @staticmethod
    def date_to_display(date):
        """Converts incoming dateIn from ISO 8601 YYYY-MM-DD to Month number, year
        If dateIn is '.' or '?' returns [UNKNOWN]. If date cannot be parsed, returns date.
        This should be picked up by dictionary checks.
        """

        ret = "[UNKNOWN]"
        if date not in [".", "?"]:
            try:
                dt = datetime.datetime.strptime(date, "%Y-%m-%d")
                ret = dt.strftime("%-d %B %Y")
            except ValueError:
                ret = date

        return ret

    @staticmethod
    def datetime_to_display(date):
        """Converts a datetime.date object to ISO 8601 format"""
        try:
            dt = date.strftime("%-d %B %Y")
        except ValueError:
            dt = date
        return dt


def main():
    du = DateUtil()
    print(du.date_to_display("2018-10-08"))
    print(du.date_to_display("2018-10"))
    print(du.date_to_display("."))
    dt = datetime.date(2018, 10, 3)
    print(du.datetime_to_display(dt))


if __name__ == "__main__":
    main()
