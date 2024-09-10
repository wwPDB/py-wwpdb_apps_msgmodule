##
# File:  ExtractMessage.py
# Date:  28-Jun-2019 E. Peisach
#
# Update:
#    2023-11-10    CS     Refactor code to add general process to parse message file; Add functions to retrieve datetime of last messages of various type.
#    2023-11-23    EP     Add getApprovalNoCorrectSubjects() and getPendingDepositorMessages()
#    2024-04-04    CS     Use context_type and context_value to find target message, and default to subject/title parsing as was used previously.
#    2024-09-09    CS     Update getLastAutoReminderDatetime() to include search on reminder-auth-to-rel
##
"""
Support for automatic extracting information from send messages


"""
__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "peisach@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"

import os
import sys
import datetime
import logging
import re
from wwpdb.utils.config.ConfigInfo import ConfigInfo
from mmcif.io.PdbxReader import PdbxReader
from mmcif_utils.persist.LockFile import LockFile
from wwpdb.io.locator.PathInfo import PathInfo
from mmcif_utils.message.PdbxMessageIo import PdbxMessageIo

logger = logging.getLogger(__name__)


class ExtractMessage(object):
    """Class to read message files and extract message date and contents
    """
    def __init__(self, siteId=None, verbose=False, log=sys.stderr):
        self.__siteId = siteId
        self.__verbose = verbose
        self.__log = log
        self.__pI = PathInfo(siteId=self.__siteId, verbose=self.__verbose, log=self.__log)
        # Parameters to tune lock file management --
        self.__timeoutSeconds = 10
        self.__retrySeconds = 0.2

        self.__depid = None  # deposition id is stored as cached class variable, see __readMsgFile() for re-use cached data
        self.__contentType = None  # contentType is stored as cached class variable
        self.__lc = []  # list of data containers for the message file as cached class variable

    def __getMsgFilePath(self, depid, contentType, test_folder=None):
        """Returns message filepath in the archive
        The following 3 types of contentType are allowed:
        messages-from-depositor
        messages-to-depositor
        notes-from-annotator
        """
        if test_folder:
            logger.info("look for message file for %s in author-provided folder %s", depid, test_folder)
            filename_msg = depid + '_' + contentType + '_P1.cif.V1'
            filepath_msg = os.path.join(test_folder, filename_msg)
        else:
            logger.info("look for message file for %s in the archive", depid)
            filepath_msg = self.__pI.getFilePath(depid, contentType=contentType, formatType="pdbx", fileSource="archive", versionId="1")

        if not os.path.exists(filepath_msg):
            logger.warning("cannot find message file for %s", depid)
            return None

        return filepath_msg

    def __readMsgFile(self, depid, contentType, b_use_cache=True, test_folder=None):
        """Parse message file to data in self.__lc.
        Message file can be either messages-from-depositor, messages-to-depositor, or notes-from-annotator
        """
        if b_use_cache and self.__depid == depid and self.__contentType == contentType and self.__lc:
            logger.info("use cached message data in self.__lc for %s, ignore new contents", depid)
            # If the same message file was just read and parsed, use the cached data in self.__lc, in order to avoid parsing
            # the same file multiple times with different functions since each function uses depid as argument.
            # This should be used for read-only functions. Be mindful of the consequence of ignoring new data written into the
            # message file while the program is running
        else:
            self.__lc = []  # must reset so that parsed data from the previous message file is not mixed with current
            filepath_msg = self.__getMsgFilePath(depid, contentType, test_folder)
            if not filepath_msg:
                return None
            logger.info("read message file for %s at %s", depid, filepath_msg)

            try:
                with LockFile(filepath_msg, timeoutSeconds=self.__timeoutSeconds, retrySeconds=self.__retrySeconds, verbose=self.__verbose, log=self.__log):
                    with open(filepath_msg, 'r') as file:
                        cif_parser = PdbxReader(file)
                        cif_parser.read(self.__lc)
            except FileExistsError as e:
                logger.info("%s filelock exists, skip with error %s", filepath_msg, e)
                self.__lc = []

            self.__depid = depid

    def __selectLastMsgByTitlePhrase(self, phrase):
        ret = None
        dc0 = self.__lc[0]
        catObj = dc0.getObj("pdbx_deposition_message_info")
        if catObj is None:
            logger.warning("cannot find pdbx_deposition_message_info category in the message file for %s", self.__depid)
            return None
        else:
            itDict = {}
            itNameList = catObj.getItemNameList()
            for idxIt, itName in enumerate(itNameList):
                itDict[str(itName).lower()] = idxIt
                #
            idxOrdinalId = itDict["_pdbx_deposition_message_info.ordinal_id"]
            idxLastCommDate = itDict["_pdbx_deposition_message_info.timestamp"]
            idxMsgSubj = itDict["_pdbx_deposition_message_info.message_subject"]

            maxOrdId = 0
            for row in catObj.getRowList():
                try:
                    ordinalId = int(row[idxOrdinalId])
                    msgsubj = row[idxMsgSubj]

                    if phrase in msgsubj:
                        if ordinalId > maxOrdId:
                            maxOrdId = ordinalId
                            ret = self.convertStrToDatetime(str(row[idxLastCommDate]))

                except Exception as e:
                    logger.error("Error processing message file for %s, %s", self.__depid, e)
        return ret

    # CS 2024-04-04 add search below by context_type
    def __selectLastMsgByContextType(self, l_context_type_to_search):
        """ return datetime of the lastest message based on context type

        input: arg l_context_type_to_search is a list of the context types to search for.
        list is used because some searches invole multiple context_type, e.g. ["release-publ", "release-nopubl"]

        output: return datetime

        """
        ret = None
        try:
            dc0 = self.__lc[0]
            catObj = dc0.getObj("pdbx_deposition_message_info")
            itDict = {}
            itNameList = catObj.getItemNameList()
            for idxIt, itName in enumerate(itNameList):
                itDict[str(itName).lower()] = idxIt

            idxOrdinalId = itDict["_pdbx_deposition_message_info.ordinal_id"]
            idxLastCommDate = itDict["_pdbx_deposition_message_info.timestamp"]
            idxContextType = itDict["_pdbx_deposition_message_info.context_type"]

            maxOrdId = 0
            for row in catObj.getRowList():
                ordinalId = int(row[idxOrdinalId])
                context_type_recorded = row[idxContextType]

                if context_type_recorded in l_context_type_to_search:
                    if ordinalId > maxOrdId:
                        maxOrdId = ordinalId
                        ret = self.convertStrToDatetime(str(row[idxLastCommDate]))

        except Exception as e:
            logger.error("Error processing message file for %s, %s", self.__depid, e)
            return None

        return ret

    # CS 2024-04-04 add validation letter search below by context_type/context_value
    def __getLastValidationByContextType(self):
        """Returns (datetime, major issue)  of the last time a validation was sent as determined by
        context_type and context_value
        """

        lastvalid = None
        major = None
        try:
            dc0 = self.__lc[0]
            catObj = dc0.getObj("pdbx_deposition_message_info")
            itDict = {}
            itNameList = catObj.getItemNameList()
            for idxIt, itName in enumerate(itNameList):
                itDict[str(itName).lower()] = idxIt

            idxOrdinalId = itDict["_pdbx_deposition_message_info.ordinal_id"]
            idxLastCommDate = itDict["_pdbx_deposition_message_info.timestamp"]
            idxContextType = itDict["_pdbx_deposition_message_info.context_type"]
            idxContextValue = itDict["_pdbx_deposition_message_info.context_value"]

            maxOrdId = 0
            for row in catObj.getRowList():
                ordinalId = int(row[idxOrdinalId])
                context_type_recorded = row[idxContextType]
                context_value_recorded = row[idxContextValue]

                if context_type_recorded == "vldtn":
                    if ordinalId > maxOrdId:
                        maxOrdId = ordinalId
                        lastvalid = self.convertStrToDatetime(str(row[idxLastCommDate]))
                        if context_value_recorded == "major-issue-in-validation":
                            major = True
                        else:
                            major = False

        except Exception as e:
            logger.error("Error processing message file for %s, %s", self.__depid, e)
            return (None, None)

        return (lastvalid, major)

    def convertStrToDatetime(self, s_datetime):
        return datetime.datetime.strptime(str(s_datetime), "%Y-%m-%d %H:%M:%S")

    def getLastMsgDatetime(self, depid, to_from="both", b_use_cache=True, test_folder=None):
        """Return last message date as python datetime, either to or from depositor.
        System-sent messages in archived notes file are not counted.
        """
        if to_from == "to":
            return self.getLastSentMessageDate(depid, True, b_use_cache=b_use_cache, test_folder=test_folder)
        elif to_from == "from":
            return self.getLastSentMessageDate(depid, False, b_use_cache=b_use_cache, test_folder=test_folder)
        elif to_from == "both":
            datetime_to = self.getLastSentMessageDate(depid, True, b_use_cache=b_use_cache, test_folder=test_folder)
            datetime_from = self.getLastSentMessageDate(depid, False, b_use_cache=b_use_cache, test_folder=test_folder)

            if not datetime_from:
                return datetime_to

            if not datetime_to:
                return datetime_from

            if datetime_to > datetime_from:
                return datetime_to
            else:
                return datetime_from
        else:
            return None

    def getLastReceivedMsgDatetime(self, depid, b_use_cache=True, test_folder=None):
        """Return date of last message from depositor as python datetime.
        """
        return self.getLastSentMessageDate(depid, False, b_use_cache=b_use_cache, test_folder=test_folder)

    def getLastSentMsgDatetime(self, depid, b_use_cache=True, test_folder=None):
        """Return date of last message to depositor as python datetime.
        System-sent messages in archived notes file are not counted.
        """
        return self.getLastSentMessageDate(depid, True, b_use_cache=b_use_cache, test_folder=test_folder)

    def getLastAutoReminderDatetime(self, depid, b_use_cache=True, test_folder=None):
        """ Return date of last reminder in notes as python datetime.
        Notes only records automatically-sent messages unless annotators specifically archived a message.
        """
        ret = None
        self.__readMsgFile(depid, contentType="notes-from-annotator", b_use_cache=b_use_cache, test_folder=test_folder)
        if len(self.__lc) >= 1:
            ret_by_context_type = self.__selectLastMsgByContextType(["reminder", "reminder-auth-to-rel"])  # CS 2024-09-09 add reminder-auth-to-rel
            if ret_by_context_type:
                ret = ret_by_context_type
            else:
                ret = self.__selectLastMsgByTitlePhrase(phrase='Still awaiting feedback for')
        else:
            logger.info("Deposition %s empty message file", depid)

        return ret

    def getLastManualReminderDatetime(self, depid, b_use_cache=True, test_folder=None):
        """Return date of last reminder message to depositor as python datetime.
        System-sent messages in archived notes file are not counted.
        """
        ret = None
        self.__readMsgFile(depid, contentType="messages-to-depositor", b_use_cache=b_use_cache, test_folder=test_folder)
        if len(self.__lc) >= 1:
            ret_by_context_type = self.__selectLastMsgByContextType(["reminder"])  # CS 2024-04-04 search by context_type first
            if ret_by_context_type:
                ret = ret_by_context_type
            else:
                ret = self.__selectLastMsgByTitlePhrase(phrase='Still awaiting feedback for')
        else:
            logger.info("Deposition %s empty message file", depid)

        return ret

    def getLastReleaseNoticeDatetime(self, depid, b_use_cache=True, test_folder=None):
        """Return date of last release notice to depositor as python datetime.
        """
        ret = None
        self.__readMsgFile(depid, contentType="messages-to-depositor", b_use_cache=b_use_cache, test_folder=test_folder)
        if len(self.__lc) >= 1:
            ret_by_context_type = self.__selectLastMsgByContextType(["release-publ", "release-nopubl"])  # CS 2024-04-04 search by context_type first
            if ret_by_context_type:
                ret = ret_by_context_type
            else:
                ret = self.__selectLastMsgByTitlePhrase(phrase='Release of')
        else:
            logger.info("Deposition %s empty message file", depid)

        return ret

    def getLastUnlockDatetime(self, depid, b_use_cache=True, test_folder=None):
        """Return date of last unlock message to depositor as python datetime.
        alias function of getLastUnlocked to standardize return type and name style for function calls
        """
        s_datetime = self.getLastUnlocked(depid, b_use_cache=b_use_cache, test_folder=test_folder)
        if s_datetime:
            return self.convertStrToDatetime(s_datetime)
        else:
            return None

    def getLastUnlocked(self, depid, b_use_cache=True, test_folder=None):
        """Returns datetime of the last time an unlocked message was sent.  This is parallel to the code in
        MessagingIo.py

        If no unlock message has been sent, will return None
        """

        # msgfile = self.__pI.getFilePath(depid, contentType="messages-to-depositor", formatType="pdbx", fileSource="archive", versionId="1")

        # if not os.path.exists(msgfile):
        #     return None

        # ret = None

        # myContainerList = []
        # ifh = open(msgfile, "r")
        # pRd = PdbxReader(ifh)
        # pRd.read(myContainerList)

        # if len(myContainerList) >= 1:
        #     c0 = myContainerList[0]

        ret = None
        self.__readMsgFile(depid, contentType="messages-to-depositor", b_use_cache=b_use_cache, test_folder=test_folder)

        if len(self.__lc) >= 1:
            c0 = self.__lc[0]
            catObj = c0.getObj("pdbx_deposition_message_info")
            if catObj is None:
                logger.debug("Deposition %s no pdbx_deposition_message_info category", depid)
                return None
            else:
                #
                # Get column name index.
                #
                itDict = {}
                itNameList = catObj.getItemNameList()
                for idxIt, itName in enumerate(itNameList):
                    itDict[str(itName).lower()] = idxIt
                    #
                idxOrdinalId = itDict["_pdbx_deposition_message_info.ordinal_id"]
                idxLastCommDate = itDict["_pdbx_deposition_message_info.timestamp"]
                idxMsgSubj = itDict["_pdbx_deposition_message_info.message_subject"]
                idxContextType = itDict["_pdbx_deposition_message_info.context_type"]

                maxUnlockOrdId = 0
                for row in catObj.getRowList():
                    try:
                        ordinalId = int(row[idxOrdinalId])
                        msgsubj = row[idxMsgSubj]
                        context_type_recorded = row[idxContextType]

                        if context_type_recorded == "system-unlocked" or msgsubj == "System Unlocked":
                            if ordinalId > maxUnlockOrdId:
                                maxUnlockOrdId = ordinalId
                                ret = str(row[idxLastCommDate])

                    except Exception as e:
                        logger.error("Error processing %s %s", depid, str(e))

        else:
            logger.debug("Deposition %s empty message file", depid)
            return None

        return ret

    def getLastValidation(self, depid, b_use_cache=True, test_folder=None):
        """Returns (datetime, major issue)  of the last time a validation was sent as determined by
        presence of attached validation pdf and parsing message.
        """

        logger.info("Starting for deposition %s", depid)

        # msgfile = self.__pI.getFilePath(depid, contentType="messages-to-depositor", formatType="pdbx", fileSource="archive", versionId="1")

        # if not os.path.exists(msgfile):
        #     return (None, None)

        # ret = None
        # major = None

        # myContainerList = []
        # ifh = open(msgfile, "r")
        # pRd = PdbxReader(ifh)
        # pRd.read(myContainerList)

        # if len(myContainerList) >= 1:
        #     c0 = myContainerList[0]

        lastvalid = None
        major = None

        self.__readMsgFile(depid, contentType="messages-to-depositor", b_use_cache=b_use_cache, test_folder=test_folder)

        if len(self.__lc) >= 1:
            logger.info("start searching for last validation letter by context_type")
            (lastvalid, major) = self.__getLastValidationByContextType()  # CS 2024-04-04 first search by context_type/context_value
            if lastvalid:
                logger.info("found last validation letter by context_type")
                return (lastvalid, major)  # return if find message by context_type/context_value

            logger.info("fail to find last validation letter by context_type, default to search by subject/text parsing")

            c0 = self.__lc[0]
            catObj = c0.getObj("pdbx_deposition_message_file_reference")
            if catObj is None:
                logger.debug("Deposition %s no pdbx_deposition_message_file_reference category", depid)
                return (None, None)

            # Get list of msgids of validtion report
            msgids = set()

            #
            # Get column name index.
            #
            itDict = {}
            itNameList = catObj.getItemNameList()
            for idxIt, itName in enumerate(itNameList):
                itDict[str(itName).lower()] = idxIt

            idxMsgId = itDict["_pdbx_deposition_message_file_reference.message_id"]
            idxContentType = itDict["_pdbx_deposition_message_file_reference.content_type"]

            for row in catObj.getRowList():
                try:
                    msgId = row[idxMsgId]
                    cT = row[idxContentType]

                    if "validation-report-annotate" in cT:
                        msgids.add(msgId)
                        # msgids collects messages with validation report attached, but not necessary letter of validation itself
                        # e.g. release letter also attached validation report, will need to check message subject later

                except Exception as e:
                    logger.exception("Error processing %s %s", depid, str(e))

            if len(msgids) == 0:
                return (None, None)

            logger.debug("Message ids: %s", msgids)

            # Now check messages

            catObj = c0.getObj("pdbx_deposition_message_info")
            if catObj is None:
                logger.debug("Deposition %s no pdbx_deposition_message_info category", depid)
                return (None, None)

            #
            # Get column name index.
            #
            itDict = {}
            itNameList = catObj.getItemNameList()
            for idxIt, itName in enumerate(itNameList):
                itDict[str(itName).lower()] = idxIt
            #
            idxTimeStamp = itDict["_pdbx_deposition_message_info.timestamp"]
            idxMsgId = itDict["_pdbx_deposition_message_info.message_id"]
            idxMsgText = itDict["_pdbx_deposition_message_info.message_text"]
            idxSendStatus = itDict["_pdbx_deposition_message_info.send_status"]
            idxMsgSubject = itDict["_pdbx_deposition_message_info.message_subject"]

            for row in catObj.getRowList():
                try:
                    timeStamp = datetime.datetime.strptime(str(row[idxTimeStamp]), "%Y-%m-%d %H:%M:%S")
                    msgId = str(row[idxMsgId])
                    status = str(row[idxSendStatus])
                    msgText = str(row[idxMsgText])
                    msgSubject = str(row[idxMsgSubject])

                    if status == "Y" and msgId in msgids:
                        if re.search("processed files are ready for your review", msgSubject, re.IGNORECASE):  # check subject subject to confirm validation letter
                            if lastvalid:
                                if timeStamp > lastvalid:
                                    # logger.debug("Updating lastvalid %s %s", lastvalid, timeStamp)
                                    lastvalid = timeStamp
                                    major = self._majorValidation(msgText)
                            else:
                                lastvalid = timeStamp
                                major = self._majorValidation(msgText)
                        else:
                            pass  # ignore message with validation letter but is not validation letter, e.g. release notice
                    else:
                        pass  # ignore message not sent; ignore message not with validation report attached
                except Exception as e:
                    logger.exception("Error processing %s %s", depid, str(e))
        else:
            logger.debug("Deposition %s empty message file", depid)

        logger.info("finished searching for last validation letter by subject/text parsing")
        logger.info("Returning (%s, %s)", lastvalid, major)
        return (lastvalid, major)

    def _majorValidation(self, msgText):
        """Returns true if there appears to be a major error in the validation test - otherwise False"""

        ret = False
        if re.search("Some major issues", msgText, re.IGNORECASE) is not None:
            ret = True

        # logger.debug("Major validation %s", ret)
        return ret

    def getLastSentMessageDate(self, depid, msgtodepositor, b_use_cache=True, test_folder=None):
        """Returns datetime  of the ast message sent.  Will return None if no messages sent.
        msgtodepositor is boolean indicating which direction message sent
        """

        logger.info("Starting for deposition %s msgtodepositor %s", depid, msgtodepositor)

        if msgtodepositor:
            msg_content = "messages-to-depositor"
        else:
            msg_content = "messages-from-depositor"

        # msgfile = self.__pI.getFilePath(depid, contentType=msg_content, formatType="pdbx", fileSource="archive", versionId="1")

        # if not os.path.exists(msgfile):
        #     return None

        # ret = None

        # myContainerList = []
        # ifh = open(msgfile, "r")
        # pRd = PdbxReader(ifh)
        # pRd.read(myContainerList)

        # if len(myContainerList) >= 1:
        #     c0 = myContainerList[0]

        ret = None
        self.__readMsgFile(depid, contentType=msg_content, b_use_cache=b_use_cache, test_folder=test_folder)

        if len(self.__lc) >= 1:
            c0 = self.__lc[0]
            # Now check messages

            catObj = c0.getObj("pdbx_deposition_message_info")
            if catObj is None:
                logger.debug("Deposition %s no pdbx_deposition_message_info category", depid)
                return None

            #
            # Get column name index.
            #
            itDict = {}
            itNameList = catObj.getItemNameList()
            for idxIt, itName in enumerate(itNameList):
                itDict[str(itName).lower()] = idxIt
            #
            idxTimeStamp = itDict["_pdbx_deposition_message_info.timestamp"]
            idxSendStatus = itDict["_pdbx_deposition_message_info.send_status"]

            lastsent = None

            for row in catObj.getRowList():
                try:
                    timeStamp = datetime.datetime.strptime(str(row[idxTimeStamp]), "%Y-%m-%d %H:%M:%S")
                    status = str(row[idxSendStatus])

                    if status == "Y":
                        if lastsent:
                            if timeStamp > lastsent:
                                # logger.debug("Updating lastvalid %s %s", lastsent, timeStamp)
                                lastsent = timeStamp
                        else:
                            lastsent = timeStamp

                except Exception as e:
                    logger.exception("Error processing %s %s", depid, str(e))

            ret = lastsent

        else:
            logger.debug("Deposition %s empty message file", depid)
            ret = None

        logger.info("Returning %s", ret)

        return ret

    def getPendingDepositorMessages(self, depid, b_use_cache=True, test_folder=None):  # pylint: disable=unused-argument
        """Returns list of messages that have been sent by depositor and pending action present"""

        logger.info("Starting for deposition %s", depid)

        dep_fpath = self.__getMsgFilePath(depid, "messages-from-depositor", test_folder=None)
        bio_fpath = self.__getMsgFilePath(depid, "messages-to-depositor", test_folder=None)

        pdbxMsgIo_frmDpstr = PdbxMessageIo(verbose=self.__verbose, log=self.__log)
        ok = pdbxMsgIo_frmDpstr.read(dep_fpath)
        if not ok:
            return []

        depRecordSetLst = (
            pdbxMsgIo_frmDpstr.getMessageInfo()
        )  # in recordSetLst we now have a list of dictionaries with item names as keys and respective data for values

        pdbxMsgIo_toDpstr = PdbxMessageIo(verbose=self.__verbose, log=self.__log)
        ok = pdbxMsgIo_toDpstr.read(bio_fpath)
        if not ok:
            # Assume all messages unacknowledged
            return depRecordSetLst

        bioStatusSetLst = (
            pdbxMsgIo_toDpstr.getMsgStatusInfo()
        )

        ret = []
        for dep in depRecordSetLst:
            msgid = dep["message_id"]

            found = False
            for s in bioStatusSetLst:
                if s["message_id"] == msgid:
                    found = True
                    if s["action_reqd"] == "Y":
                        ret.append(dep)
                    break

            if not found:
                ret.append(dep)

        return ret

    def getApprovalNoCorrectSubjects(self):
        """Returns list of subjects used for approval without corrections"""
        cI = ConfigInfo(self.__siteId)
        return cI.get("COMMUNICATION_APPROVAL_WITHOUT_CHANGES_MESSAGE_SUBJECTS")
