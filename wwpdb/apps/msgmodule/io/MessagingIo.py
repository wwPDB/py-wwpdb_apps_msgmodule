##
#
# File:    MessagingIo.py
# Author:  R. Sala
# Date:    02-Feb-2012
# Version: 0.001
# Updates:
#    2012-04-26    RPS    Created.
#    2012-07-25    RPS    Updated to use ConfigInfo for obtaining SITE_MSG_DB_USER_ID and SITE_MSG_DB_USER_PWD  
#                            credentials needed for accessing messaging database
#    2012-07-27    RPS    Improved handling for nested display of threaded discussions.
#    2013-09-03    RPS    Updated to use cif-based backend storage. Augmented functionality for sending/reading messages including support
#                            for associating file references.
#    2013-09-11    RPS    More changes in support of associating file references.
#                            Accommodating creation of milestone copies on both sides of deposition <---> annotation
#    2013-09-11    RPS    Piloting feature for automatic generation of no-reply notification emails.
#    2013-09-12    RPS    Switching from use of localtime to GMT time for standardization of message timestamps across sites.
#    2013-09-13    RPS    added checkAvailFiles() so that "Associate Files" checkboxes now only appear in UI for files that actually are available on serverside.
#                            Support for displaying confirmation of files referenced when displaying a message in UI.
#    2013-09-20    RPS    Now accommodating model file in PDB format as well as cif/pdbx format.
#    2013-09-23    RPS    Updates to extend handling of milestone file references. Corrected bug relating to handling of "model_pdb" content type.
#    2013-09-24    RPS    Updated wording of email notification to depositors.
#                            Removed unnecessary logic for milestone version consistency checking.
#                            Improved handling of milestone file references for deposit storage.
#    2013-09-27    RPS    Improvements in messaging data import/export interface.
#    2013-10-13    RPS    updateDraftState() corrected so now using GMT time now instead of local time
#    2013-10-14    RPS    Providing file upload input on Compose Message, to accommodate attachment of auxiliary file.
#    2013-10-28    RPS    Addressed issue of redundancy in registering read status unnecessarily for msgIDs already handled.  
#    2013-10-30    RPS    Support for "Notes" UI and for UI feature for classifying messages (e.g. "action required" or "unread")
#    2013-11-19    RPS    Ensuring notification email does not get sent for drafts.
#                            Correcting notification and attachment behavior for scenario when drafts converted to actual sent msgs
#    2013-11-19    RPS    Added support for obtaining content from "correspondence-to-depositor" files to integrate within starter template for message body
#    2013-12-02    RPS    Added confirmation of PDB ID to template used for notification emails to depositors. 
#    2013-12-05    RPS    Added support for handling generation of "review" version of model files in comms to depositor.
#                            Including URL to deposition site in notification emails to depositor.
#    2014-01-14    RPS    Updates in message templates processing. Modifications for "action required" and "read/unread" message tagging.
#    2014-01-22    RPS    Updated sendNotificationEmail to accommodate PRODUCTION hostnames
#    2014-01-26    RPS    Updates to help troubleshoot issue with global message status handling. getMsgTmpltDataItems() updated with default values in case no model file available.
#    2014-01-28    RPS    checkAvailFiles() and __handleFileReferences() updated to support 'val-report-full' contentType (pdf contentFormat).
#    2014-02-01    RPS    Additional updates to sendNotificationEmail to accommodate PRODUCTION hostnames
#    2014-02-07    RPS    Setting default value to "[NOT AVAILABLE]" for placeholders in message templates when cif categories are not available.
#    2014-02-25    RPS    Quick fix to eliminate bug of double-spacing when integrating correspondence-to-depositor content.
#    2014-02-25    RPS    Fix for correct parsing of citation authors from model file.
#    2014-03-04    RPS    Updates to allow tracking of instances wherein new "-annotate" version of cif model file is being propagated to Deposition side.
#                            and also allow tracking of instances wherein validation reports are being propagated to Deposition side.
#                            Also Introduced support for message templates related to validation.
#    2014-03-17    RPS    Updated to vend message content in way that supports preservation of linebreaks when copying/pasting.
#    2014-03-21    RPS    Fixed bug preventing parsing of date of last communication to depositor.
#    2014-03-24    RPS    Improved handling during submitMsg so that error message in browser regarding "problem submitting the message" is not falsely fired.
#    2014-03-25    RPS    Now creating archive copies of any notification emails by sending to 'da-notification-mail@mail.wwpdb.org'
#    2014-03-18    RPS    Support for UTF-8 capture/persistence into cif files.
#    2014-04-03    RPS    Fix to allow continued processing in event of missing 'pdbx_database_status.recvd_initial_deposition_date'
#    2014-04-30    RPS    Clean-up of code for message template handling.
#    2014-06-03    RPS    Fix for handling of files with 'cif' extension explicitly as 'pdbx' formatted files.
#    2014-06-04    RPS    Replacing use of ann_tasks with ann_tasks_v2 for generation of "public" versions of model files.
#    2014-06-09    RPS    Replacing use of ann_tasks_v2 with use of RcsbDpUtility API for generation of "public" versions of model files.
#                            Also, now accommodating up to 2 additional auxiliary file references.
#                            Updated handling for citation related info so that these data points are rendered in related message tempalte
#                            only when the given data items are valid/populated.
#    2014-08-13    RPS    Added autoMsg(self,p_depIdList,p_tmpltType="release-publ",p_sender="auto") to prototype support for automated messaging/email notification
#                             generation to be invoked by ReleaseModule.
#    2014-08-18    RPS    Removing use of defunct "BeautifulSoup" implementation in getMsgRowList()
#    2014-08-20    RPS    Updated __getContactAuthors() to use "Brain Page" contact info if email notification being sent out prior to submission (i.e. no model file avail yet)
#    2014-09-23    RPS    Integrated use of PdbxPersist to minimize reliance on parsing model file and __getContactAuthors() moved to MsgTmpltHlpr class
#                            to improve response times on front-end when sending messages in cases of very large model files (e.g. ribosomes)
#    2014-10-08    RPS    MsgTmpltHlpr updated to use __authRelStatusCode instead of __statusCode for determining expiration date used in reminder letters.
#    2014-10-16    RPS    Added exception handler to trap cases where annotator "attaches" file with unfamiliar extension.
#    2014-12-04    RPS    Updates for: "archive" of messages, new "Complete Correspondence History" view, tagging messages "for Release", checking for
#                            presence of notes. Now self.__defaultMsgTmpltType = "vldtn". Introducing file references for NMR experimental method.
#    2014-12-05    RPS    added getNotesList() method to provide inventory of Notes message IDs
#    2014-12-08    RPS    self.__getReleaseDateInfo() updated to derive correct release date in cases where entry tagged for release on a Saturday or Sunday.
#    2014-12-10    RPS    anyNotesExist() updated so that can distinguish notes actually authored by annotators from those notes that result from archived comms.
#    2014-12-18    RPS    updated to accommodate handling of automated "reminder" notifications
#    2014-12-23    RPS    short-term fix to eliminate display of '\xa0' in message text.
#    2015-01-29    RPS    Updated call to dbAPI.runSelectNQ() for getting brain contact details to work with version 2.0 of deposition system.
#    2015-02-12    RPS    Updated with measures to better track updates to messages-to-depositor data.
#    2015-02-13    RPS    Updated with file-locking measures to better manage updates to messages-to-depositor data.
#    2015-03-02    RPS    Updates per introduction of sanity check safeguards on writes to messaging cif data files.
#    2015-03-03    RPS    Eliminating output of misleading error messages that occur when no content yet present in messaging data files.
#    2015-03-06    RPS    Updated to provide reply redirect in email messages sent via autoMsg.
#    2015-03-11    RPS    Improving display of entry title within horiz rules in message templates
#    2015-04-10    RPS    Improving information provided about change request deadlines in message templates used for release notifications.
#    2015-05-08    RPS    Introducing support for custom message templates relative to different experimental methods.
#                            Adjustment to display info about change request deadlines on Saturdays
#    2015-05-29    RPS    Addressed bug whereby contacts from "brain page" were not receiving email notifications of new messages when deposition was pre-submission.
#    2015-07-10    RPS    Updates to support EM and NMR experimental methods.
#    2015-09-17    RPS    Updated so that email notifications are sent as group email to contact authors (i.e. as opposed to single dedicated copy to each contact author).
#                            Migrating template pieces used for email notifications to MessagingTemplate module.
#    2015-10-07    RPS    Added self.__getLastOutboundRprtDate() to correctly obtain value as derived from timestamp of most recent version of validation report.
#                            Activating support for multiple file attachments.
#    2015-10-13    RPS    updated MsgTmpltHlpr.__getContactAuthors() to accommodate version 1.5 backwards compatibility of database entries when querying for brain contacts.
#    2015-10-14    RPS    updated MsgTmpltHlpr with additional support for more experimental methods.
#                            Updated __sendNotificationEmail() to correct for formatting issue in greeting of email.
#    2015-10-22    RPS    introducing (but not activating) support (use of EmHeaderUtils) for generating XML header file from EM model file when required by message
#    2015-10-27    RPS    Fix to have chem shifts review files generated with .str extensions
#    2015-11-05    RPS    correcting bug with self.__rqstdAccessionIdsLst default value.
#    2015-10-28    RPS    More updates to support EM specific message templates.
#    2015-12-02    RPS    Updates to optimize response time with use of selective cif parsing and by running template processing in background.
#                            Removed obsolete processing for validation letter content (validation content no longer generated by Msgmodule)
#    2015-12-10    RPS    Updated manner in which obtaining URL for deposition webpage
#    2016-01-04    RPS    Fixed bug preventing successful persistence of "citation" category into local session database
#    2016-01-24    RPS    Updated to support auto release notifications for EM entries, and new template for EM map-only, post-annotation letter
#    2016-02-17    RPS    Introducing use of Message model class to increase encapsulation, improve code organization.
#    2016-02-23    RPS    Treating "ELECTRON CRYSTALLOGRAPHY" exp method same as "ELECTRON MICROSCOPY" for purposes of messaging UI
#    2016-02-29    RPS    Improved implementation of getMsgColList(), by eliminating use of dictionary keys() method to get list of field names.
#                            Instead using PdbxMessageCategoryStyle.getAttributeNameList('pdbx_deposition_message_info').
#    2016-06-28    RPS    Addressed bug affecting processing for EM letter templates when entry is hybrid EM/NMR deposition 
#    2016-08-09    RPS    Strengthening trace logging and preventive measures to monitor/safeguard against problematic concurrent access to messaging cif data files.
#                            Changes to support site/annotator specific footers in message templates.
#                            Updates to EM letter templates in order to correctly distinguish between map-only and map+model releases.
#                            Introducing support for standalone correspondence viewer. Providing means for detecting presence of notes archived via BMRB emails.
#    2016-09-14    ZF     Added support for group deposition message files
#    2017-08-18    RPS    Accommodating updates in "withdrawn" letter template
#    2017-10-09    RPS    Adjusting signoff content for EM Map Only cases.
#    2018-03-27    EP     Calculate one year expiration date for REL entries for reminder template
#    2018-04-23    EP     Support not copying milestones to deposit directory with switch. Transition most to logging.
#    2018-05-07    EP     Do not bump version numbers of milestones if same. Multiple messages may refer to same file.
##
"""
Class to manage persistence/retrieval of messaging data

"""
__docformat__ = "restructuredtext en"
__author__    = "Raul Sala"
__email__     = "rsala@rcsb.rutgers.edu"
__license__   = "Creative Commons Attribution 3.0 Unported"
__version__   = "V0.01"

import sys, traceback, time, os, os.path, shutil, re, operator, smtplib, base64, mimetypes, textwrap
try:
    from html import unescape
except ImportError:
    from HTMLParser import HTMLParser
from datetime import datetime, date, timedelta
from dateutil import tz
#
from mmcif_utils.message.PdbxMessage      import PdbxMessageInfo, PdbxMessageFileReference, PdbxMessageOrigCommReference, PdbxMessageStatus
from mmcif_utils.message.PdbxMessageIo    import PdbxMessageIo
from mmcif_utils.style.PdbxMessageCategoryStyle import PdbxMessageCategoryStyle
#
from mmcif.io.PdbxReader import PdbxReader
#
from wwpdb.utils.config.ConfigInfo                      import ConfigInfo
from wwpdb.apps.msgmodule.io.MessagingDataImport        import MessagingDataImport
from wwpdb.apps.msgmodule.io.MessagingDataExport        import MessagingDataExport
from wwpdb.utils.wf.dbapi.StatusDbApi                   import StatusDbApi
from wwpdb.apps.msgmodule.depict.MessagingTemplates     import MessagingTemplates
from wwpdb.apps.msgmodule.models.Message                import AutoMessage, AutoNote
from wwpdb.apps.msgmodule.io.DateUtil                   import DateUtil
#
from wwpdb.utils.dp.RcsbDpUtility                       import RcsbDpUtility
from wwpdb.utils.dp.DataFileAdapter                     import DataFileAdapter
from wwpdb.utils.wf.dbapi.dbAPI                         import dbAPI
#
from mmcif_utils.persist.PdbxPersist   import PdbxPersist
from mmcif_utils.persist.LockFile      import LockFile
from mmcif.io.IoAdapterCore            import IoAdapterCore
from mmcif_utils.trans.InstanceMapper import InstanceMapper
#
# Here for now - should be relocated.
from wwpdb.apps.msgmodule.io.EmHeaderUtils import EmHeaderUtils

import os
import filecmp

import logging
logger = logging.getLogger(__name__)


class MessagingIo(object):
    # List of categories to parse from model file
    ctgrsReqrdFrmModelFile = ['audit_author',
                          'em_author_list',
                          'struct',
                          'em_admin',
                          'database_2',
                          'pdbx_depui_entry_details',
                          'pdbx_database_status',
                          'em_admin',
                          'em_depui',
                          'citation_author',
                          'citation',
                          'pdbx_contact_author']
    
    bMakeEmXmlHeaderFiles = False
    
    def __init__(self,reqObj,verbose=False,log=sys.stderr):
        self.__lfh=log
        self.__verbose=verbose
        self.__debug=True
        self.__debugLvl2=False
        self.__devMode=False
        self.__NOTIF_TESTING = False
        self.__allowingMultiAuxFiles = True
        # If will symlink to -annotate milestones in deposit
        self.__symlinkDepositAnnotate = True
        # If will copy milestone files to deposit
        self.__copyMilestoneDeposit = False
        # If will not bump version files if identical. Message will reference same version
        self.__skipCopyIfSame = True
        #
        self.__reqObj=reqObj
        #
        # Added by ZF
        #
        self.__groupId = str(self.__reqObj.getValue("groupid"))
        # 
        self.__sObj=self.__reqObj.newSessionObj()
        self.__sessionPath=self.__sObj.getPath()
        self.__sessionRelativePath=self.__sObj.getRelativePath()        
        self.__sessionId  =self.__sObj.getId()
        #
        self.__expMethodList = (self.__reqObj.getValue("expmethod").replace('"','')).split(",") if( len(self.__reqObj.getValue("expmethod").replace('"','')) > 1 ) else []
        self.__emDeposition = True if( "ELECTRON MICROSCOPY" in self.__expMethodList or "ELECTRON CRYSTALLOGRAPHY" in self.__expMethodList ) else False
        #
        self.__siteId  = str(self.__reqObj.getValue("WWPDB_SITE_ID"))
        self.__cI=ConfigInfo(self.__siteId)
        self.__emdDialectMappingFile = self.__cI.get('SITE_EXT_DICT_MAP_EMD_FILE_PATH')
        self.__contentTypeDict = self.__cI.get('CONTENT_TYPE_DICTIONARY')
        # 
        self.__dbFilePath = os.path.join(self.__sessionPath,"modelFileData.db")
        #
        self.__notifEmailArchAddress = 'da-notification-mail@mail.wwpdb.org'
        #
        # Parameters to tune lock file management -- 
        self.__timeoutSeconds = 10
        self.__retrySeconds   = 0.2
        
        # BELOW SETTINGS ARE DEFAULTS THAT KICK IN FOR TESTING PURPOSES
        self.__testMsgFilePath = '/net/wwpdb_da/da_top/wwpdb_da_test/source/python/pdbx_v2/message/testMessageFile.cif'
        self.__msgsToDpstrFilePath = '/net/wwpdb_da/da_top/wwpdb_da_test/source/python/pdbx_v2/message/messages-to-depositor.cif'
        self.__msgsFrmDpstrFilePath = '/net/wwpdb_da/da_top/wwpdb_da_test/source/python/pdbx_v2/message/messages-from-depositor.cif'
        self.__notesFilePath = '/net/wwpdb_da/da_top/wwpdb_da_test/source/python/pdbx_v2/message/notes-from-annotator.cif'

    def setGroupId(self, groupId):
        #
        # Added by ZF
        #
        self.__groupId = groupId
        
    def initializeDataStore(self):
        """Internalize data files"""
        if( self.__isWorkflow() ):
            logger.info("--------------------------------------------")
            logger.info("Starting at %s\n" % 
                        time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
            #
            if( not os.access(self.__dbFilePath,os.R_OK) ):
                logger.debug("DB File not present")
                
                
                msgDI=MessagingDataImport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
                modelFilePath = msgDI.getFilePath(contentType='model',format='pdbx')
                
                # parse info from model file
                if( modelFilePath is not None and os.access(modelFilePath,os.R_OK) ):    
                    #
                    containerList = []
                    try:
                        #########################################################################################################
                        # parse model cif file and verify blockname
                        #########################################################################################################
                        pdbxReader = IoAdapterCore(self.__verbose,self.__lfh)
                        containerList = pdbxReader.readFile(inputFilePath=modelFilePath,selectList=MessagingIo.ctgrsReqrdFrmModelFile)

                        iCountNames = len(containerList)
                        assert( iCountNames == 1 ), ("+%s.%s() -- expecting containerList to have single member but list had %s members\n" % (self.__class__.__name__,
                                                                                                                                              sys._getframe().f_code.co_name,
                                                                                                                                              iCountNames) )                        

                        dataBlockName = containerList[0].getName().encode('utf-8')
                        logger.debug("--------------------------------------------\n")        
                        logger.debug("identified datablock name %s in sample pdbx data file at: %s\n" % (
                                dataBlockName, modelFilePath )) 
                        #
                    #
                    except:
                        logger.exception("problem processing pdbx data file: %s" % 
                                modelFilePath)
                                                             
                    try:
                        myPersist=PdbxPersist(self.__verbose,self.__lfh)
                        myPersist.setContainerList(containerList)
                        myPersist.store(self.__dbFilePath)

                        logger.debug("shelved cif data to %s\n" % self.__dbFilePath)
                        
                    except:
                        logger.exception("Failed to shelve cif data")
                    
                else:
                    logger.debug("pdbx data file not found/accessible at: %s\n" % modelFilePath)
            else:
                logger.info("skipping creation of database file b/c already exists at: %s\n" % self.__dbFilePath)
        else:
            logger.debug("Not a workflow - noop")
        #
     
    def getMsgColList(self,p_bCommHstryRqstd=False):
        ''' Retrieval of list of attributes (i.e. columns) for message data
        '''        
        self.__lfh.write("--------------------------------------------\n")        
        self.__lfh.write("Starting %s %s at %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime())))                
        rtrnList = []
        bSuccess = False
        
        commHistoryAttributesLst = ['orig_message_id','orig_deposition_data_set_id','orig_timestamp','orig_sender','orig_recipient','orig_message_subject','orig_attachments']
        # [12,13,14,15,16,17,18]
        try:
            messageStyleObj = PdbxMessageCategoryStyle()        
            rtrnList=messageStyleObj.getAttributeNameList('pdbx_deposition_message_info')
            # ['ordinal_id', 'message_id', 'deposition_data_set_id', 'timestamp', 'sender', 'context_type', 'context_value', 'parent_message_id', 'message_subject', 'message_text', 'message_type', 'send_status']
            
            if( p_bCommHstryRqstd is True ):
                rtrnList.extend(commHistoryAttributesLst)
            
            if( rtrnList ):
                bSuccess = True
                #
                if( self.__verbose ):
                    self.__lfh.write("+%s.%s() -- Column list for message info returned as:\n %s\n" % (self.__class__.__name__,
                                                                                      sys._getframe().f_code.co_name,
                                                                                      rtrnList) )
                #

        #
        except:
            traceback.print_exc(file=self.__lfh)

        return bSuccess, rtrnList
    
    def getMsg(self,p_msgId,p_depId):
        """ Get data for a single message 
            
            :Helpers:
                wwpdb.apps.msgmodule.io.MessagingDataImport
                mmcif_utils.message.PdbxMessageIo
                
            :param `p_msgId`:    unique message ID
            :param `p_depId`:    ID of deposition dataset for message being requested
            
            :Returns:
                dictionary representing a given message and its attributes
                 
        """       
        self.__lfh.write("--------------------------------------------\n")        
        self.__lfh.write("Starting %s %s at %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        contentType = self.__reqObj.getValue("content_type")
        self.__lfh.write("+%s.%s() -- contentType is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, contentType) )
        bCommHstryRqstd = True if( contentType == "commhstry" ) else False                
        msgDict = {}
        bSuccess = False
        recordSetLst = []
        '''
         {'context_value': 'validation', 'sender': 'annotator', 'context_type': 'report', 'timestamp': '2013-08-15 09:18:15', 'ordinal_id': '7', 'message_subject': 'RE: Arginine residue clarification', 'message_id': '7164e940-9f6c-43e6-9ea8-b78226c75e1f', 'deposition_data_set_id': 'D_000000', 'message_text': 'Will look into this.', 'send_status': 'N', 'message_type': 'text', 'parent_message_id': 'ff9f8dfd-4a69-43ab-815e-617ce866e0d0'}
        '''
        
        try:
            if self.__isWorkflow():
                # determine path(s) to cif datafiles that contain the data we are seeking
                msgDI=MessagingDataImport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
                if( contentType == "msgs" or bCommHstryRqstd ):
                    self.__msgsFrmDpstrFilePath = msgDI.getFilePath(contentType='messages-from-depositor',format="pdbx")
                    self.__msgsToDpstrFilePath = msgDI.getFilePath(contentType='messages-to-depositor',format="pdbx")
                if( contentType == "notes" or bCommHstryRqstd ):
                    self.__notesFilePath = msgDI.getFilePath(contentType='notes-from-annotator',format="pdbx")
                
            # obtain data from relevant datafiles based on contentType requested                
            if( contentType == "msgs" or bCommHstryRqstd ):
                self.__lfh.write("+%s.%s() -- self.__msgsFrmDpstrFilePath is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, self.__msgsFrmDpstrFilePath) )
                self.__lfh.write("+%s.%s() -- self.__msgsToDpstrFilePath is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, self.__msgsToDpstrFilePath) )
            
                if self.__msgsFrmDpstrFilePath is not None and os.access(self.__msgsFrmDpstrFilePath,os.R_OK):
                    pdbxMsgIo_frmDpstr=PdbxMessageIo(verbose=self.__verbose,log=self.__lfh)
                    ok=pdbxMsgIo_frmDpstr.read(self.__msgsFrmDpstrFilePath)
                    if( ok ):
                        recordSetLst = pdbxMsgIo_frmDpstr.getMessageInfo() # in recordSetLst we now have a list of dictionaries with item names as keys and respective data for values
                        
                if self.__msgsToDpstrFilePath is not None and os.access(self.__msgsToDpstrFilePath,os.R_OK):
                    pdbxMsgIo_toDpstr=PdbxMessageIo(verbose=self.__verbose,log=self.__lfh)
                    ok=pdbxMsgIo_toDpstr.read(self.__msgsToDpstrFilePath)
                    if( ok ):
                        recordSetLst.extend(pdbxMsgIo_toDpstr.getMessageInfo()) # in recordSetLst we now have a list of dictionaries with item names as keys and respective data for values
                        
                            
            if( contentType == "notes" or bCommHstryRqstd ):
                self.__lfh.write("+%s.%s() -- self.__notesFilePath is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, self.__notesFilePath) )
            
                if self.__notesFilePath is not None and os.access(self.__notesFilePath,os.R_OK):
                    pdbxMsgIo_notes=PdbxMessageIo(verbose=self.__verbose,log=self.__lfh)
                    ok=pdbxMsgIo_notes.read(self.__notesFilePath)
                    
                    if( ok ):
                        if( contentType == "notes" ):
                            recordSetLst = pdbxMsgIo_notes.getMessageInfo() # in recordSetLst we now have a list of dictionaries with item names as keys and respective data for values
                        elif( bCommHstryRqstd ):
                            fullNotesLst = pdbxMsgIo_notes.getMessageInfo()
                            onlyArchvdCommsLst = [record for record in fullNotesLst if( "archive" in record['message_type'] )]
                            recordSetLst.extend(onlyArchvdCommsLst)
                                        
            
            for record in recordSetLst:
                
                if( record['message_id'] == p_msgId ):
                    msgType = record['message_type']
                    record['timestamp'] = self.__convertToLocalTimeZone(record['timestamp'])
                    
                    msgText = self.__decodeCifToUtf8(record['message_text'])
                    if( msgType not in ['archive_manual','forward_manual'] ):
                            msgText = self.__protectAngleBrackets( msgText )
                    record['message_text'] = self.__protectLineBreaks( msgText )
                    
                    msgSubj = self.__decodeCifToUtf8(record['message_subject'])
                    if( msgType not in ['archive_manual','forward_manual'] ):
                            msgSubj = self.__protectLineBreaks( self.__protectAngleBrackets( msgSubj ) )
                    record['message_subject'] = msgSubj
                    
                    record['sender'] = self.__protectAngleBrackets( record['sender'] )
                    
                    msgDict = record

        #
        except:
            traceback.print_exc(file=self.__lfh)

        return msgDict

    
    def getMsgRowList( self, p_depDataSetId, p_sSendStatus='Y', p_bServerSide=False, p_iDisplayStart=None, p_iDisplayLength=None, p_sSrchFltr=None, p_colSearchDict=None, p_bThreadedRslts=False ):
        ''' Retrieval of messages for a given deposition dataset ID
            
            :param `p_depDataSetId`:       ID of deposition dataset for which list of messages being requested
            :param `p_bServerSide`:        boolean indicating whether server-side processing is being utilized
            
            ONLY USED IF p_bServerSide IS True:
            :param `p_iDisplayStart`:      DataTables related parameter for indicating start index of record
                                             for set of records currently being retrieved for display on screen 
            :param `p_iDisplayLength`:     DataTables related parameter for indicating limit of total records
                                            to be displayed on screen (i.e. only subset of entire resultset is being shown)
            :param `p_sSrchFltr`:          DataTables related parameter indicating search term against which records will be filtered
            :param `p_bThreadedRslts`:    Whether the messages are to be displayed in conventional chronological order or threaded message view
            

        '''        
        self.__lfh.write("--------------------------------------------\n")        
        self.__lfh.write("Starting %s %s at %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime())))                
        #
        contentType = str( self.__reqObj.getValue("content_type") )
        self.__lfh.write("+%s.%s() -- contentType is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, contentType) )
        #
        rtrnDict = {}
        rtrnList = fullRsltSet = []
        iTotalRecords = iTotalDisplayRecords = 0
        recordSetLst = []
        origCommsLst = []
        #
        bCommHstryRqstd = True if( contentType == "commhstry" ) else False
        
        try:
            if self.__isWorkflow():
                # determine path(s) to cif datafiles that contain the data we are seeking
                msgDI=MessagingDataImport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
                if( contentType == "msgs" or bCommHstryRqstd ):
                    self.__msgsFrmDpstrFilePath = msgDI.getFilePath(contentType='messages-from-depositor',format="pdbx")
                    self.__msgsToDpstrFilePath = msgDI.getFilePath(contentType='messages-to-depositor',format="pdbx")
                if( contentType == "notes" or bCommHstryRqstd ):
                    self.__notesFilePath = msgDI.getFilePath(contentType='notes-from-annotator',format="pdbx")
                
            # obtain data from relevant datafiles based on contentType requested                
            if( contentType == "msgs" or bCommHstryRqstd ):
                self.__lfh.write("+%s.%s() -- self.__msgsFrmDpstrFilePath is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, self.__msgsFrmDpstrFilePath) )
                self.__lfh.write("+%s.%s() -- self.__msgsToDpstrFilePath is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, self.__msgsToDpstrFilePath) )
            
                if self.__msgsFrmDpstrFilePath is not None and os.access(self.__msgsFrmDpstrFilePath,os.R_OK):
                    pdbxMsgIo_frmDpstr=PdbxMessageIo(verbose=self.__verbose,log=self.__lfh)
                    ok=pdbxMsgIo_frmDpstr.read(self.__msgsFrmDpstrFilePath)
                    if( ok ):
                        recordSetLst = pdbxMsgIo_frmDpstr.getMessageInfo() # in recordSetLst we now have a list of dictionaries with item names as keys and respective data for values
                        
                        if( bCommHstryRqstd ):
                            origCommsLst.extend( pdbxMsgIo_frmDpstr.getOrigCommReferenceInfo() )
                        
                if self.__msgsToDpstrFilePath is not None and os.access(self.__msgsToDpstrFilePath,os.R_OK):
                    pdbxMsgIo_toDpstr=PdbxMessageIo(verbose=self.__verbose,log=self.__lfh)
                    ok=pdbxMsgIo_toDpstr.read(self.__msgsToDpstrFilePath)
                    if( ok ):
                        msgsToDpstrLst = pdbxMsgIo_toDpstr.getMessageInfo()
                        rtrnDict['CURRENT_NUM_MSGS_TO_DPSTR'] = len(msgsToDpstrLst)
                        recordSetLst.extend(msgsToDpstrLst) # in recordSetLst we now have a list of dictionaries with item names as keys and respective data for values
                        
                        if( bCommHstryRqstd ):
                            origCommsLst.extend( pdbxMsgIo_toDpstr.getOrigCommReferenceInfo() )
                    
                    else:
                        # no messages content created yet
                        rtrnDict['CURRENT_NUM_MSGS_TO_DPSTR'] = 0
                else:
                    # A vary rare race condition. MessagingDataImport will create file - but might be createed simultaneously in another process during time from creation - to detecting
                    # file present.
                    # no messages content created yet
                    rtrnDict['CURRENT_NUM_MSGS_TO_DPSTR'] = 0
                            
            if( contentType == "notes" or bCommHstryRqstd ):
                self.__lfh.write("+%s.%s() -- self.__notesFilePath is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, self.__notesFilePath) )
            
                if self.__notesFilePath is not None and os.access(self.__notesFilePath,os.R_OK):
                    pdbxMsgIo_notes=PdbxMessageIo(verbose=self.__verbose,log=self.__lfh)
                    ok=pdbxMsgIo_notes.read(self.__notesFilePath)
                    
                    if( ok ):
                        if( contentType == "notes" ):
                            recordSetLst = pdbxMsgIo_notes.getMessageInfo() # in recordSetLst we now have a list of dictionaries with item names as keys and respective data for values
                            rtrnDict['CURRENT_NUM_NOTES'] = len(recordSetLst)
                        elif( bCommHstryRqstd ):
                            fullNotesLst = pdbxMsgIo_notes.getMessageInfo()
                            onlyArchvdCommsLst = [record for record in fullNotesLst if( "archive" in record['message_type'] )]
                            recordSetLst.extend(onlyArchvdCommsLst)
                            origCommsLst.extend( pdbxMsgIo_notes.getOrigCommReferenceInfo() )
                    else:
                        # no notes content created yet
                        rtrnDict['CURRENT_NUM_NOTES'] = 0
            #
            recordSetLst = [record for record in recordSetLst if( record['send_status'] == p_sSendStatus )]
            # Need to sort by datetime using e.g.:   aDateTime = datetime.strptime('2013-08-14 15:41:52', '%Y-%m-%d %H:%M:%S')
            recordSetLst.sort(key = lambda record: datetime.strptime(record['timestamp'],'%Y-%m-%d %H:%M:%S') )
            #
            if( bCommHstryRqstd ):
                if( self.__verbose and self.__debug and self.__debugLvl2 ):
                    for idx,row in enumerate(origCommsLst):
                        self.__lfh.write("\n+%s.%s() -- row[%s]: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name,idx,row ) )
                self.__augmentWithOrigCommData(recordSetLst,origCommsLst)
            #
            if( self.__verbose and self.__debug and self.__debugLvl2 ):
                for idx,row in enumerate(recordSetLst):
                    self.__lfh.write("\n+%s.%s() before processing for threading -- row[%s]: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name,idx,row ) )
            #
            if( p_bThreadedRslts ):
                self.__doThreadedHandling(recordSetLst, rtrnDict)
            #
            if( self.__verbose and self.__debug and self.__debugLvl2 ):
                for idx,row in enumerate(recordSetLst):
                    self.__lfh.write("\n+%s.%s() after processing for threading -- row[%s]: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name,idx,row ) )
            #
            fullRsltSet = self.__trnsfrmMsgDictToLst(recordSetLst,bCommHstryRqstd)
            #
            '''
            if( self.__verbose and self.__debug ):
                for idx,row in enumerate(fullRsltSet):
                    self.__lfh.write("+%s.%s() Dictionary type message list now transformed to list data type -- rowidx# %s: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name,idx,row ) )
            '''
            #
            if p_bServerSide:
                columnList = ( self.getMsgColList(bCommHstryRqstd) )[1]
                #
                iTotalRecords = len(fullRsltSet)
                
                if( p_sSrchFltr and len(p_sSrchFltr) > 1 ):
                    if( self.__debug ):
                        self.__lfh.write("+%s.%s() p_sSrchFltr is: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, p_sSrchFltr ) )
                        
                    filteredRsltSet=self.__filterRsltSet( fullRsltSet, p_sGlobalSrchFilter=p_sSrchFltr )
                    iTotalDisplayRecords = len(filteredRsltSet)
                    rtrnList = filteredRsltSet
                    
                elif( len(p_colSearchDict) > 0 ): # applying column specific filtering here    
                    fltrdRsltSet = self.__filterRsltSet( fullRsltSet, p_dictColSrchFilter=p_colSearchDict )
                    iTotalDisplayRecords = len(fltrdRsltSet)
                    rtrnList = fltrdRsltSet
                else:
                    #no search filter in place
                    iTotalDisplayRecords = iTotalRecords
                    rtrnList = fullRsltSet
                    
                ##################################################################
                # we also need to accommodate any sorting requested by the user
                ##################################################################
                
                # number of columns selected for sorting --
                iSortingCols= int(self.__reqObj.getValue('iSortingCols')) if self.__reqObj.getValue('iSortingCols') else 0
                #
                ordL=[]
                descL=[]
                for i in range(iSortingCols):
                    iS=str(i)
                    idxCol = int(self.__reqObj.getValue('iSortCol_'+iS)) if self.__reqObj.getValue('iSortCol_'+iS) else 0
                    sortFlag = self.__reqObj.getValue('bSortable_'+iS) if self.__reqObj.getValue('bSortable_'+iS) else 'false'
                    sortOrder= self.__reqObj.getValue('sSortDir_'+iS) if self.__reqObj.getValue('sSortDir_'+iS) else 'asc'
                    if sortFlag == 'true':
                        # idxCol at this point reflects display order and not necessarily the true index of the column as it sits in persistent storage
                        # so can reference "mDataProp_[idxCol]" parameter sent by DataTables which will give true name of the column being sorted
                        colName = self.__reqObj.getValue('mDataProp_'+str(idxCol) ) if self.__reqObj.getValue('mDataProp_'+str(idxCol) ) else ''
                        colIndx = columnList.index(colName)
                        #
                        if( self.__verbose ):
                            self.__lfh.write("+%s.%s() -- colIndx for %s is %s as derived from columnList is %r\n" % (self.__class__.__name__,
                                                                                              sys._getframe().f_code.co_name,
                                                                                              colName, colIndx,
                                                                                              columnList) )
                        #    
                        ordL.append(colIndx)
                        if sortOrder=='desc':
                            descL.append(colIndx)
                #
                if len(ordL)>0:
                    if( self.__verbose and self.__debug and self.__debugLvl2 ):
                        for idx,row in enumerate(rtrnList):
                            self.__lfh.write("+%s.%s() rtrnList JUST BEFORE CALL TO ORDERBY -- rowidx# %s: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name,idx,row ) )
                    rtrnList=self.__orderBy(rtrnList,ordL,descL)
                
                if( self.__verbose ):
                    self.__lfh.write("+%s.%s() -- p_iDisplayStart is %s and p_iDisplayLength is %s\n" % (self.__class__.__name__,
                                                                                      sys._getframe().f_code.co_name,
                                                                                      p_iDisplayStart,
                                                                                      p_iDisplayLength) )                
            #
        except:
            traceback.print_exc(file=self.__lfh)
        #
        if p_bServerSide:
            if( p_iDisplayLength > 0 ):
                rtrnDict['RECORD_LIST'] = rtrnList[(p_iDisplayStart):(p_iDisplayStart+p_iDisplayLength)]
            else:
                rtrnDict['RECORD_LIST'] = rtrnList                 
            rtrnDict['TOTAL_RECORDS'] = iTotalRecords
            rtrnDict['TOTAL_DISPLAY_RECORDS'] = iTotalDisplayRecords 
            return rtrnDict
        else:
            rtrnDict['RECORD_LIST'] = fullRsltSet
            return rtrnDict
       
        
    def checkAvailFiles(self, p_depDataSetId):
        ''' Retrieve list of deposition files that have been produced thus far for dataset
            
            :param `p_depDataSetId`:       ID of deposition dataset for which list of messages being requested
            
        '''        
        self.__lfh.write("--------------------------------------------\n")        
        self.__lfh.write("Starting %s %s at %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime())))                
        #
        rtrnList = []
        
        # local dictionary of files where key/value pairs are meant to be consistent with acronym/contentFormat as encoded in ConfigInfoData
        # this dictionary governs which types of files will be checked for availability as "attachments" for messages 
        fileCheckCatalog = {'model':'pdbx',
                            'model_pdb':'pdb',
                            'sf':'pdbx',
                            'val-report':'pdf',
                            'val-report-full':'pdf',
                            'val-data':'xml',
                            'val-report-slider':'png',
                            'val-report-wwpdb-2fo-fc-edmap-coef':'pdbx',
                            'val-report-wwpdb-fo-fc-edmap-coef':'pdbx',
                            'mr':'any', # 'any' mapped to 'dat' in ConfigInfoData
                            'cs':'pdbx',
                            'em-volume':'map',
                            'em-mask-volume':'map',
                            'em-volume-header':'xml'
                            }
        #
        try:
            if self.__isWorkflow():
                msgDI=MessagingDataImport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
                for token,contentFormat in fileCheckCatalog.items():
                    contentType = self.__getContentType(token)
                    fPath = msgDI.getFilePath(contentType,contentFormat)
                    if( fPath is not None and os.access(fPath,os.R_OK) ):
                        rtrnList.append(token)
            else:
                rtrnList = fileCheckCatalog.keys()
            #            
        except:
            traceback.print_exc(file=self.__lfh)
        #

        # See if validation report is included - create a hybrid if so.

        valreport = False
        for f in rtrnList:
            if 'val-report' in f:
                valreport = True
                break

        if valreport:
            rtrnList.append('val-report-batch')

        return rtrnList
    
    def getFilesRfrncd(self,p_depDataSetId,p_msgIdFilter=None):
        ''' Retrieve list of files referenced by any messages for this dataset ID
            
            :param `p_depDataSetId`:       ID of deposition dataset for which list of messages being requested
            
        '''        
        self.__lfh.write("--------------------------------------------\n")        
        self.__lfh.write("Starting %s %s at %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime())))                
        #
        recordSetLst = []
        rtrnDict = {}
        #
        try:
            if self.__isWorkflow():
                msgDI=MessagingDataImport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
                self.__msgsFrmDpstrFilePath = msgDI.getFilePath(contentType='messages-from-depositor',format="pdbx")
                self.__msgsToDpstrFilePath = msgDI.getFilePath(contentType='messages-to-depositor',format="pdbx")
                self.__lfh.write("+%s.%s() -- self.__msgsFrmDpstrFilePath is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, self.__msgsFrmDpstrFilePath) )
                self.__lfh.write("+%s.%s() -- self.__msgsToDpstrFilePath is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, self.__msgsToDpstrFilePath) )
            
            if self.__msgsFrmDpstrFilePath is not None and os.access(self.__msgsFrmDpstrFilePath,os.R_OK):
                mIIo=PdbxMessageIo(verbose=self.__verbose,log=self.__lfh)
                with LockFile(self.__msgsFrmDpstrFilePath,timeoutSeconds=self.__timeoutSeconds,retrySeconds=self.__retrySeconds,verbose=self.__verbose,log=self.__lfh) as lf, FileSizeLogger(self.__msgsFrmDpstrFilePath,verbose=self.__verbose,log=self.__lfh) as fsl:
                    pid = os.getpid()
                    ok=mIIo.read(self.__msgsFrmDpstrFilePath,"msgingmod"+str(pid))
                if( ok ):
                    recordSetLst = mIIo.getFileReferenceInfo() # in recordSetLst we now have a list of dictionaries with item names as keys and respective data for values
                    
            if self.__msgsToDpstrFilePath is not None and os.access(self.__msgsToDpstrFilePath,os.R_OK):
                mIIo2=PdbxMessageIo(verbose=self.__verbose,log=self.__lfh)
                with LockFile(self.__msgsToDpstrFilePath,timeoutSeconds=self.__timeoutSeconds,retrySeconds=self.__retrySeconds,verbose=self.__verbose,log=self.__lfh) as lf, FileSizeLogger(self.__msgsToDpstrFilePath,verbose=self.__verbose,log=self.__lfh) as fsl:
                    pid = os.getpid()
                    ok=mIIo2.read(self.__msgsToDpstrFilePath,"msgingmod"+str(pid))
                if( ok ):
                    recordSetLst.extend(mIIo2.getFileReferenceInfo()) # in recordSetLst we now have a list of dictionaries with item names as keys and respective data for values
            #
            if( self.__verbose and self.__debug and self.__debugLvl2 ):
                for idx,row in enumerate(recordSetLst):
                    self.__lfh.write("\n+%s.%s() -- row[%s]: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name,idx,row ) )
            #
            rtrnDict = self.__trnsfrmFileRefDictToLst(recordSetLst,p_msgIdFilter)
            
        except:
            traceback.print_exc(file=self.__lfh)
        #
        return rtrnDict
    
    
    def getMsgReadList( self, p_depDataSetId ):
        ''' For a given deposition dataset ID, retrieve list of messages already read 
            
            :param `p_depDataSetId`:       ID of deposition dataset for which list of messages being requested
            
        '''        
        self.__lfh.write("--------------------------------------------\n")        
        self.__lfh.write("Starting %s %s at %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime())))                
        #
        return self.__getMsgsByStatus('read_status','Y')
        
        
    def getMsgNoActionReqdList( self, p_depDataSetId ):
        ''' For a given deposition dataset ID retrieve list of messages for which action is required
            
            :param `p_depDataSetId`:       ID of deposition dataset for which list of messages being requested
            
        '''        
        self.__lfh.write("--------------------------------------------\n")        
        self.__lfh.write("Starting %s %s at %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime())))                
        #
        return self.__getMsgsByStatus('action_reqd','N')
        
        
    def getMsgForReleaseList(self, p_depDataSetId ):
        ''' For a given deposition dataset ID retrieve list of messages for which action is required
            
            :param `p_depDataSetId`:       ID of deposition dataset for which list of messages being requested
            
        '''        
        self.__lfh.write("--------------------------------------------\n")        
        self.__lfh.write("Starting %s %s at %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime())))                
        #
        return self.__getMsgsByStatus('for_release','Y')

    def getNotesList(self):
        
        rtrnList = []
        recordSetLst = []
        #
        try:
            if self.__isWorkflow():
                msgDI=MessagingDataImport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
                self.__notesFilePath = msgDI.getFilePath(contentType='notes-from-annotator',format="pdbx")
                self.__lfh.write("+%s.%s() -- self.__notesFilePath is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, self.__notesFilePath) )
                
            if os.access(self.__notesFilePath,os.R_OK):
                mIIo=PdbxMessageIo(verbose=self.__verbose,log=self.__lfh)
                with LockFile(self.__notesFilePath,timeoutSeconds=self.__timeoutSeconds,retrySeconds=self.__retrySeconds,verbose=self.__verbose,log=self.__lfh) as lf, FileSizeLogger(self.__notesFilePath,verbose=self.__verbose,log=self.__lfh) as fsl:
                    pid = os.getpid()
                    ok=mIIo.read(self.__notesFilePath,"msgingmod"+str(pid))
                if( ok ):
                    recordSetLst = mIIo.getMessageInfo() # in recordSetLst we now have a list of dictionaries with item names as keys and respective data for values
                #
                for row in recordSetLst:
                    if( self.__verbose and self.__debug ):
                        self.__lfh.write("\n+%s.%s() -- row: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name,row ) )
                    rtrnList.append( row['message_id'] )
                #
                
        except:
            traceback.print_exc(file=self.__lfh)
        #
        return rtrnList

        
    def processMsg(self,p_msgObj): 
        ''' handle processing for live or draft messages
            
            :Params:
                :param `p_msgObj`: messsage object                 
                
            :Returns:
                bOk : boolean indicating success/failure
                bPdbxMdlFlUpdtd : boolean indicating whether new version of pdbx model file is being generated by this message
                failedFileRefs : list of file references that failed processing
        
        '''
        startTime=time.time()        
        self.__lfh.write("\nStarting %s %s at %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        #
        bOk = False
        bPdbxMdlFlUpdtd = False
        msgFileRefs = []
        failedFileRefs = []
        bVldtnRprtBeingSent = False
        #
        try:
            if( self.__verbose and self.__debug ):
                self.__lfh.write("+%s.%s() -- p_msgObj.isLive is: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, p_msgObj.isLive ) )
            if( p_msgObj.isLive ): # if this a livemsg as opposed to draft then need to instantiate new PdbxMessageInfo object/acquire the new message data
                mI=PdbxMessageInfo(verbose=self.__verbose,log=self.__lfh)
                mI.set( p_msgObj.getMsgDict() )
                if( self.__verbose and self.__debug ):
                    self.__lfh.write("+%s.%s() -- p_msgObj.getMsgDict() is: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, p_msgObj.getMsgDict() ) )
            #
            outputFilePth = p_msgObj.getOutputFileTarget( self.__reqObj ) # determine which datafile needs to be updated
            if( self.__verbose and self.__debug ):
                self.__lfh.write("+%s.%s() -- outputFilePth is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, outputFilePth) )
            #
            if( p_msgObj.contentType == "msgs" and self.__isWorkflow() ):
                # in workflow environment we need to know path of messaging data file in deposit storage area
                # b/c this needs to be kept in sync as well for any updates
                msgDE=MessagingDataExport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
                depUiMsgsToDpstrFilePath = msgDE.getFilePath(contentType='messages-to-depositor',format="pdbx")
                if( self.__verbose  ):
                    self.__lfh.write("+%s.%s() -- depUiMsgsToDpstrFilePath is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, depUiMsgsToDpstrFilePath) )
            
            if not os.access(outputFilePth,os.F_OK):
                self.__lfh.write("+%s.%s() -- messaging output file not found at: %s, so instantiating a copy.\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, outputFilePth) )
                try:
                    # file may not exist b/c it is the first time that an
                    # annotator is sending a message in which case we create a new file  
                    f = open(outputFilePth, "w")
                except IOError:
                    self.__lfh.write("+%s.%s() -- problem creating messaging output file at: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, outputFilePth) )
            #
            mIIo=PdbxMessageIo(verbose=self.__verbose,log=self.__lfh)
            with LockFile(outputFilePth,timeoutSeconds=self.__timeoutSeconds,retrySeconds=self.__retrySeconds,verbose=self.__verbose,log=self.__lfh) as lf, FileSizeLogger(outputFilePth,verbose=self.__verbose,log=self.__lfh) as fsl:
                pid = os.getpid()
                bGotContent=mIIo.read(outputFilePth,"msgingmod"+str(pid)) # may return False if there was a file but the file had no content yet (i.e. no annotator messages yet)
            
            if( self._sanityCheck(p_msgObj.contentType,bGotContent,mIIo,outputFilePth) ):
                mIIo.newBlock('messages')
                #
                if( p_msgObj.isLive ):
                    mI.setOrdinalId( id=mIIo.nextMessageOrdinal() )
                    mIIo.appendMessage( mI.get() )
                    p_msgObj.messageId = mI.getMessageId()
                    if( self.__verbose and self.__debug ):
                        self.__lfh.write("+%s.%s() -- mI.get() is now: %s \n" % (self.__class__.__name__, sys._getframe().f_code.co_name, mI.get()) )
                        self.__lfh.write("+%s.%s() -- mI.getMessageId() is now: %s \n" % (self.__class__.__name__, sys._getframe().f_code.co_name, mI.getMessageId() ) )
                        self.__lfh.write("+%s.%s() -- p_msgObj.messageId is now: %s \n" % (self.__class__.__name__, sys._getframe().f_code.co_name, p_msgObj.messageId) )
                        self.__lfh.write("+%s.%s() -- p_msgObj.isReminderMsg is now: %s \n" % (self.__class__.__name__, sys._getframe().f_code.co_name, p_msgObj.isReminderMsg) )
                elif( p_msgObj.isDraft ):
                    # if this is a draft we are updating the already existing draft copy of the message which we find by matching message id
                    
                    recordSetLst=mIIo.getMessageInfo() # in recordSetLst we now have a list of dictionaries with item names as keys and respective data for values
                    #
                    rowToUpdate = None
                    for rowIdx,record in enumerate(recordSetLst):
                        if( record['message_id'] == p_msgObj.messageId ):
                            rowToUpdate = rowIdx
                            break
                        
                    #['context_value', 'sender', 'context_type', 'timestamp', 'ordinal_id', 'message_subject', 'deposition_data_set_id', 'message_text', 'send_status', 'message_type', 'message_id', 'parent_message_id']
                    mIIo.update('pdbx_deposition_message_info','send_status',p_msgObj.sendStatus,iRow=rowToUpdate)
                    mIIo.update('pdbx_deposition_message_info','message_subject',p_msgObj.messageSubject,iRow=rowToUpdate)
                    mIIo.update('pdbx_deposition_message_info','message_text',p_msgObj.messageText,iRow=rowToUpdate)
                    
                    # update timestamp
                    gmtTmStmp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
                    mIIo.update('pdbx_deposition_message_info','timestamp',gmtTmStmp,iRow=rowToUpdate)
                
                # handle any file references
                if( p_msgObj.isBeingSent and not self.__groupId): # i.e. not executed for messages remaining in "draft" state
                    
                    bSuccess, msgFileRefs, failedFileRefs = self.__handleFileReferences( p_msgObj )
                    if bSuccess:
                        flRefOrdinalId=mIIo.nextFileReferenceOrdinal()
                        for mfr in msgFileRefs:
                            # note: msgFileRefs only gets populated with given member if the associated file was successfully copied to deposition side
                            mfr.setOrdinalId(id=flRefOrdinalId)                
                            mIIo.appendFileReference(mfr.get())
                            flRefOrdinalId+=1
                            
                            if( mfr.getContentType() == 'model-annotate' and mfr.getContentFormat() == 'pdbx'):
                                bPdbxMdlFlUpdtd = True
                            if( mfr.getContentType() == 'validation-report-annotate' or mfr.getContentType() == 'validation-report-full-annotate' ):
                                bVldtnRprtBeingSent = True
                                
            
                    else:
                        # something went wrong with handling of the file references, so abort and return failure status to calling code
                        return bSuccess, False, failedFileRefs
                    
                # if this is a message to be archived or forwarded, handle reference to the original communication
                if ('archive' in p_msgObj.messageType or 'forward' in p_msgObj.messageType) and 'noorig' not in p_msgObj.messageType: 
                    self._handleOrigCommReferences(p_msgObj,mIIo)
                
                self._updateSnapshotHistory(outputFilePth)
                
                with LockFile(outputFilePth,timeoutSeconds=self.__timeoutSeconds,retrySeconds=self.__retrySeconds,verbose=self.__verbose,log=self.__lfh) as lf:
                    bOk = mIIo.write(outputFilePth)
                
                # Write message to depositor message file and send email
                if bOk and not self.__groupId:
                    if self.__isWorkflow() and p_msgObj.contentType == "msgs":
                        # update copy of messages-to-depositor file in depositor file system if necessary
                        with LockFile(depUiMsgsToDpstrFilePath,timeoutSeconds=self.__timeoutSeconds,retrySeconds=self.__retrySeconds,verbose=self.__verbose,log=self.__lfh) as lf:
                            mIIo.write(depUiMsgsToDpstrFilePath)
                        
                        try:
                            # send notification email to contact authors
                            if(  p_msgObj.isBeingSent and not p_msgObj.isReminderMsg ):
                                self.__sendNotificationEmail( p_msgObj, bVldtnRprtBeingSent )
                        except:
                            self.__lfh.write("Warning: problem sending notification email.\n")                
                            traceback.print_exc(file=self.__lfh)

                    elif self.__isWorkflow() and p_msgObj.contentType == "notes" and p_msgObj.isNoteEmail:
                        try:
                            # send notification email to contact authors
                            if(  p_msgObj.isBeingSent and not p_msgObj.isReminderMsg ):
                                self.__sendNotificationEmail( p_msgObj, bVldtnRprtBeingSent )
                        except:
                            self.__lfh.write("Warning: problem sending notification email.\n")                
                            traceback.print_exc(file=self.__lfh)

                    else:
                        # execution here is for DEV TESTING purposes
                        if self.__devMode and p_msgObj.contentType == "msgs":
                            try:
                                if( p_msgObj.isBeingSent and not p_msgObj.isReminderMsg ):
                                    self.__sendNotificationEmail( p_msgObj, bVldtnRprtBeingSent )
                            except:
                                self.__lfh.write("Warning: problem sending notification email.\n")                
                                traceback.print_exc(file=self.__lfh)
                                
            else: #failed sanity check
                self.__lfh.write("Message data append failed\n")    
        except:
            self.__lfh.write("Message data append failed\n")                
            traceback.print_exc(file=self.__lfh)
            bOk = False
            
        endTime=time.time()
        self.__lfh.write("\nCompleted %s %s at %s (%d seconds)\n" % (self.__class__.__name__,
                                                        sys._getframe().f_code.co_name,
                                                        time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                        endTime-startTime))
        #
        return bOk, bPdbxMdlFlUpdtd, failedFileRefs
    
    def _updateSnapshotHistory(self,outputFilePth):
        ''' a safety measure by which we keep up to five historical snapshots of
            the messages-to-depositor file. This allows us a means of revisiting a prior state of captured messages and 
            recovering lost data if necessary. i.e. if current messages-to-depositor file gets corrupted, deleted unexpectedly
            
            e.g.    D_XXXXXXXXXX_messages-to-depositor_P1.cif.V1.PREV0
                    D_XXXXXXXXXX_messages-to-depositor_P1.cif.V1.PREV1
                    D_XXXXXXXXXX_messages-to-depositor_P1.cif.V1.PREV2 ...
            
            :Params:
                :param `outputFilePth`: path to messages output file
        '''
        if( self.__verbose ):
            logger.info("STARTING")
                
        for i in range(4,0,-1):
            srcVrsn = ".PREV"+str(i-1)
            dstVrsn = ".PREV"+str(i)
            
            if os.access(outputFilePth+srcVrsn,os.F_OK):
                try:
                    shutil.copyfile(outputFilePth+srcVrsn, outputFilePth+dstVrsn)
                except IOError:
                    logger.error("Problem making backup of preupdate messaging file at: %s" % outputFilePth+srcVrsn)
        
        if os.access(outputFilePth,os.F_OK):
            try:
                shutil.copyfile(outputFilePth, outputFilePth+".PREV0")
            except IOError:
                logger.error("Problem making backup of preupdate messaging file at: %s" % outputFilePth )
                
        else:
            logger.error("WARNING could NOT access messages-to-depositor file at: %s" % outputFilePth )
                    
        if( self.__verbose ):
            logger.info("COMPLETED")
                    
                    
    def _getOutputFileTarget(self,contentType):
        ''' for given deposition, determine path to cif messaging data file to be updated
            
            :Params:
                :param `contentType`:                   'msgs' | 'notes'
                
            :Returns:
                returnFilePath : absolute path to cif data file containing messages or notes for the given deposition
        
        '''
        returnFilePath = None
        
        msgDI=MessagingDataImport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
        if( contentType == "msgs" ):
            returnFilePath = msgDI.getFilePath(contentType='messages-to-depositor',format="pdbx") if self.__isWorkflow() else self.__msgsToDpstrFilePath
            self.__lfh.write("+%s.%s() -- messages-to-depositor path is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, returnFilePath) )
        elif( contentType == "notes" ):
            returnFilePath = msgDI.getFilePath(contentType='notes-from-annotator',format="pdbx") if self.__isWorkflow() else self.__notesFilePath
            self.__lfh.write("+%s.%s() -- notes-from-annotator path is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, returnFilePath) )
                
        return returnFilePath
    
    def _sanityCheck(self,contentType,bGotContent,mIIo,outputFilePth):
        ''' when "highWatermark" is available, its value is used to assert that the number of records in the messaging data file is as expected
            
            :Params:
                :param `contentType`:    'msgs' | 'notes'
                :param `bGotContent`:    whether or not there is any pre-existing messages/notes data
                :param `mIIo`:           reference to the PdbxMessageIo object
                :param `outputFilePth`:  absolute path to the messaging data file being updated with a new record
                
            :Returns:
                bReturnVal : boolean -- if hwm available then indicates whether or not target datafile passes sanity check
                                        if no hwm available, then no assessment is done (True is returned)
        
        '''
        highWatermark = self.__reqObj.getValue("msgs_high_watermark") if contentType == "msgs" else self.__reqObj.getValue("notes_high_watermark")
        bReturnVal = True
        
        if( highWatermark is not None and len(highWatermark) > 0 ): # only if we have a valid high water mark can we peform the sanity check
            self.__lfh.write("+%s.%s() -- highWatermark of [%s] provided for contentType '%s' so using this for sanity check.\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, highWatermark, contentType) )
            try:
                if bGotContent:
                    nextOrdinalID = mIIo.nextMessageOrdinal()
                    currentNumRows = (nextOrdinalID-1) if (nextOrdinalID > 0) else 0
                else:
                    currentNumRows = 0
                
                assert( int(currentNumRows) >= int(highWatermark) ), ("+%s.%s() --  CRITICAL ERROR -- file at: %s, had %s records but should have had at least %s records!\n" % (self.__class__.__name__,
                                                                                                                                                  sys._getframe().f_code.co_name,
                                                                                                                                                  outputFilePth, currentNumRows, highWatermark) )
            except AssertionError:
                bReturnVal = False
                traceback.print_exc(file=self.__lfh)
        else:
            self.__lfh.write("+%s.%s() -- highWatermark not provided so skipping sanity check.\n" % (self.__class__.__name__, sys._getframe().f_code.co_name) )
            # highWatermark currently not provided in cases where automatic system messaging is invoked (e.g. release notification messages, archiving)
            
        return bReturnVal
                
    def _handleOrigCommReferences(self,p_msgObj,mIIo):
        ''' details regarding reference to original communications (i.e. in cases of archiving or forwarding) are captured 
            
            :Params:
                :param `p_msgObj`:    instance of Message class
                :param `mIIo`:           reference to the PdbxMessageIo object
                
            mIIo PdbxMessageIo object is updated with reference to PdbxMessageOrigCommReference
        
        '''
        #
        self.__lfh.write("+%s.%s() -- 'message_type' is '%s' so creating PdbxMessageOrigCommRef\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, p_msgObj.messageType ) )
                    
        origSender = self.__reqObj.getValue("orig_sender")
        origRecipient = self.__reqObj.getValue("orig_recipient")
        origDateTime = self.__reqObj.getValue("orig_date")
        origSubject = self.__reqObj.getValue("orig_subject")
        origDepId = self.__reqObj.getValue("orig_identifier")
        origAttachments = self.__reqObj.getValue("orig_attachments")
        
        mode = self.__reqObj.getValue("mode")
        if( mode and mode == "manual" ):
            origDateTime = self.__convertToGmtTimeZone(origDateTime)
        
        # special handling for fields that may contain special utf-8 characters
        origSenderAsAscii = self.__encodeUtf8ToCif(origSender)
        origRecipientAsAscii = self.__encodeUtf8ToCif(origRecipient)
        origSubjectAsAscii = self.__encodeUtf8ToCif(origSubject)
        
        msgOrigCommRef=PdbxMessageOrigCommReference(verbose=self.__verbose,log=self.__lfh)
        msgOrigCommRef.setMessageId(p_msgObj.messageId)
        msgOrigCommRef.setDepositionId(p_msgObj.depositionId)
        msgOrigCommRef.setOrigSender(origSenderAsAscii)
        msgOrigCommRef.setOrigRecipient(origRecipientAsAscii)
        msgOrigCommRef.setOrigDepositionId(origDepId)
        msgOrigCommRef.setOrigMessageSubject(origSubjectAsAscii)
        msgOrigCommRef.setOrigTimeStamp(origDateTime)
        msgOrigCommRef.setOrigAttachments(origAttachments)
        nextOrdinalId = mIIo.nextOrigCommReferenceOrdinal()
        msgOrigCommRef.setOrdinalId(id=nextOrdinalId)                
        mIIo.appendOrigCommReference(msgOrigCommRef.get())
        
    
    def autoMsg(self,p_depIdList,p_tmpltType="release-publ",p_isEmdbEntry=False,p_sender="auto"):
        """
            Method to enable release message to be automatically sent by another server-side python module (e.g. Release module)
            (i.e. as opposed to being invoked via URL request)
            
            :Helpers:
                
            :Returns:
            
        """
        #
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() --STARTING\n" % (className, methodName) )
            self.__lfh.write("+%s.%s() -- p_isEmdbEntry is:%s\n" % (className, methodName, p_isEmdbEntry) )
            self.__lfh.write("+%s.%s() -- p_depIdList is:%r\n" % (className, methodName, p_depIdList) )
            self.__lfh.write("+%s.%s() -- p_tmpltType is:%s\n" % (className, methodName, p_tmpltType) )
        
        self.__reqObj.setValue("content_type", "msgs") # as opposed to "notes"
        self.__reqObj.setValue("filesource", "archive")
        
        if( p_isEmdbEntry ):
            self.__reqObj.setValue("expmethod", "ELECTRON MICROSCOPY")
        #
        rtrnDict = {}
        #
        contextType = None
        contextVal = None
        #
        # Added by ZF
        #
        statusApi = StatusDbApi(siteId=self.__siteId, verbose=self.__verbose, log=self.__lfh)
        for depId in p_depIdList: #for depId,tmpltType in p_depIdList:
            
            self.__reqObj.setValue("identifier", depId) # IMPORTANT: enforcing value of deposition ID for all subsequent downstream processing
            #
            # Added by ZF
            #
            self.__groupId = ''
            if depId.startswith('G_'):
                self.__groupId = depId
            else:
                groupId = statusApi.getGroupId(depId)
                if groupId:
                    self.__groupId = groupId
                #
            #
            self.__reqObj.setValue('groupid', self.__groupId)
            
            # defaults for EM
            self.__reqObj.setValue("em_entry", "false")
            self.__reqObj.setValue("em_map_only", "false")
            self.__reqObj.setValue("em_map_and_model", "false")
        
            useAnnotatorName = False
            if p_tmpltType == "remind-unlocked":
                useAnnotatorName = True

            # Trigger lookup of annotator initial to name if desired by template
            if useAnnotatorName:
                self.__reqObj.setValue("useAnnotatorName", "True")

            # qualifying name of database file, in case code calling this function does so with list of depIDs
            # in which case we will need to create individual database files for each depID, but all in same session path.
            self.__dbFilePath = os.path.join(self.__sessionPath,depId+"_modelFileData.db") 
            #
            self.initializeDataStore()
            #
            templateDict= {}
            templateDict['identifier'] = depId
            self.getMsgTmpltDataItems(templateDict)
            #
            #if( 'EMDB' in templateDict['accession_ids'] ):   #if( templateDict['em_entry'] == "true" ):
            #    bEmDeposition = True
            #

            # Attach model files again (for auto-release) but not for a reminder
            attachFiles = True
            # Should this be archived as a Note?
            isNote = False

            # Default subject
            # sAccessionString is for entries being released - may not list all ids
            sAccessionIdString = templateDict['accession_ids_em_rel'] if p_isEmdbEntry else templateDict['accession_ids']
            subject = "Release of "+sAccessionIdString

            # Template specific flags
            if( p_tmpltType == "release-publ" ): 
                msgTmplt = MessagingTemplates.msgTmplt_releaseWthPblctn_em if p_isEmdbEntry else MessagingTemplates.msgTmplt_releaseWthPblctn
            elif( p_tmpltType == "release-nopubl" ): 
                msgTmplt = MessagingTemplates.msgTmplt_releaseWthOutPblctn_em if p_isEmdbEntry else MessagingTemplates.msgTmplt_releaseWthOutPblctn
            elif( p_tmpltType == "remind-unlocked" ): 
                msgTmplt = MessagingTemplates.msgTmplt_remindUnlocked
                attachFiles = False
                isNote = True
                # Need all ids
                accstr = templateDict['accession_ids']
                subject = "ARCHIVED: Please attend to your unlocked deposition session - " + accstr

            # Assemble message with templates    
            msg = (msgTmplt % templateDict)
            #
            messageDict={
                "deposition_data_set_id"          : depId,
                "sender"          : p_sender,
                "context_type"    : contextType,
                "context_value"   : contextVal,
                "message_subject" : subject,
                "message_text"    : msg,
                "send_status"     : 'Y',
                "message_type"    : 'text'
            }
            #
            #fileRefList = ['model', 'model_pdb', 'sf', 'val-report', 'val-report-full', 'val-data']
            if attachFiles:
                fileRefList = self.checkAvailFiles(depId)
            else:
                fileRefList = []

            if (self.__verbose):
                self.__lfh.write("+%s.%s() -- dep_id is:%s\n" % (className, methodName, depId) )
                self.__lfh.write("+%s.%s() -- msg is: %r\n" % (className, methodName, msg) )
                self.__lfh.write("+%s.%s() -- fileRefList is: %r\n" % (className, methodName, fileRefList) )
            
            if( templateDict['em_entry'] == "true" ):
                self.__reqObj.setValue("em_entry", "true")
                
                if( templateDict['maponly'] == "true" ):
                    self.__reqObj.setValue("em_map_only", "true")
                    
                    if 'model' in fileRefList:
                        fileRefList.remove('model')
                    if 'model_pdb' in fileRefList:
                        fileRefList.remove('model_pdb')
                        
                if( templateDict['mapandmodel'] == "true" ):    
                    self.__reqObj.setValue("em_map_and_model", "true")

            if (self.__verbose):
                self.__lfh.write("+%s.%s() -- templateDict['em_entry'] is: %r\n" % (className, methodName, templateDict['em_entry']) )
                self.__lfh.write("+%s.%s() -- templateDict['maponly'] is: %r\n" % (className, methodName, templateDict['maponly']) )
                self.__lfh.write("+%s.%s() -- templateDict['mapandmodel'] is: %r\n" % (className, methodName, templateDict['mapandmodel']) )
                self.__lfh.write("+%s.%s() -- fileRefList is now: %r\n" % (className, methodName, fileRefList) )
            #
            rtrnDict[depId] = {}
            #
            if isNote:
                autoMsgObj = AutoNote(messageDict,fileRefList,self.__verbose,self.__lfh)
            else:
                autoMsgObj = AutoMessage(messageDict,fileRefList,self.__verbose,self.__lfh)

            bOk, bPdbxMdlFlUpdtd, failedFileRefs = self.processMsg( autoMsgObj )
            #
            rtrnDict[depId]['success'] = "true" if bOk is True else "false"            
            #
            rtrnDict[depId]['pdbx_model_updated'] = "true" if bPdbxMdlFlUpdtd else "false"
            rtrnDict[depId]['append_msg'] = ""
            if( ( not bOk ) and ( len(failedFileRefs) > 0 ) ):
                sMsg = "Failure to associate message with the following file types: "+( ", ".join(failedFileRefs) )
                rtrnDict[depId]['append_msg'] = sMsg
            #
            if (self.__verbose):
                self.__lfh.write("+%s.%s() -- pdbx_model_updated is: %s\n" % (className, methodName, rtrnDict[depId]['pdbx_model_updated']) )
        #
        return rtrnDict
    
    def getMsgTmpltDataItems(self,p_returnDict):
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        if (self.__verbose):
                self.__lfh.write("+%s.%s() -- Starting.\n" % (className, methodName) )
                
        msgTmpltHelper = MsgTmpltHlpr(self.__reqObj,self.__dbFilePath,self.__verbose,self.__lfh)
        msgTmpltHelper.populateTmpltDict(p_returnDict)
    
    def getStarterMsgBody(self):
        rtrnText = None
        oL = []
        
        if self.__isWorkflow():
            
            msgDI=MessagingDataImport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
            fPath = msgDI.getFilePath(contentType='correspondence-to-depositor',format='txt')
            #if self.__devMode is True:
            #    fPath = "/net/wwpdb_da/da_top/data_internal/workflow/D_1100201324/instance/W_016/D_1100201324_correspondence-to-depositor_P1.txt.V1"
            
            if( fPath is not None and os.access(fPath,os.R_OK) ):
                ifh = open(fPath, "r")
                for line in ifh:
                    oL.append(line)
            
                rtrnText= ''.join(oL)
                    
        else:
            if self.__devMode is True:
                fPath = "/net/wwpdb_da/da_top/data_internal/workflow/D_1100201324/instance/W_016/D_1100201324_correspondence-to-depositor_P1.txt.V1"
                if( fPath is not None and os.access(fPath,os.R_OK) ):
                    ifh = open(fPath, "r")
                    for line in ifh:
                        oL.append(line)
                
                    rtrnText= ''.join(oL)
                    
            rtrnText = "Groovin' High" if( rtrnText is None ) else rtrnText
            
        
        return rtrnText

    def markMsgAsRead(self,p_msgStatusDict): 
        ''' handle request to mark message as already "read" 
            
            :Params:
                :param `p_msgStatusDict`:    dictionary representing message status entity to be submitted
                
            
            :Returns:
                boolean indicating success/failure
        
        '''
        startTime=time.time()        
        self.__lfh.write("\nStarting %s.%s() at %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        #
        bOk = False
        #
        try:
            mS=PdbxMessageStatus(verbose=self.__verbose,log=self.__lfh)
            mS.set(p_msgStatusDict)
            msgId = mS.getMessageId()
            #
            if( self.__verbose ):
                self.__lfh.write("\n---%s.%s() request to mark msg read for msgID: [%s]\n" % (self.__class__.__name__,
                                                           sys._getframe().f_code.co_name,
                                                           msgId))
            #
            if self.__isWorkflow():
                msgDI=MessagingDataImport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
                self.__msgsToDpstrFilePath = msgDI.getFilePath(contentType='messages-to-depositor',format="pdbx")
                self.__lfh.write("+%s.%s() -- self.__msgsToDpstrFilePath is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, self.__msgsToDpstrFilePath) )
            #
            if not os.access(self.__msgsToDpstrFilePath,os.F_OK):
                try:
                    f = open(self.__msgsToDpstrFilePath, "w")
                except IOError:
                    pass
            #
            mIIo=PdbxMessageIo(verbose=self.__verbose,log=self.__lfh)
            with LockFile(self.__msgsToDpstrFilePath,timeoutSeconds=self.__timeoutSeconds,retrySeconds=self.__retrySeconds,verbose=self.__verbose,log=self.__lfh) as lf, FileSizeLogger(self.__msgsToDpstrFilePath,verbose=self.__verbose,log=self.__lfh) as fsl:
                pid = os.getpid()
                ok=mIIo.read(self.__msgsToDpstrFilePath,"msgingmod"+str(pid))
            if ok:
                recordSetLst = mIIo.getMsgStatusInfo() # in recordSetLst we now have a list of dictionaries with item names as keys and respective data for values
                msgAlreadySeen = False
                for idx,record in enumerate(recordSetLst):
                    if( record['message_id'] == msgId ):
                        msgAlreadySeen = True
                        if( record['read_status'] == 'Y' ): 
                            #message had already been marked as "read" so can return True to caller
                            return True
                        else:
                            mIIo.update('pdbx_deposition_message_status','read_status','Y',idx)
                        
                mIIo.newBlock('messages')
                if not msgAlreadySeen:
                    mIIo.appendMsgReadStatus(mS.get())
                with LockFile(self.__msgsToDpstrFilePath,timeoutSeconds=self.__timeoutSeconds,retrySeconds=self.__retrySeconds,verbose=self.__verbose,log=self.__lfh) as lf:
                        mIIo.write(self.__msgsToDpstrFilePath)
                
                bOk = ok
                
            else:
                # OR if there was no container list BUT the file is accessible-->indicates no content yet b/c no messages sent to depositor yet
                if( os.access(self.__msgsToDpstrFilePath,os.W_OK) ):
                    mIIo.newBlock('messages')
                    mIIo.appendMsgReadStatus(mS.get())
                    with LockFile(self.__msgsToDpstrFilePath,timeoutSeconds=self.__timeoutSeconds,retrySeconds=self.__retrySeconds,verbose=self.__verbose,log=self.__lfh) as lf:
                        mIIo.write(self.__msgsToDpstrFilePath)
                    bOk = True
        except:
            self.__lfh.write("Message read status data update failed\n")                
            traceback.print_exc(file=self.__lfh)
            
        endTime=time.time()
        self.__lfh.write("\nCompleted %s.%s() at %s (%d seconds)\n" % (self.__class__.__name__,
                                                        sys._getframe().f_code.co_name,
                                                        time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                        endTime-startTime))
        #
        return bOk
    
    
    def areAllMsgsRead(self): 
        ''' 
            :Returns:
                boolean indicating whether all messages from depositor were read or not
        
        '''
        startTime=time.time()        
        self.__lfh.write("\nStarting %s.%s() at %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        # are all messages read?
        bAllMsgsRead = self.__globalMessageStatusCheck(p_statusToCheck='read_status', p_flagForFalseReturn="N")
         
        return bAllMsgsRead
    
        
    def areAllMsgsActioned(self): 
        ''' 
            :Returns:
                boolean indicating whether all messages from depositor had all required actions fulfilled or not
        
        '''
        startTime=time.time()        
        self.__lfh.write("\nStarting %s.%s() at %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        # are all messages "actioned"?
        bAllMsgsActioned = self.__globalMessageStatusCheck(p_statusToCheck='action_reqd', p_flagForFalseReturn="Y")
         
        return bAllMsgsActioned
    
    
    def anyReleaseFlags(self): 
        ''' 
            :Returns:
                boolean indicating whether any messages have been flagged to indicate that the entry is ready for release
        
        '''
        self.__lfh.write("\nStarting %s.%s()\n" % (self.__class__.__name__, sys._getframe().f_code.co_name) )
        # asking, no flags exist that indicate "for release"?
        bNoFlagsForRelease = self.__globalMessageStatusCheck(p_statusToCheck='for_release', p_flagForFalseReturn="Y")
        # NOTE: in order to make semantic sense, we need to return the boolean opposite of the above return value 
        return (not bNoFlagsForRelease)
    
    def anyNotesExist(self): 
        ''' 
            :Returns:
                boolean indicating whether any notes exist for this dep ID
        
        '''
        self.__lfh.write("\nStarting %s.%s()\n" % (self.__class__.__name__, sys._getframe().f_code.co_name) )
        # asking, no flags exist that indicate "for release"?
        bAnyNotesIncldngArchvdMsgs = False
        bAnnotNotes = False
        bBmrbNotes = False
        iNumNotesRecords = 0
        recordSetLst = []
        #
        try:
            if self.__isWorkflow():
                msgDI=MessagingDataImport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
                self.__notesFilePath = msgDI.getFilePath(contentType='notes-from-annotator',format="pdbx")
                self.__lfh.write("+%s.%s() -- self.__notesFilePath is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, self.__notesFilePath) )
            
            if self.__notesFilePath is not None and os.access(self.__notesFilePath,os.R_OK):
                pdbxMsgIo_notes=PdbxMessageIo(verbose=self.__verbose,log=self.__lfh)
                bGotContent=pdbxMsgIo_notes.read(self.__notesFilePath)
                
                if( bGotContent ):
                    recordSetLst = pdbxMsgIo_notes.getMessageInfo() # in recordSetLst we now have a list of notes created by annotators *as well as* any messages "archived" to notes file
                    iNumNotesRecords = len(recordSetLst)
                    if( iNumNotesRecords >= 1 ):
                        bAnyNotesIncldngArchvdMsgs = True
                         
                    
                    onlyAnnotatorNotesLst = [record for record in recordSetLst if( "archive" not in record['message_type'] )] # i.e. do NOT count messages "archived" into notes file
                    if( len(onlyAnnotatorNotesLst) >= 1 ):
                        bAnnotNotes = True
                    else:
                        # also check for external notes registered via archive mail handler but which require flagging (e.g. annotator comms originating from BMRB)
                        externalNotesReqAttn = [row for row in recordSetLst if( "_flag" in row['message_type'] )]
                        if( len(externalNotesReqAttn) >= 1 ):
                            bAnnotNotes = True
                            bBmrbNotes = True
                         
                    self.__lfh.write("\n+%s.%s() -- returning bAnnotNotes as: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, bAnnotNotes ) )
                    self.__lfh.write("\n+%s.%s() -- returning bBmrbNotes as: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, bBmrbNotes ) )
            #
            if( self.__verbose and self.__debug and self.__debugLvl2 ):
                for idx,row in enumerate(recordSetLst):
                    self.__lfh.write("\n+%s.%s() -- row[%s]: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name,idx,row ) )
            
        except:
            traceback.print_exc(file=self.__lfh)
        #
        return bAnyNotesIncldngArchvdMsgs, bAnnotNotes, bBmrbNotes, iNumNotesRecords
    
    def tagMsg(self,p_msgStatusDict): 
        ''' handle request to have message tagged with user designated classifications (i.e. "action required" or marking as "unread")
            
            :Params:
                :param `p_msgStatusDict`:    dictionary representing message status entity to be submitted
                
            
            :Returns:
                boolean indicating success/failure
        
        '''
        startTime=time.time()        
        self.__lfh.write("\nStarting %s.%s() at %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        #
        bOk = False
        #
        try:
            mS=PdbxMessageStatus(verbose=self.__verbose,log=self.__lfh)
            mS.set(p_msgStatusDict)
            msgId = mS.getMessageId()
            #
            if( self.__verbose ):
                self.__lfh.write("\n---%s.%s() request to tag msg with user classification(s) for msgID: [%s]\n" % (self.__class__.__name__,
                                                           sys._getframe().f_code.co_name,
                                                           msgId))
            #
            if self.__isWorkflow():
                msgDI=MessagingDataImport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
                self.__msgsToDpstrFilePath = msgDI.getFilePath(contentType='messages-to-depositor',format="pdbx")
                self.__lfh.write("+%s.%s() -- self.__msgsToDpstrFilePath is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, self.__msgsToDpstrFilePath) )
            #
            if not os.access(self.__msgsToDpstrFilePath,os.F_OK):
                try:
                    f = open(self.__msgsToDpstrFilePath, "w")
                except IOError:
                    pass
            #
            mIIo=PdbxMessageIo(verbose=self.__verbose,log=self.__lfh)
            msgAlreadySeen = False
            with LockFile(self.__msgsToDpstrFilePath,timeoutSeconds=self.__timeoutSeconds,retrySeconds=self.__retrySeconds,verbose=self.__verbose,log=self.__lfh) as lf, FileSizeLogger(self.__msgsToDpstrFilePath,verbose=self.__verbose,log=self.__lfh) as fsl:
                pid = os.getpid()
                ok=mIIo.read(self.__msgsToDpstrFilePath,"msgingmod"+str(pid))
            if( ok ):
                # i.e. get here if mIIo successfully read/obtained container list, 
                
                recordSetLst = mIIo.getMsgStatusInfo() # in recordSetLst we now have a list of dictionaries with item names as keys and respective data for values
                
                for idx,record in enumerate(recordSetLst):
                    if( record['message_id'] == msgId ):
                        msgAlreadySeen = True
                        mIIo.update('pdbx_deposition_message_status','action_reqd',p_msgStatusDict['action_reqd'],idx)
                        if( record['read_status'] == 'Y' ): # i.e. only pertinent updates of 'read_status' in this method are when user is setting read flag back to 'N' for unread 
                            mIIo.update('pdbx_deposition_message_status','read_status',p_msgStatusDict['read_status'],idx)
                        mIIo.update('pdbx_deposition_message_status','for_release',p_msgStatusDict['for_release'],idx)
                        break
                    
                mIIo.newBlock('messages')
                if msgAlreadySeen is not True: # which can occur if this is the first time any msgStatus is being recorded in the msgsToDpstrFile
                    mIIo.appendMsgReadStatus(mS.get())
                with LockFile(self.__msgsToDpstrFilePath,timeoutSeconds=self.__timeoutSeconds,retrySeconds=self.__retrySeconds,verbose=self.__verbose,log=self.__lfh) as lf:
                    mIIo.write(self.__msgsToDpstrFilePath)
                bOk = ok
                
            else:
                # OR if there was no container list BUT the file is accessible-->indicates no content yet b/c no messages sent to depositor yet
                if( os.access(self.__msgsToDpstrFilePath,os.W_OK) ):
                    mIIo.newBlock('messages')
                    mIIo.appendMsgReadStatus(mS.get())
                    with LockFile(self.__msgsToDpstrFilePath,timeoutSeconds=self.__timeoutSeconds,retrySeconds=self.__retrySeconds,verbose=self.__verbose,log=self.__lfh) as lf:
                        mIIo.write(self.__msgsToDpstrFilePath)
                    bOk = True
                
        except:
            self.__lfh.write("Update message tags failed\n")                
            traceback.print_exc(file=self.__lfh)
            
        endTime=time.time()
        self.__lfh.write("\nCompleted %s.%s() at %s (%d seconds)\n" % (self.__class__.__name__,
                                                        sys._getframe().f_code.co_name,
                                                        time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                        endTime-startTime))
        #
        return bOk    
    
    
    ################################################################################################################
    # ------------------------------------------------------------------------------------------------------------
    #      Private helper methods
    # ------------------------------------------------------------------------------------------------------------
    #
    def __getFileSizeBytes(self,p_filePath):
        statInfo = os.stat(p_filePath)
        fileSize = statInfo.st_size
        
        return fileSize
    
    def __globalMessageStatusCheck(self,p_statusToCheck,p_flagForFalseReturn):
        startTime=time.time()        
        self.__lfh.write("\nStarting %s.%s() at %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        self.__lfh.write("\n%s.%s() -- checking global status for: '%s'\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       p_statusToCheck) )
        
        bReturnStatus = True
        msgsFrmDpstrLst = []
        msgStatusLst = []
        
        try:
            ### GET LIST OF IDS OF MSGS FROM DEPOSITOR
            if self.__isWorkflow():
                msgDI=MessagingDataImport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
                self.__msgsFrmDpstrFilePath = msgDI.getFilePath(contentType='messages-from-depositor',format="pdbx")
                self.__msgsToDpstrFilePath = msgDI.getFilePath(contentType='messages-to-depositor',format="pdbx")
                self.__lfh.write("+%s.%s() -- self.__msgsFrmDpstrFilePath is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, self.__msgsFrmDpstrFilePath) )
                self.__lfh.write("+%s.%s() -- self.__msgsToDpstrFilePath is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, self.__msgsToDpstrFilePath) )
                
            if os.access(self.__msgsFrmDpstrFilePath,os.R_OK):
                fileSizeBytes = self.__getFileSizeBytes(self.__msgsFrmDpstrFilePath)
                if( fileSizeBytes > 0  ):
                    mIIo=PdbxMessageIo(verbose=self.__verbose,log=self.__lfh)
                    with LockFile(self.__msgsFrmDpstrFilePath,timeoutSeconds=self.__timeoutSeconds,retrySeconds=self.__retrySeconds,verbose=self.__verbose,log=self.__lfh) as lf, FileSizeLogger(self.__msgsFrmDpstrFilePath,verbose=self.__verbose,log=self.__lfh) as fsl:
                        pid = os.getpid()
                        ok=mIIo.read(self.__msgsFrmDpstrFilePath,"msgingmod"+str(pid))
                    if( ok ):
                        msgsFrmDpstrLst = mIIo.getMessageInfo() # in recordSetLst we now have a list of dictionaries with item names as keys and respective data for values
            
            if os.access(self.__msgsToDpstrFilePath,os.R_OK):
                fileSizeToDpstr = self.__getFileSizeBytes(self.__msgsToDpstrFilePath)
                if( fileSizeToDpstr > 0  ):
                    mIIo2=PdbxMessageIo(verbose=self.__verbose,log=self.__lfh)
                    with LockFile(self.__msgsToDpstrFilePath,timeoutSeconds=self.__timeoutSeconds,retrySeconds=self.__retrySeconds,verbose=self.__verbose,log=self.__lfh) as lf, FileSizeLogger(self.__msgsToDpstrFilePath,verbose=self.__verbose,log=self.__lfh) as fsl:
                        pid = os.getpid()
                        ok=mIIo2.read(self.__msgsToDpstrFilePath,"msgingmod"+str(pid))
                    if( ok ):
                        msgStatusLst = mIIo2.getMsgStatusInfo() # in recordSetLst we now have a list of dictionaries with item names as keys and respective data for values
                            
            for msg in msgsFrmDpstrLst:
                msgFound = False
                
                for msgStatus in msgStatusLst:
                    if( msg['message_id'] == msgStatus['message_id'] ):
                        msgFound = True
                        if( msgStatus[p_statusToCheck] == p_flagForFalseReturn ):
                            bReturnStatus = False
                            self.__lfh.write("+%s.%s() -- found flag of '%s' for status '%s' so returning False\n" % (self.__class__.__name__,
                                                                                                                      sys._getframe().f_code.co_name,
                                                                                                                      p_flagForFalseReturn,
                                                                                                                      p_statusToCheck) )
                            return bReturnStatus
                
                if msgFound == False and p_statusToCheck != 'for_release':
                    # handling here for instances in which the given message was not found in message_status category
                    bReturnStatus = False
                    return bReturnStatus
                
            if( p_statusToCheck == 'for_release' ): # for this status check we have to check messages authored by annotators as well for "for_release" flags
                
                if( os.access(self.__msgsToDpstrFilePath,os.R_OK) and fileSizeToDpstr > 0  ):
                    
                    annotatorMsgsLst = mIIo2.getMessageInfo()
                        
                    for row in annotatorMsgsLst:
                        
                        for msgStatus in msgStatusLst:
                            if( row['message_id'] == msgStatus['message_id'] ):
                                if( msgStatus[p_statusToCheck] == p_flagForFalseReturn ):
                                    bReturnStatus = False
                                    self.__lfh.write("+%s.%s() -- found flag of '%s' for status '%s' so returning False\n" % (self.__class__.__name__,
                                                                                                                              sys._getframe().f_code.co_name,
                                                                                                                              p_flagForFalseReturn,
                                                                                                                              p_statusToCheck) )
                                    return bReturnStatus
                
            #
            
        except:
            self.__lfh.write("check global msg '"+p_statusToCheck+"' status failed\n")                
            traceback.print_exc(file=self.__lfh)
            
        endTime=time.time()
        self.__lfh.write("\nCompleted %s.%s() at %s (%d seconds)\n" % (self.__class__.__name__,
                                                        sys._getframe().f_code.co_name,
                                                        time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                        endTime-startTime))
        #
        return bReturnStatus    
    
    def __getMsgsByStatus(self,p_statusToCheck,p_flagForInclusion):
        
        rtrnList = []
        recordSetLst = []
        #
        try:
            if self.__isWorkflow():
                msgDI=MessagingDataImport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
                self.__msgsToDpstrFilePath = msgDI.getFilePath(contentType='messages-to-depositor',format="pdbx")
                self.__lfh.write("+%s.%s() -- self.__msgsToDpstrFilePath is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, self.__msgsToDpstrFilePath) )
                
            if os.access(self.__msgsToDpstrFilePath,os.R_OK):
                fileSizeBytes = self.__getFileSizeBytes(self.__msgsToDpstrFilePath)
                if( fileSizeBytes > 0  ):
                    mIIo=PdbxMessageIo(verbose=self.__verbose,log=self.__lfh)
                    with LockFile(self.__msgsToDpstrFilePath,timeoutSeconds=self.__timeoutSeconds,retrySeconds=self.__retrySeconds,verbose=self.__verbose,log=self.__lfh) as lf, FileSizeLogger(self.__msgsToDpstrFilePath,verbose=self.__verbose,log=self.__lfh) as fsl:
                        pid = os.getpid()
                        ok=mIIo.read(self.__msgsToDpstrFilePath,"msgingmod"+str(pid))
                    if( ok ):
                        recordSetLst = mIIo.getMsgStatusInfo() # in recordSetLst we now have a list of dictionaries with item names as keys and respective data for values
                    #
                    for row in recordSetLst:
                        if( self.__verbose and self.__debug ):
                            self.__lfh.write("\n+%s.%s() -- row: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name,row ) )
                        if( row[p_statusToCheck]==p_flagForInclusion ):
                            rtrnList.append( row['message_id'] )
                    #
                    # when p_statusToCheck == 'action_reqd' is input, parent code is actually looking for cases where NO action is required
                    # so we need to return all those messages authored by the annotator so that these are not flagged with "To-Do" icon in the UI 
                    if( p_statusToCheck == 'action_reqd' ):
                        annotatorMsgsLst = mIIo.getMessageInfo()
                        for row in annotatorMsgsLst:
                            if( row['message_id'] not in rtrnList ):
                                rtrnList.append( row['message_id'] )
                
        except:
            traceback.print_exc(file=self.__lfh)
        #
        return rtrnList    
    
    def __isNotCifNull(self,p_value):
        if( p_value == "." or p_value == "?" ):
            return False
        else:
            return True
    
    def __isCifNull(self,p_value):
        return ( not self.__isNotCifNull(p_value) )
    
    def __augmentWithOrigCommData(self,p_recordSetLst,p_origCommsLst):
        for record in p_recordSetLst:
            record['orig_message_id'] = ''
            record['orig_deposition_data_set_id'] = ''
            record['orig_timestamp'] = ''
            record['orig_sender'] = ''
            record['orig_recipient'] = ''
            record['orig_message_subject'] = ''
            record['orig_attachments'] = ''
            
            for origComm in p_origCommsLst:
                if( origComm['message_id'] == record['message_id'] ):
                    record['orig_message_id'] = origComm['orig_message_id']
                    record['orig_deposition_data_set_id'] = origComm['orig_deposition_data_set_id']
                    record['orig_timestamp'] = origComm['orig_timestamp'] 
                    record['orig_sender'] = origComm['orig_sender'] 
                    record['orig_recipient'] = origComm['orig_recipient']
                    record['orig_message_subject'] = origComm['orig_message_subject'] 
                    record['orig_attachments'] = origComm['orig_attachments']
                    break
        
    def __handleFileReferences(self,p_msgObj):
        ''' For given message, processes any files referenced for "attachment"
            This involves making "annotate" snapshot copy of the file which is stored in both annotation and deposition areas
            For any references to model files, this additionally includes creation of "review" snapshot copy for storage in
            both annotation and deposition areas. 
            
            :Params:
                :param `p_depId`:                unique deposition dataset ID
                :param `p_msgId`:                unique message ID
                :param `p_fileReferencesList`:    list of files referenced by the given message
                
            
            :Returns:
                bOk : boolean indicating success/failure
                msgFileRefs : list of all file references for which processing succeeded
                failedMsgFileRefs : list of any file references for which processing failed 
        
        '''
        sClassName = self.__class__.__name__
        sMethodName = sys._getframe().f_code.co_name
        logger.info ("STARTING")
        
        sIsEmEntry = self.__reqObj.getValue("em_entry")
        sIsEmMapOnly = self.__reqObj.getValue("em_map_only")
        sIsEmMapAndModel = self.__reqObj.getValue("em_map_and_model")
        #
        workingFileRefsList = list(p_msgObj.fileReferences)
        depositionId = p_msgObj.depositionId

        logger.info("-- p_msgObj.fileReferences is: %r \n" % (p_msgObj.fileReferences ) )
        logger.info("-- workingFileRefsList is: %r \n" % workingFileRefsList )

        msgFileRefs = []
        failedMsgFileRefs = []
        bOk = True
        
        msgDI=MessagingDataImport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
        fPath = None
        contentType = None
        contentFormat = None
        
        bAtLeastOneAuxFile = False # i.e. answer to question: have we encountered at least one auxiliary file yet?
        auxFilePartNum = 0
        # Handle validation bundle
        if "val-report-batch" in workingFileRefsList:
            workingFileRefsList.remove("val-report-batch")
            # Determine available reports
            avail = self.checkAvailFiles(depositionId)
            preference = ['val-report', 'val-report-full', 'val-data', 
                          'val-report-wwpdb-2fo-fc-edmap-coef', 'val-report-wwpdb-fo-fc-edmap-coef']
            for f in preference:
                if f in avail:
                    workingFileRefsList.append(f)

        if( self.__verbose and sIsEmEntry == 'true' ):
            sType = "Map Only" if sIsEmMapOnly == 'true' else "Map and Model"
            
            logger.info("-- Processing an EM deposition of type: %s\n" % sType )
        
        # If em-volume requested - send model file as well... Should this be for map only?
        if( "em-volume" in workingFileRefsList and "model" not in workingFileRefsList ):
                
            workingFileRefsList.append("model")
            if( self.__verbose ):
                logger.info("-- 'em-volume' was selected as file reference so automatically propagating 'model' also, for 'annotate' purposes on deposit side")
                        
        for fileRef in workingFileRefsList:
            # fileRefs received correspond to content type acronyms encoded in ConfigInfoData
            
            auxFileIndx = str(fileRef.split("aux-file")[1]) if "aux-file" in fileRef else "" 
            upldFileName = ""
            
            if( len( auxFileIndx ) > 0 ):
                acronym = "aux-file"
                
                # if we're operating in mode where multiple aux file references are allowed for a single message
                # then we will use the "P"artition number to distinguish between the multiple aux-files referenced
                # NOTE: therefore when in mode where only single aux-file reference is allowed, this code has no impact
                if( not bAtLeastOneAuxFile ): # i.e. if we haven't encountered the first auxiliary file yet
                    auxFilePartNum = self.__getNextAuxFilePartNum()
                    bAtLeastOneAuxFile = True
                else:
                    auxFilePartNum += 1
            else:
                acronym = fileRef
                
            contentType,contentFormat = self.__getContentTypeAndFormat(acronym,auxFileIndx)
            
            if self.__isWorkflow():
                if acronym == 'aux-file':
                    fPath = self.__reqObj.getValue("auxFilePath"+auxFileIndx) # i.e. if aux-file then file is being provided by annotator and is NOT sourced from archive storage
                else:
                    fPath = msgDI.getFilePath(contentType,contentFormat)
                
                if( fPath is not None and os.access(fPath,os.R_OK) ):
                    
                    # make straight copy of the file to generate "-annotate" milestone version of the file
                    bOk = self.__createAnnotateMilestone(p_msgObj.depositionId,p_msgObj.messageId,fPath,acronym,contentType,contentFormat,auxFilePartNum,msgFileRefs,failedMsgFileRefs)
                    
                    if( not bOk ):
                        break
                                
                    if( fileRef == 'model' and sIsEmMapOnly != "true" ):
                        # if dealing with model file then additionally make copy of model file in which internal view items are stripped out--this serves as "-review" version of the file
                        bOk = self.__createModelReviewCopy(p_msgObj.depositionId,p_msgObj.messageId,fPath,acronym,contentType,contentFormat,msgFileRefs,failedMsgFileRefs)
                        
                        if( not bOk ):
                            break
                        
                    if( fileRef == 'cs' ):
                        # if dealing with chemical shifts file then additionally make copy of cs file in which internal view items are stripped out--this serves as "-review" version of the file
                        bOk = self.__createChemShiftsReviewCopy(p_msgObj.depositionId,p_msgObj.messageId,fPath,acronym,contentType,"nmr-star",msgFileRefs,failedMsgFileRefs)
                        
                        if( not bOk ):
                            break
                else:
                    bOk = False
                    failedMsgFileRefs.append(acronym)
                    if( self.__verbose ):
                        logger.error("-- problem with accessing fPath: %s" % fPath )
                        
                
            else: # not workflow, i.e. standalone testing, so just simulate behavior
                annotVersionNum = 1
                annotPartitionNum = 1
                generatedFilesList = [contentType+'-annotate']
                if( fileRef == 'model' ):
                     generatedFilesList.append(contentType+'-review')
                     
                for cntntTyp in generatedFilesList:
                    if contentType == 'auxiliary-file':
                        annotPartitionNum = auxFilePartNum 
                    
                    msgFileRefs.append( self.__createMsgFileReference(p_msgObj.messageId, p_msgObj.depositionId, cntntTyp, contentFormat, annotPartitionNum, annotVersionNum, upldFileName) )
            
        return bOk, msgFileRefs, failedMsgFileRefs
    
    def __createAnnotateMilestone(self,p_depId,p_msgId,fPath,acronym,contentType,contentFormat,auxFilePartNum,msgFileRefs,failedMsgFileRefs):
        """Creates the --annotete milestones and will symlink to deposit non-milestone files for the V3.0 DepUI to make available"""

        logger.debug("Starting %s %s" %(contentType, contentFormat))

        bOk = True
        bEmdCnvrtRqrd = (self.__emDeposition and acronym == "model") # i.e. need to convert model file into "emd" dialect for storage on deposition side
        
        ###############################################################################################
        # make straight copy of the file to generate "-annotate" milestone version of the file
        ###############################################################################################
        msgDE=MessagingDataExport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
        mileStoneCntntTyp = contentType+'-annotate'
        upldFileName = ""
        #
        if acronym == 'aux-file':
            # auxiliary file references require special handling
            
            try:
                if( self.__allowingMultiAuxFiles ):
                    milestoneFilePthDict = msgDE.getMileStoneFilePaths(mileStoneCntntTyp,contentFormat,auxFilePartNum)
                else:
                    milestoneFilePthDict = msgDE.getMileStoneFilePaths(mileStoneCntntTyp,contentFormat)
            except:
                bOk = False
                failedMsgFileRefs.append(acronym)
                return bOk
                
            # for cases of auxiliary files, we capture the original name of the file as uploaded by the annotator
            upldFileName = os.path.basename(fPath)
            
        else:
            milestoneFilePthDict = msgDE.getMileStoneFilePaths(mileStoneCntntTyp,contentFormat)
        #    
        logger.debug("milestonFilePthDict %s" % milestoneFilePthDict)

        # next version
        annotMilestoneFilePth = milestoneFilePthDict['annotPth']
        dpstMilestoneFilePth = milestoneFilePthDict['dpstPth'] #same filename as "archive" version of file, but different path for "deposit" area
        # last version
        curAnnotMilestoneFilePth = milestoneFilePthDict['curPth']
        curDpstMilestoneFilePth = milestoneFilePthDict['curDpstPth'] #same filename as "archive" version of file, but different path for "deposit" area

        
        if( annotMilestoneFilePth is not None ):
            # For multi-part files - always copy - easier to handle the parts business
 
            if self.__skipCopyIfSame:
                if self.__sameFile(fPath, curAnnotMilestoneFilePth):
                    logger.debug("Existing milestone good -- using %s %s" % (fPath, curAnnotMilestoneFilePth))
                    annotMilestoneFilePth = curAnnotMilestoneFilePth
                    dpstMilestoneFilePth = curDpstMilestoneFilePth
                else:
                    shutil.copyfile(fPath, annotMilestoneFilePth)
            else:
                shutil.copyfile(fPath, annotMilestoneFilePth)
                    
            if ( os.access(annotMilestoneFilePth,os.R_OK) ):
                annotVersionNum = annotMilestoneFilePth.rsplit(".V")[1]
                
                if( acronym == 'aux-file' and self.__allowingMultiAuxFiles ):
                    # use next available partition number for aux-files as supplied by calling code
                    annotPartitionNum = auxFilePartNum
                else:
                    # if not aux-file then just parse P# from the targeted filename 
                    annotPartitionNum = (annotMilestoneFilePth.split("_P")[1]).split(".",1)[0]
                    
                if( self.__verbose and self.__debug ):
                        self.__lfh.write("+%s.%s() -- annotMilestoneFilePth is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, annotMilestoneFilePth) )
                ####################################################    
                # create counterpart copy in deposition storage area
                if( dpstMilestoneFilePth is not None ):
                    if( self.__verbose and self.__debug ):
                        logger.debug("'bEmdCnvrtRqrd' is: %s" % bEmdCnvrtRqrd)
                    
                    if( bEmdCnvrtRqrd and self.__copyMilestoneDeposit ):
                        # generate "emd" dialect version of model file for storage/use on deposition side
                        # Only do this if copying to deposit directory
                        bSuccess = self.__genAnnotMilestoneEmdVrsn(p_depId, fPath, dpstMilestoneFilePth)
                        if( not bSuccess ):
                            if( self.__verbose ):
                                self.__lfh.write("+%s.%s() -- WARNING: problem creating 'emd' version of model file at: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, dpstMilestoneFilePth) )
                        #
                        mlstnFilePthDict = msgDE.getMileStoneFilePaths("em-volume-header-annotate","xml")
                        dpstEmHeaderMilestoneFilePth = mlstnFilePthDict['dpstPth']
                        
                        if( MessagingIo.bMakeEmXmlHeaderFiles == True ):
                            bSuccess2 = self.__genAnnotMilestoneEmXmlHeader(p_depId, dpstMilestoneFilePth, dpstEmHeaderMilestoneFilePth) if dpstEmHeaderMilestoneFilePth else False
                        else:
                            bSuccess2 = True
                            
                        if( not bSuccess2 ):
                            if( self.__verbose ):
                                self.__lfh.write("+%s.%s() -- WARNING: problem creating xml header version of model file at: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, dpstEmHeaderMilestoneFilePth) )
                        
                        #     
                        if( not bSuccess ) or (not bSuccess2 ):
                            bOk = False
                            failedMsgFileRefs.append(mileStoneCntntTyp)
                    else:
                        # else just propagate identifical copy to deposition side if requested
                        if self.__copyMilestoneDeposit:
                            shutil.copyfile(fPath, dpstMilestoneFilePth)
                            logger.debug("Milestone copied to deposit %s" % dpstMilestoneFilePth)
                            
                    if( (self.__copyMilestoneDeposit and os.access(dpstMilestoneFilePth,os.R_OK)) or (not self.__copyMilestoneDeposit) ):
                        msgFileRefs.append( self.__createMsgFileReference(p_msgId, p_depId, mileStoneCntntTyp, contentFormat, annotPartitionNum, annotVersionNum, upldFileName) )
                        
                    else:
                        bOk = False
                        failedMsgFileRefs.append(mileStoneCntntTyp)
                        logger.error("problem with accessing dpstMilestoneFilePth if copy to deposit enabled: %s" % dpstMilestoneFilePth)
                
                #################################### Done copy to deposit directory
                if self.__symlinkDepositAnnotate:
                    # Create symlink from contenttype in deposit to archive -annotate milestone for V3.0 of DepUI
                    # Only do this for non aux-file and non-model
                    if acronym not in ['model', 'aux-file', 'model_pdb']:
                        logger.debug("About to symlink to deposit acronym: %s contentType: %s" % (acronym, contentType))
                        depFilePthDict = msgDE.getFilePathExt(contentType = contentType, format = contentFormat,
                                                              fileSource = 'deposit', version='next')
                        logger.info("Symlink %s -> %s" % (depFilePthDict, annotMilestoneFilePth))
                        try:
                            os.symlink(annotMilestoneFilePth, depFilePthDict)
                        except:
                            logger.exception("Failed to create symlink")

            else:
                bOk = False
                failedMsgFileRefs.append(mileStoneCntntTyp)
                if( self.__verbose ):
                    self.__lfh.write("+%s.%s() -- problem with accessing annotMilestoneFilePth: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, annotMilestoneFilePth) )
                    
        return bOk
    
    
    def __createModelReviewCopy(self,p_depId,p_msgId,fPath,acronym,contentType,contentFormat,msgFileRefs,failedMsgFileRefs):
        ##################################################################################
        # if dealing with model file then make additional copy of model file in which  
        # internal view items are stripped out--this serves as "-review" version of the file
        ##################################################################################
        bOk = True
        bEmExclusion = (self.__emDeposition and acronym == "model")
        #
        logger.debug("STARTING")
        msgDE=MessagingDataExport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
        reviewCntntTyp = contentType+'-review'
        reviewFilePthDict = msgDE.getMileStoneFilePaths(reviewCntntTyp,contentFormat)
        # next version
        reviewAnnotMilestoneFilePth = reviewFilePthDict['annotPth']
        # current version
        reviewAnnotCurMilestoneFilePth = reviewFilePthDict['curPth']
        reviewDpstMilestoneFilePth = reviewFilePthDict['dpstPth'] #same filename as "archive" version of file, but different path for "deposit" area 
        reviewDpstCurMilestoneFilePth = reviewFilePthDict['curDpstPth'] #same filename as "archive" version of file, but different path for "deposit" area 

        logger.debug("reviewAnnotMilestoneFilePth: %s" % reviewAnnotMilestoneFilePth)
        if( reviewAnnotMilestoneFilePth is not None ):
            
            sourceMdlFileName = os.path.basename(fPath)
            modelFileLocalPthAbslt = os.path.join(self.__sessionPath,sourceMdlFileName) 
            logger.debug("Copy %s to %s" % (fPath, modelFileLocalPthAbslt))
            shutil.copyfile(fPath,modelFileLocalPthAbslt)
            
            if os.access(modelFileLocalPthAbslt, os.R_OK):
                
                #
                # Generate Public pdbx cif file
                #
                pdbxReviewFilePath=os.path.join(self.__sessionPath, p_depId + "_model-review_P1.cif") # filename here is arbitrary just for temporary session processing purposes
                
                try:
                    dp = RcsbDpUtility(tmpPath=self.__sessionPath, siteId=self.__siteId, verbose=self.__verbose,log=self.__lfh)
                    dp.imp(modelFileLocalPthAbslt)
                    dp.op("cif2pdbx-public")
                    logPath=os.path.join(self.__sessionPath,"cif2pdbx-public.log")
                    dp.expLog(logPath)
                    dp.exp(pdbxReviewFilePath)
                    if( not self.__debug ):
                        dp.cleanup()
                except:
                    logger.exception("Exception in generating review copy")
                
                if os.access(pdbxReviewFilePath, os.F_OK):

                    if self.__skipCopyIfSame:
                        if self.__sameFile(pdbxReviewFilePath, reviewAnnotCurMilestoneFilePth):
                            logger.debug("Existing milestone good -- using %s %s" % (pdbxReviewFilePath, reviewAnnotCurMilestoneFilePth))
                            reviewAnnotMilestoneFilePth = reviewAnnotCurMilestoneFilePth
                            reviewDpstMilestoneFilePth = reviewDpstCurMilestoneFilePth
                        else:
                            shutil.copyfile(pdbxReviewFilePath,reviewAnnotMilestoneFilePth)
                    else:
                        shutil.copyfile(pdbxReviewFilePath,reviewAnnotMilestoneFilePth)
                    
                    annotVersionNum = reviewAnnotMilestoneFilePth.rsplit(".V")[1]
                    annotPartitionNum = (reviewAnnotMilestoneFilePth.split("_P")[1]).split(".",1)[0]
                    
                    if( self.__verbose and self.__debug ):
                        logger.debug("reviewAnnotMilestoneFilePth is: %s" % reviewAnnotMilestoneFilePth)
                            
                    if reviewDpstMilestoneFilePth is not None:
                        if self.__copyMilestoneDeposit:
                            if( os.access(reviewAnnotMilestoneFilePth, os.R_OK) ):
                                shutil.copyfile(reviewAnnotMilestoneFilePth, reviewDpstMilestoneFilePth)
                                    
                                if ( os.access(reviewDpstMilestoneFilePth,os.R_OK) ):
                                    if( self.__verbose and self.__debug ):
                                        logger.debug("reviewDpstMilestoneFilePth is: %s" % reviewDpstMilestoneFilePth)
                                
                                    msgFileRefs.append( self.__createMsgFileReference(p_msgId, p_depId, reviewCntntTyp, contentFormat, annotPartitionNum, annotVersionNum ) )
                                else:
                                    bOk = False
                                    failedMsgFileRefs.append(reviewCntntTyp)
                                    logger.error("problem with accessing deposit copy of 'review' milestone model file at: %s" % reviewDpstMilestoneFilePth)
                                    
                            else:
                                bOk = False
                                failedMsgFileRefs.append(reviewCntntTyp)
                                logger.error("problem with accessing annotation copy of 'review' milestone model file at: %s" % reviewAnnotMilestoneFilePth)
                        else:
                            # For not copying to deposit, register attachment
                            msgFileRefs.append( self.__createMsgFileReference(p_msgId, p_depId, reviewCntntTyp, contentFormat, annotPartitionNum, annotVersionNum ) )
                    
                else:
                    bOk = False
                    failedMsgFileRefs.append(reviewCntntTyp)
                    if( self.__verbose ):
                        logger.error("problem with accessing session copy of 'review' milestone model file at: %s" % pdbxReviewFilePath)
        
        logger.debug("FINISHED %s" % bOk)

        return bOk
    
    def __createChemShiftsReviewCopy(self,p_depId,p_msgId,fPath,acronym,contentType,contentFormat,msgFileRefs,failedMsgFileRefs):
        ##################################################################################
        # if dealing with cs file then make additional copy of cs file in which  
        # internal view items are stripped out--this serves as "-review" version of the file
        ##################################################################################
        bOk = True
        #
        msgDE=MessagingDataExport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
        reviewCntntTyp = contentType+'-review'
        reviewFilePthDict = msgDE.getMileStoneFilePaths(reviewCntntTyp,contentFormat)
        # next version
        reviewAnnotMilestoneFilePth = reviewFilePthDict['annotPth']
        reviewDpstMilestoneFilePth = reviewFilePthDict['dpstPth'] #same filename as "archive" version of file, but different path for "deposit" area 
        # current version
        reviewAnnotCurMilestoneFilePth = reviewFilePthDict['curPth']
        reviewDpstCurMilestoneFilePth = reviewFilePthDict['curDpstPth'] #same filename as "archive" version of file, but different path for "deposit" area 
        
        if( reviewAnnotMilestoneFilePth is not None ):
            
            sourceChemShiftsFileName = os.path.basename(fPath)
            chemShiftsFileLocalPthAbslt = os.path.join(self.__sessionPath,sourceChemShiftsFileName) 
            shutil.copyfile(fPath,chemShiftsFileLocalPthAbslt)
            
            if os.access(chemShiftsFileLocalPthAbslt, os.R_OK):
                
                #
                # Generate Public pdbx cif file
                #
                nmrStarReviewFilePath=os.path.join(self.__sessionPath, p_depId + "_cs-review_P1.cif") # filename here is arbitrary just for temporary session processing purposes
                
                try:
                    dfa = DataFileAdapter(reqObj=self.__reqObj, verbose=self.__verbose, log=self.__lfh)
                    ok = dfa.pdbx2nmrstar(chemShiftsFileLocalPthAbslt, nmrStarReviewFilePath, pdbId=p_depId)
                     
                except:
                    traceback.print_exc(file=self.__lfh)
                
                if ok and os.access(nmrStarReviewFilePath, os.F_OK):
                    if self.__skipCopyIfSame:
                        if self.__sameFile(nmrStarReviewFilePath, reviewAnnotCurMilestoneFilePth):
                            logger.debug("Existing milestone good -- using %s %s" % (nmrStarReviewFilePath, reviewAnnotCurMilestoneFilePth))
                            reviewAnnotMilestoneFilePth = reviewAnnotCurMilestoneFilePth
                            reviewDpstMilestoneFilePth = reviewDpstCurMilestoneFilePth
                        else:
                            shutil.copyfile(nmrStarReviewFilePath,reviewAnnotMilestoneFilePth)
                    else:
                        shutil.copyfile(nmrStarReviewFilePath,reviewAnnotMilestoneFilePth)
                    
                    annotVersionNum = reviewAnnotMilestoneFilePth.rsplit(".V")[1]
                    annotPartitionNum = (reviewAnnotMilestoneFilePth.split("_P")[1]).split(".",1)[0]
                    
                    if( self.__verbose and self.__debug ):
                            logger.debug("reviewAnnotMilestoneFilePth is: %s" % reviewAnnotMilestoneFilePth) 
                            
                    if( reviewDpstMilestoneFilePth is not None ):
                        
                        if( os.access(reviewAnnotMilestoneFilePth, os.R_OK) ):
                            if self.__copyMilestoneDeposit:
                                shutil.copyfile(reviewAnnotMilestoneFilePth, reviewDpstMilestoneFilePth)
                                    
                                if ( os.access(reviewDpstMilestoneFilePth,os.R_OK) ):
                                    if( self.__verbose and self.__debug ):
                                        logger.debug("reviewDpstMilestoneFilePth is: %s" % reviewDpstMilestoneFilePth)
                                
                                    msgFileRefs.append( self.__createMsgFileReference(p_msgId, p_depId, reviewCntntTyp, contentFormat, annotPartitionNum, annotVersionNum ) )
                                
                                else:
                                    bOk = False
                                    failedMsgFileRefs.append(reviewCntntTyp)
                                    logger.error("problem with accessing deposit copy of 'review' milestone model file at: %s" % reviewDpstMilestoneFilePth)
                            else:
                                # For not copying to deposit, register attachment
                                msgFileRefs.append( self.__createMsgFileReference(p_msgId, p_depId, reviewCntntTyp, contentFormat, annotPartitionNum, annotVersionNum ) )
                                    
                        else:
                            bOk = False
                            failedMsgFileRefs.append(reviewCntntTyp)
                            logger.error("problem with accessing annotation copy of 'review' milestone model file at: %s" % reviewAnnotMilestoneFilePth)

                    
                else:
                    bOk = False
                    failedMsgFileRefs.append(reviewCntntTyp)
                    if( self.__verbose ):
                        self.__lfh.write("+%s.%s() -- problem with accessing session copy of 'review' milestone model file at: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, nmrStarReviewFilePath) )
        
        return bOk
    
    def __genAnnotMilestoneEmdVrsn(self,p_depId,p_srcFilePath,p_dstFilePath):
    
        bOk = True
        
        #################################################################################################################
        # for EM entries we generate version of model file that uses "emd" dialect as required by deposition side 
        #################################################################################################################
        
        #p_srcFilePath = "/net/wwpdb_da/da_top/wwpdb_da_test/source/python/pdbx_v2/tests/4UI9-em-org.cif"  # FOR PRELIM TESTING ONLY
        
        #dpstModelEmFilePth_Local = os.path.join(self.__sessionPath,p_depId+"_model-em_P1.cif") # filename here is arbitrary just for temporary session processing purposes
        dpstModelEmdFilePth_Local = os.path.join(self.__sessionPath,p_depId+"_model-emd_P1.cif") # filename here is arbitrary just for temporary session processing purposes
        
        if( self.__verbose and self.__debug ):
            logger.debug("dpstModelEmdFilePth_Local is: %s" % dpstModelEmdFilePth_Local)
        
        if( p_srcFilePath and p_dstFilePath ):
            
            startTime = time.clock()
            logger.debug("Starting at %s" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
            try:
                #
                im = InstanceMapper(verbose=self.__verbose, log=self.__lfh)
                im.setMappingFilePath(self.__emdDialectMappingFile)
                #bOk = im.translate(p_srcFilePath, dpstModelEmFilePth_Local, mode="dst-src")
                #bOk = im.translate(dpstModelEmFilePth_Local, dpstModelEmdFilePth_Local, mode="src-dst")
                bOk = im.translate(p_srcFilePath, dpstModelEmdFilePth_Local, mode="src-dst")
                self.__lfh.write("+%s %s return status %r \n" % (self.__class__.__name__, sys._getframe().f_code.co_name, bOk))
                
            except:
                traceback.print_exc(file=sys.stdout)
                bOk = False
                return bOk
    
            endTime = time.clock()
            logger.debug("Completed at %s (%.2f seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                               endTime - startTime))

            logger.debug("About to copy file %s %s %s" %(bOk, dpstModelEmdFilePth_Local, p_dstFilePath))
            if ( bOk and os.access(dpstModelEmdFilePth_Local,os.R_OK) ):
                shutil.copyfile(dpstModelEmdFilePth_Local, p_dstFilePath)
                                
        return bOk
    
    
    def __genAnnotMilestoneEmXmlHeader(self,p_depId,p_srcFilePath,p_dstFilePath):
    
        bOk = True
        
        ####################################################################################################################################
        # for EM entries we generate XML header file that corresponds with latest version of model file to be stored only on deposition side 
        ####################################################################################################################################
        
        #p_srcFilePath = "/net/wwpdb_da/da_top/wwpdb_da_test/source/python/pdbx_v2/tests/4UI9-em-org.cif"  # FOR PRELIM TESTING ONLY
        
        dpstModelEmHdrFilePth_Local = os.path.join(self.__sessionPath,p_depId+"_em-volume-header_P1.xml") # filename here is arbitrary just for temporary session processing purposes
        emHeaderUtilLocalLogPath = os.path.join(self.__sessionPath,p_depId+"_emHeaderUtil.log") 
        
        if( self.__verbose and self.__debug ):
            self.__lfh.write("+%s.%s() -- dpstModelEmHdrFilePth_Local is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, dpstModelEmHdrFilePth_Local) )
        
        if( p_srcFilePath and p_dstFilePath ):
            
            startTime = time.clock()
            self.__lfh.write("\nStarting %s %s at %s\n" % (self.__class__.__name__,
                                                           sys._getframe().f_code.co_name,
                                                           time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
            try:
                #
                emHeaderUtil = EmHeaderUtils(self.__siteId, verbose=self.__verbose, log=self.__lfh)
                bOk = emHeaderUtil.transHeader(p_srcFilePath, dpstModelEmHdrFilePth_Local, emHeaderUtilLocalLogPath)                
                
                self.__lfh.write("+%s %s return status %r \n" % (self.__class__.__name__, sys._getframe().f_code.co_name, bOk))
                
            except:
                traceback.print_exc(file=sys.stdout)
                bOk = False
                return bOk
    
            endTime = time.clock()
            self.__lfh.write("\nCompleted %s %s at %s (%.2f seconds)\n" % (self.__class__.__name__,
                                                                           sys._getframe().f_code.co_name,
                                                                           time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                                           endTime - startTime))
            
            if( bOk and os.access(dpstModelEmHdrFilePth_Local,os.R_OK) ):
                shutil.copyfile(dpstModelEmHdrFilePth_Local, p_dstFilePath)
                                
        return bOk    
    
    def __sameFile(self, f1, f2):
        """Returns True if file contents of f1 and f2 are the same"""
        if (not f1 or not f2):
            return False

        if (not os.access(f1, os.R_OK) or not os.access(f2, os.R_OK)):
            return False

        # Shallow False is to look at file if mtime and size are the same. Would be unusual 
        # for to be different - but you never know
        return filecmp.cmp(f1, f2, False)
        


    def __createMsgFileReference(self,p_msgId,p_depId,p_contentType,p_contentFormat,p_annotPartitionNum,p_annotVersionNum,p_upldFileName=None,p_storageType="archive"):
        
        mfr=PdbxMessageFileReference(verbose=self.__verbose,log=self.__lfh)
        mfr.setMessageId(p_msgId)
        mfr.setDepositionId(p_depId)   
        mfr.setStorageType(p_storageType)   
        mfr.setContentType(p_contentType)
        mfr.setContentFormat(p_contentFormat)
        if( p_upldFileName ):
            mfr.setUploadFileName(p_upldFileName)
        mfr.setPartitionNumber(p_annotPartitionNum)
        mfr.setVersionId(p_annotVersionNum)
        
        return mfr
                
    def __getContentType(self,acronym):
        contentType = None
        
        for kyContentType,configValue in self.__contentTypeDict.items():
            if acronym == configValue[1]:
                contentType = kyContentType
                break
        
        if( acronym == 'model_pdb'):
            contentType = "model"
            
        return  contentType
    
    def __getContentFormat(self,acronym,auxFileIndx):
        
        contentTypeToFormatMap = { 'model':'pdbx',
                                   'model_pdb':'pdb',
                                   'sf':'pdbx',
                                   'val-report':'pdf',
                                   'val-report-full':'pdf',
                                   'val-data':'xml',
                                   'val-report-slider':'png',
                                   'val-report-wwpdb-2fo-fc-edmap-coef':'pdbx',
                                   'val-report-wwpdb-fo-fc-edmap-coef':'pdbx',
                                   'mr':'dat',
                                   'cs':'pdbx',
                                   'em-volume':'map',
                                   'em-mask-volume':'map',
                                   'em-volume-header':'xml'
                               }
        
        contentFormat = None
        
        if( acronym in contentTypeToFormatMap.keys() ):
            contentFormat = contentTypeToFormatMap[acronym]
        elif( acronym == 'aux-file'):
            contentFormat =  self.__reqObj.getValue("auxFileType"+auxFileIndx)
            contentFormat = 'pdbx' if contentFormat == 'cif' else contentFormat
            
        return contentFormat
    
    def __getContentTypeAndFormat(self,acronym,auxFileIndx):
        
        contentType = self.__getContentType(acronym)
        contentFormat = self.__getContentFormat(acronym,auxFileIndx)
        
        return contentType,contentFormat
    
    def __getNextAuxFilePartNum(self):
        ''' 
        '''      
        #
        maxAuxPartNum = 0
        recordSetLst = []            
        #
        try:
            if self.__isWorkflow():
                msgDI=MessagingDataImport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
                self.__msgsToDpstrFilePath = msgDI.getFilePath(contentType='messages-to-depositor',format="pdbx")
                self.__lfh.write("+%s.%s() -- self.__msgsToDpstrFilePath is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, self.__msgsToDpstrFilePath) )
            
            if self.__msgsToDpstrFilePath is not None and os.access(self.__msgsToDpstrFilePath,os.R_OK):
                mIIo=PdbxMessageIo(verbose=self.__verbose,log=self.__lfh)
                with LockFile(self.__msgsToDpstrFilePath,timeoutSeconds=self.__timeoutSeconds,retrySeconds=self.__retrySeconds,verbose=self.__verbose,log=self.__lfh) as lf, FileSizeLogger(self.__msgsToDpstrFilePath,verbose=self.__verbose,log=self.__lfh) as fsl:
                    pid = os.getpid()
                    ok=mIIo.read(self.__msgsToDpstrFilePath,"msgingmod"+str(pid))
                if( ok ):
                    recordSetLst = mIIo.getFileReferenceInfo() # in recordSetLst we now have a list of dictionaries with item names as keys and respective data for values
                    
            #
            if( self.__verbose and self.__debug and self.__debugLvl2 ):
                for idx,row in enumerate(recordSetLst):
                    self.__lfh.write("\n+%s.%s() -- row[%s]: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name,idx,row ) )
            #
            for rcrd in recordSetLst:
                contentType = rcrd['content_type']
                partNumber = int(rcrd['partition_number'])
                
                if( contentType == 'auxiliary-file-annotate'):
                    if( partNumber > maxAuxPartNum ):
                        maxAuxPartNum = partNumber
            #
        except:
            traceback.print_exc(file=self.__lfh)
        
        self.__lfh.write("\n+%s.%s() -- current maxAuxPartNum: '%s' \n" % (self.__class__.__name__, sys._getframe().f_code.co_name, maxAuxPartNum ) )
        self.__lfh.write("\n+%s.%s() -- next available AuxPartNum: '%s' \n" % (self.__class__.__name__, sys._getframe().f_code.co_name, (maxAuxPartNum + 1) ) )
        
        return (maxAuxPartNum + 1)    
    
    def __sendNotificationEmail(self,p_msgObj,p_bVldtnRprtFlg=False):
        
        haveUploadFile = False   #attachment not being utilized at this time
        #
        senderEmail = self.__cI.get("SITE_NOREPLY_EMAIL", "noreply@mail.wwpdb.org")
        subject = p_msgObj.messageSubject
        recipientLst = []
        
        commHostName = self.__reqObj.getValue("hostname") # the hostname of site currently hosting the annotator's communication UI 
        #
        depId = p_msgObj.depositionId
        depEmailUrl = self.__cI.get('SITE_CURRENT_DEP_EMAIL_URL')
        archiveNotifEmails = self.__cI.get('SITE_ARCHIVE_NOTIF_EMAILS')
        #
        if( self.__verbose ):
            self.__lfh.write("\n%s.%s() -- hostname for Annotator Comm UI currently running on this server is '%s'\n" % (self.__class__.__name__,
                                                           sys._getframe().f_code.co_name,
                                                           commHostName) )
            self.__lfh.write("\n%s.%s() -- Deposit UI email URL retrieved from ConfigInfoData for current siteId of '%s' is '%s'\n" % (self.__class__.__name__,
                                                           sys._getframe().f_code.co_name,
                                                           self.__siteId,
                                                           depEmailUrl) )           
        #
        msgStrDict = {}
        #
        tmpltDict = {}
        contactAuthors = []
        self.getMsgTmpltDataItems(tmpltDict)
        contactAuthors = tmpltDict['contact_authors_list']
        
        if( self.__devMode is True ):
            contactAuthors.append(('rsala@rcsb.rutgers.edu','tester','Sala'))
            
        if (self.__verbose and self.__debug):
            self.__lfh.write("\n%s.%s() -- contactAuthors list is now: %r\n" % (self.__class__.__name__,
                                                           sys._getframe().f_code.co_name,
                                                           contactAuthors) )
        pdbId = tmpltDict['pdb_id']
        entryTitle = tmpltDict['title']
        entryAuthors = tmpltDict['entry_authors_list']
        #
        separator = ""
        greetRecipientList = ""
        for index, (emailAddrs,role,lname) in enumerate(contactAuthors):
            if index > 0:
                separator = ", "
            recipientLst.append(emailAddrs)
            greetRecipientList += (separator + ( ("Dr. "+lname) if( lname is not None and (len(lname) > 1) ) else "Sir or Madam") )
        #
        msgStrDict['tab'] = "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"
        #
        msgStrDict['dep_id'] = depId
        msgStrDict['pdb_block'] = (MessagingTemplates.emailNotif_pdbBlock % pdbId) if( pdbId is not None and pdbId != '[PDBID NOT AVAIL]') else ""
        msgStrDict['entry_authors'] = ', '.join(entryAuthors) if entryAuthors is not None else ""
        msgStrDict['entry_authors_block'] = MessagingTemplates.emailNotif_entryAuthorsBlock%msgStrDict if len(msgStrDict['entry_authors']) > 1 else "" 
        #msgStrDict['entry_authors_block'] = (MessagingTemplates.emailNotif_entryAuthorsBlock % '\n\t'.join(entryAuthors) ) if entryAuthors is not None else ""
        msgStrDict['entry_title'] = entryTitle if entryTitle is not None else ""
        msgStrDict['entry_title_block'] = MessagingTemplates.emailNotif_entryTitleBlock%msgStrDict if( len(msgStrDict['entry_title']) > 1 and msgStrDict['entry_title'] != '[NOT AVAILABLE]' ) else "<br /><br />"
        msgStrDict['msg_mime_spec'] = MessagingTemplates.emailNotif_msgBodyMimeSpec
        msgStrDict['comm_subject'] = p_msgObj.messageSubject
        msgStrDict['email_to_lname'] = greetRecipientList
        #### DELETE IF NOT NEEDED -->  msgStrDict['role'] = role
        msgStrDict['dep_email_url'] = depEmailUrl
        msgStrDict['mention_vldtn_rprts'] = "and validation report(s) " if( p_bVldtnRprtFlg is True ) else ""
        msgStrDict['orig_commui_msg_content'] = self.__protectLineBreaks( self.__decodeCifToUtf8(p_msgObj.messageText) )
        #
        msgStrDict['msg_body_main'] = MessagingTemplates.emailNotif_msgBodyMain%msgStrDict if not p_msgObj.isAutoMsg else ( (p_msgObj.messageText).replace('\n','<br />') + MessagingTemplates.emailNotif_replyRedirectFooter%msgStrDict)
        msgBody = MessagingTemplates.emailNotif_msgBodyTmplt%msgStrDict
        #
        msgStrDict['sender'] = senderEmail
        frmttedRecipList = ["<"+emailTo+">" for emailTo in recipientLst] 
        msgStrDict['receiver'] = ", ".join(frmttedRecipList)
        #
        msgStrDict['subject'] = subject + " - DEP ID: " + p_msgObj.depositionId if not p_msgObj.isAutoMsg else p_msgObj.messageSubject
        msgStrDict['mime_hdr'] = MessagingTemplates.emailNotif_mimeHdr
        msgStrDict['msg_content'] = msgBody
        #
        # Adjust subject to strip archiving 'ARCHIVED: '
        if p_msgObj.contentType == 'notes':
            spre = 'ARCHIVED: '
            if len(msgStrDict['subject']) > len(spre) and msgStrDict['subject'][0:len(spre)] == spre:
                msgStrDict['subject'] = msgStrDict['subject'][len(spre):]
            if len(msgStrDict['comm_subject']) > len(spre) and msgStrDict['comm_subject'][0:len(spre)] == spre:
                msgStrDict['comm_subject'] = msgStrDict['comm_subject'][len(spre):]
            
        # Generate message with template
        message = MessagingTemplates.emailNotif_msgTmplt%msgStrDict
        #
        try:
           smtpObj = smtplib.SMTP('localhost')
           
           # Ascii strings
           message=message.encode('ascii', 'replace')
           if sys.version_info[0] > 2:
               message=message.decode('ascii')

           smtpObj.sendmail(senderEmail, recipientLst, message)
                     
           # also send copy to notification archive
           if( archiveNotifEmails is not None and archiveNotifEmails == 'yes' ):
               smtpObj.sendmail(senderEmail, self.__notifEmailArchAddress, message)
           
           if( self.__NOTIF_TESTING ):
               # validation testing purposes
               smtpObj.sendmail(senderEmail, 'rsala@rcsb.rutgers.edu', message)
           #
           if (self.__verbose):
               self.__lfh.write("\n%s.%s() -- Successfully generated email from %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       senderEmail) )
               self.__lfh.write("\n%s.%s() -- email message was %r\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       message) )
               if( archiveNotifEmails is not None and archiveNotifEmails == 'yes' ):
                   self.__lfh.write("\n%s.%s() -- email message was also archived to %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       self.__notifEmailArchAddress) )
        except smtplib.SMTPException:
            traceback.print_exc(file=self.__lfh)
            if (self.__verbose):
                self.__lfh.write("\n%s.%s() -- Failed to generate email from %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       senderEmail) )
        #

    def __genParentMsgDict(self,p_rowList):
        parentDict = {}
        for iRow,row in enumerate(p_rowList):
            msgId=row['message_id']
            parentMsgId=row['parent_message_id']
            parentDict[msgId]=parentMsgId
            
        return parentDict
    
    def __trnsfrmFileRefDictToLst(self,p_recordSetLst,p_msgIdFilter=None):
        ''' 
            
            :param `p_recordSetLst`:       list of dictionaries, each dictionary member represents one message record with attrib name as key
                                            and corresponding data as value
            
        '''      
        rtrnDict = {}
        #
        msgFlRfrnc=PdbxMessageFileReference(verbose=self.__verbose,log=self.__lfh)
        attribList = msgFlRfrnc.get().keys()
        self.__lfh.write("\n%s.%s ----- Message File Reference attrib list is: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, attribList ) )
        #
        
        for rcrd in p_recordSetLst:
            msgId = rcrd['message_id']
            if( p_msgIdFilter and msgId != p_msgIdFilter ):
                continue
            
            storageType = rcrd['storage_type']
            if( storageType != "archive" ):
                continue
            
            contentType = rcrd['content_type']
            contentFormat = rcrd['content_format']
            partitionNum = rcrd['partition_number']
            versionId = rcrd['version_id']
            uploadFlName = rcrd['upload_file_name']
            
            #
            if( msgId not in rtrnDict ):
                rtrnDict[msgId] = []
            #    
            try:
                
                msgDI=MessagingDataImport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
                pathDict = msgDI.getMileStoneFilePaths(contentType=contentType,format=contentFormat,version=versionId,partitionNum=partitionNum)
                archiveFilePth = pathDict['annotPth']
                #
                fileRef = os.path.basename(archiveFilePth)
                toLocalSessionFilePthRltv = os.path.join(self.__sessionRelativePath,fileRef)
                toLocalSessionFilePthAbslt = os.path.join(self.__sessionPath,fileRef)
                
                if os.access(archiveFilePth, os.R_OK):
                    if( toLocalSessionFilePthRltv not in rtrnDict[msgId] ):
                        fileRefDict = {}
                        fileRefDict['upload_file_name'] = uploadFlName
                        fileRefDict['relative_file_url'] = toLocalSessionFilePthRltv
                        rtrnDict[msgId].append(fileRefDict)
                        shutil.copyfile(archiveFilePth,toLocalSessionFilePthAbslt)
                        
            except:
                self.__lfh.write("\n%s.%s ----- problem importing a file reference from archiveFilePth: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, archiveFilePth ) )
                traceback.print_exc(file=self.__lfh)
        #
        return rtrnDict
    
    def __trnsfrmMsgDictToLst(self,p_recordSetLst,p_bCommHstryRqstd=False):
        ''' 
            
            :param `p_recordSetLst`:       list of dictionaries, each dictionary member represents one message record with attrib name as key
                                            and corresponding data as value
            
        '''      
        rtrnLst = []
        #
        attribList = ( self.getMsgColList(p_bCommHstryRqstd) )[1]
        self.__lfh.write("\n%s.%s ----- Message attrib list is: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, attribList ) )
        #
        
        for rcrd in p_recordSetLst:
            row = []
            msgType = rcrd['message_type']
            
            for attrNm in attribList:
                try:
                    if attrNm in ['timestamp','orig_timestamp']:
                        ''' we have to convert from the timestamp in gmtime
                        to one in localtime for display in the user interface '''
                        
                        value = self.__convertToLocalTimeZone(rcrd[attrNm])
                        
                    elif attrNm in ['message_text', 'message_subject', 'orig_subject']:
                        # convert any content from ascii-safe to utf-8 encoded as necessary
                        msgContent = rcrd[attrNm]
                        value =  self.__decodeCifToUtf8(msgContent)
                        if( msgType not in ['archive_manual','forward_manual'] ):
                            value = self.__protectAngleBrackets( value )
                            value = self.__protectLineBreaks( value )
                        
                        
                    elif attrNm in ['sender', 'orig_sender', 'orig_recipient']:
                        # handle use of "<" and ">" in references to email addresses
                        contactInfo = rcrd[attrNm]
                        value = self.__protectAngleBrackets( contactInfo )
                    
                    else:
                        value = rcrd[attrNm]
                        
                    row.append(value if( value != '?') else '' )
                except:
                    self.__lfh.write("\n%s.%s ----- current rcrd in dict type message list is: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, rcrd ) )
                    traceback.print_exc(file=self.__lfh)
            rtrnLst.append(row)
        
        #
        return rtrnLst
    
    def __protectLineBreaks(self,p_content):
        return p_content.replace("\r\n","<br />").replace("\n","<br />")
    
    def __protectAngleBrackets(self,p_content):
        return p_content.replace("<", "&lt;").replace(">", "&gt;")
    
    def __encodeUtf8ToCif(self,p_content):
        ''' Encoding unicode/utf-8 content into cif friendly ascii
            Have to replace any ';' that begin a newline with a ' ;' in order to preserve ; matching required for multiline items
        '''
        return p_content.encode('ascii','xmlcharrefreplace').replace('\n;','\n ;').replace('\\xa0',' ')
    
    def __decodeCifToUtf8(self,p_content):
        if sys.version_info[0] < 3:
            h = HTMLParser()
            return h.unescape(p_content).replace('\\xa0',' ').encode('utf-8')
        else:
            return unescape(p_content).replace('\\xa0',' ')
    
    def __convertToLocalTimeZone(self,p_timeStamp):
        ''' convert from the timestamp in gmtime to one in localtime for display in the user interface '''
        # create timezone objects representing UTC and local timezones
        utcZone = tz.tzutc()
        localZone = tz.tzlocal()
        try:
            utcDateTime = datetime.strptime(p_timeStamp, "%Y-%m-%d %H:%M:%S")
            # the datetime object we created above from the timestamp string doesn't
            # actually know that it is UTC timezone so below we explicitly tell it that it is
            utcDateTime = utcDateTime.replace(tzinfo=utcZone)
            
            # obtain local datetime object and then use it to generate a corresponding timestamp string
            localDateTime = utcDateTime.astimezone(localZone)
            localDateTimeStr = localDateTime.strftime("%Y-%m-%d %H:%M:%S")
        
        except ValueError:
            self.__lfh.write("\n%s.%s ----- argument received, '%s', was not in expected datetime format so using data as is.\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, p_timeStamp ) )
            #traceback.print_exc(file=self.__lfh)
            return p_timeStamp
        
        
        return localDateTimeStr
    
    def __convertToGmtTimeZone(self,p_timeStamp):
        ''' convert from the timestamp in localtime to one in GMT for capture in the cif datafile '''
        # create timezone objects representing UTC and local timezones
        utcZone = tz.tzutc()
        localZone = tz.tzlocal()
        try:
            lclDateTime = datetime.strptime(p_timeStamp, "%Y-%m-%d %H:%M:%S")
            # the datetime object we created above from the timestamp string doesn't
            # actually know that it is local timezone so below we explicitly tell it that it is
            lclDateTime = lclDateTime.replace(tzinfo=localZone)
            
            # obtain GMT datetime object and then use it to generate a corresponding timestamp string
            gmtDateTime = lclDateTime.astimezone(utcZone)
            gmtDateTimeStr = gmtDateTime.strftime("%Y-%m-%d %H:%M:%S")
        
        except ValueError:
            self.__lfh.write("\n%s.%s ----- argument received, '%s', was not in expected datetime format\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, p_timeStamp ) )
            traceback.print_exc(file=self.__lfh)
            return p_timeStamp
        
        
        return gmtDateTimeStr
    
    def __doThreadedHandling(self,p_recordSetLst, p_rtrnDict):
        #
        indentDict = {}
        parentDict = self.__genParentMsgDict(p_recordSetLst)
        
        # need to establish dictionary of "indent level" for threaded display
        for rowIdx,row in enumerate(p_recordSetLst):
            msgId=row['message_id']
            #
            indentLevel=self.__getNestLevel(msgId,parentDict)
            indentDict[msgId] = indentLevel
        #
        p_recordSetLst = self.__sortMsgsForThreading(p_recordSetLst, indentDict)
        p_rtrnDict['INDENT_DICT'] = indentDict
        
    def __sortMsgsForThreading(self,p_recordSetLst,p_indentDict):
        
        p_recordSetLst = self.__parentChildProcessing(p_recordSetLst, p_indentDict)
        p_recordSetLst = self.__siblingMsgProcessing(p_recordSetLst, p_indentDict)
        p_recordSetLst = self.__parentChildProcessing(p_recordSetLst, p_indentDict,p_bFinalPass=True)
        
        return p_recordSetLst
                
    def __siblingMsgProcessing(self,p_recordSetLst,p_indentDict):
        
        correctionRqd = False
        
        for rowIdx,row in enumerate(p_recordSetLst):
            msgId=row['message_id']
            parentId=row['parent_message_id']
            timeStamp=row['timestamp']
            
            #if there is a parent msg, need to ensure that this message is ordered properly amongst any sibling messages for display
            
            if( len(parentId) > 0 and parentId != msgId ):
                prevRecrdIdx = rowIdx-1
                prevMsgId = (p_recordSetLst[prevRecrdIdx])['message_id']
                prevMsgParentId = (p_recordSetLst[prevRecrdIdx])['parent_message_id']
                prevMsgTimeStamp = (p_recordSetLst[prevRecrdIdx])['timestamp']
                #
                try:
                    # if previous msg is not a parent and prev msg is a sibling and current message is of equal thread level 
                    if ( prevMsgId != parentId ) and ( prevMsgParentId == parentId ) and ( p_indentDict[msgId] == p_indentDict[prevMsgId] ):
                        # if we're inside this if block, then we know current record is sibling of previous record
                        
                        if datetime.strptime(prevMsgTimeStamp,'%Y-%m-%d %H:%M:%S') > datetime.strptime(timeStamp,'%Y-%m-%d %H:%M:%S'):
                            # if we're inside this if block, previous sibling is actually more recent than current record and so we must reorder for proper chronological display
                             
                            if( self.__verbose and self.__debug and False ):
                                self.__lfh.write("\n\n+%s.%s() rowIdx is [%s] -- msgId is [%s] -- prevMsgId is [%s] -- indentlevel is [%s] -- indentPrev is [%s] \n" % (self.__class__.__name__, sys._getframe().f_code.co_name,rowIdx,msgId,prevMsgId,p_indentDict[msgId],p_indentDict[prevMsgId]  ) )
                                self.__lfh.write("\n\n+%s.%s() new index is [%s] \n" % (self.__class__.__name__, sys._getframe().f_code.co_name,prevRecrdIdx) )
                            p_recordSetLst.insert( prevRecrdIdx, p_recordSetLst.pop(rowIdx) )
                            correctionRqd = True
                            if( self.__verbose and self.__debug and False):
                                for idx,row in enumerate(p_recordSetLst):
                                    self.__lfh.write("\n+%s.%s() -- row[%s]: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name,idx,row ) )
                            break
                except:
                    self.__lfh.write("\n+%s.%s() -- prevMsgId is [%s] and msgId is [%s]\n" % (self.__class__.__name__, sys._getframe().f_code.co_name,prevMsgId,msgId  ) )
                    self.__lfh.write("\n+%s.%s() -- p_indentDict is %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name,p_indentDict ) )
                    traceback.print_exc(file=self.__lfh)
                    
        if correctionRqd:
            return self.__siblingMsgProcessing(p_recordSetLst, p_indentDict)
        else:
            return p_recordSetLst
        
         
            
    def __parentChildProcessing(self,p_recordSetLst,p_indentDict,p_bFinalPass=False):
        
        correctionRqd = False
        
        if( self.__verbose and self.__debug ):
            self.__lfh.write("\n\n+%s.%s() p_bFinalPass is %s \n" % (self.__class__.__name__, sys._getframe().f_code.co_name,p_bFinalPass  ) )
        
        # reconcile indent level with correct chronological order if necessary
        for rowIdx,row in enumerate(p_recordSetLst):
            msgId=row['message_id']
            parentId=row['parent_message_id']
            
            '''if there is a parent msg, need to check that row just before this row is the parent msg, and if not we need to reorder the list 
               so that each child does come directly after its parent row or after its direct sibling predecessor for display purposes
            '''
            if( len(parentId) > 0 and parentId != msgId ):
                prevRecrdIdx = rowIdx-1
                prevMsgId = (p_recordSetLst[prevRecrdIdx])['message_id']
                prevMsgParentId =  (p_recordSetLst[prevRecrdIdx])['parent_message_id']
                #
                try:
                     
                    # if previous msg is not a parent and prev msg is not a sibling and msg is not a root msg (root msg means indent level = 0) 
                    if ( prevMsgId != parentId ) and ( prevMsgParentId != parentId ) and ( p_indentDict[msgId] > 0 ):
                        
                        # if this is the final pass to identify any orphaned children that may have resulted from sibling processing 
                        # and if the current message is of equal or higher thread level and
                        if (p_bFinalPass is True):
                            if( p_indentDict[msgId] >= p_indentDict[prevMsgId] ):
                                insertIdx = self.__findIdxOfInsertion(msgId,parentId,p_recordSetLst)
                                #
                                if( self.__verbose and self.__debug ):
                                    self.__lfh.write("\n\n+%s.%s() rowIdx is [%s] -- msgId is [%s] -- prevMsgId is [%s] -- indentlevel is [%s] -- indentPrev is [%s] \n" % (self.__class__.__name__, sys._getframe().f_code.co_name,rowIdx,msgId,prevMsgId,p_indentDict[msgId],p_indentDict[prevMsgId]  ) )
                                    self.__lfh.write("\n\n+%s.%s() new index is [%s] \n" % (self.__class__.__name__, sys._getframe().f_code.co_name,insertIdx) )
                                    self.__lfh.write("\n\n+%s.%s() p_bFinalPass is [%s] \n" % (self.__class__.__name__, sys._getframe().f_code.co_name,p_bFinalPass) )
                                #
                                p_recordSetLst.insert( insertIdx, p_recordSetLst.pop(rowIdx) )
                                correctionRqd = True
                                if( self.__verbose and self.__debug and self.__debugLvl2 ):
                                    for idx,row in enumerate(p_recordSetLst):
                                        self.__lfh.write("\n+%s.%s() -- row[%s]: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name,idx,row ) )
                                break
                                
                        else:    
                            insertIdx = self.__findIdxOfInsertion(msgId,parentId,p_recordSetLst)
                            #
                            if( self.__verbose and self.__debug ):
                                self.__lfh.write("\n\n+%s.%s() rowIdx is [%s] -- msgId is [%s] -- prevMsgId is [%s] -- indentlevel is [%s] -- indentPrev is [%s] \n" % (self.__class__.__name__, sys._getframe().f_code.co_name,rowIdx,msgId,prevMsgId,p_indentDict[msgId],p_indentDict[prevMsgId]  ) )
                                self.__lfh.write("\n\n+%s.%s() new index is [%s] \n" % (self.__class__.__name__, sys._getframe().f_code.co_name,insertIdx) )
                            #
                            p_recordSetLst.insert( insertIdx, p_recordSetLst.pop(rowIdx) )
                            correctionRqd = True
                            if( self.__verbose and self.__debug and self.__debugLvl2 ):
                                for idx,row in enumerate(p_recordSetLst):
                                    self.__lfh.write("\n+%s.%s() -- row[%s]: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name,idx,row ) )
                            break
                except:
                    self.__lfh.write("\n+%s.%s() -- prevMsgId is [%s] and msgId is [%s]\n" % (self.__class__.__name__, sys._getframe().f_code.co_name,prevMsgId,msgId  ) )
                    self.__lfh.write("\n+%s.%s() -- p_indentDict is %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name,p_indentDict ) )
                    traceback.print_exc(file=self.__lfh)
        
        if correctionRqd:
            return self.__parentChildProcessing(p_recordSetLst, p_indentDict, p_bFinalPass)
        else:
            return p_recordSetLst    
    

    def __findRowIdxOfMsg(self,msgId,recordSet):
        ''' for given message ID, return index indicating position in record list 
            
            :Params:
                :param `msgId`:       message ID of given message
                :param `recordSet`:   list of message records (each member of the list is itself a list of values 
                                        corresponding to attributes comprising the message record)
                :param `msgIdx`:      index indicating column in individual message record that corresponds to the message ID
            
            :Returns:
                integer indicating position of given message record in the list of message records
        
        '''  
        rtrnVal = None
        for rowIdx,row in enumerate(recordSet):
            if( msgId == row['message_id'] ):
                rtrnVal = rowIdx
        return rtrnVal

    def __findIdxOfInsertion(self,msgId,parentId,recordSetLst):
        ''' called when it is necessary to move a message entry to a different position in the list of message records.
            for given message ID, return index indicating new position in record list 
            
            :Params:
                :param `msgId`:        message ID of given message
                :param `parentId`:     message ID of given message's parent message
                :param `recordSetLst`: list of message records (each member of the list is itself a dictionary of values corresponding to the message record)
                
            
            :Returns:
                integer indicating new target position of given message record in the list of message records
        
        ''' 
        newIdx = None
        newIdx = (self.__findRowIdxOfMsg(parentId,recordSetLst)) + 1
        
        return newIdx

   
    def __orderBy(self, sortlist, orderby=[], desc=[]):
        '''orderBy(sortlist, orderby, desc) >> List
        
        @sortlist: list to be sorted
        @orderby: list of field indexes
        @desc: list of field indexes that are to be sorted descending'''
        
        if( len(sortlist) > 0 ):
            dType = type(sortlist[0]) # list or dict
                
            for colIndx in reversed(orderby):
                if dType is dict:
                    sortlist.sort(key=lambda dictEntry: ( dictEntry.items() )[0][1][colIndx], reverse=(colIndx in desc))
                    
                elif dType is list:
                    sortlist.sort(key=operator.itemgetter(colIndx), reverse=(colIndx in desc))
                
        return sortlist

    def __getNestLevel(self,msgId,pD):
        """  Traverse the parent tree to the root counting the number of steps.
        """
        id=msgId
        ind=0
        while self.__hasParent(id,pD):
            ind+=1
            id=pD[id]
        return ind
        
    def __hasParent(self,id,pD):
        """ id --> parentId?
        """
        if id not in pD:
            return False
        elif pD[id] == id:
            return False
        else:
            return True

    def __filterRsltSet( self, p_rsltSetList, p_sGlobalSrchFilter=None, p_dictColSrchFilter=None ):
        ''' Performs filtering of resultset. Accommodates two mutually-exclusive filter modes: global search and column specific search modes.
            
            :Params:
                :param `p_sGlobalSrchFilter`:      DataTables related parameter indicating global search term against which records will be filtered
                :param `p_dictColSrchFilter`:      DataTables related parameter indicating column-specific search term against which records will be filtered

        ''' 
        fltrdList=[]
        
        if p_sGlobalSrchFilter:
            if( self.__verbose and self.__debug):
                self.__lfh.write("+%s.%s() -- performing global search for string '%s'\n" % (self.__class__.__name__,
                                                                                           sys._getframe().f_code.co_name,
                                                                                           p_sGlobalSrchFilter ) )
            for trueRowIdx,rcrd in enumerate(p_rsltSetList):
                for field in rcrd:
                    if( p_sGlobalSrchFilter.lower() in str(field).lower() ):
                        # for each record satisfying search we need to remember the true row index so that
                        # we can tag the record with this data as it is manipulated in the front end 
                        # therefore if user submits an edit against this record we use this true row index
                        # when registering updates for corresponding record in the persistent data store\
                        # cannot rely on any client-side row index which may incorrect due to reordering/filtering
                        fltrdList.append({trueRowIdx:rcrd})
                        break
        
        elif p_dictColSrchFilter:
            if( self.__verbose and self.__debug):
                self.__lfh.write("+%s.%s() -- performing column-specific searches with search dictionary: %r \n" % (self.__class__.__name__,
                                                                                           sys._getframe().f_code.co_name,
                                                                                           p_dictColSrchFilter.items() ) )
            if( self.__verbose and self.__debug and self.__debugLvl2 ):
                self.__lfh.write("+%s.%s() -- performing column-specific searches against recordset: %r \n" % (self.__class__.__name__,
                                                                                           sys._getframe().f_code.co_name,
                                                                                           p_rsltSetList ) )
            #
            bAllCriteriaMet = False
            for trueRowIdx,rcrd in enumerate(p_rsltSetList):
                #
                for key in p_dictColSrchFilter.keys():
                    if( p_dictColSrchFilter[key].lower() in str(rcrd[key]).lower() ):
                        bAllCriteriaMet = True
                    else:
                        bAllCriteriaMet = False
                        break
                #
                if bAllCriteriaMet:
                    # again appending in form of dictionary as per explanation in block for global search filtering above 
                    fltrdList.append({trueRowIdx:rcrd})
        
        return fltrdList
    
    def __isWorkflow(self):
        """ Determine if currently operating in Workflow Managed environment
        
            :Returns:
                boolean indicating whether or not currently operating in Workflow Managed environment
        """
        #
        fileSource  = str(self.__reqObj.getValue("filesource")).lower()
        #
        if fileSource and fileSource in ['archive','autogroup','wf-archive','wf_archive','wf-instance','wf_instance']:
            #if the file source is any of the above then we are in the workflow manager environment
            return True
        else:
            #else we are in the standalone dev environment
            return False
        
class MsgTmpltHlpr(object):
    
    def __init__(self,reqObj,dbFilePath,verbose=False,log=sys.stderr):
        self.__lfh=log
        self.__verbose=verbose
        self.__debug=True
        self.__debugLvl2=False
        #
        self.__depSystemVrsn2 = True
        #
        self.__reqObj=reqObj
        # 
        self.__sObj=self.__reqObj.newSessionObj()
        self.__sessionPath=self.__sObj.getPath()
        self.__sessionRelativePath=self.__sObj.getRelativePath()        
        self.__sessionId  =self.__sObj.getId()
        #
        self.__siteId  = str(self.__reqObj.getValue("WWPDB_SITE_ID"))
        self.__cI=ConfigInfo(self.__siteId)
        self.__contentTypeDict = self.__cI.get('CONTENT_TYPE_DICTIONARY')
        self.__fileFormatExtDict = self.__cI.get('FILE_FORMAT_EXTENSION_DICTIONARY')
        self.__annotatorUserNameDict = self.__cI.get('ANNOTATOR_USER_NAME_DICT')
        #
        self.__expMethodList = (self.__reqObj.getValue("expmethod").replace('"','')).split(",") if( len(self.__reqObj.getValue("expmethod").replace('"','')) > 1 ) else []
        self.__emDeposition = True if( "ELECTRON MICROSCOPY" in self.__expMethodList or "ELECTRON CRYSTALLOGRAPHY" in self.__expMethodList ) else False
        self.__expMethodMapToAccessID = {"X-RAY DIFFRACTION":"PDB ID %(pdb_id)s",
                                         "ELECTRON MICROSCOPY":"EMDB ID %(emdb_id)s",
                                         "ELECTRON CRYSTALLOGRAPHY":"EMDB ID %(emdb_id)s",
                                         "SOLID-STATE NMR":"PDB ID %(pdb_id)s",
                                         "SOLUTION NMR":"PDB ID %(pdb_id)s",
                                         "NEUTRON DIFFRACTION":"PDB ID %(pdb_id)s"}
        
        self.__idMapToString = {"PDB":"PDB ID %(pdb_id)s",
                                 "EMDB":"EMDB ID %(emdb_id)s",
                                 "BMRB":"PDB ID %(pdb_id)s"}
        
        self.__idMapToString_EM = {"PDB":"PDB entry ID %(pdb_id)s",
                                 "EMDB":"EMDB entry ID %(emdb_id)s",
                                 "BMRB":"PDB entry ID %(pdb_id)s"}
        
        if( self.__verbose and self.__debug ):
            for value in self.__expMethodList:
                self.__lfh.write("+%s.%s() value found in self.__expMethodList: %s \n" % (self.__class__.__name__,
                                                                                              sys._getframe().f_code.co_name, value ) )
        #
        self.__depId = self.__reqObj.getValue("identifier")
        self.__dbFilePath = dbFilePath
        self.__pdbxPersist = None
        self.__dataBlockName = None
        self.__modelFilePath = None
        self.__messagingFilePath = None
        #
        self.__contactAuths = []
        #
        self.__rqstdAccessionIdsLst = []
        #
        self.__pdbId = None
        self.__entryAuthrs = []
        self.__title = None
        #
        self.__emdbId = None
        self.__emEntryAuthrs = []
        self.__emTitle = None
        self.__emHaveMap = False
        self.__emHaveModel = False
        self.__emMapAndModelEntry = False
        self.__emMapOnly = False
        self.__emModelOnly = False
        self.__emSameTitleAsPDB = "yes" # default to yes
        self.__emSameAuthorsAsPDB = "yes" # default to yes
        self.__statusCodeEmMap = None
        self.__dpstnDateEmMap = None
        self.__releaseDateEmMap = None
        self.__expireDateEmMap = None
        self.__entryStatusEmMap = None
        #
        self.__statusCode = None
        self.__entryStatus = None
        self.__postRelStatus = None
        #
        self.__authRelStatusCode = None
        self.__authApprovalType = None # only used to determine default msg template
        #
        self.__holdDate = None # used as expire_date and to build coordRelBlock and entryStatus
        self.__expireDate = None
        self.__initRecvdDate = None
        self.__pdbReleaseDate = None
        #
        self.__citAuthors = []
        self.__citTitle = None  
        self.__citJournalAbbrev = None 
        self.__citJournalVolume = None 
        self.__citPageFirst = None
        self.__citPageLast = None
        self.__citYear = None
        self.__citPdbxDbIdDOI = None # used only to determine default msg tmplt type
        self.__citPdbxDbIdPubMed = None # used only to determine default msg tmplt type
        #
        self.__procSite = None # processing site used in statement regarding upcoming Thursday cutoff date
        self.__pdbxAnnotator = ""
        self.__annotatorFullName = ""
        self.__closingSiteDetails = ""
        #
        self.__defaultMsgTmpltType = "vldtn"
        #
        self.__lastCommDate = None 
        self.__lastOutboundRprtDate = None 
        self.__lastOutboundRprtDateEm= None 
        self.__lastUnlockDate = None
        #        
        self.__emMapReleased = False
        self.__emModelReleased = False
        self.__emMapPreviouslyReleased = False
        self.__emCoordPreviouslyReleased = False
        self.__emMapAndModelJointRelease = False
        #
        self.__releaseDate = None
        self.__thursPreRlsClause = None
        self.__thursWdrnClause = None
        #
        self.__setup()
        
    def __setup(self):
        #
        if not( self.__isWorkflow() ): # i.e. we are in standalone dev/testing context
            self.__pdbId = '4J2N'
            self.__entryAuthrs = ['Parker, Charlie', 'Davis, Miles']
            self.__title = 'Rhapsody in Blue'
            #
            self.__emdbId = 'EMD-5127'
            self.__emEntryAuthrs = ['Young, Lester', 'Stitt, Sonny']
            self.__emTitle = "EM Title"
            self.__emSameTitleAsPDB = "no"
            self.__emSameAuthorsAsPDB = "no"
            self.__entryStatusEmMap = "Hold until Whenever (test em map status)"
            self.__dpstnDateEmMap = "01/01/1000 (dpstn date em map)"
            self.__releaseDateEmMap = "01/01/1000 (release date em map)"
            self.__expireDateEmMap = "01/01/1000 (expire date em map)"
            #
            self.__statusCode = "TEST_CODE"
            self.__statusCodeEmMap = "TEST EM STATUS CODE"
            self.__authRelStatusCode = "TEST STATUS (auth rel status code)"
            self.__entryStatus = "Hold until Whenever (test entry status)"
            
            self.__holdDate = "01/01/1000 (hold date)"
            self.__expireDate = "01/01/1000 (expire date)"
            self.__initRecvdDate = "01/01/1000 (initial receipt date)"
            self.__pdbReleaseDate = "01/01/1000 (PDB release date)"
            
            self.__citAuthors = ['Parker, Charlie', 'Davis, Miles']
            self.__citTitle = "TEST CITATION TITLE"
            self.__citJournalAbbrev = "TEST CITATION JRNL ABBREV"
            self.__citJournalVolume = "TEST CITATION JRNL VOL"
            self.__citPageFirst = "TEST CITATION 1ST PAGE"
            self.__citPageLast = "TEST CITATION LAST PAGE"
            self.__citYear = "TEST CITATION YEAR"
            
            self.__procSite = "RCSB [TEST]" # processing site used in statement regarding upcoming Thursday cutoff date
            self.__annotatorFullName = "[TEST]"
            self.__closingSiteDetails = "[TEST]"
            
            self.__lastCommDate = "01/01/1000"
            self.__lastOutboundRprtDate= "01/01/1000 (last outbound report date)"
            self.__lastOutboundRprtDateEm= "01/01/1000 (last EM outbound report date)"
            
            self.__releaseDate = "01/01/1000 (release date)"
            self.__thursPreRlsClause = "If you have changes to make to the entry, please inform us by noon local time on 01/01/1000 (next Thursday cutoff date)"
            self.__thursWdrnClause = "If this is incorrect or if you have any questions please inform us by noon local time on 01/01/1000 (next Thursday cutoff date)"
            
        else: # we are in workflow managed environment
            try:
                self.__getContactAuthors() # making this call here b/c even if we don't have a model file/database, we will obtain brain contact info from status database
                
                if os.access(self.__dbFilePath,os.R_OK):
                    if not self.__pdbxPersist:
                        self.__initPdbxPersist()
                    
                    # extract info from database
                    self.__getEntryAuthors("PDB")
                    self.__getId("PDB")
                    self.__getEntryTitle("PDB")
                    self.__getRqstdAccessionIds()
                    
                    self.__getProcessingStatusInfo()
                    # Pass info back with get_msg_templates..
                    if self.__statusCode:
                        self.__reqObj.setValue('status_code', self.__statusCode)
                    
                    if( self.__emDeposition ):
                        self.__getId("EMDB")
                        self.__getEmEntryAdminMapping()
                        if( self.__emSameTitleAsPDB == "no" ):
                            self.__getEntryTitle("EMDB")
                        if( self.__emSameAuthorsAsPDB == "no" ):
                            self.__getEntryAuthors("EMDB")
                        self.__getProcessingStatusInfoEM()
                        self.__getLastOutboundRprtDate("EMDB")
                        self.__identifyEmReleaseTargets() # call must occur after call to self.__getProcessingStatusInfo()
                    
                    self.__getCitationInfo()
                    
                    # obtain annotator specific details for msg templates
                    self.__getAnnotatorDetails()
                    
                    # determine default message template type to be used in situations where auto-launch of "Compose Message" is warranted
                    self.__getDefaultMsgTmpltType()                
                    
                    # parse info from annotation messaging file
                    msgDI=MessagingDataImport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
                    self.__messagingFilePath = msgDI.getFilePath(contentType='messages-to-depositor',format="pdbx")
                    if( self.__messagingFilePath is not None and os.access(self.__messagingFilePath,os.R_OK) ):
                        self.__getLastCommDate()
                    
                    # establish release date values
                    self.__getReleaseDateInfo()
                    
                    self.__getLastOutboundRprtDate("PDB")
                    
                else:
                    if (self.__verbose):
                        self.__lfh.write("+%s.%s() failed attempt to access: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, self.__dbFilePath ) )
                
            except:
                if (self.__verbose):
                    self.__lfh.write("+%s.%s() problem recovering data into PdbxPersist from db file at: %s\n" % (self.__class__.__name__,
                                                                                              sys._getframe().f_code.co_name, self.__dbFilePath ) )
                traceback.print_exc(file=self.__lfh)
                self.__lfh.flush()
    
    def __getAccessionIdString(self,idList):
        sAccessionIdsString = ""
        sAnd = ""
        #
        if( self.__emDeposition ):
            map = self.__idMapToString_EM
        else:
            map = self.__idMapToString
        #
        for nIndex,sId in enumerate( idList ):
            if nIndex > 0:
                sAnd = " and "
            sIDstring = map[sId]
            sAccessionIdsString += (sAnd+sIDstring)
            if self.__debug:
                self.__lfh.write("+%s.%s() index %s, sId: '%s'\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, nIndex, sId ) )
        
        return sAccessionIdsString
    
    def populateTmpltDict(self, p_returnDict):
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name

        du = DateUtil()
        ########### ACCESSION ID HANDLING ##############################################
        accessionIdList = self.__rqstdAccessionIdsLst[:]
        if "BMRB" in accessionIdList and "PDB" in accessionIdList:
            accessionIdList.remove("BMRB")
            # acknowleding here that X-RAY and NMR processing both result in a single "PDB" accession ID being granted to depositor for a given deposition (i.e. no separate "BMRB" id)
            # so the "accessionIdList" at this time only consists of single member (but perhaps this may change in the future?)
            # at this current time it's only for EM experimental method where we have to handle the generation of more than one accession ID for the same deposition 
        p_returnDict['pdb_id'] = self.__pdbId if( self.__pdbId is not None and len(self.__pdbId) > 0 and self.__pdbId != '?') else "[PDBID NOT AVAIL]"
        p_returnDict['emdb_id'] = self.__emdbId if( self.__emdbId is not None and len(self.__emdbId) > 0 and self.__emdbId != '?') else "[EMDBID NOT AVAIL]"
        p_returnDict['accession_ids'] = ( self.__getAccessionIdString(accessionIdList) )%p_returnDict
        #################################################################################
        
        p_returnDict['contact_authors_list'] = self.__contactAuths 
        #
        p_returnDict['entry_authors_list'] = self.__entryAuthrs
        p_returnDict['entry_authors_newline_list'] = "\n".join(self.__entryAuthrs) if( self.__entryAuthrs is not None and len(self.__entryAuthrs) > 0 ) else "[NOT AVAILABLE]"
        p_returnDict['entry_authors_csv_list'] = ", ".join(self.__entryAuthrs) if( self.__entryAuthrs is not None and len(self.__entryAuthrs) > 0 ) else "[NOT AVAILABLE]"
        #
        p_returnDict['title'] = self.__title if( self.__title is not None and len(self.__title) > 0 ) else "[NOT AVAILABLE]"
        #
        p_returnDict['status_code'] = self.__statusCode if( self.__statusCode is not None and len(self.__statusCode) > 0 ) else "[NOT AVAILABLE]"
        p_returnDict['entry_status'] = self.__entryStatus if( self.__entryStatus is not None and len(self.__entryStatus) > 0 ) else "[NOT AVAILABLE]"
        p_returnDict['auth_rel_status_code'] = self.__authRelStatusCode if( self.__authRelStatusCode is not None and len(self.__authRelStatusCode) > 0 ) else "[NOT AVAILABLE]"
        p_returnDict['expire_date'] = self.__expireDate
        p_returnDict['recvd_date'] = du.date_to_display(self.__initRecvdDate) if( self.__initRecvdDate is not None and len(self.__initRecvdDate) > 0 and self.__isNotCifNull(self.__initRecvdDate) ) else "[NOT AVAILABLE]"
        p_returnDict['processing_site'] = self.__procSite if( self.__procSite is not None and len(self.__procSite) > 0 ) else "[NOT AVAILABLE]"  # processing site used in statement regarding upcoming Thursday cutoff date
        #
        p_returnDict['citation_authors'] = ",".join(self.__citAuthors) if( self.__citAuthors is not None and len(self.__citAuthors) > 0 ) else "[NOT AVAILABLE]"
        p_returnDict['citation_title'] = self.__citTitle if( self.__citTitle is not None and len(self.__citTitle) > 0 and self.__isNotCifNull(self.__citTitle) ) else "[NOT AVAILABLE]"
        p_returnDict['citation_journal_abbrev'] = self.__citJournalAbbrev if( self.__citJournalAbbrev is not None and len(self.__citJournalAbbrev) > 0 and self.__isNotCifNull(self.__citJournalAbbrev) ) else "[NOT AVAILABLE]"
        p_returnDict['citation_journal_volume'] = ("\nVolume:   "+self.__citJournalVolume) if( self.__citJournalVolume is not None and len(self.__citJournalVolume) > 0 and self.__isNotCifNull(self.__citJournalVolume) ) else ""
        p_returnDict['citation_page_first'] = self.__citPageFirst if( self.__citPageFirst is not None and len(self.__citPageFirst) > 0 and self.__isNotCifNull(self.__citPageFirst) ) else "[NOT AVAILABLE]"
        p_returnDict['citation_page_last'] = (" - "+self.__citPageLast) if( self.__citPageLast is not None and len(self.__citPageLast) > 0 and self.__isNotCifNull(self.__citPageLast) ) else ""
        p_returnDict['citation_pages'] = ("\nPages:   %(citation_page_first)s%(citation_page_last)s"%p_returnDict) if p_returnDict['citation_page_first'] != "[NOT AVAILABLE]" else ""
        p_returnDict['citation_year'] = ("\nYear:   "+self.__citYear) if( self.__citYear is not None and len(self.__citYear) > 0 and self.__isNotCifNull(self.__citYear) ) else ""
        p_returnDict['citation_pubmedid'] = ("\nPubMed ID:   "+self.__citPdbxDbIdPubMed) if( self.__citPdbxDbIdPubMed is not None and len(self.__citPdbxDbIdPubMed) > 0 and self.__isNotCifNull(self.__citPdbxDbIdPubMed) ) else ""
        p_returnDict['citation_doi'] = ("\nDOI:   "+self.__citPdbxDbIdDOI) if( self.__citPdbxDbIdDOI is not None and len(self.__citPdbxDbIdDOI) > 0 and self.__isNotCifNull(self.__citPdbxDbIdDOI) ) else ""
        #
        p_returnDict['default_msg_tmplt'] = self.__defaultMsgTmpltType
        #
        p_returnDict['outbound_rprt_date'] = self.__lastOutboundRprtDate if( self.__lastOutboundRprtDate is not None and len(self.__lastOutboundRprtDate) > 0 ) else "[NOT AVAILABLE]"
        #
        p_returnDict['release_date'] = self.__releaseDate if( self.__releaseDate is not None and len(self.__releaseDate) > 0 ) else "[NOT AVAILABLE]"
        p_returnDict['withdrawn_date'] = p_returnDict['release_date']
        p_returnDict['thurs_prerelease_clause'] = self.__thursPreRlsClause if( self.__thursPreRlsClause is not None and len(self.__thursPreRlsClause) > 0 ) else ""
        p_returnDict['thurs_wdrn_clause'] = self.__thursWdrnClause if( self.__thursWdrnClause is not None and len(self.__thursWdrnClause) > 0 ) else ""
        # message template closing details
        p_returnDict['annotator_group_signoff'] = MessagingTemplates.msgTmplt_annotatorGroupSignoff
        p_returnDict['site_contact_details'] = self.__closingSiteDetails
        p_returnDict['unlock_date'] = du.date_to_display(self.__lastUnlockDate) if (self.__lastUnlockDate is not None and len(self.__lastUnlockDate) > 0) else "[NOT AVAILBLE]"
        
        #############################################################
        ########### EM ENTRY PROCESSING #############################
        #############################################################
        
        ##  NOTE: PROCESSING FOR FOLLOWING IS REQUIRED REGARDLESS OF WHETHER ENTRY IS EM VS. XRAY B/C THEY SERVE AS GLOBAL FLAGS
        p_returnDict['em_entry'] = "true" if self.__emDeposition else "false"
        p_returnDict['maponly'] = "true" if self.__emMapOnly else "false"
        p_returnDict['modelonly'] = "true" if self.__emModelOnly else "false"
        p_returnDict['mapandmodel'] = "true" if self.__emMapAndModelEntry else "false"
        p_returnDict['accession_ids_em_rel'] = ""
        
        if( self.__emDeposition ):
            rqstdNumber = "multi" if( len(accessionIdList) > 1 ) else "single" # i.e. number of accession IDs requested, varies depending on possible presence of model accompanying the map
            #
            dictEntryEntries = {"single":" entry ", "multi":" entries "}
            dictItThey = {"single":" It ", "multi":" They "}
            dictThisThese = {"single":" this ", "multi":" these "}
            dictIsAre = {"single":"is ", "multi":"are "}
            dictHasHave = {"single":"has ", "multi":"have "}
            #
            p_returnDict['entry_entries'] = dictEntryEntries[rqstdNumber]
            p_returnDict['it_they'] = dictItThey[rqstdNumber]
            p_returnDict['this_these'] = dictThisThese[rqstdNumber]
            p_returnDict['is_are'] = dictIsAre[rqstdNumber]
            p_returnDict['has_have'] = dictHasHave[rqstdNumber]
            p_returnDict['entry_entries_comma'] = (dictEntryEntries[rqstdNumber]).strip()+","
            p_returnDict['it_they_lcase'] = (dictItThey[rqstdNumber]).lower()
            
            if( self.__emMapOnly ):
                p_returnDict['em_entry_authors_newline_list'] = "\n".join(self.__emEntryAuthrs) if( len(self.__emEntryAuthrs) > 0 ) else "[NOT AVAILABLE]"
                p_returnDict['em_title'] = self.__emTitle if( self.__emTitle is not None and len(self.__emTitle) > 0 ) else "[NOT AVAILABLE]"
            elif( self.__emMapAndModelEntry ):
                if( self.__emSameAuthorsAsPDB == "no" ):
                    p_returnDict['em_entry_authors_newline_list'] = "EMDB:\n"+"\n".join(self.__emEntryAuthrs) if( len(self.__emEntryAuthrs) > 0 ) else "[NOT AVAILABLE]"
                    p_returnDict['em_entry_authors_newline_list'] = p_returnDict['em_entry_authors_newline_list']+"\n\nPDB:\n"+"\n".join(self.__entryAuthrs) if( len(self.__entryAuthrs) > 0 ) else "[NOT AVAILABLE]"
                else:
                    # i.e map-and-model entry with EM authors same as PDB authors
                    p_returnDict['em_entry_authors_list'] = self.__entryAuthrs
                    p_returnDict['em_entry_authors_newline_list'] = "\n".join(self.__entryAuthrs) if( len(self.__entryAuthrs) > 0 ) else "[NOT AVAILABLE]"
                    p_returnDict['em_entry_authors_csv_list'] = ", ".join(self.__entryAuthrs) if( len(self.__entryAuthrs) > 0 ) else "[NOT AVAILABLE]"
                    
                if( self.__emSameTitleAsPDB == "no" ):
                    p_returnDict['em_title'] = ("EMDB: "+self.__emTitle) if( self.__emTitle is not None and len(self.__emTitle) > 0 ) else "EMDB: [NOT AVAILABLE]"
                else:
                    # i.e map-and-model entry with EM title same as PDB title
                    p_returnDict['em_title'] = self.__title if( self.__title is not None and len(self.__title) > 0 ) else "[NOT AVAILABLE]"
            else: # model only...does this actually occur?
                p_returnDict['em_title'] = self.__title if( self.__title is not None and len(self.__title) > 0 ) else "[NOT AVAILABLE]"
                p_returnDict['em_entry_authors_newline_list'] = "\n".join(self.__entryAuthrs) if( len(self.__entryAuthrs) > 0 ) else "[NOT AVAILABLE]"
                p_returnDict['em_entry_authors_csv_list'] = ", ".join(self.__entryAuthrs) if( len(self.__entryAuthrs) > 0 ) else "[NOT AVAILABLE]"

            #
            
            # for EM entries we have to accommodate possibilities of separate map and model releases
            emReleasedIdList = accessionIdList # default to both PDB and EMDB
             
            if( self.__emMapAndModelEntry ):
                
                self.__lfh.write("+%s.%s() self.__emMapAndModelEntry is:  %s\n" % ( className, methodName, self.__emMapAndModelEntry ) )
                self.__lfh.write("+%s.%s() self.__emMapReleased is:  %s\n" % ( className, methodName, self.__emMapReleased ) )
                self.__lfh.write("+%s.%s() self.__emModelReleased is:  %s\n" % ( className, methodName, self.__emModelReleased ) )
                self.__lfh.write("+%s.%s() self.__emMapPreviouslyReleased is:  %s\n" % ( className, methodName, self.__emMapPreviouslyReleased ) )
                self.__lfh.write("+%s.%s() self.__emCoordPreviouslyReleased is:  %s\n" % ( className, methodName, self.__emCoordPreviouslyReleased ) )
                if self.__emMapReleased and self.__emModelReleased:
                    if self.__emMapPreviouslyReleased:
                        emReleasedIdList = ["PDB"]
                    elif self.__emCoordPreviouslyReleased:
                        emReleasedIdList = ["EMDB"]
                else:
                    emReleasedIdList = ["PDB"] if self.__emModelReleased else ["EMDB"] if self.__emMapReleased else []
                
                p_returnDict['accession_ids_em_rel'] = ( self.__getAccessionIdString(emReleasedIdList) )%p_returnDict
                    
            else:
                p_returnDict['accession_ids_em_rel'] = p_returnDict['accession_ids']
                
            self.__lfh.write("+%s.%s() p_returnDict['accession_ids_em_rel'] finalized as:  %s\n" % ( className, methodName, p_returnDict['accession_ids_em_rel'] ) )
            
            emReleasedNumber = "multi" if( len(emReleasedIdList) > 1 ) else "single"
            #
            p_returnDict['entry_entries_em_rel'] = dictEntryEntries[emReleasedNumber]
            p_returnDict['it_they_em_rel'] = dictItThey[emReleasedNumber]
            p_returnDict['this_these_em_rel'] = dictThisThese[emReleasedNumber]
            p_returnDict['is_are_em_rel'] = dictIsAre[emReleasedNumber]
            p_returnDict['has_have_em_rel'] = dictHasHave[emReleasedNumber]
            p_returnDict['entry_entries_comma_em_rel'] = (dictEntryEntries[emReleasedNumber]).strip()+","
            p_returnDict['it_they_lcase_em_rel'] = (dictItThey[emReleasedNumber]).lower()
            #
            p_returnDict['status_code_em_map'] = self.__statusCodeEmMap if( self.__statusCodeEmMap is not None and len(self.__statusCodeEmMap) > 0 ) else "[NOT AVAILABLE]"
            p_returnDict['entry_status_em_map'] = self.__entryStatusEmMap if( self.__entryStatusEmMap is not None and len(self.__entryStatusEmMap) > 0 ) else "[NOT AVAILABLE]"
            p_returnDict['auth_rel_status_code_em_rel'] = p_returnDict['auth_rel_status_code']
            p_returnDict['expire_date_em_map'] = self.__expireDateEmMap
            #
            p_returnDict['outbound_rprt_date_em'] = self.__lastOutboundRprtDateEm if( self.__lastOutboundRprtDateEm is not None and len(self.__lastOutboundRprtDateEm) > 0 ) else "[NOT AVAILABLE]"
            #
            p_returnDict['caveat_records'] = " with CAVEAT records highlighting any outstanding issues" if self.__emMapAndModelEntry else ""
            p_returnDict['vldtn_rprt'] = "validation report and " if self.__emMapAndModelEntry else ""
            p_returnDict['wwpdb_and'] = "wwPDB and " if self.__emMapAndModelEntry else ""
            # message template closing details
            p_returnDict['annotator_group_signoff'] = MessagingTemplates.msgTmplt_annotatorGroupSignoff if self.__emModelOnly else MessagingTemplates.msgTmplt_annotatorGroupSignoff_em%p_returnDict
            p_returnDict['site_contact_details'] = "" if self.__emMapOnly else self.__closingSiteDetails
        #############################################################
        ########### END: EM ENTRY PROCESSING ########################
        #############################################################
        
        #############################################################
        ########### TITLE PROCESSING ################################
        #############################################################
        titleLength = len(p_returnDict['title']) if( not self.__emDeposition ) else ( len(p_returnDict['em_title']) if( len(p_returnDict['em_title']) > len(p_returnDict['title']) ) else len(p_returnDict['title']) )
        
        if( self.__debugLvl2 ):
            self.__lfh.write("+%s.%s() title is:  %s\n" % ( className, methodName, p_returnDict['title'] ) )
            if( self.__emDeposition ):
                self.__lfh.write("+%s.%s() em_title is:  %s\n" % ( className, methodName, p_returnDict['em_title'] ) )
            self.__lfh.write("+%s.%s() titleLength:  %s\n" % ( className, methodName, titleLength ) )
        
        primaryTitle = p_returnDict['em_title'] if( self.__emDeposition ) else p_returnDict['title']
            
        if( titleLength <= 100 ):
            horizLineLngth = titleLength 
        else:
            horizLineLngth = 85
            targetLength = 85
            for num in range(targetLength,targetLength-20,-1):
                if( self.__debugLvl2 ):
                    self.__lfh.write("+%s.%s() testing targetLength of:  %s\n" % ( className, methodName, num ) )
                if( len( textwrap.wrap( primaryTitle, num )[-1] ) > 10 ):  # EM: using reference to primaryTitle instead of p_returnDict['title']
                    targetLength = num
                    if( self.__debugLvl2 ):
                        self.__lfh.write("+%s.%s() found targetLength of:  %s\n" % ( className, methodName, targetLength ) )
                    break
            if( self.__debugLvl2 ):
                self.__lfh.write("+%s.%s() using targetLength of:  %s\n" % ( methodName, methodName, targetLength ) )
            p_returnDict['title'] = textwrap.fill( p_returnDict['title'], targetLength )
            if( self.__emDeposition ):
                p_returnDict['em_title'] = textwrap.fill( p_returnDict['em_title'], targetLength ) # EM: added this
        
        if( self.__emDeposition ):
            if( self.__emMapAndModelEntry and self.__emSameTitleAsPDB == "no" ):
                #p_returnDict['em_title'] = p_returnDict['em_title'] + ("&#13;&#10;PDB: "+p_returnDict['title']) if( p_returnDict['title'] is not None and len(p_returnDict['title']) > 0 ) else "&#13;&#10;PDB: [NOT AVAILABLE]"
                p_returnDict['em_title'] = p_returnDict['em_title'] + ("\nPDB: "+p_returnDict['title']) if( p_returnDict['title'] is not None and len(p_returnDict['title']) > 0 ) else "&#13;&#10;PDB: [NOT AVAILABLE]"

        p_returnDict['horiz_line'] = ''    
        for n in range(horizLineLngth):
            p_returnDict['horiz_line'] += "="
            
        #############################################################
        ########### END: TITLE PROCESSING ###########################
        #############################################################
        
        listKnownFileExtensions = list( set( self.__fileFormatExtDict.values() ) )
        listKnownFileExtensions.sort()
        p_returnDict['known_file_extensions'] = ", ".join(listKnownFileExtensions)
        #
        p_returnDict['full_name_annotator'] = self.__annotatorFullName

        if( self.__emDeposition and self.__emMapOnly ):
            p_returnDict['msg_closing'] = MessagingTemplates.msgTmplt_closing_emMapOnly%p_returnDict
        else:
            p_returnDict['msg_closing'] = MessagingTemplates.msgTmplt_closing%p_returnDict
            
    ################################################################################################################
    # ------------------------------------------------------------------------------------------------------------
    #      Private helper methods
    # ------------------------------------------------------------------------------------------------------------
    #    
    def __initPdbxPersist(self):
        self.__pdbxPersist=PdbxPersist(self.__verbose,self.__lfh)
        myInd=self.__pdbxPersist.getIndex(dbFileName=self.__dbFilePath)
        containerNameList = myInd['__containers__']
        self.__dataBlockName = containerNameList[0][0]
        if (self.__verbose):
            self.__lfh.write("+%s.%s() successfully obtained datablock name as: %s, from %s\n" % (self.__class__.__name__,
                                                                                  sys._getframe().f_code.co_name, self.__dataBlockName, self.__dbFilePath ) )
            
    def __isNotCifNull(self,p_value):
        if( p_value in [".","?"] ):
            return False
        else:
            return True
        
    def __isCifNull(self,p_value):
        return ( not self.__isNotCifNull(p_value) )

    def __getCatObj(self,p_ctgryNm):
        catObj = self.__pdbxPersist.fetchOneObject(self.__dbFilePath, self.__dataBlockName, p_ctgryNm)
            
        if catObj is None:
            if (self.__verbose):
                self.__lfh.write("\n%s.%s() -- Unable to find '%s' category in db file: %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name, p_ctgryNm, self.__dbFilePath) )
        else:
            if (self.__verbose):
                self.__lfh.write("\n%s.%s() -- Successfully found '%s' category in db file: %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name, p_ctgryNm, self.__dbFilePath) )
        return catObj
            
    def __getContactAuthors(self):
        #
        emailAddrsSoFar = []
        roleFilterVrsn1_5 = ""
        roleFilterVrsn2_0 = "valid"
        
        if os.access(self.__dbFilePath,os.R_OK):
            # i.e. ONLY if we are post submission and have a local database of info from the model file, can we get the below data points
            
            self.__initPdbxPersist()
                
            ctgryNm = 'pdbx_contact_author'
            try:
                if( self.__verbose ):
                    self.__lfh.write("+%s.%s() -- Category name sought from [%s] is: '%s'\n" % (self.__class__.__name__,
                                                                                      sys._getframe().f_code.co_name,
                                                                                      self.__dbFilePath,
                                                                                      ctgryNm) )
                #
                catObj = self.__getCatObj(ctgryNm)
                if catObj:
                    #
                    itDict={}
                    itNameList=catObj.getItemNameList()
                    for idxIt,itName in enumerate(itNameList):
                        itDict[str(itName).lower()]=idxIt
                        #
                    idEmail=itDict['_pdbx_contact_author.email']
                    idRole=itDict['_pdbx_contact_author.role']
                    idLname=itDict['_pdbx_contact_author.name_last']
                    
                    icount=0
                    
                    for row in catObj.getRowList():
                        try:
                            email = row[idEmail]
                            role = row[idRole]
                            lname = row[idLname]
                            if self.__validateEmail(email) is True and email not in emailAddrsSoFar:
                                if (self.__verbose and self.__debug):
                                    self.__lfh.write("\n%s.%s() -- [%s, %s, %s] being appended to self.__contactAuths. \n" % (self.__class__.__name__,
                                                                       sys._getframe().f_code.co_name,
                                                                       email,role,lname ) )
                                emailAddrsSoFar.append(email)
                                self.__contactAuths.append( (email,role,lname) )
                        except:
                            traceback.print_exc(file=self.__lfh)
                
            except:
                if (self.__verbose):
                    self.__lfh.write("+%s.%s() problem recovering data from PdbxPersist for category: '%s'\n" % (self.__class__.__name__,
                                                                                              sys._getframe().f_code.co_name, ctgryNm ) )
                traceback.print_exc(file=self.__lfh)
                self.__lfh.flush()
            
        # 2014-10-30, decision was made to always include validated brain page email on notification emails    
        ss = dbAPI(self.__depId,verbose=True)
        #
        if( self.__depSystemVrsn2 ):
            roleFilter = roleFilterVrsn2_0
        else:
            roleFilter = roleFilterVrsn1_5
            
        brainPageContactList = ss.runSelectNQ(table='user_data',select=['email','role','last_name'],where ={"dep_set_id":self.__depId,"role":roleFilter})
        
        if( self.__depSystemVrsn2 and len(brainPageContactList) == 0 ): # if we have no brain contacts using Dep UI system 2.0 query, double check using pre vrsn 2.0 query qualifier for backwards compatibility
            brainPageContactList = ss.runSelectNQ(table='user_data',select=['email','role','last_name'],where ={"dep_set_id":self.__depId,"role":roleFilterVrsn1_5})
            
        if( len(brainPageContactList) > 0 ):
            for email,role,lastName in brainPageContactList:
                self.__lfh.write("\n%s.%s() -- found contact author from brainpage --> [%s, %s, %s]\n" % (self.__class__.__name__,
                                                               sys._getframe().f_code.co_name,
                                                               email,role,lastName ) )
                if email not in emailAddrsSoFar:
                    if (self.__verbose and self.__debug):
                            self.__lfh.write("\n%s.%s() -- contact author from brainpage --> [%s, %s, %s] being appended to self.__contactAuths. \n" % (self.__class__.__name__,
                                                               sys._getframe().f_code.co_name,
                                                               email,role,lastName ) )
                    self.__contactAuths.append( [email,role,lastName] )
                
    def __getEntryAuthors(self,p_IdType="PDB"):
        
        if( p_IdType == "PDB"):
            ctgryNm = 'audit_author'
            itemNm = 'name'
        elif( p_IdType == "EMDB"):
            ctgryNm = 'em_author_list'
            itemNm = 'author'
        
        try:
            if( self.__verbose ):
                self.__lfh.write("+%s.%s() -- Category name sought from [%s] is: '%s'\n" % (self.__class__.__name__,
                                                                                  sys._getframe().f_code.co_name,
                                                                                  self.__dbFilePath,
                                                                                  ctgryNm) )
            #
            catObj = self.__getCatObj(ctgryNm)
            if catObj:
                #
                # Get column name index.
                #
                itDict={}
                itNameList=catObj.getItemNameList()
                for idxIt,itName in enumerate(itNameList):
                    itDict[str(itName).lower()]=idxIt
                    #
                idxFullName=itDict['_'+ctgryNm+'.'+itemNm]
                
                for row in catObj.getRowList():
                    try:
                        name = row[idxFullName]
                        if (self.__verbose):
                            self.__lfh.write("\n%s.%s() -- %s.%s found as: %s\n" % (self.__class__.__name__,
                                                           sys._getframe().f_code.co_name,
                                                           ctgryNm, itemNm, name) )
                        if( p_IdType == "PDB"):
                            self.__entryAuthrs.append( name )
                        elif( p_IdType == "EMDB"):
                            self.__emEntryAuthrs.append( name )
                        
                    except:
                        pass
                    
        except:
            if (self.__verbose):
                self.__lfh.write("+%s.%s() problem recovering data from PdbxPersist for category: '%s'\n" % (self.__class__.__name__,
                                                                                          sys._getframe().f_code.co_name, ctgryNm ) )
            traceback.print_exc(file=self.__lfh)
            self.__lfh.flush()        

    def __getId(self,p_IdType):
        
        ctgryNm = 'database_2'
        
        try:
            if( self.__verbose ):
                self.__lfh.write("+%s.%s() -- Category name sought from [%s] is: '%s'\n" % (self.__class__.__name__,
                                                                                  sys._getframe().f_code.co_name,
                                                                                  self.__dbFilePath,
                                                                                  ctgryNm) )
            #
            catObj = self.__getCatObj(ctgryNm)
            if catObj:
                #
                # Get column name index.
                #
                itDict={}
                itNameList=catObj.getItemNameList()
                for idxIt,itName in enumerate(itNameList):
                    itDict[str(itName).lower()]=idxIt
                    #
                idxDbId=itDict['_database_2.database_id']
                idxDbCode=itDict['_database_2.database_code']
                
                
                for row in catObj.getRowList():
                    try:
                        dbId = row[idxDbId]
                        dbCode = row[idxDbCode]
                        
                        if( dbId.upper() == p_IdType ):
                            if( p_IdType == "PDB"):
                                self.__pdbId = dbCode
                            elif( p_IdType == "EMDB"):
                                self.__emdbId = dbCode
                        
                    except:
                        pass
                    
        except:
            if (self.__verbose):
                self.__lfh.write("+%s.%s() problem recovering data from PdbxPersist for category: '%s'\n" % (self.__class__.__name__,
                                                                                          sys._getframe().f_code.co_name, ctgryNm ) )
            traceback.print_exc(file=self.__lfh)
            self.__lfh.flush() 
            
    def __getEmEntryAdminMapping(self):
        
        if( not self.__emModelOnly and not self.__emMapOnly ):
            ctgryNm = 'em_depui'
            
            try:
                if( self.__verbose ):
                    self.__lfh.write("+%s.%s() -- Category name sought from [%s] is: '%s'\n" % (self.__class__.__name__,
                                                                                      sys._getframe().f_code.co_name,
                                                                                      self.__dbFilePath,
                                                                                      ctgryNm) )
                #
                catObj = self.__getCatObj(ctgryNm)
                if catObj:
                    #
                    # Get column name index.
                    #
                    itDict={}
                    itNameList=catObj.getItemNameList()
                    for idxIt,itName in enumerate(itNameList):
                        itDict[str(itName).lower()]=idxIt
                        #
                    idxSameTitleAsPDB=itDict['_'+ctgryNm+'.same_title_as_pdb']
                    idxSameAuthorsAsPDB=itDict['_'+ctgryNm+'.same_authors_as_pdb']
                    
                    
                    for row in catObj.getRowList():
                        try:
                            self.__emSameTitleAsPDB = ( row[idxSameTitleAsPDB] ).lower() 
                            self.__emSameAuthorsAsPDB = ( row[idxSameAuthorsAsPDB] ).lower()
                            
                        except:
                            pass
                        
            except:
                if (self.__verbose):
                    self.__lfh.write("+%s.%s() problem recovering data from PdbxPersist for category: '%s'\n" % (self.__class__.__name__,
                                                                                              sys._getframe().f_code.co_name, ctgryNm ) )
                traceback.print_exc(file=self.__lfh)
                self.__lfh.flush()
            
        else:
            if( self.__emMapOnly ):
                self.__emSameTitleAsPDB = "no" 
                self.__emSameAuthorsAsPDB = "no"

            
    def __getEntryTitle(self,p_IdType="PDB"):
        
        if( p_IdType == "PDB"):
            ctgryNm = 'struct'
            itemNm = 'title'
        elif( p_IdType == "EMDB"):
            ctgryNm = 'em_admin'
            itemNm = 'title'
            
        
        try:
            if( self.__verbose ):
                self.__lfh.write("+%s.%s() -- Category name sought from [%s] is: '%s'\n" % (self.__class__.__name__,
                                                                                  sys._getframe().f_code.co_name,
                                                                                  self.__dbFilePath,
                                                                                  ctgryNm) )
            #
            catObj = self.__getCatObj(ctgryNm)
            if catObj:
                #
                # Get column name index.
                #
                itDict={}
                itNameList=catObj.getItemNameList()
                for idxIt,itName in enumerate(itNameList):
                    itDict[str(itName).lower()]=idxIt
                    #
                idxTitle=itDict['_'+ctgryNm+'.'+itemNm]
                
                
                for row in catObj.getRowList():
                    try:
                        if( p_IdType == "PDB"):
                            self.__title = row[idxTitle]
                        elif( p_IdType == "EMDB" ):
                            self.__emTitle = row[idxTitle]
                        
                    except:
                        pass
                    
        except:
            if (self.__verbose):
                self.__lfh.write("+%s.%s() problem recovering data from PdbxPersist for category: '%s'\n" % (self.__class__.__name__,
                                                                                          sys._getframe().f_code.co_name, ctgryNm ) )
            traceback.print_exc(file=self.__lfh)
            self.__lfh.flush()
            
                
    def __getProcessingStatusInfo(self):
        ctgryNm = 'pdbx_database_status'
        try:
            if( self.__verbose ):
                self.__lfh.write("+%s.%s() -- Category name sought from [%s] is: '%s'\n" % (self.__class__.__name__,
                                                                                  sys._getframe().f_code.co_name,
                                                                                  self.__dbFilePath,
                                                                                  ctgryNm) )
            #
            catObj = self.__getCatObj(ctgryNm)
            if catObj:
                #
                # Get column name index.
                #
                itDict={}
                itNameList=catObj.getItemNameList()
                for idxIt,itName in enumerate(itNameList):
                    itDict[str(itName).lower()]=idxIt
                    #
                idxStatusCode=itDict['_pdbx_database_status.status_code']
                idxAuthRelStatusCode=itDict['_pdbx_database_status.author_release_status_code']
                idxAuthApprovalType=itDict['_pdbx_database_status.author_approval_type']
                idxHoldDate=itDict['_pdbx_database_status.date_hold_coordinates']
                idxInitRecvdDate=itDict['_pdbx_database_status.recvd_initial_deposition_date']
                idxDatePdbRelease=itDict['_pdbx_database_status.date_of_ndb_release']
                idxProcSite=itDict['_pdbx_database_status.process_site']
                idxPdbAnnotator=itDict['_pdbx_database_status.pdbx_annotator']
                idxPostRelStatus=itDict.get('_pdbx_database_status.post_rel_status', None)
                
                
                for row in catObj.getRowList():
                    try:
                        self.__statusCode = (str(row[idxStatusCode])).upper()
                        self.__authRelStatusCode = str(row[idxAuthRelStatusCode])
                        self.__authApprovalType = str(row[idxAuthApprovalType])
                        self.__holdDate = str(row[idxHoldDate])
                        self.__initRecvdDate = str(row[idxInitRecvdDate])
                        self.__pdbReleaseDate = str(row[idxDatePdbRelease])
                        self.__procSite = str(row[idxProcSite])
                        if idxPostRelStatus:
                            self.__postRelStatus = str(row[idxPostRelStatus])
                        else:
                            self.__postRelStatus = '?'
                    except:
                        pass

                du = DateUtil()
                self.__holdDate = "[none listed]" if( self.__holdDate is None or len(self.__holdDate) < 1 or self.__isCifNull(self.__holdDate) ) else \
                    du.date_to_display(self.__holdDate)
                    
                if( self.__statusCode is not None and len(self.__statusCode) > 0 ):
                    if( self.__statusCode == 'HPUB'):
                        self.__entryStatus = "Hold for publication"
                    elif( self.__statusCode == 'HOLD'):
                        self.__entryStatus = "Hold until "+self.__holdDate
                    else:
                        self.__entryStatus = "[entry status placeholder]"
                else:
                    self.__entryStatus = "[none listed]"
                    self.__statusCode = "[none listed]"
    
                
                if( self.__authRelStatusCode.upper() == 'HOLD' ):
                    self.__expireDate = self.__holdDate
                elif( self.__authRelStatusCode.upper() in ['HPUB', 'REL'] ):
                    if( self.__initRecvdDate is not None and len(self.__initRecvdDate) > 0 and self.__isNotCifNull(self.__initRecvdDate) ):
                        initRecvdDate = datetime.strptime(self.__initRecvdDate, '%Y-%m-%d')
                        
                        # determine expire date as 1 year from initial received date (taking into account possibility of init recv'd date being 2/29 (a leap year)  
                        if( initRecvdDate.month == 2 and initRecvdDate.day == 29 ):
                            initRecvdDatePlusOneYr = initRecvdDate.replace( year=(initRecvdDate.year+1), month=3, day=1 )
                        else:
                            initRecvdDatePlusOneYr = initRecvdDate.replace(year=(initRecvdDate.year+1))
                        du = DateUtil()
                        self.__expireDate = du.datetime_to_display(initRecvdDatePlusOneYr)
                        
                    else:
                        self.__expireDate = '[NO DATE AVAIL]'
                else:
                    self.__expireDate = '[NO DATE AVAIL]'
                    
        except:
            if (self.__verbose):
                self.__lfh.write("+%s.%s() problem recovering data from PdbxPersist for category: '%s'\n" % (self.__class__.__name__,
                                                                                          sys._getframe().f_code.co_name, ctgryNm ) )
            traceback.print_exc(file=self.__lfh)
            self.__lfh.flush() 
                 
    def __getProcessingStatusInfoEM(self):
        ctgryNm = 'em_admin'
        try:
            if( self.__verbose ):
                self.__lfh.write("+%s.%s() -- Category name sought from [%s] is: '%s'\n" % (self.__class__.__name__,
                                                                                  sys._getframe().f_code.co_name,
                                                                                  self.__dbFilePath,
                                                                                  ctgryNm) )
            #
            catObj = self.__getCatObj(ctgryNm)
            if catObj:
                #
                # Get column name index.
                #
                itDict={}
                itNameList=catObj.getItemNameList()
                for idxIt,itName in enumerate(itNameList):
                    itDict[str(itName).lower()]=idxIt
                    #
                idxStatusCode=itDict['_em_admin.current_status']
                idxDpstnDate=itDict['_em_admin.deposition_date']
                idxDpstnSite=itDict['_em_admin.deposition_site']
                idxMapReleaseDate=itDict['_em_admin.map_release_date']
                #idxHeaderReleaseDate=itDict['_em_admin.header_release_date']
                                
                for row in catObj.getRowList():
                    try:
                        self.__statusCodeEmMap = (str(row[idxStatusCode])).upper()
                        self.__dpstnDateEmMap = str(row[idxDpstnDate])
                        self.__releaseDateEmMap = str(row[idxMapReleaseDate])
                                                    
                    except:
                        pass
                
                if( self.__statusCodeEmMap is not None and len(self.__statusCodeEmMap) > 0 ):
                    if( self.__statusCodeEmMap == 'HPUB'):
                        self.__entryStatusEmMap = "Hold for publication"
                    elif( self.__statusCodeEmMap == 'HOLD'):
                        self.__entryStatusEmMap = "Hold until "+self.__holdDate
                    else:
                        self.__entryStatusEmMap = "[entry status placeholder]"
                else:
                    self.__entryStatusEmMap = "[none listed]"
                    self.__statusCodeEmMap = "[none listed]"
                
                if( self.__dpstnDateEmMap is not None and len(self.__dpstnDateEmMap) > 0 and self.__isNotCifNull(self.__dpstnDateEmMap) ):
                    dpstnDateEmMap = datetime.strptime(self.__dpstnDateEmMap, '%Y-%m-%d')
                    
                    # determine expire date as 1 year from initial received date (taking into account possibility of init recv'd date being 2/29 (a leap year)  
                    if( dpstnDateEmMap.month == 2 and dpstnDateEmMap.day == 29 ):
                        dpstnDateEmMapPlusOneYr = dpstnDateEmMap.replace( year=(dpstnDateEmMap.year+1), month=3, day=1 )
                    else:
                        dpstnDateEmMapPlusOneYr = dpstnDateEmMap.replace(year=(dpstnDateEmMap.year+1))
                    du = DateUtil()
                    self.__expireDateEmMap = du.datetime_to_display(dpstnDateEmMapPlusOneYr)
                    
                else:
                    self.__expireDateEmMap = '[NO DATE AVAIL]'
                
                    
        except:
            if (self.__verbose):
                self.__lfh.write("+%s.%s() problem recovering data from PdbxPersist for category: '%s'\n" % (self.__class__.__name__,
                                                                                          sys._getframe().f_code.co_name, ctgryNm ) )
            traceback.print_exc(file=self.__lfh)
            self.__lfh.flush() 


    def __getAnnotatorDetails(self):
        
        procSite = self.__procSite.upper()
        
        useAnnotatorName = self.__reqObj.getValueOrDefault('useAnnotatorName', None)

        annotatorId = self.__reqObj.getValue("sender").upper()
        if useAnnotatorName:
            annotatorId=self.__pdbxAnnotator

        if( procSite == "RCSB" ):
            #self.__closingSiteDetails = MessagingTemplates.msgTmplt_site_contact_details_rcsb_em if( self.__emDeposition ) else MessagingTemplates.msgTmplt_site_contact_details_rcsb
            self.__closingSiteDetails = MessagingTemplates.msgTmplt_site_contact_details_rcsb
        elif( procSite == "PDBE" ):
            self.__closingSiteDetails = MessagingTemplates.msgTmplt_site_contact_details_pdbe
        elif( procSite == "PDBJ" ):
            self.__closingSiteDetails = MessagingTemplates.msgTmplt_site_contact_details_pdbj
        
        if self.__annotatorUserNameDict:
            userNameMap = self.__annotatorUserNameDict.get(procSite,None)
        else:
            userNameMap = None

        if userNameMap:
            annotatorDict = userNameMap.get(annotatorId, None)
            
            if annotatorDict:
                fname = annotatorDict.get("fname", "")
                lname = annotatorDict.get("lname", "")
                
                self.__annotatorFullName = fname + " " + lname
                     
                 
    def __getCitationInfo(self):
        #######################
        ## citation authors ##
        #######################
        ctgryNm = 'citation_author'
        try:
            if( self.__verbose ):
                self.__lfh.write("+%s.%s() -- Category name sought from [%s] is: '%s'\n" % (self.__class__.__name__,
                                                                                  sys._getframe().f_code.co_name,
                                                                                  self.__dbFilePath,
                                                                                  ctgryNm) )
            #
            catObj = self.__getCatObj(ctgryNm)
            if catObj:
                #
                # Get column name index.
                #
                itDict={}
                itNameList=catObj.getItemNameList()
                for idxIt,itName in enumerate(itNameList):
                    itDict[str(itName).lower()]=idxIt
                    #
                
                idxFullName=itDict['_citation_author.name']
                idxCitationId=itDict['_citation_author.citation_id']
                
                
                for row in catObj.getRowList():
                    try:
                        name = row[idxFullName]
                        citationId = row[idxCitationId]
                        
                        if( citationId.lower() == 'primary' ):
                            self.__citAuthors.append( name )
                            if (self.__verbose):
                                self.__lfh.write("\n%s.%s() -- citation_author.name found as: %s\n" % (self.__class__.__name__,
                                                               sys._getframe().f_code.co_name,
                                                               name) )
                        
                    except:
                        pass
                
        except:
            if (self.__verbose):
                self.__lfh.write("+%s.%s() problem recovering data from PdbxPersist for category: '%s'\n" % (self.__class__.__name__,
                                                                                          sys._getframe().f_code.co_name, ctgryNm ) )
            traceback.print_exc(file=self.__lfh)
            self.__lfh.flush() 
        
        ##############
        ## citation ##
        ##############
        ctgryNm = 'citation'
        try:
            if( self.__verbose ):
                self.__lfh.write("+%s.%s() -- Category name sought from [%s] is: '%s'\n" % (self.__class__.__name__,
                                                                                  sys._getframe().f_code.co_name,
                                                                                  self.__dbFilePath,
                                                                                  ctgryNm) )
            #
            catObj = self.__getCatObj(ctgryNm)
            if catObj:
                #
                # Get column name index.
                #
                itDict={}
                itNameList=catObj.getItemNameList()
                for idxIt,itName in enumerate(itNameList):
                    itDict[str(itName).lower()]=idxIt
                    #
                
                idxId=itDict['_citation.id']
                idxTitle=itDict['_citation.title']
                idxJrnlAbbrev=itDict['_citation.journal_abbrev']
                idxJrnlVolume=itDict['_citation.journal_volume']
                idxPageFirst=itDict['_citation.page_first']
                idxPageLast=itDict['_citation.page_last']
                idxYear=itDict['_citation.year']
                idxPdbxDbIdDOI=itDict['_citation.pdbx_database_id_doi'] if '_citation.pdbx_database_id_doi' in itDict else None
                idxPdbxDbIdPubMed=itDict['_citation.pdbx_database_id_pubmed'] if '_citation.pdbx_database_id_pubmed' in itDict else None
                
                for row in catObj.getRowList():
                    try:
                        citationId = row[idxId]
                        
                        if( citationId.lower() == 'primary' ):
                            self.__citTitle = row[idxTitle]  
                            self.__citJournalAbbrev = row[idxJrnlAbbrev] 
                            self.__citJournalVolume = row[idxJrnlVolume] 
                            self.__citPageFirst = row[idxPageFirst]
                            self.__citPageLast = row[idxPageLast]
                            self.__citYear = row[idxYear]
                            self.__citPdbxDbIdDOI = row[idxPdbxDbIdDOI] if idxPdbxDbIdDOI else ""
                            self.__citPdbxDbIdPubMed = row[idxPdbxDbIdPubMed] if idxPdbxDbIdPubMed else ""
                            
                            if (self.__verbose):
                                self.__lfh.write("\n%s.%s() -- citation.title found as: %s\n" % (self.__class__.__name__,
                                                               sys._getframe().f_code.co_name,
                                                               self.__citTitle) )
                        
                    except:
                        pass
                
        except:
            if (self.__verbose):
                self.__lfh.write("+%s.%s() problem recovering data from PdbxPersist for category: '%s'\n" % (self.__class__.__name__,
                                                                                          sys._getframe().f_code.co_name, ctgryNm ) )
            traceback.print_exc(file=self.__lfh)
            self.__lfh.flush() 
            
    def __getRqstdAccessionIds(self):
        
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        self.__lfh.write("--------------------------------------------\n")        
        self.__lfh.write("Starting %s.%s() at %s\n" % (className,
                                                       methodName,
                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        #
        accessionIdsLst = []
        ctgryNm = 'pdbx_depui_entry_details'
        try:
            ctgryObj = self.__getCatObj(ctgryNm)
            
            if ctgryObj:
                fullRsltSet = ctgryObj.getRowList()
                iTotalRecords = len(fullRsltSet)
                
                if (self.__verbose and self.__debug ):
                    self.__lfh.write("+%s.%s() -- fullRsltSet obtained as: %r\n" % (className, methodName, fullRsltSet) )
                
                assert( iTotalRecords == 1 ), ("+%s.%s() -- expecting '%s' category to contain a single record but had %s records\n" % (className,methodName,ctgryNm,iTotalRecords) )
                #
                #
                # Get column name index.
                #
                itDict={}
                itNameList=ctgryObj.getItemNameList()
                for idxIt,itName in enumerate(itNameList):
                    itDict[str(itName).lower()]=idxIt
                    #
                
                idxRqstdIdTypes=itDict['_pdbx_depui_entry_details.requested_accession_types']
                #
                for row in ctgryObj.getRowList():
                    try:
                        if (self.__verbose and self.__debug ):
                            self.__lfh.write("+%s.%s() -- found 'requested_accession_types' field at index: %s with value: %s\n" % (className, methodName, idxRqstdIdTypes, (fullRsltSet[0])[idxRqstdIdTypes]) )
                            
                        self.__rqstdAccessionIdsLst = ( row[idxRqstdIdTypes] ).split(",")
                        self.__lfh.write("+%s.%s() self.__rqstdAccessionIdsLst being assigned as: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, self.__rqstdAccessionIdsLst ) )
                        
                        if( self.__emDeposition ):
                            if( "PDB" in self.__rqstdAccessionIdsLst ):
                                self.__emHaveModel = True
                            if( "EMDB" in self.__rqstdAccessionIdsLst ):
                                self.__emHaveMap = True
                            if( self.__emHaveMap and self.__emHaveModel ):
                                self.__emMapAndModelEntry = True
                            if( self.__emHaveMap and not self.__emHaveModel ):
                                self.__emMapOnly = True
                            if( self.__emHaveModel and not self.__emHaveMap ):
                                self.__emModelOnly = True
                    except:
                        pass
                
        except:
            traceback.print_exc(file=self.__lfh)
            

    def __getDefaultMsgTmpltType(self):
        # code for determining default letter template
        
        '''
        If _pdbx_database_status.status_code= ( HPUB or HOLD ) and _pdbx_database_status.author_approval_type = implicit  -> launch approval_implicit_letter_template
        If _pdbx_database_status.status_code= ( HPUB or HOLD ) and _pdbx_database_status.author_approval_type = explicit  -> launch approval_explicit_letter_template
        If _pdbx_database_status.status_code= REL and ( _citation.pdbx_database_id_DOI is not null OR _citation.pdbx_database_id_PubMed is not null ) -> launch Release_withpublication_letter_template
        If _pdbx_database_status.status_code= REL and ( _citation.pdbx_database_id_DOI is null AND _citation.pdbx_database_id_PubMed is null ) -> launch Release_withoutpublication_letter_template
        If _pdbx_database_status.status_code= WAIT -> launch reminder_letter_template
        If _pdbx_database_status.status_code= WDRN -> launch withdrawn_letter_template
        If it is an EM map only entry --> if _pdbx_database_status.status_code= AUTH -> launch mapOnly-AuthStatus_letter_template
        '''

        if( self.__emDeposition ):
            statusCode = self.__statusCodeEmMap
        else:
            # Not a question mark
            if self.__postRelStatus is not None and len(self.__postRelStatus) > 1:
                statusCode = self.__postRelStatus
            else:
                statusCode = self.__statusCode

        if( statusCode and len(statusCode) > 1 ):
            if( statusCode == 'HPUB' or statusCode == 'HOLD'):
                if( self.__authApprovalType and len(self.__authApprovalType) > 1 ):
                    if( self.__authApprovalType.lower() == 'implicit' ):
                        self.__defaultMsgTmpltType = "approval-impl"
                    elif( self.__authApprovalType.lower() == 'explicit' ):
                        self.__defaultMsgTmpltType = "approval-expl"
            elif( statusCode == 'REL' ):
                if(  ( self.__citPdbxDbIdDOI and len(self.__citPdbxDbIdDOI) > 1 ) or ( self.__citPdbxDbIdPubMed and len(self.__citPdbxDbIdPubMed) > 1 ) ):
                    self.__defaultMsgTmpltType = "release-publ"
                else:
                    self.__defaultMsgTmpltType = "release-nopubl"
            elif( statusCode == 'WAIT' ):
                self.__defaultMsgTmpltType = "reminder"
            elif( statusCode == 'WDRN' ):
                self.__defaultMsgTmpltType = "withdrawn"
            elif( statusCode == 'AUTH' ):
                if( self.__emDeposition and self.__emMapOnly ):
                    self.__defaultMsgTmpltType = "maponly-authstatus-em"
        
        if (self.__verbose):
            self.__lfh.write("\n%s.%s() -- default message template is: %s\n\n" % (self.__class__.__name__,
                                                                                 sys._getframe().f_code.co_name,
                                                                                 self.__defaultMsgTmpltType) )

    def __getLastCommDate(self):
        # Retrieves last message sent date as well as last unlocked message
        myContainerList=[]
        ifh = open(self.__messagingFilePath, "r")
        pRd=PdbxReader(ifh)
        pRd.read(myContainerList)
        if len(myContainerList) >= 1:
            c0=myContainerList[0]
            catObj=c0.getObj("pdbx_deposition_message_info")
            if catObj is None:
                if (self.__verbose):
                    self.__lfh.write("\n%s.%s() -- Unable to find 'pdbx_deposition_message_info' category in file: %s\n" % (self.__class__.__name__,
                                                           sys._getframe().f_code.co_name,
                                                           self.__messagingFilePath) )
            else:
                #
                # Get column name index.
                #
                itDict={}
                itNameList=catObj.getItemNameList()
                for idxIt,itName in enumerate(itNameList):
                    itDict[str(itName).lower()]=idxIt
                    #
                idxOrdinalId=itDict['_pdbx_deposition_message_info.ordinal_id']
                idxLastCommDate=itDict['_pdbx_deposition_message_info.timestamp']
                idxMsgSubj=itDict['_pdbx_deposition_message_info.message_subject']
                
                maxOrdId=0
                maxUnlockOrdId=0
                for row in catObj.getRowList():
                    try:
                        ordinalId = int(row[idxOrdinalId])

                        if ordinalId > maxOrdId:
                            maxOrdId = ordinalId
                            self.__lastCommDate = str(row[idxLastCommDate])

                        msgsubj = row[idxMsgSubj]
                        if msgsubj == 'System Unlocked':
                            if ordinalId > maxUnlockOrdId:
                                maxUnlockOrdId = ordinalId
                                self.__lastUnlockDate = str(row[idxLastCommDate]).split(' ')[0]
                    except Exception as e:
                        pass
        
        else:
            if (self.__verbose):
                    self.__lfh.write("\n%s.%s() -- Unable to find 'pdbx_deposition_message_info' category in empty cif file: %s\n" % (self.__class__.__name__,
                                                           sys._getframe().f_code.co_name,
                                                           self.__messagingFilePath) )
                    
            
    def __getLastOutboundRprtDate(self,p_IdType="PDB"):
        """Returns a string of last timestamp"""
        
        if( p_IdType == "PDB"):
            cType = 'validation-report-full-annotate'
            frmt = 'pdf'
        elif( p_IdType == "EMDB"):
            cType = 'correspondence-to-depositor'
            frmt = 'txt'
            
        msgDI=MessagingDataImport(self.__reqObj,verbose=self.__verbose,log=self.__lfh)
        pathDict = msgDI.getMileStoneFilePaths(contentType=cType,format=frmt)
        archiveFilePth = pathDict['annotPth']
                    
        if( archiveFilePth is not None ):
            du = DateUtil()
            lastModfdTime = du.datetime_to_display(date.fromtimestamp( os.path.getmtime(archiveFilePth)))
            lastOutboundRprtDate = lastModfdTime
            if (self.__verbose):
                self.__lfh.write("\n%s.%s() -- 'lastOutboundRprtDate was found to be [%s].\n" % (self.__class__.__name__,
                                                               sys._getframe().f_code.co_name, lastOutboundRprtDate) )
        else:
            lastOutboundRprtDate = "[NO DATE AVAIL]"
            
        if( p_IdType == "PDB"):
            self.__lastOutboundRprtDate = lastOutboundRprtDate
        elif( p_IdType == "EMDB"):
            self.__lastOutboundRprtDateEm = lastOutboundRprtDate     
    
    def __identifyEmReleaseTargets(self):
        
        if( "REL" in [self.__statusCodeEmMap, self.__statusCode ]):
            if( self.__statusCodeEmMap != self.__statusCode ):
                if( self.__statusCodeEmMap == "REL" ):
                    self.__emMapReleased = True
                else:
                    self.__emModelReleased = True
            else:
                self.__emMapReleased = True
                self.__emModelReleased = True
                
                # if both map and model are currently at REL, have to verify if one of the components had already been released separately
                relDateEmMap = datetime.strptime(self.__releaseDateEmMap, '%Y-%m-%d')
                relDateEmCoord = datetime.strptime(self.__pdbReleaseDate, '%Y-%m-%d')
                
                if relDateEmMap and relDateEmCoord:
                    if( relDateEmMap < relDateEmCoord ):
                        self.__emMapPreviouslyReleased = True
                        if (self.__verbose):
                            self.__lfh.write("\n%s.%s() -- 'Map found to be previously released on: %s.\n" % (self.__class__.__name__,
                                                                           sys._getframe().f_code.co_name, relDateEmMap) )
                    elif( relDateEmCoord < relDateEmMap ):
                        self.__emCoordPreviouslyReleased = True
                        if (self.__verbose):
                            self.__lfh.write("\n%s.%s() -- 'Coordinates found to be previously released on: %s.\n" % (self.__class__.__name__,
                                                                           sys._getframe().f_code.co_name, relDateEmCoord) )
                    elif( relDateEmMap == relDateEmCoord ):
                        self.__emMapAndModelJointRelease = True
                        if (self.__verbose):
                            self.__lfh.write("\n%s.%s() -- 'Joint release of map and coordinates on: %s.\n" % (self.__class__.__name__,
                                                                           sys._getframe().f_code.co_name, relDateEmCoord) )
            
        
        
    def __getReleaseDateInfo(self):
        
        '''determine release date which is Wednesday of week following the current week.
        '''
        dateRef = date.today()
        weekDayIndex = dateRef.weekday() # Monday through Sunday represented by indexes 0 through 6
        
        if( weekDayIndex >= 5 ):
            # advancing dateRef to point at closest coming Monday if this code invoked on a Saturday or Sunday 
            dateRef += timedelta(days=(7-weekDayIndex))
            
        while dateRef.weekday() != 6:
            # advancing to next coming Sunday 
            dateRef += timedelta(days=1)
        
        # now that we're at Sunday, advance to Wednesday
        dateRef += timedelta(days=3)

        du = DateUtil()
        self.__releaseDate = du.datetime_to_display(dateRef)
        
        '''also need to determine date threshold of acceptable change request from depositor, which is Thursday (morning, before 11am) of week prior to Wednesday release
        '''
        notherDateRef = date.today() # to determine which day of the week today is (as opposed to numerical date)
        now = datetime.now() # to determine what the current time is
        
        if ( notherDateRef.weekday() > 3 and notherDateRef.weekday() < 5 ) or ( notherDateRef.weekday() == 3 and now.hour >= 11 ): # i.e. if we're past the deadline for acceptable change requests
            self.__thursPreRlsClause = ""
            self.__thursWdrnClause = ""
        else:
            # i.e. if we're still before the deadline for acceptable change requests
            processingSite = self.__procSite if( self.__procSite is not None and len(self.__procSite) > 0 ) else "[not available]"

            if( notherDateRef.weekday() != 3 ):
                # not Thursday yet, so can tell depositor that they have until noon of Thursday this week to communicate any changes
                
                while notherDateRef.weekday() != 3:
                    # advancing to next coming Thursday (Monday through Sunday represented as 0 through 6)
                    notherDateRef += timedelta(days=1)

                self.__thursPreRlsClause = 'If you have changes to make to the entry, please inform us by noon local time at %s on Thursday %s.' % (processingSite, du.datetime_to_display(notherDateRef))
                self.__thursWdrnClause = """
If this is incorrect or if you have any questions please inform us by noon local time at %s on Thursday %s.""" % (processingSite, du.datetime_to_display(notherDateRef))
            
            else:
                # else today is Thursday before 11am, so need to tell depositor that they have to communicate any changes by noon today!
                self.__thursPreRlsClause = 'If you have changes to make to the entry please inform us by today, noon local time at '+processingSite+'.'
                self.__thursWdrnClause = """
If you have changes to make to the entry please inform us by today, noon local time at """+processingSite+""".
"""
 
        self.__thursWdrnClause += """
Please use the latest annotated mmCIF file (attached) to start a new deposition if you need to redeposit the structure."""

    def __isWorkflow(self):
        """ Determine if currently operating in Workflow Managed environment
        
            :Returns:
                boolean indicating whether or not currently operating in Workflow Managed environment
        """
        #
        fileSource  = str(self.__reqObj.getValue("filesource")).lower()
        #
        if fileSource and fileSource in ['archive','wf-archive','wf_archive','wf-instance','wf_instance']:
            #if the file source is any of the above then we are in the workflow manager environment
            return True
        else:
            #else we are in the standalone dev environment
            return False
        
    def __validateEmail(self,email):
    
        if len(email) > 7:
            if re.match(r"[^@]+@[^@]+\.[^@]+", email) != None:
                return True
        if(self.__verbose):
            self.__lfh.write("\n%s.%s() -- following email address found to be invalid: %s\n" % (self.__class__.__name__,
                                                           sys._getframe().f_code.co_name,
                                                           email) )
        return False

class FileSizeLogger(object):
    """ Simple class to support trace logging for file size before and after a given action
    """
    
    def __init__(self, filePath, verbose=False, log=sys.stderr):
        """ Prepare the file size logger. Specify the file to report on 
        """
        self.__filePath = filePath
        #
        self.__lfh =log
        self.__verbose=verbose
        self.__debug=True
        #
            
    def __enter__(self):
        filesize = os.stat(self.__filePath).st_size
        if(self.__verbose and self.__debug):
            self.__lfh.write("+%s -- filesize for %s before call: %s bytes.\n" % (self.__class__.__name__, self.__filePath, filesize) )
                
        return self
    
    def __exit__(self, type, value, traceback):
        filesize = os.stat(self.__filePath).st_size
        if(self.__verbose and self.__debug):
            self.__lfh.write("+%s -- filesize for %s after call: %s bytes.\n" % (self.__class__.__name__, self.__filePath, filesize) )            

    
