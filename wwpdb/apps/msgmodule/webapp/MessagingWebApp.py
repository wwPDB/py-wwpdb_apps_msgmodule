##
# File:  MessagingWebApp.py
# Date:  26-Apr-2012
# Updates:
# 
# 2012-04-26    RPS    Ported from prior XXXWebApp implementation template.
# 2013-09-03    RPS    Updated to use cif-based backend storage. Augmented functionality for sending/reading messages.
# 2013-09-13    RPS    added _checkAvailFiles() so that "Associate Files" checkboxes now only appear in UI for files that actually are available on serverside.
#                            Support for displaying confirmation of files referenced when displaying a message in UI.
# 2013-10-04    RPS    Providing file upload input on Compose Message, to accommodate attachment of auxiliary file.
# 2013-10-30    RPS    Support for "Notes" UI and for UI feature for classifying messages (e.g. "action required" or "unread")
# 2013-11-19    RPS    Correcting notification and attachment behavior for scenario when drafts converted to actual sent msgs
# 2014-01-14    RPS    Updates to support notifications to WFM for global "action required" and "read/unread" message status.
# 2014-01-24    RPS    Corrected so that default value for action required in markMsgAsRead is 'Y'.
# 2014-03-04    RPS    Updates to allow tracking of instances wherein new "-annotate" version of cif model file is being propagated to Deposition side.
#                            and also allow tracking of instances wherein validation reports are being propagated to Deposition side.
# 2014-03-21    RPS    Interim fix to prevent cif parsing errors due to unmatched ';'
# 2014-03-17    RPS    Support for UTF-8 capture/persistence into cif files.
# 2014-04-08    RPS    Fix to allow association of messages with uploaded auxiliary files with Common Tool format <fileName>.<extension>.<versionNum> 
# 2014-06-09    RPS    Now accommodating up to 2 additional auxiliary file references. And aborting submission of new message when problem arises
#                        with handling of file references.
# 2014-10-16    RPS    Added explicit error messages for cases where annotator "attaches" file with unfamiliar extension.
# 2014-12-04    RPS    Updates for: "archive" of messages, new "Complete Correspondence History" view, tagging messages "for Release", checking for
#                        presence of notes.
# 2014-12-10    RPS    checkGlobalMsgStatus() updated so that can distinguish notes actually authored by annotators from those notes that result from archived comms.
# 2014-12-18    RPS    updated to accommodate handling of automated "reminder" notifications
# 2014-12-23    RPS    short-term fix to eliminate display of '\xa0' in message text.
# 2015-01-29    RPS    Added functionality to allow annotator to disable/enable '*' indicator shown in WFM UI that signals presence of annotator-authored notes.
# 2015-03-02    RPS    Updates per introduction of sanity check safeguards on writes to messaging cif data files.
# 2015-07-28    RPS    Introduced support for "Unlock Dep UI" button and for flagging note/comms from BMRB.
# 2015-12-02    RPS    Updates to optimize response time to user by running template processing in background.
# 2016-02-03    RPS    Calling checkGlobalMsgStatus() after a message requiring "notes" flag is archived by email handler, so that flag is properly refreshed for display in WFM UI
# 2016-02-17    RPS    Introducing use of Message model class to increase encapsulation, improve code organization.
# 2016-08-09    RPS    Introducing support for standalone correspondence viewer. Providing means for detecting presence of notes archived via BMRB emails.
# 2016-09-07    RPS    Providing separate, dedicated means for activating/deactivating UI flag for detecting presence of notes archived via BMRB emails (vs. standard annotator authored notes).
# 2016-09-14    ZF     Added __checkGroupDeposition() function to support for group deposition
##
"""
wwPDB Messaging web request and response processing modules.

This software was developed as part of the World Wide Protein Data Bank
Common Deposition and Annotation System Project

Copyright (c) 2012 wwPDB

This software is provided under a Creative Commons Attribution 3.0 Unported
License described at http://creativecommons.org/licenses/by/3.0/.

"""
__docformat__ = "restructuredtext en"
__author__    = "Raul Sala"
__email__     = "rsala@rcsb.rutgers.edu"
__license__   = "Creative Commons Attribution 3.0 Unported"
__version__   = "V0.07"

import os, sys, time, types, string, traceback, ntpath, threading, shutil
try:
    from html.parser import HTMLParser
except ImportError:
    from HTMLParser import HTMLParser
from json import loads, dumps
from time import localtime, strftime

from wwpdb.utils.session.WebRequest                    import InputRequest,ResponseContent
from wwpdb.apps.msgmodule.depict.MessagingDepict        import MessagingDepict
from wwpdb.apps.msgmodule.io.MessagingIo                import MessagingIo
from wwpdb.utils.wf.dbapi.StatusDbApi                   import StatusDbApi
from wwpdb.apps.msgmodule.models.Message                import Message
#
#from wwpdb.apps.msgmodule.utils.WfTracking              import WfTracking
#
from wwpdb.io.locator.DataReference                     import DataFileReference
from wwpdb.utils.config.ConfigInfo                      import ConfigInfo
#
from wwpdb.utils.wf.dbapi.WfDbApi                       import WfDbApi
#
from wwpdb.apps.wf_engine.engine.WFEapplications        import getdepUIPassword
#
from wwpdb.io.locator.PathInfo                          import PathInfo

class MessagingWebApp(object):
    """Handle request and response object processing for the wwPDB messaging tool application.
    
    """
    def __init__(self,parameterDict={},verbose=False,log=sys.stderr,siteId="WWPDB_DEV"):
        """
        Create an instance of `MessagingWebApp` to manage an messaging web request.

         :param `parameterDict`: dictionary storing parameter information from the web request.
             Storage model for GET and POST parameter data is a dictionary of lists.
         :param `verbose`:  boolean flag to activate verbose logging.
         :param `log`:      stream for logging.
          
        """
        self.__verbose=verbose
        self.__lfh=log
        self.__debug=False
        self.__siteId=siteId
        self.__cI=ConfigInfo(self.__siteId)
        self.__topPath=self.__cI.get('SITE_WEB_APPS_TOP_PATH')
        self.__topSessionPath  = self.__cI.get('SITE_WEB_APPS_TOP_SESSIONS_PATH')
        self.__templatePath = os.path.join(self.__topPath,"htdocs","msgmodule")
        #

        if isinstance(parameterDict, dict):
            self.__myParameterDict=parameterDict
        else:
            self.__myParameterDict={}

        if (self.__verbose):
            self.__lfh.write("+MessagingWebApp.__init() - REQUEST STARTING ------------------------------------\n" )
            self.__lfh.write("+MessagingWebApp.__init() - dumping input parameter dictionary \n" )                        
            self.__lfh.write("%s" % (''.join(self.__dumpRequest())))
            
        self.__reqObj=InputRequest(self.__myParameterDict,verbose=self.__verbose,log=self.__lfh)
        #
        self.__reqObj.setValue("TopSessionPath", self.__topSessionPath)
        self.__reqObj.setValue("TemplatePath",   self.__templatePath)
        self.__reqObj.setValue("TopPath",        self.__topPath)
        self.__reqObj.setValue("WWPDB_SITE_ID",  self.__siteId)
        os.environ["WWPDB_SITE_ID"]=self.__siteId
        #
        self.__reqObj.setDefaultReturnFormat(return_format="html")
        #
        if (self.__verbose):
            self.__lfh.write("-----------------------------------------------------\n")
            self.__lfh.write("+MessagingWebApp.__init() Leaving _init with request contents\n" )            
            self.__reqObj.printIt(ofh=self.__lfh)
            self.__lfh.write("---------------MessagingWebApp - done -------------------------------\n")   
            self.__lfh.flush()
            
    def doOp(self):
        """ Execute request and package results in response dictionary.

        :Returns:
             A dictionary containing response data for the input request.
             Minimally, the content of this dictionary will include the
             keys: CONTENT_TYPE and REQUEST_STRING.
        """
        stw=MessagingWebAppWorker(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
        rC=stw.doOp()
        if (self.__debug):
            rqp=self.__reqObj.getRequestPath()
            self.__lfh.write("+MessagingWebApp.doOp() operation %s\n" % rqp)
            self.__lfh.write("+MessagingWebApp.doOp() return format %s\n" % self.__reqObj.getReturnFormat())
            if rC is not None:
                self.__lfh.write("%s" % (''.join(rC.dump())))
            else:
                self.__lfh.write("+MessagingWebApp.doOp() return object is empty\n")
                

        #
        # Package return according to the request return_format -
        #
        return rC.get()

    def __dumpRequest(self):
        """Utility method to format the contents of the internal parameter dictionary
           containing data from the input web request.

           :Returns:
               ``list`` of formatted text lines 
        """
        retL=[]
        retL.append("\n\-----------------MessagingWebApp().__dumpRequest()-----------------------------\n")
        retL.append("Parameter dictionary length = %d\n" % len(self.__myParameterDict))            
        for k,vL in self.__myParameterDict.items():
            retL.append("Parameter %30r :" % k)
            for v in vL:
                retL.append(" ->  %r\n" % v)
        retL.append("-------------------------------------------------------------\n")                
        return retL
    

class MessagingWebAppWorker(object):
    def __init__(self, reqObj=None, verbose=False,log=sys.stderr):
        """
         Worker methods for the wwPDB messaging application

         Performs URL - application mapping and application launching
         for wwPDB messaging tool.
         
         All operations can be driven from this interface which can
         supplied with control information from web application request
         or from a testing application.
        """
        self.__verbose=verbose
        self.__lfh=log
        self.__debug=True
        self.__reqObj=reqObj
        self.__sObj=None
        self.__sessionId=None
        self.__sessionPath=None
        self.__rltvSessionPath=None
        self.__siteId  = str(self.__reqObj.getValue("WWPDB_SITE_ID"))
        self.__cI=ConfigInfo(self.__siteId)
        #
        # Added by ZF
        #
        self.__checkGroupDeposition()
        #
        self.__appPathD={'/service/messaging/environment/dump':                     '_dumpOp',
                         '/service/messaging/launch':                               '_launchOp',
                         '/service/messaging/get_dtbl_data':                        '_getDataTblData',
                         '/service/messaging/get_dtbl_config_dtls':                 '_getDataTblConfigDtls',
                         '/service/messaging/mark_msg_read':                        '_markMsgAsRead',
                         '/service/messaging/tag_msg':                              '_tagMsg',
                         '/service/messaging/submit_msg':                           '_submitMsg',
                         '/service/messaging/update_draft_state':                   '_updateDraftState',
                         '/service/messaging/delete_row':                           '_deleteRowOp',
                         '/service/messaging/test_see_json':                        '_getDataTblDataRawJsonOp',
                         '/service/messaging/exit_not_finished':                    '_exit_notFinished',
                         '/service/messaging/exit_finished':                        '_exit_finished',
                         '/service/messaging/get_msg':                              '_getMsg',
                         '/service/messaging/check_avail_files':                    '_checkAvailFiles',
                         '/service/messaging/get_files_rfrncd':                     '_getFilesRfrncd',
                         '/service/messaging/check_global_msg_status':              '_checkGlobalMsgStatus',
                         '/service/messaging/archive_msg':                          '_archiveMsg',
                         '/service/messaging/forward_msg':                          '_forwardMsg',
                         '/service/messaging/display_msg':                          '_displayMsg',
                         '/service/messaging/view_correspondence':                  '_viewCorrespondence',
                         '/service/messaging/verify_depid':                         '_verifyDepositionId',
                         '/service/messaging/toggle_notes_flagging':                '_toggleNotesFlaggingOp',
                         '/service/messaging/get_depui_pwd':                        '_getDepUiPwd',
                         '/service/messaging/get_msg_tmplts':                       '_getMsgTmplts',
                         ###############  below are URLs to be used for WFM environ######################
                         '/service/messaging/new_session/wf':                       '_launchOp',
                         '/service/messaging/new_session/':                         '_launchOp',
                         '/service/messaging/new_session':                          '_launchOp',
                         '/service/messaging/wf/test':                              '_wfDoSomethingOp',
                         '/service/messaging/wf/launch':                            '_wfLaunchOp',
                         '/service/messaging/wf/exit_not_finished':                 '_exit_notFinished',
                         '/service/messaging/wf/exit_finished':                     '_exit_finished'
                         ###################################################################################################
                         }
        
    def doOp(self):
        """Map operation to path and invoke operation.  
        
            :Returns:

            Operation output is packaged in a ResponseContent() object.
        """
        return self.__doOpException()
    
    def __doOpNoException(self):
        """Map operation to path and invoke operation.  No exception handling is performed.
        
            :Returns:

            Operation output is packaged in a ResponseContent() object.
        """
        #
        reqPath=self.__reqObj.getRequestPath()
        if reqPath not in self.__appPathD:
            # bail out if operation is unknown -
            rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
            rC.setError(errMsg='Unknown operation')
            return rC
        else:
            mth=getattr(self,self.__appPathD[reqPath],None)
            rC=mth()
        return rC

    def __doOpException(self):
        """Map operation to path and invoke operation.  Exceptions are caught within this method.
        
            :Returns:

            Operation output is packaged in a ResponseContent() object.
        """
        #
        try:
            reqPath=self.__reqObj.getRequestPath()
            if reqPath not in self.__appPathD:
                # bail out if operation is unknown -
                rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
                rC.setError(errMsg='Unknown operation')
            else:
                mth=getattr(self,self.__appPathD[reqPath],None)
                rC=mth()
            return rC
        except:
            traceback.print_exc(file=self.__lfh)
            rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
            rC.setError(errMsg='Operation failure')
            return rC

    ################################################################################################################
    # ------------------------------------------------------------------------------------------------------------
    #      Top-level REST methods
    # ------------------------------------------------------------------------------------------------------------
    #
    def _dumpOp(self):
        rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
        rC.setHtmlList(self.__reqObj.dump(format='html'))
        return rC

    def _launchOp(self):
        """ Launch wwPDB messaging module interface
            
            :Helpers:
                wwpdb.apps.msgmodule.depict.MessagingDepict
                
            :Returns:
                Operation output is packaged in a ResponseContent() object.
                The output consists of a HTML starter container page for quicker return to the client.
                This container page is then populated with content via AJAX calls.
        """
        if (self.__verbose):
            self.__lfh.write("+MessagingWebAppWorker._launchOp() Starting now\n")
        # determine if currently operating in Workflow Managed environment
        bIsWorkflow = self.__isWorkflow()
        #
        self.__getSession()
        #
        fileSource = str(self.__reqObj.getValue("filesource")).strip().lower()
        depId = str(self.__reqObj.getValue("identifier")).upper()
        #
        self.__reqObj.setDefaultReturnFormat(return_format="html")        
        rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
        #
        if (self.__verbose):
            self.__lfh.write("+MessagingWebAppWorker._launchOp() workflow flag is %r\n" % bIsWorkflow)
        
        if ( bIsWorkflow ):
            # Update WF status database --
            '''
            bSuccess = self.__updateWfTrackingDb("open")
            if( not bSuccess ):
                rC.setError(errMsg="+MessagingWebAppWorker._launchOp() - TRACKING status, update to 'open' failed for session %s \n" % self.__sessionId )
            else:
                if (self.__verbose):
                    self.__lfh.write("+MessagingWebAppWorker._launchOp() Tracking status set to open\n")
            '''                
        else:
            if( fileSource and fileSource == "rcsb_dev"):
                pass
        #
        if (self.__verbose):
            self.__lfh.write("+MessagingWebAppWorker._launchOp() Call MessagingDepict with workflow %r\n" % bIsWorkflow)
        #
        msgngDpct=MessagingDepict(self.__verbose,self.__lfh)
        msgngDpct.setSessionPaths(self.__reqObj)
        oL = msgngDpct.doRender(self.__reqObj,bIsWorkflow)
        rC.setHtmlText( '\n'.join(oL) )
        #
        return rC
    
    def _verifyDepositionId(self):
        
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        rtrnDict = {}
        #
        depId = str(self.__reqObj.getValue("identifier")).upper()
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- Starting.\n" % (className, methodName) )        

        self.__getSession()
        #
        if (self.__verbose):
            self.__lfh.write("\n%s.%s() -- dep_id is: %s\n" % (className,
                                                       methodName,
                                                       depId) )
        #
        self.__reqObj.setReturnFormat(return_format="json")
        rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
        #
        pI=PathInfo(siteId=self.__siteId,sessionPath=self.__sessionPath,verbose=self.__verbose,log=self.__lfh)
        archivePth = pI.getArchivePath(depId)
        #
        if (self.__verbose):
            self.__lfh.write("\n%s.%s() -- archivePth is: %s\n" % (className,
                                                       methodName,
                                                       archivePth) )
        if (self.__verbose):
            self.__lfh.write("\n%s.%s() -- os.access(archivePth,os.F_OK) is: %s\n" % (className,
                                                       methodName,
                                                       os.access(archivePth,os.F_OK) ) )
        #
        rtrnDict['found'] = 'y' if( os.access(archivePth,os.F_OK) ) else 'n'
        
        rC.addDictionaryItems( rtrnDict )
        
        return rC 
            
    
    def _viewCorrespondence(self):
        """ Launch wwPDB messaging module interface -- "View All Correspondence" view 
            
            :Helpers:
                wwpdb.apps.msgmodule.depict.MessagingDepict
                
            :Returns:
                Operation output is packaged in a ResponseContent() object.
                The output consists of a HTML starter container page for quicker return to the client.
                This container page is then populated with content via AJAX calls.
        """
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() Starting now\n"%(className,methodName) )
        self.__getSession()
        #
        self.__reqObj.setDefaultReturnFormat(return_format="html")        
        self.__reqObj.setValue("filesource", "archive") # setting here for downstream processing
        rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
        #
        msgngDpct=MessagingDepict(self.__verbose,self.__lfh)
        msgngDpct.setSessionPaths(self.__reqObj)
        oL = msgngDpct.doRenderAllCorrespondence(self.__reqObj)
        rC.setHtmlText( '\n'.join(oL) )
        #
        return rC
    
        
    def _getDataTblConfigDtls(self):
        """ Get config details for staging display of messages for given deposition dataset in webpage.
            Data returned includes HTML <table> starter template for displaying messages
            and various settings for column, cell specific display details, validation behavior, etc. 
            
            :Helpers:
                wwpdb.apps.msgmodule.depict.MessagingDepict
                
            :Returns:
                Operation output is packaged in a ResponseContent() object.
                The output consists of JSON object which has two primary properties:
                    'html' --> <table> template representing the category with column headers for
                                each attribute defined for the category and conforming to structure
                                expected by jQuery DataTables plugin
                    'dtbl_config_dict' --> multi-layered dictionary of display/validation settings to be used
                                     for configuring the behavior of the DataTable 
        """
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        rtrnDict = {}
        dtblConfigDict = {}
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- Starting.\n" % (className, methodName) )        

        self.__getSession()
        
        depId = self.__reqObj.getValue("identifier")
        if (self.__verbose):
            self.__lfh.write("\n%s.%s() -- dep_id is:%s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       depId) )
        #
        sendStatus = self.__reqObj.getValue("send_status")
        #
        sUseThreadedRsltSet = self.__reqObj.getValue("usethreaded")
        bUseThreadedRsltSet = False
        #
        if( sUseThreadedRsltSet and sUseThreadedRsltSet == 'true' ):
             bUseThreadedRsltSet = True                                                       
        #
        self.__reqObj.setReturnFormat(return_format="json")
        rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
        #
        msgingIo = MessagingIo(self.__reqObj,self.__verbose,self.__lfh)
        rsltSetDict = msgingIo.getMsgRowList( p_depDataSetId=depId, p_sSendStatus=sendStatus, p_bServerSide=False, p_bThreadedRslts=bUseThreadedRsltSet ) #serverside False b/c only getting indentation info necessary for initial config
        #
        msgingDpct = MessagingDepict(verbose=self.__verbose,log=self.__lfh)
        dataTblTmplt, dtblConfigDict = msgingDpct.getDataTableTemplate( self.__reqObj )
        #dataTblTmplt = msgngDpct.getDataTableTemplate( depId, msgColList, bTrnspsdTbl, catObjDict['col_displ_name'] )  # example of supporting user friendly column display names
        #
        if('CURRENT_NUM_MSGS_TO_DPSTR' in rsltSetDict):
            dtblConfigDict['CURRENT_NUM_MSGS_TO_DPSTR'] = rsltSetDict['CURRENT_NUM_MSGS_TO_DPSTR'] # adding member representing total #records of msgs to depositor (live AND draft)
        if('CURRENT_NUM_NOTES' in rsltSetDict):
            dtblConfigDict['CURRENT_NUM_NOTES'] = rsltSetDict['CURRENT_NUM_NOTES'] # adding member representing total #records of notes (live AND draft)
        #
        if( bUseThreadedRsltSet and 'INDENT_DICT' in rsltSetDict ):
            dtblConfigDict['INDENT_DICT'] = rsltSetDict['INDENT_DICT'] # adding member representing threading/indent-formatting defined per msgid
        #
        rtrnDict['html'] = ''.join(dataTblTmplt)
              
        # stuff config dict into return dictionary for return to web page
        rtrnDict['dtbl_config_dict'] = dtblConfigDict
        
        rC.addDictionaryItems( rtrnDict )
        
        return rC    
    
    def _getDataTblData(self):
        """ Get data needed to populate DataTable for displaying messages for given deposition dataset ID 
            
            :Helpers:
                wwpdb.apps.msgmodule.depict.MessagingDepict
                wwpdb.apps.msgmodule.io.MessagingIo
                
            :Returns:
                Operation output is packaged in a ResponseContent() object.
                The output consists of a JSON object representing the category/items
                and conforming to structure expected by jQuery DataTables plugin for rendering
        """       
        #
        self.__getSession()
        #sUseServerSide = str( self.__reqObj.getValue("serverside") )
        depId = self.__reqObj.getValue("identifier")
        sendStatus = self.__reqObj.getValue("send_status")
        sUseThreadedRsltSet = self.__reqObj.getValue("usethreaded")
        sContentType = self.__reqObj.getValue("content_type")
        bCommHstryRqstd = True if sContentType == "commhstry" else False
        #
        if( sUseThreadedRsltSet and sUseThreadedRsltSet == 'true' ):
            bUseThreadedRsltSet = True
        else:
            bUseThreadedRsltSet = False
        #
        rsltSetDict = {}
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- dep_id is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, depId) )
        #
        self.__reqObj.setReturnFormat(return_format="json")
        rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
        #
        msgingIo = MessagingIo(self.__reqObj,self.__verbose,self.__lfh)
        bOk, msgColList = msgingIo.getMsgColList(bCommHstryRqstd)
        #
        #if( sUseServerSide == 'true' ):
        # DataTables related query string params:
        iDisplayStart = int( self.__reqObj.getValue("iDisplayStart") )
        iDisplayLength = int( self.__reqObj.getValue("iDisplayLength") )
        sEcho = int( self.__reqObj.getValue("sEcho") ) # casting to int as recommended by DataTables
        
        ##################################################################
        # we need to accommodate any search filtering taking place
        # any search strings provided are processed by MsgingIo against
        # resultset of data that was encoded in ascii on its way into cif datafile
        # and then decoded back to UTF8 for return/display in browser.
        # so we must mimic the same process for any search terms before we test
        # such terms for potential matches with the data in the resultset.
        # this is why self.__encodeForSearching() is invoked
        ##################################################################
        
        # handling for single global search term
        sSearch = self.__reqObj.getRawValue("sSearch")
        sSearch = self.__encodeForSearching(sSearch)
        self.__lfh.write("+%s.%s() -- sSearch is: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, sSearch) )
        
        ############## in below block we are accommodating any requests for column-specific search filtering ###################################
        numColumns = len(msgColList)
        colSearchDict = {}
        for n in range(0,numColumns):
            qryStrParam = "sSearch_"+str(n)
            qryBoolParam = "bSearchable_"+str(n)
            
            bIsColSearchable = (self.__reqObj.getValue(qryBoolParam) == "true")
            if bIsColSearchable:
                srchString = self.__reqObj.getValue(qryStrParam)
                if srchString and len(srchString) > 1:
                    colSearchDict[n] = self.__encodeForSearching(srchString)
                    if (self.__verbose and self.__debug ):
                        self.__lfh.write("+%s.%s() -- search term for field[%s] is: %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, n, colSearchDict[n]) )
        ########################################################################################################################################
        rsltSetDict = msgingIo.getMsgRowList( p_depDataSetId=depId, p_sSendStatus=sendStatus, p_bServerSide=True, p_iDisplayStart=iDisplayStart, p_iDisplayLength=iDisplayLength, p_sSrchFltr=sSearch, p_colSearchDict=colSearchDict, p_bThreadedRslts=bUseThreadedRsltSet )
        msgRecordList = rsltSetDict['RECORD_LIST']       
        '''
        else: # 2014-09-10, RPS: non-serverside processing is only being used in _getDataTblConfigDtls() -- consider removing?
            rsltSetDict = msgingIo.getMsgRowList( p_depDataSetId=depId, p_sSendStatus=sendStatus, p_bServerSide=False,  p_bThreadedRslts=bUseThreadedRsltSet )
            msgRecordList = rsltSetDict['RECORD_LIST']
            iDisplayStart = 0 #always getting full resultset when not using server=side processing so setting iDisplayStart explicitly to 0
        '''
        #
        msgngDpct = MessagingDepict(verbose=self.__verbose,log=self.__lfh)
        dataTblDict = msgngDpct.getJsonDataTable( msgRecordList, msgColList, iDisplayStart )
        #if( sUseServerSide == 'true' ):
        dataTblDict['sEcho'] = sEcho
        dataTblDict['iTotalRecords'] = rsltSetDict['TOTAL_RECORDS']
        dataTblDict['iTotalDisplayRecords'] = rsltSetDict['TOTAL_DISPLAY_RECORDS']
        #
        rC.addDictionaryItems( dataTblDict )
        
        return rC

    def _getDataTblDataRawJsonOp(self):
        ''' for DEV -- return payload input to DataTables to be displayed on webpage as JSON object for inspection'''
        
        if (self.__verbose):
            self.__lfh.write("+MessagingWebAppWorker._getDataTblDataRawJsonOp() starting\n")

        self.__getSession()
        #iDisplayStart = int( self.__reqObj.getValue("iDisplayStart") )
        #iDisplayLength = int( self.__reqObj.getValue("iDisplayLength") )
        #sEcho = int( self.__reqObj.getValue("sEcho") ) # casting to int as recommended by DataTables
        sEcho = 10
        
        depId = self.__reqObj.getValue("identifier")
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- dep_id is:%s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, depId) )
        #
        self.__reqObj.setDefaultReturnFormat(return_format="html")

        rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
        
        msgingIo = MessagingIo(self.__reqObj,self.__verbose,self.__lfh)
        bOk, msgColList = msgingIo.getMsgColList(p_sSchemaType='INDEX')
        msgRecordList, iTotalRecords, iTotalDisplayRecords = msgingIo.getMsgRowList( depId, True, 0, 20, "" )
        #
        msgngDpct = MessagingDepict(verbose=self.__verbose,log=self.__lfh)
        dataTblDict = msgngDpct.getJsonDataTable( msgRecordList, msgColList, 0 )
        dataTblDict['sEcho'] = sEcho
        dataTblDict['iTotalRecords'] = iTotalRecords
        dataTblDict['iTotalDisplayRecords'] = iTotalDisplayRecords
        
        rC.setHtmlText( str(dataTblDict) )
        
        return rC
    
        
    def _getMsgTmplts(self):
        """ Get  
            
            :Helpers:
                
            :Returns:
                 
        """
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        rtrnDict = {}
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- Starting.\n" % (className, methodName) )        
        #
        self.__getSession()
        #
        bIsWorkflow = self.__isWorkflow()
        #
        self.__reqObj.setReturnFormat(return_format="json")
        rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
        #
        bIsWorkflow = self.__isWorkflow()
        msgngDpct=MessagingDepict(self.__verbose,self.__lfh)
        msgngDpct.setSessionPaths(self.__reqObj)
        oL = msgngDpct.getMsgTmplts(self.__reqObj,bIsWorkflow)
            
        rtrnDict['html'] = '\n'.join(oL)
        
        statusCode=self.__reqObj.getValueOrDefault('status_code', default=None)
        if statusCode:
            rtrnDict['status_code'] = statusCode
        
        rC.addDictionaryItems( rtrnDict )
        
        return rC 
    
    def _checkGlobalMsgStatus(self):
        """ Method serves to check and register/communicate latest collective messaging status conditions 
            (i.e. conditions that refer not to any particular message but to the entire collection of messages)    
            e.g. are there any unread messages?, are there any messages requiring action?, any messages associated with release?
            NOTE: method also invokes call to update database with latest condition flags.
            
            :Returns: JSON returned to the browser containing relevant name/value pairs for various conditions
                 
        """
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        rtrnDict = {}
        bAllMsgsRead = False
        bAllMsgsActioned = False
        bAnyFlagsForRelease = False
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- Starting.\n" % (className, methodName) )        

        self.__getSession()
        #
        depId = str(self.__reqObj.getValue("identifier"))
        #
        if (self.__verbose):
            self.__lfh.write("\n%s.%s() -- dep_id is:%s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       depId) )
        #
        activateNotesFlagging = self._getNotesFlaggingStatus()
        #        
        self.__reqObj.setReturnFormat(return_format="json")
        rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
        #
        bAllMsgsRead = self.__checkAllMsgsRead()
        bAllMsgsActioned = self.__checkAllMsgsActioned()
        bAnyFlagsForRelease = self.__checkAnyReleaseFlags()
        bAnyNotesIncldngArchvdMsgs, bAnnotNotes, bBmrbNotes, iNumNotesRecords = self.__checkAnyNotesExist()
        
        #self.__lfh.write("+%s.%s() -- bAnyNotesIncldngArchvdMsgs is '%s' -- bAnnotNotes is '%s' -- iNumNotesRecords is '%s' for DEPID %s \n" % (className, methodName,bAnyNotesIncldngArchvdMsgs,bAnnotNotes,iNumNotesRecords,depId))
        
        if( bAllMsgsRead is True ):
            rtrnDict['all_msgs_read'] = 'true'
            newMsgsFlag = ''
        else:
            rtrnDict['all_msgs_read'] = 'false'
            newMsgsFlag = 'N' # i.e. "N"ew message(s) present
        
        if( bAllMsgsActioned is True ):
            rtrnDict['all_msgs_actioned'] = 'true'
            msgsNeedActionFlag = ''
        else:
            rtrnDict['all_msgs_actioned'] = 'false'
            msgsNeedActionFlag = 'T' # i.e. "T"odo action(s) present
            
        if( bAnyFlagsForRelease is True ):
            rtrnDict['any_msgs_for_release'] = 'true'
            msgsForReleaseFlag = 'R' # i.e. for "R"elease
        else:
            rtrnDict['any_msgs_for_release'] = 'false'
            msgsForReleaseFlag = '' # i.e. for "R"elease
            
        if( bAnnotNotes is True ):
            notesExistFlag = "*"
        else:
            notesExistFlag = ""
        
        if( bBmrbNotes is True ):
            bmrbNotesExistFlag = "B"
        else:
            bmrbNotesExistFlag = ""
                
        if( bAnyNotesIncldngArchvdMsgs is True ):
            rtrnDict['any_notes_exist'] = 'true'
        else:
            rtrnDict['any_notes_exist'] = 'false'
            
        rtrnDict['num_notes_records'] = iNumNotesRecords
        
        aggregateFlag = newMsgsFlag+msgsNeedActionFlag+msgsForReleaseFlag
        #
        if( activateNotesFlagging == "true" ):
            aggregateFlag += notesExistFlag
        #
        rtrnDict['notes_flag_active'] = activateNotesFlagging
        
        if( bBmrbNotes is True ):
            # only send this key/value pair back if there are actually BMRB notes present, otherwise UI has no need to know
            activateNotesFlaggingBmrb = self._getNotesFlaggingStatus("bmrb")        
            rtrnDict['notes_flag_active_bmrb'] = activateNotesFlaggingBmrb
            
            if( activateNotesFlaggingBmrb == "true" ):
                aggregateFlag += bmrbNotesExistFlag
                
        #
        bSuccess = self._updateWfNotifyStatus(depId,aggregateFlag)
        if( self.__verbose and bSuccess ):
            self.__lfh.write("+%s.%s() -- NOTIFY status updated to '%s' for DEPID %s \n" % (className, methodName,aggregateFlag,depId))
        
        rC.addDictionaryItems( rtrnDict )
        
        return rC    

    def _getNotesFlaggingStatus(self,subtype=""):
        """ Gets flagging status for notes content
            Relies on simple mechanism of having a file containing "true" or "false" value, 
            indicating whether or not visual flag (icon in user interface) for notes content should be activate 
            
            :param `subtype`:    optional parameter indicating subtype of Notes being handled for current request (e.g. "bmrb" notes)  
                
            :Returns: value of current flagging status
                 
        """
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- Starting.\n" % (className, methodName) )        
        #
        activateNotesFlagging="true"  # default
        depId = str(self.__reqObj.getValue("identifier"))
        #
        if (self.__verbose):
            self.__lfh.write("\n%s.%s() -- depId is: %s\n" % (className, methodName, depId) )
            self.__lfh.write("+%s.%s() -- notes subtype: %s\n" % (className, methodName, subtype) )
        #
        notesFlaggingStatusFilePathAbs = self.__getNotesFlaggingStatusFilePath(subtype=subtype)
        #
        if( os.access(notesFlaggingStatusFilePathAbs,os.F_OK) ):
            try:
                fp=open(notesFlaggingStatusFilePathAbs,'r')
                lines=fp.readlines()
                fp.close()
                activateNotesFlagging=lines[0][:-1]
            except:
                if (self.__verbose):
                    self.__lfh.write("\n%s.%s() -- problem reading notesFlaggingStatusFilePathAbs at:%s\n" % (self.__class__.__name__,
                                                               sys._getframe().f_code.co_name,
                                                               notesFlaggingStatusFilePathAbs) )
        else: 
            # file doesn't exist yet which is true if this is first time annotator is accessing messaging UI for the given depID
            # so we create the file and set default status of "ON"
            fp=open(notesFlaggingStatusFilePathAbs,'w')
            fp.write("%s\n" % "true")
            fp.close()        
        #
        return activateNotesFlagging
    
    def _toggleNotesFlaggingOp(self):
        """ Toggles flagging status for notest content
       
            :Returns:
                Operation output is packaged in a ResponseContent() object.
        """
        rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
        fstatus = self._toggleNotesFlagging()
        rC.setText(fstatus)
        return rC

    def _toggleNotesFlagging(self,subtype=""):
        """ Toggles flagging status for notes content
            Relies on simple mechanism of having a file containing "true" or "false" value, 
            indicating whether or not visual flag (icon in user interface) for notes content should be activate 
            
            :param `subtype`:    optional parameter indicating subtype of Notes being handled for current request (e.g. "bmrb" notes)  
                
            :Returns: value of new flagging status is echoed back to calling code
                 
        """
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        depId = str(self.__reqObj.getValue("identifier"))
        subtypeFromBrwser = str(self.__reqObj.getValue("subtype"))
        #
        if( subtypeFromBrwser and len(subtypeFromBrwser) > 1 ):
            subTypeRqstd = subtypeFromBrwser
        else:
            subTypeRqstd = subtype
            
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- Starting.\n" % (className, methodName) )
            self.__lfh.write("+%s.%s() -- depId: %s\n" % (className, methodName, depId) )
            self.__lfh.write("+%s.%s() -- notes subTypeRqstd: %s\n" % (className, methodName, subTypeRqstd) )
        #
        self.__getSession()
        #
        if( len(subTypeRqstd) > 1 ):
            activateNotesFlagging = str(self.__reqObj.getValue("activate_notes_flagging_"+subTypeRqstd))
        else:
            activateNotesFlagging = str(self.__reqObj.getValue("activate_notes_flagging"))
        #
        if (self.__verbose):
            self.__lfh.write("\n%s.%s() -- desired flag_status is: %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       activateNotesFlagging) )
        #
        notesFlaggingStatusFilePathAbs = self.__getNotesFlaggingStatusFilePath(subtype=subTypeRqstd)
        #
        if( os.access(notesFlaggingStatusFilePathAbs,os.F_OK) ):
            try:
                fp=open(notesFlaggingStatusFilePathAbs,'w')
                fp.write("%s\n" % activateNotesFlagging)
                fp.close()   
            except:
                if (self.__verbose):
                    self.__lfh.write("\n%s.%s() -- problem writing to notesFlaggingStatusFilePathAbs at:%s\n" % (self.__class__.__name__,
                                                               sys._getframe().f_code.co_name,
                                                               notesFlaggingStatusFilePathAbs) )
        else: 
            fp=open(notesFlaggingStatusFilePathAbs,'w')
            fp.write("%s\n" % activateNotesFlagging)
            fp.close()        
        #
        return activateNotesFlagging

    def __checkGroupDeposition(self):
        #
        # Added by ZF
        #
        """ Check if the depId is belonged to a group deposition
        """
        depId = str(self.__reqObj.getValue("identifier")).upper()
        if not depId:
            return
        elif depId.startswith('G_') and len(depId) == 9:
            self.__reqObj.setValue('groupid', depId)
            return
        #
        statusApi = StatusDbApi(siteId=self.__siteId, verbose=self.__verbose, log=self.__lfh)
        groupId = statusApi.getGroupId(depId)
        if groupId:
            self.__reqObj.setValue('groupid', groupId)
        #

    def __getNotesFlaggingStatusFilePath(self, subtype=""):
        """ Get  
            
            :Helpers:
                
            :Returns:
                 
        """
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        depId = str(self.__reqObj.getValue("identifier"))
        #
        # Added by ZF
        #
        groupId = str(self.__reqObj.getValue("groupid"))
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- Starting.\n" % (className, methodName) )        
            self.__lfh.write("+%s.%s() -- depId: %s\n" % (className, methodName, depId) )
            self.__lfh.write("+%s.%s() -- notes subtype: %s\n" % (className, methodName, subtype) )
        #
        suffix = "_" + subtype.upper() if len(subtype) > 1 else ""
        #
        pI=PathInfo(siteId=self.__siteId,sessionPath=self.__sessionPath,verbose=self.__verbose,log=self.__lfh)
        #
        # Added by ZF
        #
        if groupId:
            archivePth = pI.getArchivePath(groupId)
        else:
            archivePth = pI.getArchivePath(depId)
        #
        notesFlaggingStatusFilePathAbs=os.path.join(archivePth,"NOTES_FLAGGING_STATUS"+suffix)
        #
        if (self.__verbose):
            self.__lfh.write("\n%s.%s() -- returning notesFlaggingStatusFilePathAbs as:%s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       notesFlaggingStatusFilePathAbs) )
        # 
        return notesFlaggingStatusFilePathAbs
    
        
    def __checkAllMsgsRead(self):
        """ Get  
            
            :Helpers:
                
            :Returns:
                 
        """
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        bAllMsgsRead = False
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- Starting.\n" % (className, methodName) )        

        #
        msgingIo = MessagingIo(self.__reqObj,self.__verbose,self.__lfh)
        #
        bAllMsgsRead = msgingIo.areAllMsgsRead()
        
        return bAllMsgsRead
    
    def __checkAllMsgsActioned(self):
        """ Get  
            
            :Helpers:
                
            :Returns:
                 
        """
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        bAllMsgsActioned = False
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- Starting.\n" % (className, methodName) )        

        #
        msgingIo = MessagingIo(self.__reqObj,self.__verbose,self.__lfh)
        #
        bAllMsgsActioned = msgingIo.areAllMsgsActioned()
        
        return bAllMsgsActioned
    
    def __checkAnyReleaseFlags(self):
        """ Checks whether there are any messages flagged to indicate entry is ready for release.
            
            :Helpers:
                
            :Returns:
                 
        """
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        bForRelease = False
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- Starting.\n" % (className, methodName) )        

        #
        msgingIo = MessagingIo(self.__reqObj,self.__verbose,self.__lfh)
        #
        bForRelease = msgingIo.anyReleaseFlags()
        
        return bForRelease 
    
    def __checkAnyNotesExist(self):
        """ Get  
            
            :Helpers:
                
            :Returns:
                 
        """
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- Starting.\n" % (className, methodName) )        

        #
        msgingIo = MessagingIo(self.__reqObj,self.__verbose,self.__lfh)
        #
        return msgingIo.anyNotesExist()
    
    def _updateWfNotifyStatus(self,p_depId,p_status):
        #
        bSuccess = False
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- Starting.\n" % (className, methodName) ) 
        try:
            #wft=WfTracking(verbose=self.__verbose,log=self.__lfh)
            #bSuccess = wft.setDepMsgNotifyStatus(depId=p_depId,status=p_status)
            #
            # Removed by ZF
            #
            #wfApi= WfDbApi(verbose=True)
            #sql = "update deposition set notify = '"+p_status+"' where dep_set_id = '" + p_depId.upper() + "'"
            #wfApi.runUpdateSQL(sql);
            #
            # Added by ZF
            #
            statusApi = StatusDbApi(siteId=self.__siteId, verbose=self.__verbose, log=self.__lfh)
            groupId = str(self.__reqObj.getValue("groupid"))
            if groupId:
                sql = "update batch_user_data set notify = '"+p_status+"' where dep_set_id = '" + groupId.upper() + "'"
                statusApi.runUpdate( table = 'batch_user_data', where = { 'dep_set_id' : groupId.upper() }, data = { 'notify' : p_status } )
                entryList = statusApi.getEntryIdList(groupId=groupId)
                for entry in entryList:
                    statusApi.runUpdate( table = 'deposition', where = { 'dep_set_id' : entry }, data = { 'notify' : p_status } )
                #
            else:
                statusApi.runUpdate( table = 'deposition', where = { 'dep_set_id' : p_depId.upper() }, data = { 'notify' : p_status } )
            #
            bSuccess = True   
        except:
            bSuccess = False
            if (self.__verbose):
                self.__lfh.write("+%s.%s() - TRACKING status, update to '%s' failed for depID %s \n" % (className, methodName,p_status,p_depId))
            traceback.print_exc(file=self.__lfh)                    
        #
        return bSuccess
    
    def _getFilesRfrncd(self):
        """ Retrieve list of files referenced by any messages for this dataset ID  
            
            :Helpers:
                
            :Returns:
                 
        """
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        rtrnDict = {}
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- Starting.\n" % (className, methodName) )        

        self.__getSession()
        #
        depId = self.__reqObj.getValue("identifier")
        #
        if (self.__verbose):
            self.__lfh.write("\n%s.%s() -- dep_id is:%s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       depId) )
        #
        self.__reqObj.setReturnFormat(return_format="json")
        rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
        #
        msgingIo = MessagingIo(self.__reqObj,self.__verbose,self.__lfh)
        #
        rtrnDict['files_rfrncd'] = msgingIo.getFilesRfrncd( depId )
        
        rC.addDictionaryItems( rtrnDict )
        
        return rC

    def _checkAvailFiles(self):
        """ Get  
            
            :Helpers:
                
            :Returns:
                 
        """
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        rtrnDict = {}
        fileLst = []
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- Starting.\n" % (className, methodName) )        

        self.__getSession()
        #
        depId = self.__reqObj.getValue("identifier")
        #
        if (self.__verbose):
            self.__lfh.write("\n%s.%s() -- dep_id is:%s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       depId) )
        #
        self.__reqObj.setReturnFormat(return_format="json")
        rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
        #
        msgingIo = MessagingIo(self.__reqObj,self.__verbose,self.__lfh)
        fileLst = msgingIo.checkAvailFiles( depId )
        #
        rtrnDict['file_list'] = fileLst
        
        rC.addDictionaryItems( rtrnDict )
        
        return rC
    
    def _getMsg(self):
        """ Get data for a single message 
            
            :Helpers:
                wwpdb.apps.msgmodule.io.MessagingIo
                
            :Returns:
                JSON response representing a given message and its attributes
                 
        """
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        rtrnDict = {}
        msgDict = {}
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- Starting.\n" % (className, methodName) )        

        self.__getSession()
        #
        depId = self.__reqObj.getValue("identifier")
        msgId = self.__reqObj.getValue("msg_id")
        #
        if (self.__verbose):
            self.__lfh.write("\n%s.%s() -- dep_id is: %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       depId) )
        #
        self.__reqObj.setReturnFormat(return_format="json")
        rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
        #
        msgingIo = MessagingIo(self.__reqObj,self.__verbose,self.__lfh)
        msgDict = msgingIo.getMsg( msgId, depId )
        #
             
        # stuff config dict into return dictionary for return to web page
        rtrnDict['msg_dict'] = msgDict
        
        rC.addDictionaryItems( rtrnDict )
        
        return rC
    
    def _displayMsg(self):
        """ Get  
            
            :Helpers:
                
            :Returns:
                 
        """
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        bIsWorkflow = self.__isWorkflow()
        #
        rtrnDict = {}
        msgDict = {}
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- Starting.\n" % (className, methodName) )        

        self.__getSession()
        #
        depId = self.__reqObj.getValue("identifier")
        msgId = self.__reqObj.getValue("msg_id")
        #
        if (self.__verbose):
            self.__lfh.write("\n%s.%s() -- dep_id is:%s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       depId) )
        #
        rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
        #
        msgingIo = MessagingIo(self.__reqObj,self.__verbose,self.__lfh)
        msgDict = msgingIo.getMsg( msgId, depId )
        self.__lfh.write("+%s.%s() msgDict is:  %r\n" % ( className, methodName, msgDict ) )
        #
        if( msgDict['parent_message_id'] != msgDict['message_id'] ):
            parentMsgDict = msgingIo.getMsg( msgDict['parent_message_id'], depId )
            msgDict['parent_msg_sndr'] = parentMsgDict['sender']
            msgDict['parent_msg_datetime'] = parentMsgDict['timestamp']
            msgDict['parent_msg_text'] = parentMsgDict['message_text']
            msgDict['parent_msg_dsply'] = ""
        else:
            msgDict['parent_msg_sndr'] = ""
            msgDict['parent_msg_datetime'] = ""
            msgDict['parent_msg_text'] = ""
            msgDict['parent_msg_dsply'] = ""
            msgDict['parent_msg_dsply'] = "displaynone"
        
        msgDict['files_referenced'] = (msgingIo.getFilesRfrncd( depId, msgId )).get(msgId,[])
        msgngDpct=MessagingDepict(self.__verbose,self.__lfh)
        msgngDpct.setSessionPaths(self.__reqObj)
        oL = msgngDpct.doRenderDisplayMsg(self.__reqObj,bIsWorkflow,msgDict)
        rC.setHtmlText( '\n'.join(oL) )
        #
        return rC
    
    def _archiveMsg(self):
        return self._propagateMsg("archive")

    def _forwardMsg(self):
        return self._propagateMsg("forward")

    def _propagateMsg(self,actionType):
        """             
            :Helpers:
                
            :Returns:
            
        """
        #
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        #
        self.__getSession()
        #depId = self.__reqObj.getValue("entry_id") # getValue("identifier")
        mode = self.__reqObj.getValue("mode")
        if( mode and mode == "manual" ):
            depIdCsvLst = self.__reqObj.getValue("target_identifier")
            origDepId = self.__reqObj.getValue("identifier")
            msgType = actionType+"_manual"
        else:
            depIdCsvLst = self.__reqObj.getValue("identifier")
            origDepId = None
            msgType = actionType+"_auto"
            
        flagMsg = self.__reqObj.getValue("flag")
        if( flagMsg and flagMsg == "y"):
            msgType = msgType + "_flag"
        #
        subject = self.__reqObj.getRawValue("subject")
        subject = subject if subject is not None and len(subject) > 1 else "test subject"
        #
        if( actionType == "archive" ):
            subjPrefix = "ARCHIVED: "
        else:
            #subjPrefix = "FWD: " if msgCategory != "reminder" else "" 
            subjPrefix = "FWD: "
        #
        ''' setting below request parameters for downstream processing'''
        self.__reqObj.setValue("subject", subjPrefix + subject) # setting here for downstream processing
        self.__reqObj.setValue("send_status", 'Y') # setting here for downstream processing
        self.__reqObj.setValue("filesource", "archive") # setting here for downstream processing
        self.__reqObj.setValue("message_type", msgType) # setting here for downstream processing
        self.__reqObj.setValue("orig_identifier", origDepId) # only used when archive action is via "manual" mode
        contentType = "notes" if( actionType == "archive" ) else "msgs"
        self.__reqObj.setValue("content_type", contentType) # setting here for downstream processing (as opposed to "msgs")
        self.__reqObj.setValue("message_state", "livemsg") # setting here for downstream processing
        #
        rtrnDict ={}
        rtrnDict['success'] = {}
        rtrnDict['success']['job'] = 'error'
        #
        self.__reqObj.setReturnFormat(return_format="json")
        rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
        #
        msgingIo = MessagingIo(self.__reqObj,self.__verbose,self.__lfh)
        #
        depIdLst = depIdCsvLst.upper().split(",")
        #
        # Added by ZF
        #
        statusApi = StatusDbApi(siteId=self.__siteId, verbose=self.__verbose, log=self.__lfh)
        for depId in depIdLst:
            self.__reqObj.setValue("identifier", depId) # setting here for downstream processing
            #
            # Added by ZF
            #
            groupId = ''
            if depId.startswith('G_'):
                groupId = depId
            else:
                found_groupId = statusApi.getGroupId(depId)
                if found_groupId:
                    groupId = found_groupId
                #
            #
            self.__reqObj.setValue('groupid', groupId)
            msgingIo.setGroupId(groupId)
            #
            msgObj = Message.fromReqObj(self.__reqObj,self.__verbose,self.__lfh)
            #
            if (self.__verbose):
                self.__lfh.write("+%s.%s() -- dep_id is: %s\n" % (className, methodName, depId) )
            #            
            bOk, bPdbxMdlFlUpdtd, failedFileRefs = msgingIo.processMsg(msgObj)
            #
            rtrnDict['success'][depId] = "true" if bOk is True else "false"            
        #
        bGlobalStatusCheckReqd = False
        #
        '''typically only notes actually authored by annotators trigger the Notes indicator in the WFM UI, but annotators have 
        requested that notes generated via emails archived/flagged from BMRB should also trigger the indicator'''
        if(  (bOk is True) and (contentType == "notes") and (self._getNotesFlaggingStatus() == 'false') and (flagMsg == "y") ):
            self.__reqObj.setValue("activate_notes_flagging",'true')
            self._toggleNotesFlagging()
            bGlobalStatusCheckReqd = True
        #
        if(  (bOk is True) and (contentType == "notes") and (self._getNotesFlaggingStatus("bmrb") == 'false') and (flagMsg == "y") ):
            self.__reqObj.setValue("activate_notes_flagging_bmrb",'true')
            self._toggleNotesFlagging("bmrb")
            bGlobalStatusCheckReqd = True
        #
        if bGlobalStatusCheckReqd:
            self._checkGlobalMsgStatus()
            
        rtrnDict['success']['job'] = 'ok'
        
        rC.addDictionaryItems( rtrnDict )
        #
        return rC
        
    def _submitMsg(self):
        return self._processMsg('livemsg')

    def _updateDraftState(self):
        return self._processMsg('draft')
    
    def _processMsg(self,p_msgState):
        """             
            :Helpers:
                
            :Returns:
            
        """
        #
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        #
        self.__getSession()
        #
        rtrnDict ={}
        #
        self.__reqObj.setReturnFormat(return_format="json")
        self.__reqObj.setValue("message_state", p_msgState) # setting here for downstream processing, used only for processing purposes
                                                            # NOTE: this field is not part of the PdbxMessage data structure, and thus is not persisted to data file.
        rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
        #
        msgObj = Message.fromReqObj(self.__reqObj,self.__verbose,self.__lfh)
        #
        msgingIo = MessagingIo(self.__reqObj,self.__verbose,self.__lfh)
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() checking for attached files.\n"%(className, methodName) )
            
        for cnt in range(1,4):
            auxFl = "aux-file"+str(cnt)
            if( self.__isFileUpload(auxFl) ):
                if (self.__verbose):
                    self.__lfh.write("+%s.%s() found attached file.\n"%(className, methodName ) )
                if( self.__uploadFile(auxFl) ):
                    msgObj.fileReferences.append(auxFl)
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- msgObj.depositionId is: %s\n" % (className, methodName, msgObj.depositionId) )
            self.__lfh.write("+%s.%s() -- msgObj.messageText is: %r\n" % (className, methodName, msgObj.messageText) )
            self.__lfh.write("+%s.%s() -- msgObj.getFileReferences() is: %r\n" % (className, methodName, msgObj.getFileReferences() ) )
        #
            
        bOk, bPdbxMdlFlUpdtd, failedFileRefs = msgingIo.processMsg( msgObj )
        #
        if bOk:
            rtrnDict['success'] = "true"
            if(  (msgObj.contentType == "notes") and (self._getNotesFlaggingStatus() == 'false') ):
                self.__reqObj.setValue("activate_notes_flagging",'true')
                self._toggleNotesFlagging()
        else:
            rtrnDict['success'] = "false"            
        #
        rtrnDict['pdbx_model_updated'] = "true" if bPdbxMdlFlUpdtd else "false"
        rtrnDict['append_msg'] = ""
        if( ( not bOk ) and ( len(failedFileRefs) > 0 ) ):
            sMsg = "Failure to associate message with the following file types: "+( ", ".join(failedFileRefs) )
            if( 'aux-file' in failedFileRefs ):
                sMsg += "\nIf you are attaching an auxiliary file, please ensure that the file name ends with a known file extension/type."
            rtrnDict['append_msg'] = sMsg
        
        if bPdbxMdlFlUpdtd:
            wfApi = WfDbApi(verbose = True)
            pw = getdepUIPassword(wfApi,msgObj.depositionId) 
            rtrnDict['depid_pw'] = pw
                        
        rC.addDictionaryItems( rtrnDict )
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- msgObj.messageId is:%s\n" % (className, methodName, msgObj.messageId) )
            self.__lfh.write("+%s.%s() -- msgObj.depositionId is:%s\n" % (className, methodName, msgObj.depositionId) )
            self.__lfh.write("+%s.%s() -- msgObj.getFileReferences() is: %r\n" % (className, methodName, msgObj.getFileReferences() ) )
            self.__lfh.write("+%s.%s() -- pdbx_model_updated is: %s\n" % (className, methodName, rtrnDict['pdbx_model_updated']) )
        #
        return rC
    
    def _getDepUiPwd(self):
        """             
            :Helpers:
                
            :Returns:
            
        """
        #
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        #
        self.__getSession()
        depId = self.__reqObj.getValue("identifier")
        #
        rtrnDict ={}
        #
        self.__reqObj.setReturnFormat(return_format="json")
        rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
        #
        wfApi = WfDbApi(verbose = True)
        pw = getdepUIPassword(wfApi,depId) 
        rtrnDict['depui_pwd'] = pw
                        
        rC.addDictionaryItems( rtrnDict )
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- dep_id is:%s\n" % (className, methodName, depId) )
        #
        return rC
    
    def __encodeUtf8ToCif(self,p_content):
        ''' Encoding unicode/utf-8 content into cif friendly ascii
            Have to replace any ';' that begin a newline with a ' ;' in order to preserve ; matching required for multiline items
        '''
        return p_content.encode('ascii','xmlcharrefreplace').replace('\n;','\n ;').replace('\\\\xa0',' ')
    
    def __decodeCifToUtf8(self,p_content):
        h = HTMLParser()
        return h.unescape(p_content).encode('utf-8')
    
    def __encodeForSearching(self,p_content):
        ##################################################################
        # we need to accommodate any search filtering taking place
        # at this point we have a resultset of data that was encoded in ascii
        # on its way into cif datafile and then decoded back to UTF8 for 
        # return/display in browser.
        # so we must mimic the same process for any search terms before we test
        # such terms for potential matches with the data in the resultset
        ##################################################################
        sSrchAsciiXfrm = p_content.encode('ascii','xmlcharrefreplace')
        if( sSrchAsciiXfrm and len(sSrchAsciiXfrm) > 0 ):
            if( self.__debug ):
                self.__lfh.write("+%s.%s() sSrchAsciiXfrm is: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, sSrchAsciiXfrm ) )
                
            sSrchUtf8 = self.__decodeCifToUtf8(sSrchAsciiXfrm)
            if( self.__debug ):
                self.__lfh.write("+%s.%s() sSrchUtf8 is: %r\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, sSrchUtf8 ) )
                
            return sSrchUtf8
        else:
            return None

    def _markMsgAsRead(self):
        """             
            :Helpers:
                
            :Returns:
            
        """       
        #
        self.__getSession()
        depId = self.__reqObj.getValue("identifier")
        msgId = self.__reqObj.getValue("msg_id")
        actionReqd = self.__reqObj.getValue("action_reqd")
        forReleaseFlg = self.__reqObj.getValue("for_release")
        
        #
        rtrnDict ={}
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- dep_id is:%s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, depId) )
        #
        self.__reqObj.setReturnFormat(return_format="json")
        rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
        #
        actionReqd = 'Y' if( actionReqd is None or len(actionReqd) < 1 ) else actionReqd
        forReleaseFlg = 'N' if( forReleaseFlg is None or len(forReleaseFlg) < 1 ) else forReleaseFlg
        #
        msgingIo = MessagingIo(self.__reqObj,self.__verbose,self.__lfh)
        msgStatusDict={
            "message_id"                :   msgId,
            "deposition_data_set_id"    :   depId,
            "read_status"   :   "Y",
            "action_reqd"   :   actionReqd,
            "for_release" :   forReleaseFlg
        }
        bOk = msgingIo.markMsgAsRead(msgStatusDict)
        #
        if bOk:
            rtrnDict['success'] = "true"
        else:
            rtrnDict['success'] = "false"
            
        rC.addDictionaryItems( rtrnDict )
        
        return rC
        
        
    def _tagMsg(self):
        """             
            :Helpers:
                
            :Returns:
            
        """       
        #
        self.__getSession()
        depId = self.__reqObj.getValue("identifier")
        msgId = self.__reqObj.getValue("msg_id")
        actionReqdFlg = self.__reqObj.getValue("action_reqd")
        readStatusFlg = self.__reqObj.getValue("read_status")
        forReleaseFlg = self.__reqObj.getValue("for_release")
        #
        rtrnDict ={}
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- dep_id is:%s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, depId) )
            self.__lfh.write("+%s.%s() -- msgId is:%s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, msgId) )
            self.__lfh.write("+%s.%s() -- actionReqdFlg is:%s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, actionReqdFlg) )
            self.__lfh.write("+%s.%s() -- readStatusFlg is:%s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, readStatusFlg) )
            self.__lfh.write("+%s.%s() -- forReleaseFlg is:%s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, forReleaseFlg) )
        #
        self.__reqObj.setReturnFormat(return_format="json")
        rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
        #
        msgingIo = MessagingIo(self.__reqObj,self.__verbose,self.__lfh)
        msgStatusDict={
            "message_id"                :   msgId,
            "deposition_data_set_id"    :   depId,
            "read_status"   :   readStatusFlg,
            "action_reqd"   :   actionReqdFlg,
            "for_release" :   forReleaseFlg
        }
        bOk = msgingIo.tagMsg(msgStatusDict)
        #
        if bOk:
            rtrnDict['success'] = "true"
        else:
            rtrnDict['success'] = "false"
            
        rC.addDictionaryItems( rtrnDict )
        
        return rC        
        
        
    def _deleteRowOp(self):
        #
        rtrnDict = {}
        #
        if( self.__debug ):
            self.__lfh.write("++++++++++++STARTING %s %s at %s\n" % (self.__class__.__name__,
                                   sys._getframe().f_code.co_name,
                                   time.strftime("%Y %m %d %H:%M:%S", time.localtime())))       
        #
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        self.__getSession()
        depId = self.__reqObj.getValue("identifier")
        rowIdx = self.__reqObj.getValue("row_idx")
        #
        rowIdx = int(rowIdx.replace("row_", ""))
        #
        if (self.__verbose):
            self.__lfh.write("+%s.%s() -- dep_id is:%s\n" % (className, methodName, depId) )
            #self.__lfh.write("+%s.%s() -- new_value[] is:%s\n" % (className, methodName, newMultiValue ) )
        #
        self.__reqObj.setReturnFormat('json')
        rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)
        #
        msgingIo = MessagingIo(self.__reqObj,self.__verbose,self.__lfh)
        if( self.__debug ):
            self.__lfh.write("++++++++++++%s %s just before call to msgingIo.deleteRow at %s\n" % (self.__class__.__name__,
                                   sys._getframe().f_code.co_name,
                                   time.strftime("%Y %m %d %H:%M:%S", time.localtime())))       
        #        
        ok = msgingIo.deleteRow( depId,rowIdx )
        #
        if( self.__debug ):
            self.__lfh.write("++++++++++++%s %s just after call to msgingIo.deleteRow at %s\n" % (self.__class__.__name__,
                                   sys._getframe().f_code.co_name,
                                   time.strftime("%Y %m %d %H:%M:%S", time.localtime())))       
        #
        if( ok ):
            rtrnDict['status'] = "OK"
        else:
            rtrnDict['status'] = "ERROR"
        #
        rC.addDictionaryItems( rtrnDict )
        #
        if( self.__debug ):
            self.__lfh.write("++++++++++++COMPLETING %s %s at %s\n" % (self.__class__.__name__,
                                   sys._getframe().f_code.co_name,
                                   time.strftime("%Y %m %d %H:%M:%S", time.localtime())))       
        #        
        return rC    

    def _exit_finished(self):
        """ Exiting General Annotation Messaging Module when annotator has completed all necessary processing
        
            CURRENTLY NOT USED
        
        """
        return self.__exitMessagingMod(mode='completed')
    
    def _exit_notFinished(self):
        """ Exiting General Annotation Messaging Module when annotator has NOT completed all necessary processing
            and user intends to resume use of lig module at another point to continue updating data.
            
            CURRENTLY NOT USED
            
        """
        return self.__exitMessagingMod(mode='unfinished')
                  
    ################################################################################################################
    # ------------------------------------------------------------------------------------------------------------
    #      Private helper methods
    # ------------------------------------------------------------------------------------------------------------
    #
    def __uploadFile(self,fileTag='file'):
        #
        #
        className = self.__class__.__name__
        methodName = sys._getframe().f_code.co_name
        #
        if (self.__verbose):
            self.__lfh.write("++%s.%s() - file upload starting at %s \n"%(className, methodName, time.strftime("%Y %m %d %H:%M:%S", time.localtime())) )
                             
        rawFile=None
        fileName=""
        #
        # Copy upload file to session directory - 
        try:
            rawFile=self.__reqObj.getRawValue(fileTag)
            fileName = str(rawFile.filename)
            #
            # Need to deal with some platform issues -
            #
            if (fileName.find('\\') != -1) :
                # likely windows path -
                fName=ntpath.basename(fileName)
            else:
                fName=os.path.basename(fileName)
                
            #
            if (self.__verbose):
                self.__lfh.write("++%s.%s() - uploaded file %s\n" % (className, methodName, rawFile.filename) )
                self.__lfh.write("++%s.%s() - base file   %s\n" % (className, methodName, fName) )        
            #
            # Store upload file in session directory - 

            fPathAbs=os.path.join(self.__sessionPath,fName)
            ofh=open(fPathAbs,'w')
            ofh.write(rawFile.file.read())
            ofh.close()
            
            for cnt in range(1,4):
                auxFl = "aux-file"+str(cnt)
                
                if( fileTag == auxFl ):
                    fileType = os.path.splitext(fileName)[1].strip(".") if len( os.path.splitext(fileName)[1] ) > 1 else "n/a"
                    if( fileType.startswith("V") and fileType.split("V")[1].isdigit() ):
                        fileType = (fileName.rsplit(".V")[0]).rsplit(".")[1]
                        if( fileType == "cif" ):
                            # if file extension is "cif" we handle formally as format type "pdbx"
                            fileType = "pdbx" 
                    
                    self.__reqObj.setValue("auxFileName"+str(cnt),fName)
                    self.__reqObj.setValue("auxFilePath"+str(cnt),fPathAbs)
                    self.__reqObj.setValue("auxFileType"+str(cnt),fileType)
                    
                    if (self.__verbose):
                        self.__lfh.write("+%s.%s() auxFileName%s: %s\n" %(className, methodName, cnt, fileName) )
                        self.__lfh.write("+%s.%s() auxFilePath%s: %s\n" %(className, methodName, cnt, fPathAbs) )
                        self.__lfh.write("+%s.%s() auxFileType%s: %s\n" %(className, methodName, cnt, fileType) )
                
        except:
            if (self.__verbose):
                self.__lfh.write("++%s.%s() File upload processing failed for %s\n" % (className, methodName, str(rawFile.filename) ) )        
                traceback.print_exc(file=self.__lfh)                            

            return False
       
        return True
    
    
    def __exitMessagingMod(self,mode):
        """ CURRENTLY NOT USED
        
            Function to accommodate user request to exit messaging module,
            close interface, and return to workflow manager interface.
            Supports different 'modes' = ('completed' | 'unfinished')
            
            :Params:
                ``mode``:
                    'completed' if annotator has completed all edits to mmCif data and wishes to
                        conclude work in the General Annotation mmCif Messaging.
                    'unfinished' if annotator wishes to leave General Annotation mmCif Messaging but resume work at a later point.
                    
            :Returns:
                ResponseContent object.
        """
        if (self.__verbose):
            self.__lfh.write("--------------------------------------------\n")                    
            self.__lfh.write("+MessagingWebAppWorker.__exitMessagingMod() - starting\n")
        #
        if (mode == 'completed'):
            state = "closed(0)"
        elif (mode == 'unfinished'):
            state = "waiting"
        #
        bIsWorkflow = self.__isWorkflow()
        #
        self.__getSession()
        sessionId   = self.__sessionId
        depId  =  self.__reqObj.getValue("identifier")
        instId =  self.__reqObj.getValue("instance")
        classId = self.__reqObj.getValue("classID")
        fileSource = str(self.__reqObj.getValue("filesource")).strip().lower()
        #
        if (self.__verbose):
            self.__lfh.write("--------------------------------------------\n")                    
            self.__lfh.write("+MessagingWebAppWorker.__exitMessagingMod() - depId   %s \n" % depId)
            self.__lfh.write("+MessagingWebAppWorker.__exitMessagingMod() - instId  %s \n" % instId)
            self.__lfh.write("+MessagingWebAppWorker.__exitMessagingMod() - classID %s \n" % classId)
            self.__lfh.write("+MessagingWebAppWorker.__exitMessagingMod() - sessionID %s \n" % sessionId)
            self.__lfh.write("+MessagingWebAppWorker.__exitMessagingMod() - filesource %r \n" % fileSource)

        #
        self.__reqObj.setReturnFormat('json')
        #
        rC=ResponseContent(reqObj=self.__reqObj, verbose=self.__verbose,log=self.__lfh)        
        #
        # Update WF status database and persist chem comp assignment states -- ONLY if lig module was running in context of wf-engine
        #
        if( bIsWorkflow ):
            try:
                bOkay = self.__saveMessagingModState()
                if( bOkay ):
                    bSuccess = self.__updateWfTrackingDb(state)
                    if( not bSuccess ):
                        rC.setError(errMsg="+MessagingWebAppWorker.__exitMessagingMod() - TRACKING status, update to '%s' failed for session %s \n" % (state,sessionId) )
                else:
                    rC.setError(errMsg="+MessagingWebAppWorker.__exitMessagingMod() - problem saving cif file")
                
            except:
                if (self.__verbose):
                    self.__lfh.write("+MessagingWebAppWorker.__exitMessagingMod() - problem saving cif file")
                traceback.print_exc(file=self.__lfh)
                rC.setError(errMsg="+MessagingWebAppWorker.__exitMessagingMod() - exception thrown on saving cif file")                  
       
        else:
            try:
                bOkay = self.__saveMessagingModState()
                if( bOkay ):
                    if( self.__verbose ):
                        self.__lfh.write("+%s.%s successfully saved cif file to session directory %s at %s\n" % (self.__class__.__name__,
                                   sys._getframe().f_code.co_name, self.__sessionPath,
                                   time.strftime("%Y %m %d %H:%M:%S", time.localtime()))) 
                else:
                    if( self.__verbose ):
                        self.__lfh.write("+%s.%s failed to save cif file to session directory %s at %s\n" % (self.__class__.__name__,
                                   sys._getframe().f_code.co_name, self.__sessionPath,
                                   time.strftime("%Y %m %d %H:%M:%S", time.localtime())))                     
                    rC.setError(errMsg="+MessagingWebAppWorker.__exitMessagingMod() - problem saving cif file")
                
            except:
                if( self.__verbose ):
                    self.__lfh.write("+%s.%s failed to save cif file to session directory %s at %s\n" % (self.__class__.__name__,
                                   sys._getframe().f_code.co_name, self.__sessionPath,
                                   time.strftime("%Y %m %d %H:%M:%S", time.localtime())))  
                traceback.print_exc(file=self.__lfh)
                rC.setError(errMsg="+MessagingWebAppWorker.__exitMessagingMod() - exception thrown on saving cif file")                  
            if (self.__verbose):
                    self.__lfh.write("+MessagingWebAppWorker.__exitMessagingMod() - Not in WF environ so skipping status update to TRACKING database for session %s \n" % sessionId)
        #
        return rC

    def __updateWfTrackingDb(self,p_status):
        """ Private function used to udpate the Workflow Status Tracking Database
        
            :Params:
                ``p_status``: the new status value to which the deposition data set is being set
                
            :Helpers:
                wwpdb.apps.msgmodule.utils.WfTracking.WfTracking
                    
            :Returns:
                ``bSuccess``: boolean indicating success/failure of the database update
        """        
        #
        bSuccess = False
        #
        sessionId   = self.__sessionId
        depId  =  self.__reqObj.getValue("identifier").upper()
        instId =  self.__reqObj.getValue("instance")
        classId=  str(self.__reqObj.getValue("classID")).lower()
        #
        try:
            wft=WfTracking(verbose=self.__verbose,log=self.__lfh)
            wft.setInstanceStatus(depId=depId,
                                  instId=instId,
                                  classId=classId,
                                  status=p_status)
            bSuccess = True
            if (self.__verbose):
                self.__lfh.write("+MessagingWebAppWorker.__updateWfTrackingDb() -TRACKING status updated to '%s' for session %s \n" % (p_status,sessionId))
        except:
            bSuccess = False
            if (self.__verbose):
                self.__lfh.write("+MessagingWebAppWorker.__updateWfTrackingDb() - TRACKING status, update to '%s' failed for session %s \n" % (p_status,sessionId))
            traceback.print_exc(file=self.__lfh)                    
        #
        return bSuccess

    def __saveMessagingModState(self):
        """ PLACEHOLDER
        """
        '''
        exprtDirPath=None
        exprtFilePath=None
        fileSource = str(self.__reqObj.getValue("filesource")).strip().lower()
        depId  =  self.__reqObj.getValue("identifier")
        instId =  self.__reqObj.getValue("instance")
        #classId = self.__reqObj.getValue("classid")
        sessionId=self.__sessionId
        #
        if (self.__verbose):
            self.__lfh.write("--------------------------------------------\n")                    
            self.__lfh.write("+MessagingWebAppWorker.__saveMessagingModState() - dataFile   %s \n" % dataFile)
        #
        if fileSource in ['archive','wf-archive','wf_archive']:
            self.__lfh.write("+MessagingWebAppWorker.__saveMessagingModState() - processing archive | filesource %r \n" % fileSource)            
            dfRef=DataFileReference()
            dfRef.setDepositionDataSetId(depId)
            dfRef.setStorageType('archive')
            dfRef.setContentTypeAndFormat('model','pdbx')
            dfRef.setVersionId('latest')
              
            if (dfRef.isReferenceValid()):                  
                exprtDirPath=dfRef.getDirPathReference()
                exprtFilePath=dfRef.getFilePathReference()
                sP=dfRef.getSitePrefix()
                if (self.__verbose):
                    self.__lfh.write("+MessagingWebAppWorker.__saveMessagingModState() site prefix             : %s\n" % sP)
                    self.__lfh.write("+MessagingWebAppWorker.__saveMessagingModState() CC assign details export directory path: %s\n" % exprtDirPath)
                    self.__lfh.write("+MessagingWebAppWorker.__saveMessagingModState() CC assign details export file      path: %s\n" % exprtFilePath)
            else:
                self.__lfh.write("+MessagingWebAppWorker.__saveMessagingModState() Bad archival reference for id %s \n" % depId)

        elif (fileSource in ['wf-instance','wf_instance']):
            self.__lfh.write("+MessagingWebAppWorker.__saveMessagingModState() - processing instance | filesource %r \n" % fileSource)                 
            dfRef=DataFileReference()
            dfRef.setDepositionDataSetId(depId)
            dfRef.setWorkflowInstanceId(instId)            
            dfRef.setStorageType('wf-instance')
            dfRef.setContentTypeAndFormat('model','pdbx')
            dfRef.setVersionId('latest')
            
            if (dfRef.isReferenceValid()):
                exprtDirPath=dfRef.getDirPathReference()
                exprtFilePath=dfRef.getFilePathReference()
                sP=dfRef.getSitePrefix()                
                if (self.__verbose):
                    self.__lfh.write("+MessagingWebAppWorker.__saveMessagingModState() site prefix             : %s\n" % sP)                    
                    self.__lfh.write("+MessagingWebAppWorker.__saveMessagingModState() CC assign details export directory path: %s\n" % exprtDirPath)
                    self.__lfh.write("+MessagingWebAppWorker.__saveMessagingModState() CC assign details export           path: %s\n" % exprtFilePath)
            else:
                self.__lfh.write("+MessagingWebAppWorker.__saveMessagingModState() Bad wf-instance reference for id %s wf id %s\n" % (depId,instId))                
        elif (fileSource in ['rcsb_dev']):
            self.__lfh.write("+MessagingWebAppWorker.__saveMessagingModState() - processing for 'rcsb_dev' filesource.\n")                             
            exprtDirPath=self.__sessionPath
            exprtFilePath=os.path.join(self.__sessionPath,"rcsb_dev_testCifFileSave.cif")
        else:
            self.__lfh.write("+MessagingWebAppWorker.__saveMessagingModState() - processing undefined | filesource %r \n" % fileSource)                             
            exprtDirPath=self.__sessionPath
            exprtFilePath=os.path.join(self.__sessionPath,dataFile)

        # export updated mmCif file here
        # bOk = callExportFile Function 
        msgingIo = MessagingIo(self.__reqObj,self.__verbose,self.__lfh)
        bOk = msgingIo.doExport(exprtDirPath,exprtFilePath)
        '''
        return bOk

    def __isFileUpload(self,fileTag='file'):
        """ Generic check for the existence of request paramenter "file".
        """ 
        try:
            stringtypes = (unicode, str)
        except NameError:
            stringtypes = (str, bytes)

        # Gracefully exit if no file is provide in the request object - 
        fs=self.__reqObj.getRawValue(fileTag)
        if self.__verbose and self.__debug:
            self.__lfh.write("+MessagingWebApp.__isFileUpload() - type of 'fs' is %s\n"% type(fs))
        
        if (fs is None) or isinstance(fs, stringtypes):
            return False
        if self.__verbose and self.__debug:
            self.__lfh.write("+MessagingWebApp.__isFileUpload() - file upload is found to be True\n")
        return True

    def __getSession(self):
        """ Join existing session or create new session as required.
        """
        #
        self.__sObj=self.__reqObj.newSessionObj()
        self.__sessionId=self.__sObj.getId()
        self.__sessionPath=self.__sObj.getPath()
        self.__rltvSessionPath=self.__sObj.getRelativePath()
        if (self.__verbose):
            self.__lfh.write("------------------------------------------------------\n")                    
            self.__lfh.write("+MessagingWebApp.__getSession() - creating/joining session %s\n" % self.__sessionId)
            #self.__lfh.write("+MessagingWebApp.__getSession() - workflow storage path    %s\n" % self.__workflowStoragePath)
            self.__lfh.write("+MessagingWebApp.__getSession() - session path %s\n" % self.__sessionPath)

    def __setSemaphore(self):
        sVal = str(time.strftime("TMP_%Y%m%d%H%M%S", time.localtime()))
        self.__reqObj.setValue('semaphore',sVal)
        return sVal

    def __openSemaphoreLog(self,semaphore="TMP_"):
        sessionId  =self.__reqObj.getSessionId()        
        fPathAbs=os.path.join(self.__sessionPath,semaphore+'.log')
        self.__lfh=open(fPathAbs,'w')

    def __closeSemaphoreLog(self,semaphore="TMP_"):
        self.__lfh.flush()
        self.__lfh.close()
        
    def __postSemaphore(self,semaphore='TMP_',value="OK"):
        sessionId  =self.__reqObj.getSessionId()        
        fPathAbs=os.path.join(self.__sessionPath,semaphore)
        fp=open(fPathAbs,'w')
        fp.write("%s\n" % value)
        fp.close()        
        return semaphore

    def __semaphoreExists(self,semaphore='TMP_'):
        sessionId  =self.__reqObj.getSessionId()        
        fPathAbs=os.path.join(self.__sessionPath,semaphore)
        if (os.access(fPathAbs,os.F_OK)):
            return True
        else:
            return False

    def __getSemaphore(self,semaphore='TMP_'):

        sessionId=self.__reqObj.getSessionId()        
        fPathAbs=os.path.join(self.__sessionPath,semaphore)
        if (self.__verbose):
            self.__lfh.write("+MessagingWebApp.__getSemaphore() - checking %s in path %s\n" % (semaphore,fPathAbs))
        try:
            fp=open(fPathAbs,'r')
            lines=fp.readlines()
            fp.close()
            sval=lines[0][:-1]
        except:
            sval="FAIL"
        return sval

    def __openChildProcessLog(self,label="TMP_"):
        sessionId  =self.__reqObj.getSessionId()        
        fPathAbs=os.path.join(self.__sessionPath,label+'.log')
        self.__lfh=open(fPathAbs,'w')
        
    def __processTemplate(self,fn,parameterDict={}):
        """ Read the input HTML template data file and perform the key/value substitutions in the
            input parameter dictionary.
            
            :Params:
                ``parameterDict``: dictionary where
                key = name of subsitution placeholder in the template and
                value = data to be used to substitute information for the placeholder
                
            :Returns:
                string representing entirety of content with subsitution placeholders now replaced with data
        """
        tPath =self.__reqObj.getValue("TemplatePath")
        fPath=os.path.join(tPath,fn)
        ifh=open(fPath,'r')
        sIn=ifh.read()
        ifh.close()
        return (  sIn % parameterDict )
    
    def __isWorkflow(self):
        """ Determine if currently operating in Workflow Managed environment
        
            :Returns:
                boolean indicating whether or not currently operating in Workflow Managed environment
        """
        #
        fileSource  = str(self.__reqObj.getValue("filesource")).lower()
        #
        if (self.__verbose):
            self.__lfh.write("+MessagingWebAppWorker.__isWorkflow() - filesource is %s\n" % fileSource)
        #
        # add wf_archive to fix PDBe wfm issue -- jdw 2011-06-30
        #
        if fileSource in ['archive','wf-archive','wf_archive','wf-instance','wf_instance']:
            #if the file source is any of the above then we are in the workflow manager environment
            return True
        else:
            #else we are in the standalone dev environment
            return False

class RedirectDevice:
    def write(self, s):
        pass

if __name__ == '__main__':
    sTool=MessagingWebApp()
    d=sTool.doOp()
    for k,v in d.items():
        sys.stdout.write("Key - %s  value - %r\n" % (k,v))
