#
# File:     doServiceRequestWebOb.wsgi
# Created:  26-Sep-2018
#
# Updated:
# 26-Sep-2018 EP    Ported from fcgi version
"""
This top-level responder for requests to /services/.... url for the
wwPDB General Annotation editor application framework.
This version depends on WSGI and WebOb.
Adapted from mod_wsgi version -
"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.07"

import sys
import traceback
import logging

from webob import Request, Response
from wwpdb.apps.msgmodule.webapp.MessagingWebApp import MessagingWebApp
from wwpdb.utils.config.ConfigInfo import getSiteId

# Create logger
FORMAT = "[%(levelname)s]-%(module)s.%(funcName)s: %(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

#  - URL mapping and application specific classes are launched from MessagingWebApp()

l2 = logging.getLogger("wwpdb.apps.msgmodule.io.MessagingDataImport")
l2.setLevel(logging.INFO)
l2 = logging.getLogger("wwpdb.apps.msgmodule.io.MessagingDataExport")
l2.setLevel(logging.INFO)


class MyRequestApp(object):
    """Handle server interaction using FCGI/WSGI and WebOb Request
    and Response objects.
    """

    def __init__(self, textString="doServiceRequest() - WebOb version", verbose=True, log=sys.stderr):
        """ """
        self.__text = textString
        self.__verbose = verbose
        self.__lfh = log

    def __dumpEnv(self, request):
        outL = []
        # outL.append('<pre align="left">')
        outL.append("\n------------------doServiceRequest()------------------------------\n")
        outL.append("Web server request data content:\n")
        outL.append("Text initialization:   %s\n" % self.__text)
        try:
            outL.append("Host:         %s\n" % request.host)
            outL.append("Path:         %s\n" % request.path)
            outL.append("Method:       %s\n" % request.method)
            outL.append("Query string: %s\n" % request.query_string)
            outL.append("Parameter List:\n")
            for name, value in request.params.items():
                outL.append("Request parameter:    %s:  %r\n" % (name, value))
        except:  # noqa: E722 pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)

        outL.append("\n------------------------------------------------\n\n")
        # outL.append("</pre>")
        return outL

    def __call__(self, environment, responseApplication):
        """WSGI callable entry point"""
        myRequest = Request(environment)
        #
        myParameterDict = {}
        siteId = getSiteId()
        try:
            if "WWPDB_SITE_ID" in environment:
                siteId = environment["WWPDB_SITE_ID"]
                self.__lfh.write("+MyRequestApp.__call__() - WWPDB_SITE_ID environ variable captured as %s\n" % siteId)
            # """
            # for name,value in environment.items():
            #     self.__lfh.write("+MyRequestApp.__call__() - ENVIRON parameter:    %s:  %r\n" % (name,value))
            # """
            for name, value in myRequest.params.items():
                if name not in myParameterDict:
                    myParameterDict[name] = []
                myParameterDict[name].append(value)
                self.__lfh.write("+MyRequestApp.__call__() - REQUEST parameter:    %s:  %r\n" % (name, value))
            myParameterDict["request_path"] = [myRequest.path.lower()]
        except:  # noqa: E722 pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.__lfh.write("+MyRequestApp.__call__() - contents of request data\n")
            self.__lfh.write("%s" % ("".join(self.__dumpEnv(request=myRequest))))

        ###
        #  At this point we have everything needed from the request !
        ###
        myResponse = Response()
        myResponse.status = "200 OK"
        myResponse.content_type = "text/html"
        ###
        #   Application specific functionality called here --
        #   Application receives path and parameter info only!
        ###
        msgmodule = MessagingWebApp(parameterDict=myParameterDict, verbose=self.__verbose, log=self.__lfh, siteId=siteId)
        rspD = msgmodule.doOp()
        myResponse.content_type = rspD["CONTENT_TYPE"]

        if sys.version_info[0] > 2:
            if isinstance(rspD["RETURN_STRING"], str):
                myResponse.text = rspD["RETURN_STRING"]
            else:
                myResponse.body = rspD["RETURN_STRING"]
        else:
            myResponse.body = rspD["RETURN_STRING"]

        ####
        ###
        return myResponse(environment, responseApplication)


##
#   NOTE -  verbose setting is set here ONLY!
##
application = MyRequestApp(textString="doServiceRequest() - WebOb version", verbose=True, log=sys.stderr)
