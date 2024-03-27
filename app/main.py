#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__      = "Neil R. Shapiro"
__copyright__   = "Copyright 2019-2023, Psychoanalytic Electronic Publishing"
__license__     = "Apache 2.0"
__version__     = "2023.0607/v2.3.030"   # new admin char details report
__status__      = "Development/Libs/Loader"  

"""
Main entry module for PEP version of OPAS API

This API server is based on the existing PEP-Web API 1.0.  The data returned 
may have additional fields but should be otherwise compatible with PEP API clients
such as PEP-Easy.

It's an initial version of the Opas Solr Server (API); it's not "generic", it's
schema and functionality dependent on PEP's needs (who is funding development).  

To Install (at least in windows)
  rem python 3.7 required
  python -m venv .\env
  .\venv\Scripts\activate.bat
  pip install --trusted-host pypi.python.org -r requirements.txt
  rem if it complains pip is not up to date
  python -m pip install --upgrade pip

Run with:
    uvicorn server:app --reload

    or for debug:

    uvicorn main:app --debug --log-level=debug

(Debug set up in this file as well: app = FastAPI(debug=True))

Endpoint and model documentation automatically available when server is running at:

  http://subdomain.domain:port/docs
  (components above as needed)

  (base URL + "/docs")
  e.g.,
  http://localhost:8000/docs
  etc.

Important Build and Usage Notes:

  - Most of the Solr functions are handled by PySolr, except for one which still uses Solrpy: /v2/Database/TermCounts/"
    and OPAS uses a copy--a modified version of Solrpy--because of an error I found.  Error handling is different
    between the two libraries too.
    
       #TODO: Switch to the equivalent PySolr calls, see testTermLists.py
  
"""
#----------------------------------------------------------------------------------------------
# Coding Standards
#----------------------------------------------------------------------------------------------
# Original code came from camelCase, my previously main code standard.  However, to make
# it at least a bit more acceptible to modern Pythonistas, I've converted variables and
# functions to snake_case.  There is still a mix, which does serve a purpose of separating
# names by use.

#Naming standards: 
    #- converted variables (still may be some camelCase) in snake_case
    #- class names in upper camelCase per python standards
    #- model attributes - lower camelCase
    #- database fields/attributes - snake_case
    #- solr attributes - snake_case
    #- api path parameters - upper CamelCase
    #- api query parameters - all lowercase (unseparated)

#----------------------------------------------------------------------------------------------
# Revisions (Changelog) 
#----------------------------------------------------------------------------------------------
# See https://github.com/Psychoanalytic-Electronic-Publishing/OpenPubArchive-Content-Server/wiki/CHANGELOG.md

# --------------------------------------------------------------------------------------------
# IMPORTANT TODOs (List)
# --------------------------------------------------------------------------------------------
# - Fix abstract return - Leadin needs to be inside HTML tagging (though works fine for display right now)
# - Think about what to do when a Solr query is in error, and solr dumps back.
# - Finish transition to PySolr...extended query (and perhaps another endpoint) still uses solrpy

# --------------------------------------------------------------------------------------------
# Not necessarily in the server code, but General TODOs
# --------------------------------------------------------------------------------------------
# - Add in detail for putciteashere gen from pepkbd3-abstract-html.xslt
#
#----------------------------------------------------------------------------------------------

import sys
sys.path.append('./config')
sys.path.append('./libs')
sys.path.append('./libs/solrpy')  # slightly patched version of solrpy

import os.path
import time
import datetime
from datetime import datetime
import re
import wget
import io
import urllib.parse
import random

from urllib import parse

import uvicorn
from fastapi import FastAPI, Query, Body, Path, Header, Security, Depends, HTTPException # File, Form, UploadFile, Cookie
from fastapi.openapi.utils import get_openapi
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.security.api_key import APIKeyQuery, APIKeyCookie, APIKeyHeader, APIKey
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, FileResponse, StreamingResponse # RedirectResponse
from starlette.middleware.cors import CORSMiddleware
import starlette.status as httpCodes
from typing import Union
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

# TIME_FORMAT_STR = '%Y-%m-%dT%H:%M:%SZ' # moved to opasConfig

from pydantic import ValidationError
# import solrpy as solr # needed for extended search
import config.opasConfig as opasConfig
import logging
logger = logging.getLogger(__name__)

import localsecrets
import libs.opasAPISupportLib as opasAPISupportLib
from configLib.opasCoreConfig import SOLR_DOCS # , EXTENDED_CORES_DEFAULTS, EXTENDED_CORES, SOLR_AUTHORS, SOLR_GLOSSARY, SOLR_DEFAULT_CORE 

from errorMessages import *
import models
import opasCentralDBLib
import opasFileSupport
import opasGenSupportLib
import opasQueryHelper
import opasSchemaHelper
import opasDocPermissions
import opasSolrPyLib
import opasPySolrLib
from opasPySolrLib import search_text_qs # , search_text
import opasPDFStampCpyrght
import opasCacheSupport
from opasArticleIDSupport import ArticleID
from opasMetadataCache import metadata_cache
cached_metadata = metadata_cache.get_cached_data()
expert_pick_image = ["", ""]

# Check text server version
text_server_ver = None
text_server_url = localsecrets.SOLRURL
ocd = opasCentralDBLib.opasCentralDB()
database_update_date = ocd.get_update_date_database()
ocd = None
all_source_codes = cached_metadata["ALL_CODES"]
# all_source_codes.append("OAJPSI")

PARAMS = {'wt':'json'}
url = f"{localsecrets.SOLRURL}admin/info/system"
if localsecrets.SOLRUSER is not None:
    # need username and password
    r = requests.get(url = url, params = PARAMS, auth=HTTPBasicAuth(localsecrets.SOLRUSER, localsecrets.SOLRPW))
else:
    r = requests.get(url = url, params = PARAMS)

if r.status_code == 200:
    ver_json = r.json()
    try:
        text_server_ver = ver_json["lucene"]["lucene-spec-version"]
    except KeyError:
        text_server_ver = ver_json["lucene"]["solr-spec-version"]

# to protect documentation, use: app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
#app = FastAPI()

app = FastAPI(
    debug=False,
    title="OpenPubArchive Server (OPAS) API for PEP-Web",
    description = """
<b>An Open Source Full-Text Journal and Book Server and API by Psychoanalytic
Electronic Publishing (PEP)</b>

This API server is an initial version of the OPAS Solr-based Server (API);
it's not "fully generic", the schema and functionality were developed around
the new full-functionality API Client meant to replace PEP's full-text
journal, book, and video database, PEP-Web. But PEP has developed it as open
source, so it could be a start for an extended or general purpose server.

While Solr already provides an API that is generic, the OPAS API wraps a
higher level API on top of that, which based on PEP's experience, provides
functionality that should simplify client development significantly. This is
in fact the second version of such a high level API (the first was written
around DTSearch). That it simplifies development was shown by PEP-Easy, which
was a very simple client written in javascript to the V1.0 version of the
API. Having a higher level API also simplifies porting as PEP did when the
underlying full-text indexing software changed from DTSearch to Solr.

One key feature of this API is true paragraph level search...something not
easily implemented in Solr in a straightforward way. That functionality
requires a schema design where paragraphs are separately indexed using Solr's
advanced nested indexing. The PEP-Web Docs schema implements this.

Note that a few of the endpoints require an API key. If so, it is listed in
the description of the endpoint (not in the query parameter list). If a
function requiring one is called without the API key, you will get an
authentication error.

***

*IMPORTANT UPDATE REGARDING THE PEP SCHEMA*: The current PEP Docs Schema does 
not index all documents by paragraphs, since to provide search results more 
compatible with PEP's original DTSearch based implementation, term proximity 
search is instead approximated by a preset word distance value, as was done 
in DTSearch. However, SE and GW are indexed by paragraph as well, and thus 
the paratext (true paragraph search) feature of the API works for those book 
series. Thus, all searches using the "paratext" parameter will only currently 
search these two book series. This of course only applies to the current 
PEPWebDocs core, and for other databases (cores), the paragraph level 
indexing and searches may still be fully utilized. The PEP loader program,
which are specific to the PEP-Schema, can be used to turn on and off 
paragraph level indexing for other documents. This is controlled in a simple 
manner by the SRC_CODES_TO_INCLUDE_PARAS list constant in the loaderConfig.py 
file.

***

*INTERACTIVE DOCS USAGE NOTE*: For those using the interactive Docs interface
to test out endpoints, the built-in feature has a significant issue: the 
input field widths are too small for some of our potential parameter values.
A workaround for this is to get a browser extension called `Stylebot`, which
will let you add a local CSS rule to fix that. Add this rule for the URL
where the docs are located for you:

            .swagger-ui .parameters-col_description input[type=text] {
                   max-width: 1300px;
                   width: 100%;
            }
            

and that will enlarge the fields persistently.

*INTERACTIVE DOCS USAGE NOTE2*: For those using the interactive Docs interface
to test out endpoints, the built-in feature has a second significant issue: 
When using GET and the interactive documentation to try out several of the endpoints
notably the search endpoints, you must clear the "Request Body" field before submitting.
The "Request Body" field only applies to POST, but the code is shared for maintenance
and efficiency, but this trips up the interface for interactive docs.  The "Request Body"
is used with POST to provide a data structure input with more resolution than the endpoint parameters.

***    
   """,
        version = f"{__version__}",
        static_directory=r"./docs",
        swagger_static={
            "favicon": "pepfavicon"
            },
)

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex = localsecrets.CORS_REGEX, 
    allow_origins = localsecrets.CORS_ORIGINS, 
    allow_credentials = True,
    allow_methods = ["*"],
    allow_headers = ["*"],
)

import opasWhatsNewCache
whatsnewdb = opasWhatsNewCache.whatsNewDB()
import opasCacheMostViewed
mostviewedcache = opasCacheMostViewed.mostViewedCache()
import opasCacheMostCited
mostcitedcache = opasCacheMostCited.mostCitedCache()
# from config import msgdb
import opasMessageLib
msgdb = opasMessageLib.messageDB()

msg = 'Started at %s' % datetime.today().strftime('%Y-%m-%d %H:%M:%S"')
logger.info(msg)

def find_client_id(request: Request,
                   response: Response,
                   client_id: int = 0
                  ):
    """
    ALWAYS returns a client ID.
    
    Dependency for client_id:
           gets it from header;
           if not there, gets it from query param;
           if not there, defaults to 0 (server is client)
    """
    ret_val = 0
    if client_id == 0 or client_id is None:
        ret_val = opasConfig.NO_CLIENT_ID
        client_id = request.headers.get(opasConfig.CLIENTID, None)
        client_id_qparam = request.query_params.get(opasConfig.CLIENTID, None)
        client_id_cookie = request.cookies.get(opasConfig.CLIENTID, None)
        if client_id is not None and client_id not in ('0', 0):
            ret_val = client_id
            msg = f"client-id from header: {ret_val} "
            logger.debug(msg)
        elif client_id_qparam is not None:
            ret_val = client_id_qparam
            msg = f"client-id from param: {ret_val} "
            logger.debug(msg)
        elif client_id_cookie is not None:
            ret_val = client_id_cookie
            msg = f"client-id from cookie: {ret_val} "
            logger.debug(msg)
        #else:
            #ret_val = opasConfig.NO_CLIENT_ID # no client id (default)
    else:
        ret_val = client_id

    #  client_id type will be fixed, if necessary, in validate, or errors logged
    if ret_val != opasConfig.NO_CLIENT_ID:
        ret_val = opasDocPermissions.validate_client_id(ret_val, caller_name="FindClientID")

    return ret_val

# ############################################################################
# Dependence routines
# ############################################################################
def get_client_id(response: Response,
                  request: Request,
                  client_id: int=Header(0, title=opasConfig.TITLE_CLIENT_ID, description=opasConfig.DESCRIPTION_CLIENT_ID)
                  ):
    """
    Dependency for client id: see find_client_id
    """
    ret_val = find_client_id(request, response, client_id)   
    return ret_val

def get_client_session(response: Response,
                       request: Request,
                       client_session: str=Header(None, title=opasConfig.TITLE_CLIENT_SESSION, description=opasConfig.DESCRIPTION_CLIENT_SESSION), 
                       client_id: int=Header(0, title=opasConfig.TITLE_CLIENT_ID, description=opasConfig.DESCRIPTION_CLIENT_ID)
                       ):
    """
    Dependency for client_session id:
           gets it from header;
           if not there, gets it from query param;
           if not there, gets it from a cookie
           
    Call routine in library so other routines can get resolve from there as well       
    """
    caller_name = "get_client_session"
    
    if client_session == 'None': # Not sure where this is coming from as string, but fix it.
        client_session = None
        
    if client_session is not None:
        session_id = client_session
        #if opasConfig.DEBUG_TRACE:
            #print ("-" * 100)
            #msg = f"{datetime.now().time().isoformat()}: [client-session from header]: {session_id} "
            #print (msg)
    else:
        session_id = opasDocPermissions.find_client_session_id(request, response, client_session)
        
    # if there's no client session, get a session_id from PaDS without logging in
    if session_id is None:
        # get one from PaDS, without login info
        # session_info, pads_session_info = opasDocPermissions.pads_get_session(client_id=client_id)
        # New...let's try to isolate when this happens by creating an error.  We get a session from PaDS, but not if there's no client ID.
        client_id = find_client_id(request, response) # client_id == 0 to force it to search header etc.
        if client_id == opasConfig.NO_CLIENT_ID:
            # No client id, not allowed to get session.
            msg = ERR_MSG_CALLER_IDENTIFICATION_ERROR + f" URL: {request.url._url} Headers:{request.headers} "
            #
            # 20230322 New switch to demote these froms errors to info for now...they dominate the logs.
            #  (and decided to change them to warning when not demoted.)
            if opasConfig.DEMOTE_PREVALENT_LOG_ENTRIES:
                logger.info(msg)
            else:
                logger.warning(msg)
                
            raise HTTPException(
                status_code=httpCodes.HTTP_428_PRECONDITION_REQUIRED,
                detail=ERR_MSG_CALLER_IDENTIFICATION_ERROR
            )
        else:
            #session_info = opasDocPermissions.get_authserver_session_info(session_id=session_id,
                                                                          #client_id=client_id,
                                                                          #request=request)

            ocd, session_info = opasDocPermissions.get_session_info(request,
                                                                   response,
                                                                   session_id=client_session,
                                                                   client_id=client_id,
                                                                   caller_name=caller_name)
            
            try:
                session_id = session_info.session_id
                logger.info(f"Client {client_id} request w/o sessionID: {request.url._url}. Called PaDS, returned {session_id}") 
            except Exception as e:
                # We didn't get a session id
                msg = f"SessionID not received from authserver for client {client_id} and session {client_session}.  Headers:{request.headers}. Raising Exception 424 ({e})."
                logger.error(msg)
                raise HTTPException(
                    status_code=httpCodes.HTTP_424_FAILED_DEPENDENCY,
                    detail=ERR_MSG_PASSWORD + f" ({msg} - {e})", 
                )

    if session_id is None or len(session_id) < 12:
        # don't report these errors
        if session_id == "":
            session_id = "BLANKSESSIONID123"
        else:
            msg = f"SessionID:[{session_id}] was not resolved. Request:{request.url._url}. Raising Exception 424."
            if re.search("GW\.000|SE\.000", request.url._url) is None:
                logger.error(msg)
            raise HTTPException(
                status_code=httpCodes.HTTP_424_FAILED_DEPENDENCY,
                detail=ERR_MSG_PASSWORD + f" {msg}", 
        )
        
    return session_id       
    
security = HTTPBasic()
def login_via_pads(request: Request,
                   response: Response, 
                   credentials: HTTPBasicCredentials = Depends(security),
                   client_id:int=Depends(get_client_id)
                   ):

    # ocd = opasCentralDBLib.opasCentralDB()
    caller_name = "login_via_pads"
    
    session_id = opasDocPermissions.find_client_session_id(request, response)
    # Ok, login
    pads_session_info = opasDocPermissions.authserver_login(username=credentials.username,
                                                      password=credentials.password,
                                                      session_id=session_id,
                                                      client_id=client_id)
  
    if pads_session_info is None or pads_session_info.IsValidLogon == False:
        raise HTTPException(
            status_code=httpCodes.HTTP_401_UNAUTHORIZED,
            detail=ERR_MSG_PASSWORD, 
            headers={"WWW-Authenticate": "Basic"},
        )
    else:
        ocd, session_info = opasDocPermissions.get_session_info(request,
                                                               response,
                                                               session_id=pads_session_info.SessionId,
                                                               client_id=client_id,
                                                               pads_session_info=pads_session_info, 
                                                               caller_name=caller_name)

        # Confirm that the request-response cycle completed successfully.
        ret_val = session_info
    
    return ret_val

#-----------------------------------------------------------------------------
api_key_query = APIKeyQuery(name=localsecrets.API_KEY_NAME, auto_error=False)
api_key_header = APIKeyHeader(name=localsecrets.API_KEY_NAME, auto_error=False)
api_key_cookie = APIKeyCookie(name=localsecrets.API_KEY_NAME, auto_error=False)
#-----------------------------------------------------------------------------
async def get_api_key(api_key_query: str = Security(api_key_query),
                      api_key_header: str = Security(api_key_header),
                      api_key_cookie: str = Security(api_key_cookie),
                      ):

    if api_key_query == localsecrets.API_KEY:
        return api_key_query
    elif api_key_header == localsecrets.API_KEY:
        return api_key_header
    elif api_key_cookie == localsecrets.API_KEY:
        return api_key_cookie
    else:
        raise HTTPException(
            status_code=httpCodes.HTTP_403_FORBIDDEN,
            detail=ERR_CREDENTIALS
        )

# ############################################################################
# End Dependence routines
# ############################################################################
def log_endpoint(request, client_id=None, session_id=None, path_params=True, level="info", trace=False):
    url = urllib.parse.unquote(f"....{request.url}")
    text = f"*************[{client_id}:{session_id}]:{url}*************"
    if client_id == 3: # PaDS, sends a lot of requests at once, so mute unless debug mode
        logger.debug(text)
    else:
        if trace: # force it, as warning
            logger.warning(text) # controlled logging
        elif level == "info":
            logger.info(text)    # controlled logging
        elif level == "warning":
            logger.warning(text) # controlled logging
        elif level == "error":
            logger.error(text)   # controlled logging
        else:
            logger.debug(text)            
            
def log_endpoint_time(request, ts, level="info", trace=False):
    text = f"***{request['path']} response time: {time.time() - ts}***"
    if opasConfig.LOG_CALL_TIMING:
        if trace: # force it, as warning
            logger.warning(text) # controlled logging
        elif level == "info" or level == "debug":
            logger.info(text)    # controlled logging
        elif level == "warning": 
            logger.warning(text) # controlled logging
        elif level == "error":
            logger.error(text)   # controlled logging
        else:
            logger.debug(text)            

# ############################################################################
# EndPoints
# ############################################################################
#-----------------------------------------------------------------------------
@app.put("/v2/Admin/LogLevel/", tags=["Admin"], summary=opasConfig.ENDPOINT_SUMMARY_LOGLEVEL)
async def admin_set_loglevel(response: Response, 
                             request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),
                             level:str=Query(None, title="Log Level", description="DEBUG, INFO, WARNING, or ERROR"),
                             #sessionid: str=Query(None, title="SessionID", description="Filter by this Session ID"),
                             client_id:int=Depends(get_client_id),
                             client_session:str= Depends(get_client_session), 
                             api_key: APIKey = Depends(get_api_key), 
                            ):
    
    """
    ## Function
       ### Change the logging level of the server to DEBUG, INFO, WARNING, or ERROR.
       
       If level is not supplied, just report current level.

    ## Return Type
       Returns a string confirming the new logger level, or an error message.  

    ## Status
       This endpoint is working.

    ## Sample Call
         /v2/Admin/LogLevel/

    ## Notes
       ### Requires API key.
       ### Requires Admin level user for changes.

       The input string is case insensitive.
       
       ## Potential Errors
       N/A
    
    """
    levels = {10: "DEBUG", 20: "INFO", 30: "WARNING", 40: "ERROR", 50: "CRITICAL"}
    logger = logging.getLogger() # Get root logger
    curr_level = levels.get(logger.level, str(logger.level)) # return string of level if not in levels
    # see if user is an admin
    admin = False
    if client_session is not None:
        ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id)
        if ocd.verify_admin(session_info):
            admin = True
    
    # not necessary, since setLevel accepts the same strings
    # but this protects from name input errors
    # (as does the Exception)
    # At this point there's no reason I see to accept numeric levels
    err = ""

    change = False
    ret_val = f"Specified log level is set to: {curr_level}."
    if admin:
        level = level.upper()
        if level == "DEBUG":
            lev_int = logging.DEBUG
        elif level == "INFO":
            lev_int = logging.INFO
        elif level == "WARNING" or level == "WARN":
            lev_int = logging.WARNING
        elif level == "ERROR":
            lev_int = logging.ERROR
        else:
            err = f"Specified log level {level} not recognized. Log level still set to: {curr_level}"
    else:
        if curr_level != level:
            err = f"Log level can only be changed by an admin. Log level still set to: {curr_level}"
        else:
            err = f"Log level already set to: {curr_level}"

    try:
        if admin:
            new_level = opasAPISupportLib.set_log_level(level_int=lev_int)
            if new_level != curr_level:
                change = True

    except Exception as e:
        err = f"Exception setting or getting LogLevel: {e} Log level set to: {levels.get(logger.level, str(logger.level))}.  Was {curr_level}"
        logger.error(err)
        ret_val = err
    else:
        if change:
            ret_val = f"Log level set to: {levels.get(logger.level, str(logger.level))}.  Was {curr_level}" 
            logger.info(ret_val) 
    return ret_val

#-----------------------------------------------------------------------------
@app.get("/v2/Admin/Reports/{report}", response_model=models.Report, tags=["Admin"], summary=opasConfig.ENDPOINT_SUMMARY_REPORTS)
async def admin_reports(response: Response, 
                        request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),
                        report: models.ReportTypeEnum=Path(..., title=opasConfig.TITLE_REPORT_REQUESTED, description=opasConfig.DESCRIPTION_REPORT_REQUESTED),
                        sessionid: str=Query(None, title=opasConfig.TITLE_SESSION_ID_FILTER, description=opasConfig.DESCRIPTION_SESSION_ID_FILTER),
                        userid:str=Query(None, title=opasConfig.TITLE_USERID_FILTER, description=opasConfig.DESCRIPTION_USERID_FILTER),
                        endpointidlist:str=Query(None, title=opasConfig.TITLE_ENDPOINTID_LIST, description=opasConfig.DESCRIPTION_ENDPOINTID_LIST), 
                        startdate: str=Query(None, title=opasConfig.TITLE_STARTDATE, description=opasConfig.DESCRIPTION_STARTDATE), 
                        enddate: str=Query(None, title=opasConfig.TITLE_ENDDATE, description=opasConfig.DESCRIPTION_ENDDATE),
                        matchstr: str=Query(None, title=opasConfig.TITLE_REPORT_MATCHSTR, description=opasConfig.DESCRIPTION_REPORT_MATCHSTR), 
                        limit: int=Query(100, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                        offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET),
                        getfullcount:bool=Query(False, title=opasConfig.TITLE_GETFULLCOUNT, description=opasConfig.DESCRIPTION_GETFULLCOUNT),
                        loggedinrecords:bool=Query(True, title=opasConfig.TITLE_LOGGEDINRECORDS, description=opasConfig.DESCRIPTION_LOGGEDINRECORDS),
                        sortorder: str=Query("ASC", title=opasConfig.TITLE_SORTORDER, description=opasConfig.DESCRIPTION_SORTORDER),
                        download:bool=Query(False, title=opasConfig.TITLE_DOWNLOAD, description=opasConfig.DESCRIPTION_DOWNLOAD), 
                        client_id:int=Depends(get_client_id), 
                        client_session:str= Depends(get_client_session), 
                        api_key: APIKey = Depends(get_api_key)
                       ):
    """
    ## Function
       ### Returns a report in JSON per the Reports

    ## Return Type
       models.Report

    ## Status
       This endpoint is working.

    ## Sample Call
         /v2/Admin/Reports/

    ## Notes
       ### Requires API key.
       ### Requires Admin level user  
    
       For session-views and user-searches reports:
         matchstr does a regex search of the url (endpoint plus parameters)
         e.g.,
           /Documents/Document/AIM.023.0227A/
         
       For document-view-log matchstr does a regex search of the document type, e.g.,
         PDF
         
       Note as the examples above, you don't need to include special regex wildcards
         (it matches anywhere in the text)

    ## Potential Errors
       Note document_views_report returns the current RDS database values, not the values
         that are in the Solr database as updated during the latest Solr update. values
         match vw_reports_document_views
         

    """
    
    caller_name = "[v2/Admin/Reports]"
    #if opasConfig.DEBUG_TRACE: print(caller_name)

    opasDocPermissions.verify_header(request, "Reports") # for debugging client call
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")

    ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)
    if session_info.admin != True:
        # watch to see if PaDS is using the reports as an admin or non-admin user, if admin, change reports to admin only
        ret_val = f"Report {report} request by non-admin user ({session_info.username} id: {session_info.session_id})."
        logger.error(ret_val)
        raise HTTPException(
            status_code=httpCodes.HTTP_401_UNAUTHORIZED, 
            detail=ret_val
        )       
    else:
        msg = f"Report {report} request by admin user ({session_info.username}) id: {session_info.session_id})."
        logger.info(msg)
        if opasConfig.PADS_INFO_TRACE: print (msg)

    userid_condition = ""
    sessionid_condition = ""
    extra_condition = ""
    standard_filter = "1"
    limited_count = 0
    ret_val = None
    if sortorder in ["asc","desc","ASC","DESC"]:
        sortorder = sortorder.upper()
    else:
        ret_val = f"Unkown sort order supplied: {sortorder}.  Ignored."
        logger.error(ret_val)
        raise HTTPException(
            status_code=httpCodes.HTTP_400_BAD_REQUEST, 
            detail=ret_val
        )

    if sessionid is not None:
        sessionid_condition = f" AND session_id={sessionid}"

    # this user id will be whatever I get from the authentication system, not the numeric userid I have
    if userid is not None:
        userid_condition = f" AND global_uid={userid}"
        
    if endpointidlist is not None and report == models.ReportTypeEnum.sessionLog:
        endpoint_list_items = [x.strip() for x in endpointidlist.split(',')]
        endpoint_tuple = tuple(endpoint_list_items)
        endpoint_condition = f"AND endpoint_id IN {endpoint_tuple}" # note: this queries the view; table column is api_endpoint_id
    else:
        endpoint_condition = ""
        
    try:
        if startdate is not None:
            startdt = opasGenSupportLib.datestr2mysql(startdate)
            if enddate is not None:
                enddt = opasGenSupportLib.datestr2mysql(enddate)
                date_condition = f" AND last_update BETWEEN {startdt} AND {enddt}"
            else:
                date_condition = f" AND last_update >= {startdt}"
        else:
            if enddate is not None:
                enddt = opasGenSupportLib.datestr2mysql(enddate)
                date_condition = f" AND last_update <= {enddt}"
            else: # both None
                date_condition = ""
    except Exception as e:
        logger.warning(f"Bad start or end date specified to reports {e}")

    limit_clause = ""
    if limit is not None:
        limit_clause = f"LIMIT {limit}"
        if offset != 0:
            limit_clause += f" OFFSET {offset}"
    else:
        limit = 0 # (None) for ResponseInfo since it has to be an integer

    report_view = None
    header = None
    if report == models.ReportTypeEnum.documentViewLog:
        standard_filter = "1 = 1 " 
        if matchstr is not None:
            extra_condition = f" AND type RLIKE '{matchstr}'"
        report_view = "vw_reports_document_activity"
        orderby_clause = f"ORDER BY last_update {sortorder}"
        header = ["user id",
                  "session id",
                  "document id",
                  "view type",
                  "last update",
                  "document activity id"]
        
    elif report == models.ReportTypeEnum.sessionLog:
        standard_filter = "1 = 1 "
        if not loggedinrecords:
            report_view = "vw_reports_session_activity_not_logged_in"
            orderby_clause = ""
            #if sortorder == "DESC":
                #report_view = "vw_reports_session_activity_not_logged_in_desc"
        else:
            report_view = "vw_reports_session_activity" # default built in sort, ASC
            orderby_clause = ""
            #if sortorder == "DESC":
                #report_view = "vw_reports_session_activity_desc"
                
        if matchstr is not None:
            extra_condition = f" AND params RLIKE '{matchstr}'"
        orderby_clause = f"ORDER BY last_update {sortorder}"
        header = ["user id",
                  "session id",
                  "session start",
                  "session end",
                  "item of interest",
                  "endpoint",
                  "endpoint id", 
                  "params",
                  "status code",
                  "status message",
                  "last update",
                  "session activity id"]
    
    elif report == models.ReportTypeEnum.userSearches:
        standard_filter = "endpoint rlike '.*Search' "
        if matchstr is not None:
            extra_condition = f" AND params RLIKE '{matchstr}'"
        report_view = "vw_reports_user_searches"
        orderby_clause = f"ORDER BY last_update {sortorder}"
        header = ["user id", 
                  "session id",
                  "session start",
                  "item of interest",
                  "endpoint",
                  "params",
                  "status code",
                  "status message",
                  "last update",
                  "search activity id"]
    
    elif report == models.ReportTypeEnum.documentViews:
        standard_filter = "1 = 1 " 
        report_view = "vw_reports_document_views"
        orderby_clause = f"ORDER BY views {sortorder}"
        header = ["document id",
                  "view type",
                  "view count"]
        
    elif report == models.ReportTypeEnum.characterCounts:
        report_view = "vw_reports_charcounts"
        orderby_clause = f"ORDER BY jrnlgrpname {sortorder}"
        header = ["jrnl grp",
                  "jrnl code",
                  "grp inclusion",
                  "grp start year",
                  "jrnl start year",
                  "year count",
                  "char count",
                  "no space char count", 
                  "up to year"
                  ]
    elif report == models.ReportTypeEnum.characterCountsDetails:
        report_view = "vw_reports_charcounts_details"
        orderby_clause = f"ORDER BY jrnlcode, year {sortorder}"
        header = ["jrnlcode",
                  "year",
                  "vol",
                  "char count",
                  "no space char count",
                  "article count",
                  "earliest year", 
                  "latest year"
                  ]
    elif report == models.ReportTypeEnum.characterCountsBookDetails:
        report_view = "vw_reports_charcounts_sub_books_byvol"
        orderby_clause = f"ORDER BY jrnlcode, year {sortorder}"
        header = ["jrnlcode",
                  "year",
                  "vol",
                  "char count",
                  "no space char count",
                  "article count",
                  "earliest year", 
                  "latest year"
                  ]
    else:
        report_view = None

    if report_view is None:
        raise HTTPException(
            status_code=httpCodes.HTTP_404_NOT_FOUND, 
            detail=f"Report {report}: " + ERR_MSG_REPORT_NOT_FOUND
        )        

    # Get report
    
    # with order by for return
    select = f"""SELECT * from {report_view}
                 WHERE {standard_filter}
                 {date_condition}
                 {userid_condition}
                 {sessionid_condition}
                 {endpoint_condition}
                 {extra_condition}
                 {orderby_clause}
                 """
    
    # without order by for full count
    select_count = f"""SELECT * from {report_view}
                       WHERE {standard_filter}
                       {date_condition}
                       {userid_condition}
                       {sessionid_condition}
                       {endpoint_condition}
                       {extra_condition}
                     """
   
    if getfullcount:
        count = ocd.get_select_count(select_count) # without order by
    else:
        count = -1
        
    if count == 0:
        raise HTTPException(
            status_code=httpCodes.HTTP_404_NOT_FOUND, 
            detail=ERR_MSG_NO_DATA_FOR_REPORT
        )               
    else:
        # now add the limit clause
        select += f"{limit_clause};"
        
        if download:
            # Download CSV of selected set.  Returns only response with download, not usual documentList
            #   response to client
            results = ocd.get_select_as_list(select)
            df = pd.DataFrame(results)
            stream = io.StringIO()
            df.to_csv(stream, header=header, index = False)
            response = StreamingResponse(iter([stream.getvalue()]),
                                         media_type="text/csv"
                                               )
            response.headers["Content-Disposition"] = f"attachment; filename={report_view}.csv"
            ret_val = response
        else:
            # this comes back as a list of ReportListItems
            ts = time.time()
            results = ocd.get_select_as_list_of_models(select, model=models.ReportListItem, model_type="Generic")
            limited_count = len(results)
            if count > 0:
                full_count_complete_checked = limited_count >= count
            else:
                full_count_complete_checked = None
                count = None
                
            response_info = models.ResponseInfo(count = limited_count,
                                                fullCount = count,
                                                totalMatchCount = count,
                                                limit = limit,
                                                offset = offset,
                                                listType="reportlist",
                                                fullCountComplete = full_count_complete_checked,
                                                request=f"{request.url._url}",
                                                timeStamp = datetime.utcfromtimestamp(time.time()).strftime(opasConfig.TIME_FORMAT_STR)                     
                                                )   

            if count == 0:
                raise HTTPException(
                    status_code=httpCodes.HTTP_404_NOT_FOUND, 
                    detail=ERR_MSG_NO_DATA_FOR_REPORT
                )       

            logger.debug(f"Watched: Admin Report Query Complete. DateCondition: {date_condition} Rows: {limited_count} Time={time.time() - ts}")
            report_struct = models.ReportStruct( responseInfo = response_info, 
                                                 responseSet = results
                                                 )

            ret_val = models.Report(report = report_struct)

    if response.status_code is None:
        response_code = 200
    else:
        response_code = response.status_code
        
    ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_ADMIN_REPORTS,
                                api_endpoint_method=opasCentralDBLib.API_ENDPOINT_METHOD_GET,
                                item_of_interest=report, 
                                session_info=session_info, 
                                params=request.url._url,
                                return_status_code = response_code,
                                status_message=opasCentralDBLib.API_STATUS_SUCCESS
                                )

    #  return it.
    return ret_val

#-----------------------------------------------------------------------------
@app.get("/v2/Admin/Sitemap/", tags=["Admin"], response_model=models.SiteMapInfo, summary=opasConfig.ENDPOINT_SUMMARY_SITEMAP)
async def admin_sitemap(response: Response, 
                        request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),
                        path: str=Query(localsecrets.SITEMAP_PATH, title=opasConfig.TITLE_SITEMAP_PATH, description=opasConfig.DESCRIPTION_SITEMAP_PATH),
                        size: int=Query(8000, title=opasConfig.TITLE_SITEMAP_RECORDS_PER_FILE, description=opasConfig.DESCRIPTION_SITEMAP_RECORDS_PER_FILE),
                        max_records: int=Query(200000, title=opasConfig.TITLE_SITEMAP_MAX_RECORDS, description=opasConfig.DESCRIPTION_SITEMAP_MAX_RECORDS),
                        api_key: APIKey = Depends(get_api_key), 
                        client_id:int=Depends(get_client_id),
                        client_session:str= Depends(get_client_session)
                       ):
    
    """
    ## Function
       ### Generate a Sitemap for Google.

    ## Return Type
       SiteMapInfo model pointing to sitemapindex and list of files, e.g.,
       
            {
               "siteMapIndex": "X:\\AWS_S3\\AWSProd PEP-Web-Google/sitemapindex.xml",
               "siteMapList": [
                 "X:\\AWS_S3\\AWSProd PEP-Web-Google/sitemap1.xml",
                 "X:\\AWS_S3\\AWSProd PEP-Web-Google/sitemap2.xml",
                 "X:\\AWS_S3\\AWSProd PEP-Web-Google/sitemap3.xml",
                 "X:\\AWS_S3\\AWSProd PEP-Web-Google/sitemap4.xml"
               ]
            }

    ## Status
       This endpoint is working.

    ## Sample Call
         /v2/Admin/Sitemap/?size=100&max_records=500

    ## Notes
       ### Requires API key
       ### Requires client_id in the header (use 'client-id' in call header)
       ### Requires Admin Level User Session
       
    ## Potential Errors
       N/A
    
    """
    caller_name = "[v2/Admin/Sitemap]"
    #if opasConfig.DEBUG_TRACE: print(caller_name)
    
    import opasSiteMap
    # path variable/parameter defaults to localsecrets.SITEMAP_PATH
    try:
        SITEMAP_OUTPUT_FILE = path + localsecrets.PATH_SEPARATOR + "sitemap" # don't include xml extension here, it's added
        SITEMAP_INDEX_FILE = path + localsecrets.PATH_SEPARATOR + "sitemapindex.xml"
    except Exception as e:
        SITEMAP_OUTPUT_FILE = ".." + localsecrets.PATH_SEPARATOR + "sitemap"             # don't include xml extension here, it's added
        SITEMAP_INDEX_FILE = ".." + localsecrets.PATH_SEPARATOR + "sitemapindex.xml"
   
    if client_session is not None:
        ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)
        # see if user is an admin or if this is a localtest
        if ocd.verify_admin(session_info):
            try:
                # returns a list of the sitemap files (since split)
                sitemap_list = opasSiteMap.metadata_export(SITEMAP_OUTPUT_FILE, total_records=max_records, records_per_file=size)
                # sitemap_index = opasSiteMap.opas_sitemap_index(output_file=SITEMAP_INDEX_FILE, sitemap_list=sitemap_list)
                ret_val = models.SiteMapInfo(siteMapIndex=SITEMAP_INDEX_FILE, siteMapList=sitemap_list)
        
            except Exception as e:
                ret_val=f"AdminError: Sitemap Error: {e}"
                logger.error(ret_val)
                raise HTTPException(
                    status_code=httpCodes.HTTP_500_INTERNAL_SERVER_ERROR, 
                    detail=ret_val
                )
        else:
            ret_val=f"AdminError: Unauthorized.  Admin access required for SiteMap."
            logger.error(ret_val)
            raise HTTPException(
                status_code=httpCodes.HTTP_401_UNAUTHORIZED, 
                detail=ret_val
            )
            
    return ret_val

#-----------------------------------------------------------------------------
@app.get("/v2/Api/LiveDoc", tags=["API documentation"], summary=opasConfig.ENDPOINT_SUMMARY_DOCUMENTATION)
async def api_live_doc(api_key: APIKey = Depends(get_api_key)):
    """
    ## Function
       ### This returns an HTML page with interactive api documentation.

    ## Return Type
       HTML

    ## Status
       This endpoint is working.

    ## Sample Call
         /v2/Api/LiveDoc/

    ## Notes
       ### Requires API key.
        
    ## Potential Errors
        N/A

    """
    # This is especially useful if/when we turn off the free documentation by changing the app call to:
        #app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    # or for downloading the doc.
    
    response = get_swagger_ui_html(openapi_url="/v2/Api/OpenapiSpec", title="docs")

    return response

#-----------------------------------------------------------------------------
@app.get("/v2/Api/OpenapiSpec", tags=["API documentation"], summary=opasConfig.ENDPOINT_SUMMARY_OPEN_API)
async def api_openapi_spec(api_key: APIKey = Depends(get_api_key)):
    """
    ## Function
       ### This returns openapi documentation in JSON that can be loaded to Swagger

    ## Return Type
       Open API spec in JSON

    ## Status
       This endpoint is working.

    ## Sample Call
         /v2/Api/OpenapiSpec/

    ## Notes
       ### Requires API key

    ## Potential Errors
       N/A

    """
    #Also a good demo of the simplicity to add APIKey to any endpoint. The APIKey
    #can be added to the header (see postman example), in the interactive documentation
    #it's just a matter of pressing the lock icon and filling out the form.  It can also
    #be supplied as a query parameter in the endpoint call or a cookie.

    response = JSONResponse(get_openapi(title="FastAPI", version=1, routes=app.routes))
    return response

#-----------------------------------------------------------------------------
@app.get("/v2/Api/Status/", response_model=models.APIStatusItem, response_model_exclude_unset=True, tags=["API documentation"], summary=opasConfig.ENDPOINT_SUMMARY_API_STATUS)
async def api_status(response: Response, 
                     request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST)
                    ):
    """
    ## Function
       ### Return the status of the API to check if it's online/available.
       
       This is a low overhead function and suitable as a heartbeat check.

    ## Return Type
       models.APIStatusItem

    ## Status
       Status: Working

    ## Sample Call
         /v2/Api/Status/

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    try:
        api_status_item = models.APIStatusItem(timeStamp = datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%M:%SZ'), 
                                               opas_version = __version__, 
                                              )

    except ValidationError as e:
        logger.error("ValidationError", e.json())
        raise HTTPException(
            status_code=httpCodes.HTTP_400_BAD_REQUEST,
            detail=ERR_MSG_STATUS_DATA_ISSUE + f": {e}"
        )

    return api_status_item

#-----------------------------------------------------------------------------
@app.post("/v2/Client/Configuration/", response_model=models.ClientConfigList, response_model_exclude_unset=True, tags=["Client"], summary=opasConfig.ENDPOINT_SUMMARY_SAVE_CONFIGURATION, status_code=201)
async def client_save_configuration(response: Response, 
                                    request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),
                                    configuration:models.ClientConfigList=Body(None, embed=False, title=opasConfig.TITLE_ADMINCONFIG, decription=opasConfig.DESCRIPTION_ADMINCONFIG), # allows full specification
                                    client_id:int=Depends(get_client_id), 
                                    client_session:str= Depends(get_client_session), 
                                    api_key: APIKey = Depends(get_api_key)
                                    ):
    """
    ## Function
       ### Persistently store any "global" (not tied to a specific user) settings for the client app.

       A client can store multiple settings under different names. As of the 2021.02.13.Alpha, a client
       can save multiple settings in a single call.  The list of settings, when converted to JSON, look like this:

       Since the function allows a list of settings, the configName and configSettings must be a member
       of a configList, even if there's only one set of configSettings being saved, e.g.,
       
        {
          "configList": [{
                            "configName": "test_client_test_0",
                            "configSettings": {
                                               "a": 1,
                                               "b": 2,
                                               "c": 8
                                               }
                         }]
        }

        Likewise, to save multiple settings, add to the list:
       
        {
          "configList": [{
                            "configName": "test_client_test_0",
                            "configSettings": {
                                               "a": 1,
                                               "b": 2,
                                               "c": 8
                                               }
                         },
                         {
                            "configName": "test_client_test_1",
                            "configSettings": {
                                               "a": 2,
                                               "b": 4,
                                               "c": 8
                                               }
                         }]
        }

       It's important to note that each of the settings are stored separately in the underlying database
       under the configName specified.  They are not stored as a set, and can be retrieved individually,
       or in other combinations.

    ## Return Type
       models.ClientConfigList

    ## Status
       This endpoint is working.

    ## Sample Call
         /v2/Client/Configuration/

    ## Notes
       ### Requires API key
       ### Requires client_id in the header (use 'client-id' in call header)

    ## Potential Errors
       If one of the configNames already exists for that client app (based on that client_id),
       an error 409 is returned.

    """
    #  api_key needs to be supplied, e.g., in the header
    #  set database for confg, overwriting previous

    #  return current config (old if it fails).
    caller_name = "client_save_configuration"
    #if opasConfig.DEBUG_TRACE: print(caller_name)

    opasDocPermissions.verify_header(request, caller_name) # for debugging client call
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")

    ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)

    # ensure user is admin
    ret_val = None

    # for now, just use API_KEY as the requirement.  
    #TODO Later admin?  if ocd.verify_admin(session_info):
    ret_val, msg = ocd.save_client_config(session_id=client_session,
                                          client_id=client_id, 
                                          client_configuration=configuration,
                                          replace=False)
    status_code = ret_val
    if status_code not in (200, 201):
        raise HTTPException(
            status_code=status_code, 
            detail=msg
        )
    else:
        status_code = httpCodes.HTTP_200_OK
        ret_val = configuration
        
    #ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_DATABASE_CLIENT_CONFIGURATION,
                                #api_endpoint_method=opasCentralDBLib.API_ENDPOINT_METHOD_POST, 
                                #session_info=session_info, 
                                #params=request.url._url,
                                #status_message=opasCentralDBLib.API_STATUS_SUCCESS, 
                                #return_status_code = status_code
                                #)
    return ret_val

#-----------------------------------------------------------------------------
@app.put("/v2/Client/Configuration/", response_model=models.ClientConfigList, response_model_exclude_unset=True, tags=["Client"], summary=opasConfig.ENDPOINT_SUMMARY_SAVEORREPLACE_CONFIGURATION, status_code=201)
async def client_update_configuration(response: Response, 
                                      request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),
                                      configuration:models.ClientConfigList=Body(None, embed=False, title=opasConfig.TITLE_ADMINCONFIG, decription=opasConfig.DESCRIPTION_ADMINCONFIG), # allows full specification
                                      client_id:int=Depends(get_client_id), 
                                      client_session:str= Depends(get_client_session), 
                                      api_key: APIKey = Depends(get_api_key)
                                      ):

    """
    ## Function
       ### Update global configuration settings for the client.
       
       Identical in all other ways to the POST call, in this case, if the configname already exists for that client app, the previous settings
       are overwritten.

    ## Return Type
       models.ClientConfigList (one or more configs)

    ## Status
       This endpoint is working.

    ## Sample Call
         /v2/Client/Configuration/

    ## Notes
       ### Requires API key
       ### Requires client_id in the header (use 'client-id' in call header)
       
    ## Potential Errors
       N/A

    """
    #  api_key needs to be supplied, e.g., in the header
    #  set database for confg, overwriting previous
    #  return current config (old if it fails).

    caller_name = "client_update_configuration"
    #if opasConfig.DEBUG_TRACE: print(caller_name)

    opasDocPermissions.verify_header(request, "ClientUpdateConfig") # for debugging client call
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")
    msg = ""

    ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)

    # ensure user is admin
    ret_val = None
    # see if it exists
    configName = configuration.configList[0].configName
    curr_config = ocd.get_client_config(client_id=client_id, 
                                        client_config_name=configName
                                       )    

    #if 1: # for now, just use API_KEY as the requirement.  Later admin?  if ocd.verify_admin(session_info):
    try:
        status_code, msg = ocd.save_client_config(session_id=session_info.session_id,
                                                  client_id=client_id, 
                                                  client_configuration=configuration,
                                                  replace=True)
    except Exception as e:
        logger.error(f"ConfigError: Trapped error saving client config: {e}")

    if status_code not in (200, 201):
        logger.error(f"ConfigError: HTTPException Called: {status_code}: {msg}")
        raise HTTPException(
            status_code=status_code, # HTTP_xxx
            detail=msg
        )
    else:
        if curr_config is None: # == ClientConfigList(configList=[])
            # didn't exist, so return 201
            response.status_code = httpCodes.HTTP_201_CREATED   
        else:
            response.status_code = httpCodes.HTTP_200_OK       # already there, updated, return 200
            
        ret_val = configuration

    #ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_DATABASE_CLIENT_CONFIGURATION,
                                #api_endpoint_method=opasCentralDBLib.API_ENDPOINT_METHOD_PUT, 
                                #session_info=session_info, 
                                #params=request.url._url,
                                #status_message=opasCentralDBLib.API_STATUS_SUCCESS, 
                                #return_status_code = status_code
                                #)
    return ret_val

#-----------------------------------------------------------------------------
@app.get("/v2/Client/Configuration/", response_model=models.ClientConfigList, response_model_exclude_unset=True, tags=["Client"], summary=opasConfig.ENDPOINT_SUMMARY_GET_CONFIGURATION)
async def client_get_configuration(response: Response, 
                                   request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),
                                   configname:str=Query(..., title=opasConfig.TITLE_ADMINCONFIGNAME, description=opasConfig.DESCRIPTION_ADMINCONFIGNAME, min_length=4),
                                   api_key: APIKey = Depends(get_api_key), 
                                   client_id:int=Depends(get_client_id), 
                                   client_session:str= Depends(get_client_session)
                                   ): 

    """
    ## Function
       ### Return saved configuration settings for the client_id (use 'client-id' in call header)

       Client-specific Administrative function to store the global settings for the client app.
       A client can store multiple settings under different names.

       TODO: Consider whether to require logging in as an admin to use this feature.
       (first need to consider whether the app would also be logged into the central auth (e.g., PaDS)
       is made, since the only admin login is local login!)

    ## Return Type
       models.ClientConfigList (one or more configs)

    ## Status
       This endpoint is working.

    ## Sample Call
       Return a single config
         /v2/Client/Configuration/?configname="test_client_test_1"

       Return multiple configs
         /v2/Client/Configuration/?configname="test_client_test_1, test_client_test_2"

    ## Notes
       ### Requires API key
       ### Requires client_id in the header (use 'client-id' in call header)
       
    ## Potential Errors
       N/A

    """
    caller_name = "client_get_configuration"
    #if opasConfig.DEBUG_TRACE: print(caller_name)

    # maybe no session id when they get this, so don't check here
    # opasDocPermissions.verify_header(request, "ClientGetConfig") # for debugging client call
    ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)

    # for now, just use API_KEY as the requirement.  Later admin?  
    ret_val = ocd.get_client_config(client_id=client_id,
                                    client_config_name=configname)


    if ret_val is None:
        status_code = httpCodes.HTTP_404_NOT_FOUND
        raise HTTPException(
            status_code=httpCodes.HTTP_404_NOT_FOUND, 
            detail=f"Configname {configname} Not found"
        )
    else:
        status_code = httpCodes.HTTP_200_OK

    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")
    logger.debug(f"Client/Config Endpoint (not recorded): Status Code: {status_code}")
    #ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_DATABASE_CLIENT_CONFIGURATION,
                                #api_endpoint_method=opasCentralDBLib.API_ENDPOINT_METHOD_GET, 
                                #session_info=session_info, 
                                #params=request.url._url,
                                #status_message=opasCentralDBLib.API_STATUS_SUCCESS, 
                                #return_status_code = status_code
                                #)

    #  return it.
    return ret_val

#-----------------------------------------------------------------------------
@app.delete("/v2/Client/Configuration/", response_model=models.ClientConfigList, response_model_exclude_unset=True, tags=["Client"], summary=opasConfig.ENDPOINT_SUMMARY_DELETE_CONFIGURATION, status_code=200)
async def client_del_configuration(response: Response, 
                                   request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),
                                   configname:str=Query(..., title=opasConfig.TITLE_ADMINCONFIGNAME, description=opasConfig.DESCRIPTION_ADMINCONFIGNAME, min_length=4),
                                   client_id:int=Depends(get_client_id), 
                                   client_session:str= Depends(get_client_session), 
                                   api_key: APIKey = Depends(get_api_key)
                                   ): 

    """
    ## Function
       <b>Delete the specified configuration or configurations (settings) for the specified client_id</b>

    ## Return Type
       models.ClientConfigList (one or more configs, the ones deleted) 

    ## Status
       This endpoint is working.

    ## Sample Call
        Delete a single config
          /v2/Client/Configuration/?configname="test_client_test_1"

        Delete multiple configs
          /v2/Client/Configuration/?configname="test_client_test_1, test_client_test_2"

    ## Notes
       ### Requires API key
       ### Requires client_id in the header (use 'client-id' in call header)
       
    ## Potential Errors
       N/A

    """
    caller_name = "client_del_configuration"
    #if opasConfig.DEBUG_TRACE: print(caller_name)
    
    opasDocPermissions.verify_header(request, caller_name) # for debugging client call
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")

    ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)

    #for now, just use API_KEY as the requirement.  Later admin?
    ret_val = ocd.del_client_config(client_id=client_id,
                                    client_config_name=configname)

    if ret_val is None:
        raise HTTPException(
            status_code=httpCodes.HTTP_404_NOT_FOUND, 
            detail=f"Configname {configname} Not found"
        )        

    #ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_DATABASE_CLIENT_CONFIGURATION,
                                #api_endpoint_method=opasCentralDBLib.API_ENDPOINT_METHOD_DELETE, 
                                #session_info=session_info, 
                                #params=request.url._url,
                                #status_message=opasCentralDBLib.API_STATUS_SUCCESS
                                #)
    #  return it.
    return ret_val

#-----------------------------------------------------------------------------
@app.get("/v2/Session/BasicLogin/", response_model=models.SessionInfo, response_model_exclude_unset=True, tags=["Session"], summary=opasConfig.ENDPOINT_SUMMARY_LOGIN_BASIC)
def session_login_basic(response: Response, 
                        request: Request,
                        client_id:int=Depends(get_client_id),
                        session_info=Depends(login_via_pads)
                        ):
    """
    ## Function
       <b>Basic login Authentication.</b>

       Supply username and password in the header which is secure in https.

    ## Return Type
       models.SessionInfo

    ## Status
       This endpoint is working.

    ## Sample Call
         /v2/Session/BasicLogin/

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    caller_name = "[v2/Session/BasicLogin]"   
    if opasConfig.DEBUG_TRACE: print(caller_name)

    session_id = session_info.session_id
    session_info.api_direct_login = True
    if session_info is not None:
        ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=session_id, client_id=client_id, caller_name=caller_name)
        # Save it for later; most importantly, overwrite any existing cookie!
        if session_info.authenticated:
            logger.info("Successful basic login - saved OpasSessionID Cookie")
            opasAPISupportLib.save_opas_session_cookie(request, response, session_id)
            #opas_session_cookie = request.cookies.get(opasConfig.OPASSESSIONID, None)
            session_info.api_direct_login = True

        if session_info.is_valid_login:
            response.set_cookie(
                opasConfig.OPASSESSIONID,
                value=f"{session_id}",
                domain=localsecrets.COOKIE_DOMAIN
            )
                       
    else:
        logger.error(f"LoginError: Bad login")
        raise HTTPException(
            status_code=httpCodes.HTTP_400_BAD_REQUEST,
            detail=ERR_MSG_SESSION_ID_ERROR
        )

    return session_info

#-----------------------------------------------------------------------------
@app.get("/v2/Session/Login/", response_model=models.SessionInfo, response_model_exclude_unset=True, tags=["Session"], summary=opasConfig.ENDPOINT_SUMMARY_LOGIN) # I like it under Users so I did them both.
def session_login(response: Response, 
                  request: Request,
                  username=None, 
                  password=None, 
                  grant_type=None, 
                  client_id:int=Depends(get_client_id), 
                  client_session:str= Depends(get_client_session)
                  ):
    """
    ## Function
       ### Insecure login Authentication.
    
       Supply username and password.
    
    ## Return Type
       models.SessionInfo
    
    ## Status
       This endpoint is working.
    
    ## Sample Call
         /v2/Session/BasicLogin/

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    # for debugging client call
    opasDocPermissions.verify_header(request, "Login")
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")

    opas_session_cookie = request.cookies.get(opasConfig.OPASSESSIONID, None)
    if opas_session_cookie != client_session and opas_session_cookie is not None:
        # logout of any opas session
        opasDocPermissions.authserver_logout(opas_session_cookie, response=response)

    pads_session_info = opasDocPermissions.authserver_login(username=username,
                                                            password=password,
                                                            session_id=client_session) # don't pass session
    # New session id, need to get rest of session_info (below)
    session_id = pads_session_info.SessionId

    if pads_session_info.IsValidLogon != True:
        if pads_session_info.pads_disposition is not None:
            detail = f"{pads_session_info.pads_disposition}"
        else:
            detail = ERR_MSG_BAD_LOGIN_CREDENTIALS + f" {pads_session_info.pads_status_response}"

        raise HTTPException(
            status_code=httpCodes.HTTP_401_UNAUTHORIZED, 
            detail=detail 
        )
    else:
        # need to do this, saves to db, but we're not directly using return values here
        session_info = opasDocPermissions.get_authserver_session_info(session_id,
                                                                      client_id,
                                                                      pads_session_info=pads_session_info,
                                                                      request=request)
        
        # Save it for eating later; most importantly, overwrite any existing cookie!
        #opas_session_cookie = request.cookies.get(opasConfig.OPASSESSIONID, None)
        if session_info.is_valid_login:
            response.set_cookie(
                opasConfig.OPASSESSIONID,
                value=f"{session_id}",
                domain=localsecrets.COOKIE_DOMAIN
            )
            #session_info.api_direct_login = True
            logger.debug("Successful login - saved OpasSessionID Cookie")

    try:
        login_return_item = models.LoginReturnItem(token_type = "bearer", 
                                                   session_id = session_id,
                                                   authenticated=pads_session_info.IsValidLogon,
                                                   error_message = None,
                                                   scope = None
                                                   )
    except ValidationError as e:
        logger.error(f"LoginError: Validation error: {e.json()}")             
        detail = ERR_MSG_VALIDATION_ERROR + f" {e}"
        # Solr Error
        raise HTTPException(
            status_code=httpCodes.HTTP_400_BAD_REQUEST, 
            detail=detail 
        )    
   
    return login_return_item

#-----------------------------------------------------------------------------
@app.get("/v2/Session/Logout/", response_model_exclude_unset=True, tags=["Session"], summary=opasConfig.ENDPOINT_SUMMARY_LOGOUT) # I like it under Users so I did them both.
def session_logout_user(response: Response, 
                        request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST), 
                        client_id:int=Depends(get_client_id), 
                        client_session:str= Depends(get_client_session)
                        ):  
    """
    ## Function
       ### Close the user's session, and log them out.
       
    ## Return Type
       models.SessionInfo

    ## Status
       This endpoint is working.

    ## Sample Call
         /v2/Session/Logout/

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """

    session_id = client_session
    ocd = opasCentralDBLib.opasCentralDB()
    if session_id is not None and session_id != "":
        session_info = ocd.get_session_from_db(session_id)
        if session_info is not None:
            #direct_login = session_info.api_direct_login
            session_end_time = datetime.utcfromtimestamp(time.time())
            if session_info is not None:
                session_info.session_end = session_end_time
                ocd.end_session(session_id=session_id)
    
            #if direct_login:
            response.delete_cookie(key=opasConfig.OPASSESSIONID,path="/", domain=localsecrets.COOKIE_DOMAIN)
            opasDocPermissions.authserver_logout(session_id, request=request, response=response)

        # logged out
        session_info = models.SessionInfo(session_id=session_id)
    return session_info

#-----------------------------------------------------------------------------
@app.get("/v2/Session/Status/", response_model=models.ServerStatusItem, response_model_exclude_unset=True, tags=["Session"], summary=opasConfig.ENDPOINT_SUMMARY_SERVER_STATUS)
async def session_status(response: Response, 
                         request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),
                         moreinfo: bool=Query(False, title=opasConfig.TITLE_MOREINFO, description=opasConfig.DESCRIPTION_MOREINFO),
                         client_id:int=Depends(get_client_id), 
                         client_session:str= Depends(get_client_session)
                         ):
    """
    ## Function
       ### Return the status of the database and text server.  Some field returns depend on the user's security level.

    ## Return Type
       models.ServerStatusItem (includes extra fields for admins)

    ## Status
       Status: Working

    ## Sample Call
         /v2/Session/Status/

    ## Notes
       2020-09-09 - client-session (id) is not longer needed except if extended admin information is required

    ## Potential Errors
       N/A

    """
    global text_server_ver # solr ver
    global database_update_date # database last update date
    admin = False
    caller_name = "[v2/Session/Status]"
    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name} {client_session}: ")

    # see if user is an admin
    if client_session is not None:
        ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)
        if ocd.verify_admin(session_info):
            admin = True
    else:
        ocd = opasCentralDBLib.opasCentralDB()
        
    db_ok = ocd.open_connection()
    solr_ok = opasPySolrLib.check_solr_docs_connection()
    config_name = None
    mysql_ver = None
    hierarchical_server_ver = f"{text_server_ver}/{__version__}"
    
    if admin:
        try:
            sitemap_path = localsecrets.SITEMAP_PATH
        except Exception as e: 
            sitemap_path = "Not Set in localsecrets!"
            logger.error(f"SITEMAP_PATH needs to be set ({e}).") # added for setup error notice 2022-06-06

        try:
            google_metadata_path = localsecrets.GOOGLE_METADATA_PATH
        except Exception as e: 
            google_metadata_path = "Not Set in localsecrets!"
            logger.error(f"GOOGLE_METADATA_PATH needs to be set ({e}).") # added for setup error notice 2022-06-06

        try:
            pdf_originals_path = localsecrets.PDF_ORIGINALS_PATH
        except Exception as e: 
            pdf_originals_path = "Not Set in localsecrets!"
            logger.error(f"PDF_ORIGINALS_PATH needs to be set ({e}).") # added for setup error notice 2022-06-06
            
        try:
            image_source_path = localsecrets.IMAGE_SOURCE_PATH
        except Exception as e: 
            image_source_path = "Not Set in localsecrets!"
            logger.error(f"IMAGE_SOURCE_PATH needs to be set ({e}).") # added for setup error notice 2022-06-06

        try:
            image_expert_picks_path = localsecrets.IMAGE_EXPERT_PICKS_PATH
        except Exception as e: 
            image_expert_picks_path = "Not Set in localsecrets!"
            logger.error(f"IMAGE_EXPERT_PICKS_PATH needs to be set ({e}).") # added for setup error notice 2022-06-06

        try:
            xml_originals_path = localsecrets.XML_ORIGINALS_PATH
        except Exception as e: 
            xml_originals_path = "Not Set in localsecrets!"
            logger.error(f"XML_ORIGINALS_PATH needs to be set ({e}).") # added for setup error notice 2022-06-06

        config_name = localsecrets.CONFIG
        mysql_ver = ocd.get_mysql_version()
        try:
            server_status_item = models.ServerStatusItem(text_server_ok = solr_ok,
                                                         db_server_ok = db_ok,
                                                         dataSource = opasConfig.DATA_SOURCE + database_update_date, 
                                                         user_ip = request.client.host,
                                                         timeStamp = datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%M:%SZ'), 
                                                         text_server_version = hierarchical_server_ver,
                                                         serverContent=opasPySolrLib.metadata_get_document_statistics(session_info),
                                                         session_id=client_session, 
                                                         # admin only fields
                                                         text_server_url = localsecrets.SOLRURL,
                                                         opas_version = __version__, 
                                                         db_server_url = localsecrets.DBHOST,
                                                         db_server_version = mysql_ver,
                                                         sitemap_path = sitemap_path,
                                                         google_metadata_path = google_metadata_path,
                                                         pdf_originals_path = pdf_originals_path,
                                                         image_source_path = image_source_path,
                                                         image_expert_picks_path = image_expert_picks_path,
                                                         xml_originals_path = xml_originals_path,
                                                         #cors_regex=localsecrets.CORS_REGEX, # see moreinfo
                                                         #library_versions=library_versions,  # see moreinfo
                                                         config_name = config_name
                                                         )
            
            if moreinfo:
                import pydantic
                import starlette
                import pysolr
                import fastapi
                import lxml
                sql_library_version = ocd.get_mysql_version()
                library_versions = {"mysql.connector": sql_library_version,
                                    "fastapi": fastapi.__version__,
                                    "pysolr": pysolr.__version__,
                                    "pydantic": pydantic.version.VERSION,
                                    "starlette": starlette.__version__,
                                    "lxml":lxml.__version__,
                                    "uvicorn": uvicorn.__version__, 
                                   }
                server_status_item.library_versions = library_versions
                server_status_item.cors_regex = localsecrets.CORS_REGEX

        except ValidationError as e:
            logger.error("ValidationError", e.json())
            raise HTTPException(
                status_code=httpCodes.HTTP_400_BAD_REQUEST,
                detail=ERR_MSG_STATUS_DATA_ISSUE
            )
    else:
        try:
            if moreinfo:
                server_status_item = models.ServerStatusItem(text_server_ok = solr_ok,
                                                             db_server_ok = db_ok,
                                                             dataSource = opasConfig.DATA_SOURCE + database_update_date, 
                                                             text_server_version = hierarchical_server_ver,
                                                             opas_version = __version__, 
                                                             serverContent=opasPySolrLib.metadata_get_document_statistics(session_info), 
                                                             user_ip = request.client.host,
                                                             session_id=client_session, 
                                                             timeStamp = datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%M:%SZ'), 
                                                             )
            else:
                server_status_item = models.ServerStatusItem(text_server_ok = solr_ok,
                                                             db_server_ok = db_ok,
                                                             dataSource = opasConfig.DATA_SOURCE + database_update_date, 
                                                             text_server_version = hierarchical_server_ver,
                                                             opas_version = __version__, 
                                                             user_ip = request.client.host,
                                                             session_id=client_session, 
                                                             timeStamp = datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%M:%SZ'), 
                                                             )
                
        except ValidationError as e:
            logger.error("ValidationError", e.json())
            raise HTTPException(
                status_code=httpCodes.HTTP_400_BAD_REQUEST,
                detail=ERR_MSG_STATUS_DATA_ISSUE
            )

    ocd.close_connection()
    return server_status_item

#-----------------------------------------------------------------------------
@app.get("/v2/Session/WhoAmI/", response_model=models.SessionInfo, response_model_exclude_unset=False, tags=["Session"], summary=opasConfig.ENDPOINT_SUMMARY_WHO_AM_I)
async def session_whoami(response: Response,
                         request: Request,
                         client_id:int=Depends(get_client_id), 
                         client_session:str= Depends(get_client_session)
                         ):
    """
    ## Function
       ### Return data about the session

    ## Return Type
       models.SessionInfo

    ## Status
       This endpoint is working.

    ## Sample Call
         /v2/Session/WhoAmI/

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    caller_name = "[v2/Session/WhoAmI]"   
    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name} {client_session}: ")

    opasDocPermissions.verify_header(request, caller_name) # for debugging client call
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")

    if client_session is not None:
        ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)
    else:
        logger.error(f"WhoAmIError: Client-Session ID not provided")
        raise HTTPException(
            status_code=httpCodes.HTTP_400_BAD_REQUEST,
            detail=ERR_MSG_SESSION_ID_ERROR
        )

    return(session_info)

#-----------------------------------------------------------------------------
@app.post("/v2/Database/AdvancedSearch/", response_model=Union[models.DocumentList, models.ErrorReturn], response_model_exclude_unset=True, response_model_exclude_none=True, tags=["Database"], summary=opasConfig.ENDPOINT_SUMMARY_SEARCH_ADVANCED)  #  removed for now: response_model=models.DocumentList, 
async def database_advanced_search(response: Response, 
                                   request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),
                                   apimode: str=Header(None, title=opasConfig.TITLE_API_MODE, description=opasConfig.DESCRIPTION_API_MODE), 
                                   # Request body parameters - allows full specification of parameters in the body
                                   solrqueryspec: models.SolrQuerySpec=Body(None, embed=True),
                                   # Query parameters
                                   advanced_query: str=Query(None, title=opasConfig.TITLE_ADVANCEDSEARCHQUERY, description=opasConfig.DESCRIPTION_ADVANCEDSEARCHQUERY),
                                   filter_query: str=Query(None, title=opasConfig.TITLE_ADVANCEDSEARCHFILTERQUERY, description=opasConfig.DESCRIPTION_ADVANCEDSEARCHFILTERQUERY),
                                   return_fields: str=Query(None, title=opasConfig.TITLE_RETURN_FIELDS, description=opasConfig.DESCRIPTION_RETURN_FIELDS),
                                   highlight_fields: str=Query("text_xml", title=opasConfig.TITLE_HIGHLIGHT_FIELDS, description=opasConfig.DESCRIPTION_HIGHLIGHT_FIELDS),
                                   def_type: str=Query("lucene", title=opasConfig.TITLE_DEF_TYPE, description=opasConfig.DESCRIPTION_DEF_TYPE),
                                   facet_fields: str=Query(None, title=opasConfig.TITLE_FACETFIELDS, description=opasConfig.DESCRIPTION_FACETFIELDS), 
                                   sort: str=Query("score desc", title=opasConfig.TITLE_SORT, description=opasConfig.DESCRIPTION_SORT),
                                   limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                                   offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET), 
                                   client_id:int=Depends(get_client_id), 
                                   client_session:str= Depends(get_client_session)
                                   ):
    """
    ## Function
    ### Advanced search in Solr query syntax.

    IMPORTANT NOTE: This endpoint is intended for client (user interface) developers to take advantage of the
    full Solr query functionality, while still returning the usual OPAS DocumentList return model, including conversion
    from XML to HTML.  If the developer wishes to specify the fields for return data, endpoint /v2/ExtendedSearch should be used.  
    
    This endpoint is not used, for example, in PEP-Easy because it isn't needed for basic query and retrieval operations which are built in
    to other endpoints.  PEP-Easy uses the v1 API exclusively (and not the Solr API or the v2 API).
    
    The documentation below on how to use the advanced query is intimately tied to the schemas (data structure based on the PEP DTDs).
    But it's not possible to provide query examples and sample data returns without tying them to a schema.  
    
    Currently, this endpoint only queries the Docs core (database), which is the main content database. Other cores include the Glossary core, the Authors core,
    and the References core.  Endpoint /v2/ExtendedSearch should be used to query other cores.
    
    ### Nested document search.
    
    Nested document search is used with the PEP Solr Schemas to search with paragraphs
    (or bibliographic entries--references) being the "atomic" unit of search (the record, in common database terms),
    while also using the context of the parent element and whole document metadata to filter.  So for example, you can find matches
    in paragraphs in "dreams", and the scope of the boolean logic of word searches must fall within
    a single paragraph (or reference).
    
    Per the Solr API, a nested document search must ensure that the matches for the different levels don't overlap or an error will occur.
    To prevent this, and to provide selectivity of the different child types, the art_level and parent_tag fields are included, which
    can be used to select the child (art_level:2), and the parent (art_level:1) and parent_tag (see below).
    
    For example, these PEP DTD elements (tags) are indexed as nested, art_level:2 documents in the pepwebdocs core
    with their parent tags as field <i>parent_tag</i>.

        * para in "doc" - this is mapped to the entire doc, minus the reference section (PEP Requirements).
          Advanced query must use "(p_body OR p_summaries OR p_appxs)"
            -- Note "doc" is a mapped (convenience) name for non-advanced query types and expanded to "(p_body OR p_summaries OR p_appxs)".  The entire document would be (p_body OR p_summaries OR "biblios" OR p_appxs)
        * para in "headings" - for advanced query use actual schema name "(p_heading)"
        * para in "quotes" - for advanced query use actual schema name "(p_quote)"
        * para in "dreams" - for advanced query use actual schema name "(p_dream)" 
        * para in "poems" - for advanced query use actual schema name "(p_poem)"
        * para in "notes" - for advanced query use actual schema name "(p_note)"
        * para in "dialogs" - for advanced query use actual schema name "(p_dialog)"
        * para in "panels" - for advanced query use actual schema name "(p_panel)"
        * para in "captions" - for advanced query use actual schema name "(p_caption)"
        * para in "biblios" - for advanced query use actual schema name "(p_biblio)"
            -- (each be/binc under PEP element bib, i.e., the references)
        * para in "appendixes" - for advanced query use actual schema name "(p_appendixes)"
        * para in "summaries" - for advanced query use actual schema name "(p_summaries)"

    Note that para is always the field name for convenience, but in some cases, it's logically something else, like a reference
    (which is comprised of an xml element be rather than xml element para), or even just cdata
    in the element

    Here's an example of a note "child" record:

        "id":"IJAPS.013.0191A.67",
        "parent_tag":"p_note",
        "para":"<p><i>Wikipedia</i>, Enlightenment, p. 1, emphasis added.</p>",
        "_version_":1655037031541112832,
        "art_level":2

    Solr keeps the relation to the parent for us, for which we use the special query syntax below.

    For SE and GW, child records include concordance data. For example:

        "id":"GW.001.0003A.1",
        "para":"<p lgrid=\"GWA3a3\" lgrx=\"SEA115a11\" lgrtype=\"GroupIDTrans\" lang=\"de\">Ich entschließe mich hier, ... einen einzelnen ...",
        "para_lgrid":"GWA3a3",
        "para_lgrx":["SEA115a11"]


    #### Sample nested queries

    <b>Find documents with paragraphs which contain "successful therapy" and "methods":</b>

    `{!parent which='art_level:1'} (art_level:2 AND parent_tag:(p_body OR p_summaries OR p_appxs) AND para:("successful therapy" AND methods))`

    <b>Find documents with paragraphs which contain Gabbard but NOT impact (the minus is shorthand for NOT):</b>

    `{!parent which='art_level:1'} (art_level:2 AND parent_tag:(p_body OR p_summaries OR p_appxs) AND para:(Gabbard AND -impact))`

    <b>Find documents with the words flying and falling in the same paragraph in a dream:</b>

    `{!parent which="art_level:1"} art_level:2 AND parent_tag:p_dream AND para:(flying AND falling)`

    To return the matching "full" paras as records, you can directly search the child documents.  However, you will not
    get the parent document data as part of the return in this case (the fields will be in the return records, but the data will just be 'null').
    The matching paras will be in the field para in the return records, and there will be no marked hits.

    ##### Child Only Query Example

    `parent_tag:p_dream AND para:(flying AND wings)`

    ##### Child Only Return data example (abbreviated) from above example (null fields removed)

       ```
       "response":{"numFound":3,"start":0,"maxScore":11.844459,"docs":[
           {
             "parent_tag":"p_dream",
             "para":"<p><i>I'm on an island beach looking out over a large body of water. High above the beach just making its way out over the water is an enormous flying contraption whose wings appear to be made of silk that is beige or golden in color. Roosting on the wings are exotic birds whose plumage is also this color. It's moving slowly out over the water. It's a remarkable sight</i>.</p>",
             "art_level":2,
             "score":11.844459},
           {
             "parent_tag":"p_dream",
             "para":"<p>I was with my brother, going to a meeting. I felt sick, and soiled my pants. It was a mess. Then I showed him where he could get fresh clothes—odd, <impx type=\"TERM2\" rx=\"ZBK.069.0001c.YN0004867612400\" grpname=\"CAUSE\">cause</impx> I was the one that made the mess.—Then we're back at the meeting, where I should be able to find my brother, but I can't. I'm looking all over, moving faster, and then…. I started to fly! A woman applauded and encouraged my flying. And I said “I like to do it this smooth way, don't need to flap wings harder.” Then I was flying over a college campus, a festive scene, but then I realized I'd forgotten my brother and felt guilty.</p>",
             "art_level":2,
             "score":10.832498},
           {
             "parent_tag":"p_dream",
             "para":"<p2>eyes, but at the same time I am full of joy, because it is a beautiful <impx type=\"TERM2\" rx=\"ZBK.069.0001d.YN0013300865870\" grpname=\"DEATH\">death</impx>. At this <impx type=\"TERM2\" rx=\"ZBK.069.0001m.YP0001417964900\" grpname=\"MOMENT\">moment</impx> the sliding stops, and I see someone who is flying. He glides downward several times; then he again wings upwards. I am watching him at these performances, and a young man with a pince nez is also looking up. I am no longer alone. He is singing a very nice Russian song, which I have heard already. As the aviator disappears, the scene changes and I am no longer in the N<su>***</su> Valley in G<su>***</su> but in the S<su>***</su> Park in B<su>***</su>, and I am singing or humming the song too. He asks me whether he may accompany me. I tell him that he may walk with me to the optometrist. A piece of my lorgnette had broken off, and I want to have it repaired. I start to cross the street at the S<su>***</su> Park, but the young man has disappeared. Then I awake.</p2>",
             "art_level":2,
             "score":8.666403
           }
           ]
       ```

    ### Full Control using the Post Body
    
    The Post body can be used instead of query paramaters, and uses structures that give you more control over the search and results.
    
    The full body structure is shown by default in the body area of the interactive docs for the API.  Of course, in many fields the data
    structure shows data types in the value area rather than the values you might use, so you will need to replace those with
    your own data.
    
    #### Simple Examples of the post body-based query spec
    
    {
       "solrqueryspec": {
            "returnFormat": "HTML",
            "facetMinCount": 1,
            "solrQuery": {
                "searchQ": "{!parent which='art_level:1'} art_level:2 AND parent_tag:p_dream AND para:mother"
            },
            "solrQueryOpts": {
                 "qOper": "AND",
                 "defType": "edismax",
                 "hl": "true",
                 "hlFields": "text_xml",
                 "hlMethod": "unified",
                 "hlFragsize": "0",
                 "hlMaxAnalyzedChars": 2520000,
                 "hlMaxKWICReturns": 5,
                 "hlMultiterm": "true",
                 "hlTagPost": "@@@@#",
                 "hlTagPre": "#@@@@",
                 "hlUsePhraseHighlighter": "true",
                 "queryDebug": "on",
                 "moreLikeThisCount": 5
            }
        }
    }

    ### Thesaurus matching

    Solr thesaurus matching is either on or off for a field, it's not something you can specify as a query parameter to Solr.
    So for thesaurus matches, where desirable, a secondary field is defined with the suffix _syn that has the PEP thesaurus enabled.

    This currently includes
        * text -> text_syn
        * title -> title_syn
        * para -> para_syn

    ### Final Notes

       * Solr advanced queries are case sensitive. The boolean connector AND must be in caps.
       * important to understand the applicable schema to query (unless you only search the default field text)

    ## Return Type
       models.DocumentList or models.ErrorReturn

    ## Status
       Status: Still in Development and testing

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    caller_name = "[v2/Database/AdvancedSearch]"
    #if opasConfig.DEBUG_TRACE: print(caller_name)

    opasDocPermissions.verify_header(request, caller_name) # for debugging client call
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")

    ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)
    # session_id = session_info.session_id
    
    try:
        apimode = apimode.lower()
    except:
        apimode = "production"

    if re.search(r"/Search/", request.url._url):
        logger.debug("Search Request: %s", request.url._url)

    #For the interactive docs, watch for the schema coming back and ignore if that's the case
    if solrqueryspec is not None:
        if solrqueryspec.label == "string":
            solrqueryspec = None

    solr_query_spec = \
        opasQueryHelper.parse_to_query_spec(solr_query_spec=solrqueryspec, 
                                            query=advanced_query, 
                                            filter_query = filter_query,
                                            def_type = def_type, # edisMax, disMax, or None
                                            summary_fields=return_fields, 
                                            highlight_fields = highlight_fields, 
                                            facet_fields = facet_fields, 
                                            sort = sort,
                                            limit=limit, 
                                            offset=offset, 
                                            req_url=request.url._url
                                            )
    # try the query
    ret_val, ret_status = search_text_qs(solr_query_spec,
                                         #authenticated=session_info.authenticated
                                         session_info=session_info,
                                         request=request,
                                         caller_name=caller_name
                                         )

    #  if there's a Solr server error in the call, it returns a non-200 ret_status[0]
    if ret_status[0] != httpCodes.HTTP_200_OK:
        #  throw an exception rather than return an object (which will fail)
        #TODO: should we let this fall through to log the endpoint even though there was an error?
        return ret_val # function returns - models.ErrorReturn
        #raise HTTPException(
            #status_code=ret_status[0], 
            #detail=f"Bad Solr Search Request. {ret_status[1].reason}:{ret_status[1].body}"
        #)
    else:
        if ret_val != {}:
            matches = len(ret_val.documentList.responseSet)
            ret_val.documentList.responseInfo.request = request.url._url
        else:
            matches = 0

        statusMsg = f"{matches} hits"
        logger.debug(statusMsg)

    # client_host = request.client.host
    ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_DATABASE_ADVANCEDSEARCH,
                                api_endpoint_method=opasCentralDBLib.API_ENDPOINT_METHOD_POST, 
                                session_info=session_info, 
                                params=request.url._url,
                                return_status_code = ret_status[0], 
                                status_message=statusMsg
                                )

    return ret_val

#---------------------------------------------------------------------------------------------------------
@app.post("/v2/Database/Glossary/Search/", response_model=models.DocumentList, response_model_exclude_unset=True, tags=["Database"], summary=opasConfig.ENDPOINT_SUMMARY_GLOSSARY_SEARCH_POST)
@app.get("/v2/Database/Glossary/Search/", response_model=models.DocumentList, response_model_exclude_unset=True, tags=["Database"], summary=opasConfig.ENDPOINT_SUMMARY_GLOSSARY_SEARCH)
async def database_glossary_search_v2(response: Response, 
                                      request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),
                                      # qtermlist is only for POST
                                      qtermlist: models.SolrQueryTermList=None, # allows full specification
                                      fulltext1: str=Query(None, title=opasConfig.TITLE_FULLTEXT1, description=opasConfig.DESCRIPTION_FULLTEXT1),
                                      sourcelangcode: str=Query("EN", title=opasConfig.TITLE_SOURCELANGCODE+" TITLE", description=opasConfig.DESCRIPTION_SOURCELANGCODE+" DESC"), 
                                      paratext: str=Query(None, title=opasConfig.TITLE_PARATEXT, description=opasConfig.DESCRIPTION_PARATEXT),
                                      parascope: str=Query("doc", title=opasConfig.TITLE_PARASCOPE, description=opasConfig.DESCRIPTION_PARASCOPE),
                                      synonyms: bool=Query(False, title=opasConfig.TITLE_SYNONYMS_BOOLEAN, description=opasConfig.DESCRIPTION_SYNONYMS_BOOLEAN),
                                      facetquery: str=Query(None, title=opasConfig.TITLE_FACETQUERY, description=opasConfig.DESCRIPTION_FACETQUERY),
                                      sort: str=Query("score desc", title=opasConfig.TITLE_SORT, description=opasConfig.DESCRIPTION_SORT),
                                      facetfields: str=Query(None, title=opasConfig.TITLE_FACETFIELDS, description=opasConfig.DESCRIPTION_FACETFIELDS), 
                                      formatrequested: str=Query("HTML", title=opasConfig.TITLE_RETURNFORMATS, description=opasConfig.DESCRIPTION_RETURNFORMATS),
                                      limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                                      highlightlimit: int=Query(opasConfig.DEFAULT_MAX_KWIC_RETURNS, title=opasConfig.TITLE_MAX_KWIC_COUNT, description=opasConfig.DESCRIPTION_MAX_KWIC_COUNT),
                                      offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET), 
                                      client_id:int=Depends(get_client_id), 
                                      client_session:str= Depends(get_client_session)
                                      ):
    """
    ## Function
       ### Search the glossary records in the doc core (not the glossary core -- it doesn't support sub paras important for full-text search).

       This is just a convenience function to search a specific book, the glossary (ZBK.069), in the doc core (pepwebdoc).

    ## Return Type
       models.DocumentList

    ## Status
       This endpoint is working.

    ## Sample Call
         /v2/Database/Glossary/Search/

    ## Notes
       To include a termlist (qtermlist), use POST.
       
       Sample qtermlist:
       ```
       {"qtermlist":
            {   "artLevel": 2, 
               "qt" :
               [
                       {
                               "parent":"quotes",
                               "field" : "para",
                               "words": "love",
                               "synonyms": "false"
                           },
                           {
                               "parent":"dreams",
                               "field" : "para",
                               "words": "sex",
                               "synonyms": "false"
                           }				
               ]
            }
        }
       ```

    ## Potential Errors
       When using GET and the interactive documentation to try out this endpoint, you must clear the "Request Body" field before submitting.
       The "Request Body" field only applies to POST, but the code is shared, which trips up the interface for interactive docs.
    
    """
    caller_name = "[v2/Database/Glossary/Search]"
    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name} {client_session}: ")
    
    opasDocPermissions.verify_header(request, "database_glossary_search_v2") # for debugging client call
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")
    ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)

    ret_val = await database_search(response,
                                    request,
                                    qtermlist=qtermlist,
                                    fulltext1=fulltext1,
                                    smarttext=None, 
                                    paratext=paratext, #  no advanced search. Only words, phrases, prox ~ op, and booleans allowed
                                    parascope=parascope,
                                    similarcount=0, 
                                    synonyms=synonyms,
                                    facetquery=None, 
                                    sourcename=None, 
                                    sourcecode="ZBK",
                                    volume="69",
                                    sourcetype=None, 
                                    sourcelangcode=sourcelangcode,
                                    articletype=None, 
                                    issue=None, 
                                    author=None, 
                                    title=None,
                                    startyear=None,
                                    endyear=None,
                                    citecount=None,
                                    viewcount=None,
                                    viewperiod=None,
                                    formatrequested=formatrequested, 
                                    highlightlimit=highlightlimit, 
                                    facetfields=facetfields, 
                                    sort=sort,
                                    limit=limit,
                                    offset=offset,
                                    client_session=client_session,
                                    client_id=client_id,
                                    override_endpoint_id=opasCentralDBLib.API_DATABASE_GLOSSARY_SEARCH
                                    )
    if ret_val != {}:
        matches = len(ret_val.documentList.responseSet)
    else:
        matches = 0

    statusMsg = f"{matches} hits"
    logger.debug(statusMsg)

    ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_DATABASE_GLOSSARY_SEARCH,
                                session_info=session_info, 
                                params=request.url._url,
                                status_message=statusMsg
                                )

    return ret_val

#---------------------------------------------------------------------------------------------------------
@app.post("/v2/Database/Search/", response_model=Union[models.DocumentList, models.ErrorReturn], response_model_exclude_unset=True, tags=["Database"], summary=opasConfig.ENDPOINT_SUMMARY_SEARCH_POST)
@app.get("/v2/Database/Search/", response_model=Union[models.DocumentList, models.ErrorReturn], response_model_exclude_unset=True, tags=["Database"], summary=opasConfig.ENDPOINT_SUMMARY_SEARCH_V2)
async def database_search(response: Response, 
                              request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),
                              #qtermlist only works with Post
                              qtermlist: models.SolrQueryTermList=Body(None, embed=True, title=opasConfig.TITLE_QTERMLIST, decription=opasConfig.DESCRIPTION_QTERMLIST), # allows full specification
                              fulltext1: str=Query(None, title=opasConfig.TITLE_FULLTEXT1, description=opasConfig.DESCRIPTION_FULLTEXT1),
                              smarttext: str=Query(None, title=opasConfig.TITLE_SMARTSEARCH, description=opasConfig.DESCRIPTION_SMARTSEARCH),
                              paratext: str=Query(None, title=opasConfig.TITLE_PARATEXT, description=opasConfig.DESCRIPTION_PARATEXT),
                              parascope: str=Query(None, title=opasConfig.TITLE_PARASCOPE, description=opasConfig.DESCRIPTION_PARASCOPE),
                              synonyms: bool=Query(False, title=opasConfig.TITLE_SYNONYMS_BOOLEAN, description=opasConfig.DESCRIPTION_SYNONYMS_BOOLEAN),
                              facetquery: str=Query(None, title=opasConfig.TITLE_FACETQUERY, description=opasConfig.DESCRIPTION_FACETQUERY),
                              # filters (Solr query filter)
                              sourcename: str=Query(None, title=opasConfig.TITLE_SOURCENAME, description=opasConfig.DESCRIPTION_SOURCENAME, min_length=2),  
                              sourcecode: str=Query(None, title=opasConfig.TITLE_SOURCECODE, description=opasConfig.DESCRIPTION_SOURCECODE, min_length=2), 
                              sourcetype: str=Query(None, title=opasConfig.TITLE_SOURCETYPE, description=opasConfig.DESCRIPTION_PARAM_SOURCETYPE), 
                              sourcelangcode: str=Query(None, min_length=2, title=opasConfig.TITLE_SOURCELANGCODE, description=opasConfig.DESCRIPTION_SOURCELANGCODE), 
                              volume: str=Query(None, title=opasConfig.TITLE_VOLUMENUMBER, description=opasConfig.DESCRIPTION_VOLUMENUMBER), 
                              issue: str=Query(None, title=opasConfig.TITLE_ISSUE, description=opasConfig.DESCRIPTION_ISSUE),
                              author: str=Query(None, title=opasConfig.TITLE_AUTHOR, description=opasConfig.DESCRIPTION_AUTHOR), 
                              title: str=Query(None, title=opasConfig.TITLE_TITLE, description=opasConfig.DESCRIPTION_TITLE),
                              articletype: str=Query(None, title=opasConfig.TITLE_ARTICLETYPE, description=opasConfig.DESCRIPTION_ARTICLETYPE),
                              startyear: str=Query(None, title=opasConfig.TITLE_STARTYEAR, description=opasConfig.DESCRIPTION_STARTYEAR), 
                              endyear: str=Query(None, title=opasConfig.TITLE_ENDYEAR, description=opasConfig.DESCRIPTION_ENDYEAR), 
                              citecount: str=Query(None, title=opasConfig.TITLE_CITECOUNT, description=opasConfig.DESCRIPTION_CITECOUNT),   
                              viewcount: str=Query(None, title=opasConfig.TITLE_VIEWCOUNT, description=opasConfig.DESCRIPTION_VIEWCOUNT),    
                              viewperiod: int=Query(4, title=opasConfig.TITLE_VIEWPERIOD, description=opasConfig.DESCRIPTION_VIEWPERIOD),     
                              # return set control (removed returnFields 2020-09-10)
                              #returnfields: str=Query(None, title="Fields to return (see limitations)", description="Comma separated list of field names"),
                              abstract:bool=Query(False, title="Return an abstract with each match", description="True to return an abstract"),
                              formatrequested: str=Query("HTML", title=opasConfig.TITLE_RETURNFORMATS, description=opasConfig.DESCRIPTION_RETURNFORMATS),
                              similarcount: int=Query(0, title=opasConfig.TITLE_SIMILARCOUNT, description=opasConfig.DESCRIPTION_SIMILARCOUNT),
                              highlightlimit: int=Query(opasConfig.DEFAULT_MAX_KWIC_RETURNS, title=opasConfig.TITLE_MAX_KWIC_COUNT, description=opasConfig.DESCRIPTION_MAX_KWIC_COUNT),
                              facetfields: str=Query(None, title=opasConfig.TITLE_FACETFIELDS, description=opasConfig.DESCRIPTION_FACETFIELDS), 
                              facetmincount: int=Query(1, description=opasConfig.DESCRIPTION_FACETMINCOUNT),
                              facetlimit: int=Query(15, description=opasConfig.DESCRIPTION_FACETLIMIT),
                              facetoffset: int=Query(0, description=opasConfig.DESCRIPTION_FACETOFFSET),
                              sort: str=Query("score desc", title=opasConfig.TITLE_SORT, description=opasConfig.DESCRIPTION_SORT),
                              limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                              offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET), 
                              client_id:int=Depends(get_client_id), 
                              client_session:str= Depends(get_client_session),
                              override_endpoint_id=opasCentralDBLib.API_DATABASE_SEARCH
                             ):
    """
    ## Function
       ### Search the database per one or more of the fields specified.

       Some of the fields should probably be deprecated, but for now, they support PEP-Easy,
       as configured to use the GVPi based PEP Server
       endyear isn't needed, because startyear can handle the ranges (and better than before).
       journal is also configured to take anything that would have otherwise been entered in sourcename

    ## Return Type
       models.DocumentList or models.ErrorReturn

    ## Status
       Status: Working

       - in perpetual development to extend and improve features
          - smarttext now detects title search under much more restrictive inputs
            due to user input where terrm search was favored
       - Other engines, e.g., disMax, edisMax also not yet implemented


    ## Sample Call
         /v2/Database/Search/?author=Blum&sourcecode=AOP&fulltext1=transference

    ## Notes
       - 2020-09-10 removed returnFields...covered in querySpec for POST version

    ## Limitations
       When using GET and the interactive documentation to try out this endpoint, you must clear the "Request Body" field before submitting.
       The "Request Body" field only applies to POST, but the code is shared, which trips up the interface for interactive docs.

    ## Potential Errors
       N/A

    """
    ts = time.time()
    caller_name = "[v2/Database/Search]"
    if opasConfig.DEBUG_TRACE:
        composite = f"smarttext: {smarttext} / paratext {paratext} / facetquery: {facetquery} "
        print(f"{datetime.now().time().isoformat()}: {caller_name} - {composite}")
    
    # get client_id and session directly
    client_id_from_header, client_session_from_header = opasDocPermissions.verify_header(request, "Search") # for debugging client call
    if client_session_from_header != client_session and (client_session_from_header == 2 or client_session_from_header == 3):
        logger.warning(f"*************[{client_id}:{client_session}] dependence vs direct [{client_id_from_header}:{client_session_from_header}]")

    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")
    errors, mod_args = opasQueryHelper.check_search_args( smarttext=smarttext,
                                                          fulltext1=fulltext1,
                                                          paratext=paratext,
                                                          author=author,
                                                          title=title,
                                                          startyear=startyear,
                                                          endyear=endyear
                                                        )
    if errors:
        detail = ERR_MSG_QUERY_FRAGMENT # "Query had too few characters or was unbalanced."
        logger.error(detail)
        raise HTTPException(
            status_code=opasConfig.httpCodes.HTTP_425_TOO_EARLY,
            detail=detail
        )
    
    ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)

    # session_id = session_info.session_id 

    if re.search(r"/Search/", request.url._url):
        logger.debug("Search Request: %s", request.url._url)

    if opasConfig.LOCAL_TRACE:
        if fulltext1 is not None:
            print("+****Trace: Client Search Fulltext1: '%s'" % fulltext1) # tracing
        if smarttext is not None:
            print("+****Trace: Client Search Smarttext: '%s'" % smarttext) # tracing
        if author is not None:
            print("+****Trace: Client Search Author: '%s'" % author) # tracing

    analysis_mode = False

    # don't set parascope, unless they set paratext and forgot to set parascope
    if paratext is not None and parascope is None:
        parascope = "doc"
        
    # current_year = datetime.utcfromtimestamp(time.time()).strftime('%Y')
    # this does intelligent processing of the query parameters and returns a smaller set of solr oriented         
    # params (per pydantic model SolrQuery), ready to use
    solr_query_spec = \
        opasQueryHelper.parse_search_query_parameters(solrQueryTermList=None,
                                                      source_name=sourcename,
                                                      source_code=sourcecode,
                                                      source_type=sourcetype,
                                                      source_lang_code=sourcelangcode,
                                                      paratext=mod_args.get("paratext", None), # search within paragraphs
                                                      parascope=parascope, # scope for par_search
                                                      similar_count=similarcount, # Turn on morelikethis for the search, return this many similar docs for each
                                                      fulltext1=mod_args.get("fulltext1", None),  # more flexible search, including fields, anywhere in the doc, across paras
                                                      smarttext=mod_args.get("smarttext", None), # experimental detection of what user wants to query
                                                      synonyms=synonyms, 
                                                      facetquery=facetquery, 
                                                      vol=volume,
                                                      issue=issue,
                                                      author=author,
                                                      title=title,
                                                      articletype=articletype, 
                                                      startyear=startyear,
                                                      endyear=endyear,
                                                      citecount=citecount,
                                                      viewcount=viewcount,
                                                      viewperiod=viewperiod,
                                                      highlightlimit=highlightlimit, 
                                                      facetfields=facetfields,
                                                      facetmincount=facetmincount,
                                                      facetlimit=facetlimit,
                                                      facetoffset=facetoffset,                                                        
                                                      abstract_requested=abstract,
                                                      formatrequested=formatrequested, 
                                                      limit=limit,
                                                      offset=offset,
                                                      sort=sort,
                                                      req_url = request.url._url
                                                      )

    ret_val, ret_status = search_text_qs(solr_query_spec, 
                                         extra_context_len=opasConfig.DEFAULT_KWIC_CONTENT_LENGTH,
                                         limit=limit,
                                         offset=offset,
                                         session_info=session_info, 
                                         request=request,
                                         caller_name=caller_name
                                         )

    #  if there's a Solr server error in the call, it returns a non-200 ret_status[0]
    if ret_status[0] != httpCodes.HTTP_200_OK:
        #  throw an exception rather than return an object (which will fail)
        try:
            detail = ERR_MSG_BAD_SEARCH_REQUEST + f" {ret_status[1]['reason']}:{ret_status[1]['body']}"
        except Exception as e:
            logger.warning(e)
            detail = ERR_MSG_BAD_SEARCH_REQUEST # "Bad Search Request"
            
        raise HTTPException(
            status_code=ret_status[0],
            detail=detail
        )

    if ret_val != {}:
        matches = len(ret_val.documentList.responseSet)
        # ret_val.documentList.responseInfo.request = request.url._url
    else:
        matches = 0

    logger.debug(f"....matches = {matches}")
    # fill in additional return structure status info
    statusMsg = f"{matches} hits; similar documents per result requested:{similarcount}; queryAnalysis: {analysis_mode}"
    logger.debug("Done with search.")

    item_of_interest = opasAPISupportLib.get_query_item_of_interest(solrQuery=solr_query_spec.solrQuery)
    
    ocd.record_session_endpoint(api_endpoint_id=override_endpoint_id, # 20211008 - defaults to this search call (41)
                                session_info=session_info,
                                item_of_interest=item_of_interest, 
                                params=request.url._url,
                                return_status_code = ret_status[0], 
                                status_message=statusMsg
                                )

    log_endpoint_time(request, ts=ts, level="debug")
    return ret_val

#---------------------------------------------------------------------------------------------------------
@app.get("/v2/Database/SearchAnalysis/", response_model=Union[models.TermIndex, models.ErrorReturn], response_model_exclude_unset=True, tags=["Database"], summary=opasConfig.ENDPOINT_SUMMARY_SEARCH_ANALYSIS)  #  remove validation response_model=models.DocumentList, 
@app.post("/v2/Database/SearchAnalysis/", response_model=Union[models.TermIndex, models.ErrorReturn], response_model_exclude_unset=True, tags=["Database"], summary=opasConfig.ENDPOINT_SUMMARY_SEARCH_ANALYSIS)  #  remove validation response_model=models.DocumentList, 
def database_searchanalysis(response: Response, 
                            request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),
                            #qtermlist is used only for post
                            qtermlist: models.SolrQueryTermList=Body(None, embed=True, title=opasConfig.TITLE_QTERMLIST, decription=opasConfig.DESCRIPTION_QTERMLIST), # allows full specification
                            fulltext1: str=Query(None, title=opasConfig.TITLE_FULLTEXT1, description=opasConfig.DESCRIPTION_FULLTEXT1),
                            smarttext: str=Query(None, title=opasConfig.TITLE_SMARTSEARCH, description=opasConfig.DESCRIPTION_SMARTSEARCH),
                            paratext: str=Query(None, title=opasConfig.TITLE_PARATEXT, description=opasConfig.DESCRIPTION_PARATEXT),
                            parascope: str=Query(None, title=opasConfig.TITLE_PARASCOPE, description=opasConfig.DESCRIPTION_PARASCOPE),
                            synonyms: bool=Query(False, title=opasConfig.TITLE_SYNONYMS_BOOLEAN, description=opasConfig.DESCRIPTION_SYNONYMS_BOOLEAN),
                            facetquery: str=Query(None, title=opasConfig.TITLE_FACETQUERY, description=opasConfig.DESCRIPTION_FACETQUERY),
                            # filters (Solr query filter)
                            sourcename: str=Query(None, title=opasConfig.TITLE_SOURCENAME, description=opasConfig.DESCRIPTION_SOURCENAME, min_length=2),  
                            sourcecode: str=Query(None, title=opasConfig.TITLE_SOURCECODE, description=opasConfig.DESCRIPTION_SOURCECODE, min_length=2),
                            sourcetype: str=Query(None, title=opasConfig.TITLE_SOURCETYPE, description=opasConfig.DESCRIPTION_PARAM_SOURCETYPE), 
                            sourcelangcode: str=Query(None, title=opasConfig.TITLE_SOURCELANGCODE, description=opasConfig.DESCRIPTION_SOURCELANGCODE), 
                            volume: str=Query(None, title=opasConfig.TITLE_VOLUMENUMBER, description=opasConfig.DESCRIPTION_VOLUMENUMBER), 
                            issue: str=Query(None, title=opasConfig.TITLE_ISSUE, description=opasConfig.DESCRIPTION_ISSUE),
                            author: str=Query(None, title=opasConfig.TITLE_AUTHOR, description=opasConfig.DESCRIPTION_AUTHOR), 
                            title: str=Query(None, title=opasConfig.TITLE_TITLE, description=opasConfig.DESCRIPTION_TITLE),
                            articletype: str=Query(None, title=opasConfig.TITLE_ARTICLETYPE, description=opasConfig.DESCRIPTION_ARTICLETYPE),
                            startyear: str=Query(None, title=opasConfig.TITLE_STARTYEAR, description=opasConfig.DESCRIPTION_STARTYEAR), 
                            endyear: str=Query(None, title=opasConfig.TITLE_ENDYEAR, description=opasConfig.DESCRIPTION_ENDYEAR), 
                            citecount: str=Query(None, title=opasConfig.TITLE_CITECOUNT, description=opasConfig.DESCRIPTION_CITECOUNT),   
                            viewcount: str=Query(None, title=opasConfig.TITLE_VIEWCOUNT, description=opasConfig.DESCRIPTION_VIEWCOUNT),    
                            viewperiod: int=Query(4, title=opasConfig.TITLE_VIEWPERIOD, description=opasConfig.DESCRIPTION_VIEWPERIOD),     
                            # return set control
                            abstract:bool=Query(False, title="Return an abstract with each match", description="True to return an abstract"),
                            formatrequested: str=Query("HTML", title=opasConfig.TITLE_RETURNFORMATS, description=opasConfig.DESCRIPTION_RETURNFORMATS),
                            similarcount: int=Query(0, title=opasConfig.TITLE_SIMILARCOUNT, description=opasConfig.DESCRIPTION_SIMILARCOUNT),
                            highlightlimit: int=Query(opasConfig.DEFAULT_MAX_KWIC_RETURNS, title=opasConfig.TITLE_MAX_KWIC_COUNT, description=opasConfig.DESCRIPTION_MAX_KWIC_COUNT),
                            facetfields: str=Query(None, title=opasConfig.TITLE_FACETFIELDS, description=opasConfig.DESCRIPTION_FACETFIELDS), 
                            facetmincount: int=Query(1, title="Minimum count to return a facet"),
                            facetlimit: int=Query(15, title="Maximum number of facet values to return"),
                            facetoffset: int=Query(0, title="Offset that can be used for paging through a facet"),
                            sort: str=Query("score desc", title=opasConfig.TITLE_SORT, description=opasConfig.DESCRIPTION_SORT),
                            limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                            offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET),
                            client_id:int=Depends(get_client_id), 
                            client_session:str= Depends(get_client_session)
                            ):
    """
    ## Function
       ### Mirror function to search to request an analysis of the search parameters.

    ## Return Type
       models.TermIndex (for v2)

    ## Status
       Status: Working, but in perpetual development to improve

    ## Sample Call
         /v2/Database/SearchAnalysis/
         
    ## Notes
       6/2020 - The returnfields parameter was removed as not useful here.

    ## Limitations
       When using GET and the interactive documentation to try out this endpoint, you must clear the "Request Body" field before submitting.
       The "Request Body" field only applies to POST, but the code is shared, which trips up the interface for interactive docs.

    ## Potential Errors
       N/A

    """

    # don't set parascope, unless they set paratext and forgot to set parascope
    if paratext is not None and parascope is None:
        parascope = "doc"

    errors, mod_args = opasQueryHelper.check_search_args(
                                             smarttext=smarttext,
                                             fulltext1=fulltext1,
                                             paratext=paratext,
                                             author=author,
                                             title=title,
                                             startyear=startyear,
                                             endyear=endyear
                                           )
    if errors:
        detail = f"SearchAnalysisError: {ERR_MSG_QUERY_FRAGMENT}" # Query had too few characters or was unbalanced."
        logger.error(detail)
        
        raise HTTPException(
            status_code=opasConfig.httpCodes.HTTP_425_TOO_EARLY,
            detail=detail
        )
    
    # this does intelligent processing of the query parameters and returns a smaller set of solr oriented         
    # params (per pydantic model SolrQuery), ready to use
    solr_query_spec = \
        opasQueryHelper.parse_search_query_parameters(solrQueryTermList=qtermlist,
                                                      source_name=sourcename,
                                                      source_code=sourcecode,
                                                      source_type=sourcetype,
                                                      source_lang_code=sourcelangcode,
                                                      paratext=mod_args.get("paratext", None), # search within paragraphs
                                                      parascope=parascope, # scope for par_search
                                                      similar_count=similarcount, # Turn on morelikethis for the search, return this many similar docs for each
                                                      fulltext1=mod_args.get("fulltext1", None),  # more flexible search, including fields, anywhere in the doc, across paras
                                                      smarttext=mod_args.get("smarttext", None), # experimental detection of what user wants to query
                                                      synonyms=synonyms, 
                                                      facetquery=facetquery, 
                                                      vol=volume,
                                                      issue=issue,
                                                      author=author,
                                                      title=title,
                                                      articletype=articletype, 
                                                      startyear=startyear,
                                                      endyear=endyear,
                                                      citecount=citecount,
                                                      viewcount=viewcount,
                                                      viewperiod=viewperiod,
                                                      highlightlimit=highlightlimit, 
                                                      facetfields=facetfields,
                                                      facetmincount=facetmincount,
                                                      facetlimit=facetlimit,
                                                      facetoffset=facetoffset,                                                        
                                                      abstract_requested=abstract,
                                                      formatrequested=formatrequested, 
                                                      limit=limit,
                                                      offset=offset,
                                                      sort=sort,
                                                      req_url = request.url._url
                                                      )

    solr_query_spec.urlRequest = request.url._url
    solr_query_params = solr_query_spec.solrQuery

    # We don't always need full-text, but if we need to request the doc later we'll need to repeat the search parameters plus the docID
    ret_val = opasPySolrLib.search_analysis(query_list=solr_query_params.searchAnalysisTermList, 
                                            filter_query = solr_query_params.filterQ,
                                            def_type = "lucene",
                                            #query_analysis=analysis_mode,
                                            #more_like_these = None,
                                            full_text_requested=False,
                                            limit=limit, 
                                            api_version="v2"
                                            )

    logger.debug("Done with search analysis.")

    return ret_val

#---------------------------------------------------------------------------------------------------------
@app.get("/v2/Database/SmartSearch/", response_model=models.DocumentList, response_model_exclude_unset=True, tags=["Database"], summary=opasConfig.ENDPOINT_SUMMARY_SEARCH_SMARTSEARCH) 
async def database_smartsearch(response: Response, 
                               request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),  
                               smarttext: str=Query(None, title=opasConfig.TITLE_SMARTSEARCH, description=opasConfig.DESCRIPTION_SMARTSEARCH),
                               facetquery: str=Query(None, title=opasConfig.TITLE_FACETQUERY, description=opasConfig.DESCRIPTION_FACETQUERY),
                               # filters, v1 naming
                               sort: str=Query("score desc", title=opasConfig.TITLE_SORT, description=opasConfig.DESCRIPTION_SORT),
                               abstract:bool=Query(False, title="Return an abstract with each match", description="True to return an abstract"),
                               similarcount: int=Query(0, title=opasConfig.TITLE_SIMILARCOUNT, description=opasConfig.DESCRIPTION_SIMILARCOUNT),
                               formatrequested: str=Query("HTML", title=opasConfig.TITLE_RETURNFORMATS, description=opasConfig.DESCRIPTION_RETURNFORMATS),
                               highlightlimit: int=Query(opasConfig.DEFAULT_MAX_KWIC_RETURNS, title=opasConfig.TITLE_MAX_KWIC_COUNT, description=opasConfig.DESCRIPTION_MAX_KWIC_COUNT),
                               facetfields: str=Query(None, title=opasConfig.TITLE_FACETFIELDS, description=opasConfig.DESCRIPTION_FACETFIELDS), 
                               limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                               offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET), 
                               client_id:int=Depends(get_client_id), 
                               client_session:str= Depends(get_client_session)
                               ):
    """
    ## Function

    ### Convenience function for testing the smarttext searching (smartsearch).
    
       With fewer parameters, it's easier to
       test and experiment with in the OpenAPI interface.  The results would be the same calling the Search endpoint and providing the
       same argument to smarttext, but if this is all you need, use it directly.

       Also, this endpoint provides an easier way to isolate and document the types of calls interpreted by smarttext.

    ## Return Type
       models.DocumentList

    ## Status
       Status: Working, but in perpetual development to improve

    ## Sample Call
         /v2/Database/SmartSearch/
         
       Below, here are some values and the style you can submit in parameter smarttext.  To see what they
       are interpreted to in Solr syntax, see the scopeQuery field of ResponseInfo in the response, where the first
       value is Solr q and the second is fq.

            "scopeQuery": [
                [
                  "*:*",
                  "art_id:(NLP.001.0001A)"
                ]
            ]

    1) Author Names and dates (best detection to use standard form, parentheses around the date.)
        Last names should be initial capitalized without first initials, e.g.,

       Shapiro, Shapiro, Zinner and Berkowitz (1977)   --> 1 hit

       Tuckett and Fonagy 2012                         --> 1 hit

       Kohut, H. & Wolf, E. S. (1978)                  --> 1 hit

    2) Author names without dates, e.g.,

       Shapiro, Shapiro, Zinner and Berkowitz          --> 3 hits

    3) Special IDs

       a) DOIs, e.g., (all 1 hit each)

          10.1111/j.1745-8315.2012.00606.x

          10.3280/PU2019-004002

       b) PEP Locators (IDs)

          AOP.033.0079A

          PEP/AOP.033.0079A

    4) Search any schema field, use Solr type notation, but one schema field per entry. 
       art_type: ART or COM

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    opasDocPermissions.verify_header(request, "SmartSearch") # for debugging client call
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")

    ret_val = await database_search(response,
                                    request,
                                    fulltext1=None,
                                    paratext=None, 
                                    parascope=None,
                                    smarttext=smarttext, 
                                    synonyms=False,
                                    facetquery=None, 
                                    similarcount=similarcount, 
                                    sourcecode=None,
                                    sourcename=None, 
                                    sourcetype=None, 
                                    sourcelangcode=None, 
                                    volume=None,
                                    issue=None, 
                                    author=None,
                                    title=None,
                                    articletype=None, 
                                    startyear=None,
                                    endyear=None, 
                                    citecount=None,   
                                    viewcount=None,   
                                    viewperiod=None,
                                    highlightlimit=highlightlimit, 
                                    facetfields=facetfields,
                                    facetmincount=1,
                                    facetlimit=15,
                                    facetoffset=0,
                                    abstract=abstract,
                                    sort=sort,
                                    formatrequested=formatrequested, 
                                    limit=limit,
                                    offset=offset,
                                    client_id=client_id,
                                    client_session=client_session
                                    )
    return ret_val

#---------------------------------------------------------------------------------------------------------
@app.get("/v2/Database/MoreLikeThis/", response_model=models.DocumentList, response_model_exclude_unset=True, tags=["Database"], summary=opasConfig.ENDPOINT_SUMMARY_MORELIKETHIS) 
async def database_morelikethis(response: Response, 
                                request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),  
                                morelikethis: str=Query(None, title=opasConfig.TITLE_MORELIKETHIS, description=opasConfig.DESCRIPTION_MORELIKETHIS),
                                facetquery: str=Query(None, title=opasConfig.TITLE_FACETQUERY, description=opasConfig.DESCRIPTION_FACETQUERY),
                                sort: str=Query("score desc", title=opasConfig.TITLE_SORT, description=opasConfig.DESCRIPTION_SORT),
                                abstract:bool=Query(False, title="Return an abstract with each match", description="True to return an abstract"),
                                similarcount: int=Query(5, title=opasConfig.TITLE_SIMILARCOUNT, description=opasConfig.DESCRIPTION_SIMILARCOUNT),
                                formatrequested: str=Query("HTML", title=opasConfig.TITLE_RETURNFORMATS, description=opasConfig.DESCRIPTION_RETURNFORMATS),
                                limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                                offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET), 
                                client_id:int=Depends(get_client_id), 
                                client_session:str= Depends(get_client_session)
                               ):
    """
    ## Function

    ### Convenience function for sending a single article ID and returning similarcount entries.  

    ## Return Type
       models.DocumentList

    ## Status
       Working, but in perpetual development to improve

    ## Sample Call
       N/A

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    opasDocPermissions.verify_header(request, "MoreLikeThis") # for debugging client call
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")

    ret_val = await database_search(response,
                                    request,
                                    fulltext1=None,
                                    paratext=None, 
                                    parascope=None,
                                    smarttext=morelikethis, 
                                    synonyms=False,
                                    facetquery=None, 
                                    similarcount=similarcount, 
                                    sourcecode=None,
                                    sourcename=None, 
                                    sourcetype=None, 
                                    sourcelangcode=None, 
                                    volume=None,
                                    issue=None, 
                                    author=None,
                                    title=None,
                                    articletype=None, 
                                    startyear=None,
                                    endyear=None, 
                                    citecount=None,   
                                    viewcount=None,   
                                    viewperiod=None,
                                    highlightlimit=0,
                                    facetfields=None,
                                    facetmincount=1,
                                    facetlimit=0,
                                    facetoffset=0,
                                    abstract=abstract,
                                    sort=sort,
                                    formatrequested=formatrequested, 
                                    limit=limit,
                                    offset=offset,
                                    client_id=client_id,
                                    client_session=client_session,
                                    override_endpoint_id=opasCentralDBLib.API_DATABASE_MORELIKETHIS
                                    )
    return ret_val

#---------------------------------------------------------------------------------------------------------
@app.get("/v2/Database/RelatedToThis/", response_model=models.DocumentList, response_model_exclude_unset=True, tags=["Database"], summary=opasConfig.ENDPOINT_SUMMARY_RELATEDTOTHIS) 
async def database_related_to_this(response: Response, 
                                   request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),  
                                   relatedToThis: str=Query(None, title=opasConfig.TITLE_RELATEDTOTHIS, description=opasConfig.DESCRIPTION_RELATEDTOTHIS),
                                   sort: str=Query("score desc", title=opasConfig.TITLE_SORT, description=opasConfig.DESCRIPTION_SORT),
                                   abstract:bool=Query(False, title="Return an abstract with each match", description="True to return an abstract"),
                                   formatrequested: str=Query("HTML", title=opasConfig.TITLE_RETURNFORMATS, description=opasConfig.DESCRIPTION_RETURNFORMATS),
                                   limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                                   offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET), 
                                   client_id:int=Depends(get_client_id), 
                                   client_session:str= Depends(get_client_session)
                                  ):
    """
    ## Function

    ### Find all related documents as recorded in the Solr schema by a field with a commmon ID value.

       The document ID of the main document of interest is specified in the relatedTothis parameter, and the endpoint looks up the value
       in that document's relatedrx field (defined in opasConfig as RELATED_RX_FIELDNAME), and returns all matching documents,
       EXCEPT the main document you specify.
    
       Returns 0 documentList records if there are no related documents or there's no data in the relatedrx field is the relatedToThis document.

    ## Return Type
       models.DocumentList

    ## Status
       Status: Working

    ## Sample Call
          https://api.pep-web.org/v2/Database/RelatedToThis/?relatedToThis=IJP.078.0335A

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    ts = time.time()
    
    ret_val = None
    caller_name = "[v2/Database/RelatedDocuments]"
    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name}")

    opasDocPermissions.verify_header(request, "RelatedDocuments") # for debugging client call
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")
    ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)
    
    try:
        return_fields = opasConfig.DOCUMENT_ITEM_TOC_FIELDS
        solr_query_spec = opasQueryHelper.parse_search_query_parameters(forced_searchq=f"art_id:{relatedToThis}",
                                                                        forced_filterq=None,
                                                                        return_field_set=return_fields,
                                                                        sort=sort, 
                                                                        req_url = request.url._url
                                                                        )
    
        ret_val, ret_status = search_text_qs(solr_query_spec, 
                                             extra_context_len=opasConfig.DEFAULT_KWIC_CONTENT_LENGTH,
                                             session_info=session_info, 
                                             request=request,
                                             caller_name=caller_name
                                             )
    except Exception as e:
        logger.error(f"Exception: {e}")
    else:
        # we have the main document record.  Now let's look for related documents.
        try:
            responseSet = ret_val.documentList.responseSet
            if responseSet != []:
                response = responseSet[0]
                try:
                    related_fieldname_data = response.relatedrx
                except Exception as e:
                    logger.warning(f"response not documentlistitem: {e}")
                    related_fieldname_data = None
            else:
                related_fieldname_data = None
            
            if related_fieldname_data is not None:
                solr_query_spec = opasQueryHelper.parse_search_query_parameters(forced_searchq=f"{opasConfig.RELATED_RX_FIELDNAME}:{related_fieldname_data} || art_id:{related_fieldname_data}",
                                                                                forced_filterq=f"NOT art_id:{relatedToThis}",
                                                                                return_field_set=opasConfig.DOCUMENT_ITEM_SUMMARY_FIELDS, 
                                                                                abstract_requested=abstract,
                                                                                req_url = request.url._url
                                                                                )
            
                ret_val, ret_status = search_text_qs(solr_query_spec, 
                                                     extra_context_len=opasConfig.DEFAULT_KWIC_CONTENT_LENGTH,
                                                     limit=limit,
                                                     offset=offset,
                                                     session_info=session_info, 
                                                     request=request,
                                                     caller_name=caller_name
                                                     )

            else:
                # return empty set response
                responseInfo = models.ResponseInfo(count = 0,
                                                   fullCount = 0,
                                                   totalMatchCount = 0,
                                                   listType="documentlist",
                                                   fullCountComplete = True,
                                                   request=f"{request.url._url}",
                                                   timeStamp = datetime.utcfromtimestamp(time.time()).strftime(opasConfig.TIME_FORMAT_STR)                     
                                                   )
       
                documentListStruct = models.DocumentListStruct( responseInfo = responseInfo, 
                                                                responseSet = []
                                                                )
                documentList = models.DocumentList(documentList = documentListStruct)
                ret_val = documentList
                

        except Exception as e:
            logger.error(f"Exception: {e}")

    log_endpoint_time(request, ts=ts, level="debug")
    return ret_val

#---------------------------------------------------------------------------------------------------------
@app.get("/v2/Database/MostViewed/", response_model=models.DocumentList, response_model_exclude_unset=True, tags=["Database"], summary=opasConfig.ENDPOINT_SUMMARY_MOST_VIEWED)
def database_mostviewed(response: Response,
                        request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),
                        # period is str because it can be "all"
                        pubperiod: int=Query(None, title=opasConfig.TITLE_PUBLICATION_PERIOD, description=opasConfig.DESCRIPTION_PUBLICATION_PERIOD),
                        # viewperiod=4 for last 12 months
                        viewperiod: int=Query(4, title=opasConfig.TITLE_MOST_VIEWED_PERIOD, description=opasConfig.DESCRIPTION_MOST_VIEWED_PERIOD), # 4=Prior year, per PEP-Web design
                        viewcount: int=Query(None, title=opasConfig.TITLE_VIEWCOUNT, description=opasConfig.DESCRIPTION_VIEWCOUNT_INT),
                        author: str=Query(None, title=opasConfig.TITLE_AUTHOR, description=opasConfig.DESCRIPTION_AUTHOR), 
                        title: str=Query(None, title=opasConfig.TITLE_TITLE, description=opasConfig.DESCRIPTION_TITLE),
                        sourcename: str=Query(None, title=opasConfig.TITLE_SOURCENAME, description=opasConfig.DESCRIPTION_SOURCENAME),  
                        sourcecode: str=Query(None, title=opasConfig.TITLE_SOURCECODE, description=opasConfig.DESCRIPTION_SOURCECODE), 
                        sourcetype: str=Query("journal", title=opasConfig.TITLE_SOURCETYPE, description=opasConfig.DESCRIPTION_PARAM_SOURCETYPE),
                        abstract:bool=Query(False, title=opasConfig.TITLE_RETURN_ABSTRACTS_BOOLEAN, description=opasConfig.DESCRIPTION_RETURN_ABSTRACTS),
                        similarcount: int=Query(0, title=opasConfig.TITLE_SIMILARCOUNT, description=opasConfig.DESCRIPTION_SIMILARCOUNT),
                        stat:bool=Query(False, title=opasConfig.TITLE_STATONLY, description=opasConfig.DESCRIPTION_STATONLY),
                        limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT), # by PEP-Web standards, we want 10, but 5 is better for PEP-Easy
                        offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET), 
                        download:bool=Query(False, title=opasConfig.TITLE_DOWNLOAD, description=opasConfig.DESCRIPTION_DOWNLOAD),
                        cached:bool=Query(True, title=opasConfig.TITLE_CACHED, description=opasConfig.DESCRIPTION_CACHED),
                        update_cache: bool=Query(False, title=opasConfig.TITLE_UPDATE_CACHE, description=opasConfig.DESCRIPTION_UPDATE_CACHE),
                        client_id:int=Depends(get_client_id), 
                        client_session:str=Depends(get_client_session)
                        ):
    """
    ## Function
    ### Return a list of documents which are the most downloaded (viewed)

        viewperiod = 0: lastcalendaryear (also 5, preferred)
                     1: lastweek
                     2: lastmonth
                     3: last6months
                     4: last12months
                     5: lastcalendaryear

    ## Return Type
       models.DocumentList (or returns response if a download is requested )

    ## Status
       This endpoint is working.
       For whatever reason, async works here, without a wait on the long running database call.  And
         adding the await makes it never return

    ## Sample Call
         /v2/Database/MostViewed/

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    caller_name = "[v2/Database/MostViewed]"
    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name} {client_session}: ")
    
    ts = time.time()
    ret_val = None
    query_arg_error = None
    
    opasDocPermissions.verify_header(request, "MostViewed") # for debugging client call
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")

    ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)

    if viewperiod < 0 or viewperiod > 4:
        query_arg_error = f"Most Viewed: viewperiod: {viewperiod}.  Range should be 0-4 (int), 0:lastcalendaryear 1:lastweek 2:lastmonth, 3:last6months, 4:last12months"
    if sourcetype is not None: # none is ok
        sourcetype = opasConfig.normalize_val(sourcetype, opasConfig.VALS_SOURCE_TYPE, None)
        if sourcetype is None: # trap error on None, default
            query_arg_error = opasConfig.norm_val_error(opasConfig.VALS_SOURCE_TYPE, "Most Viewed: sourcetype")

    # exit on arg error        
    if query_arg_error is not None:
        status_message = f"Error: {query_arg_error}"
        logger.error(status_message)
        ret_val = None
        raise HTTPException(
            status_code=httpCodes.HTTP_400_BAD_REQUEST, 
            detail=status_message
        )        

    if download == True:
        # Download CSV of selected set.  Returns only response with download, not usual documentList
        #   response to client
        if limit == opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS:
            limit = None

        views = ocd.most_viewed_generator( publication_period=pubperiod, # Number of publication years to include (counting back from current year, 0 means current year)
                                           viewperiod=viewperiod,        # used here for sort and limiting viewcount results (more_than_clause)
                                           viewcount=viewcount,          # cutoff at this minimum number of views for viewperiod column
                                           author=author,
                                           title=title,
                                           source_name=sourcename, 
                                           source_code=sourcecode,
                                           source_type=sourcetype,       # see VALS_SOURCE_TYPE (norm_val applied in opasCenralDBLib)
                                           select_clause=opasConfig.VIEW_MOSTVIEWED_DOWNLOAD_COLUMNS, 
                                           limit=limit,
                                           offset=offset,
                                           session_info=session_info
                                           )            
        header = ["Document", "Last Week", "Last Month", "Last 6 Months", "Last 12 Months", "Last Calendar Year"]
        df = pd.DataFrame(views)
        stream = io.StringIO()
        df.to_csv(stream, header=header, index = False)
        response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
        response.headers["Content-Disposition"] = "attachment; filename=pepviews.csv"
        # Don't record endpoint use (not a user request, just a default) but do record download
        status_message = "Success"
        status_code = httpCodes.HTTP_200_OK
        ocd.record_session_endpoint( api_endpoint_id=opasCentralDBLib.API_DATABASE_MOSTVIEWED,
                                     session_info=session_info, 
                                     params=request.url._url,
                                     return_status_code = status_code,
                                     status_message=status_message
                                     )
        ret_val = response

    else: # go ahead! Search Solr
        # if no special paramaters, then use the cache. It doesn't make sense otherwise.
        if cached and all(v is None for v in [pubperiod, viewcount, author, title, sourcename,
                                               sourcecode, sourcename, sourcetype, abstract,
                                               stat, similarcount]):

            ret_val = mostviewedcache.get_most_viewed(viewperiod=viewperiod,
                                                      limit=limit,
                                                      offset=0,
                                                      session_info=session_info,
                                                      forced_update=update_cache)
        else:
            try:
                # we want the last year (default, per PEP-Web) of views, for all articles (journal articles)
                ret_val, ret_status = opasCacheSupport.document_get_most_viewed( publication_period=pubperiod,
                                                                              view_period=viewperiod,   # 0:lastcalendaryear 1:lastweek 2:lastmonth, 3:last6months, 4:last12months
                                                                              view_count=viewcount, 
                                                                              author=author,
                                                                              title=title,
                                                                              source_name=sourcename, 
                                                                              source_code=sourcecode,
                                                                              source_type=sourcetype,   # see VALS_SOURCE_TYPE (norm_val applied in opasCenralDBLib)
                                                                              abstract_requested=abstract, 
                                                                              req_url=request.url._url,
                                                                              stat=stat, 
                                                                              limit=limit, 
                                                                              offset=offset,
                                                                              download=download, 
                                                                              mlt_count=similarcount, 
                                                                              session_info=session_info,
                                                                              request=request
                                                                              )
    
                if ret_val is None:
                    status_message = f"MostViewedError: Bad request"
                    logger.error(status_message)
                    raise HTTPException(
                        status_code=httpCodes.HTTP_400_BAD_REQUEST, 
                        detail=status_message
                    )        
                    
            except Exception as e:
                ret_val = None
                status_message = f"MostViewedError: Exception: {e}"
                logger.error(status_message)
                raise HTTPException(
                    status_code=httpCodes.HTTP_400_BAD_REQUEST, 
                    detail=status_message
                )        
            else:
                if isinstance(ret_val, models.ErrorReturn):
                    detail = ret_val.error + " - " + ret_val.error_description                
                    logger.error(f"MostViewedError: {detail}")
                    raise HTTPException(
                        status_code=ret_val.httpcode, 
                        detail = detail
                    )
        
    log_endpoint_time(request, ts=ts, level="debug")
    return ret_val  # document_list

#---------------------------------------------------------------------------------------------------------
@app.get("/v2/Database/MostCited/", response_model=models.DocumentList, response_model_exclude_unset=True, tags=["Database"], summary=opasConfig.ENDPOINT_SUMMARY_MOST_CITED)
def database_mostcited(response: Response,
                       request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),  
                       citeperiod: str=Query('5', title=opasConfig.TITLE_MOST_CITED_PERIOD, description=opasConfig.DESCRIPTION_MOST_CITED_PERIOD),
                       citecount: str=Query(None, title=opasConfig.TITLE_CITECOUNT, description=opasConfig.DESCRIPTION_CITECOUNT),   
                       pubperiod: int=Query(None, title=opasConfig.TITLE_PUBLICATION_PERIOD, description=opasConfig.DESCRIPTION_PUBLICATION_PERIOD),
                       author: str=Query(None, title=opasConfig.TITLE_AUTHOR, description=opasConfig.DESCRIPTION_AUTHOR), 
                       title: str=Query(None, title=opasConfig.TITLE_TITLE, description=opasConfig.DESCRIPTION_TITLE),
                       sourcename: str=Query(None, title=opasConfig.TITLE_SOURCENAME, description=opasConfig.DESCRIPTION_SOURCENAME),  
                       sourcecode: str=Query(None, title=opasConfig.TITLE_SOURCECODE, description=opasConfig.DESCRIPTION_SOURCECODE), 
                       sourcetype: str=Query(None, title=opasConfig.TITLE_SOURCETYPE, description=opasConfig.DESCRIPTION_PARAM_SOURCETYPE),
                       abstract:bool=Query(False, title=opasConfig.TITLE_RETURN_ABSTRACTS_BOOLEAN, description=opasConfig.DESCRIPTION_RETURN_ABSTRACTS),
                       similarcount: int=Query(0, title=opasConfig.TITLE_SIMILARCOUNT, description=opasConfig.DESCRIPTION_SIMILARCOUNT),
                       stat:bool=Query(False, title=opasConfig.TITLE_STATONLY, description=opasConfig.DESCRIPTION_STATONLY),
                       limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                       offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET), 
                       download:bool=Query(False, title=opasConfig.TITLE_DOWNLOAD, description=opasConfig.DESCRIPTION_DOWNLOAD), 
                       cached:bool=Query(True, title=opasConfig.TITLE_CACHED, description=opasConfig.DESCRIPTION_CACHED),
                       update_cache: bool=Query(False, title=opasConfig.TITLE_UPDATE_CACHE, description=opasConfig.DESCRIPTION_UPDATE_CACHE),
                       client_id:int=Depends(get_client_id), 
                       client_session:str= Depends(get_client_session)
                       ):
    """
    ## Function
       ### Return a list of documents for a SourceCode source (and optional year specified in query params).

       If you don't request abstracts returned, document permissions will not be checked or returned.
       This is intended to speed up retrieval, especially for returning large numbers of
       articles (e.g., for downloads.)
   
       MoreLikeThese, controlled by similarcount, is set to be off by default (0)
   
    ## Return Type
       models.DocumentList

    ## Status
       This endpoint is working.

    ## Sample Call
         /v2/Database/MostCited/

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    caller_name = "[v2/Database/MostCited]"
    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name} {client_session}: ")
    
    ret_val = True
    ts = time.time()
    
    opasDocPermissions.verify_header(request, "MostCited") # for debugging client call 
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")

    ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)

    # session_id = session_info.session_id 

    if download == True:
        # turn off similarity, waste of time
        similarcount = 0
        if limit == opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS:
            limit = 299999 # force no limit!

        # Download CSV of selected set.  Returns only response with download, not usual documentList
        #   response to client
        cites = ocd.most_cited_generator( cited_in_period=citeperiod,   # in past 5 years (or IN {5, 10, 20, or ALL}
                                          citecount=citecount,          # cited this many times
                                          publication_period=pubperiod, # Number of publication years to include (back from current year, 0 for current)
                                          author=author,
                                          title=title,
                                          source_name=sourcename, 
                                          source_code=sourcecode,
                                          source_type=sourcetype,  # see VALS_SOURCE_TYPE (norm_val applied in opasCenralDBLib)
                                          select_clause=opasConfig.VIEW_MOSTCITED_DOWNLOAD_COLUMNS, 
                                          limit=limit,
                                          offset=offset
                                          )

        header = ["Document", "Last 5 Years", "Last 10 years", "Last 20 years", "All years"]
        df = pd.DataFrame(cites)
        stream = io.StringIO()
        if len(df) > 0:
            df.to_csv(stream, header=header, index = False)
            response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
            response.headers["Content-Disposition"] = "attachment; filename=pepcited.csv"
            ret_val = response
            status_message = "Success"
            # Don't record endpoint use (not a user request, just a default) but do record download
            status_code = httpCodes.HTTP_200_OK
            ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_DATABASE_MOSTCITED,
                                        session_info=session_info, 
                                        params=request.url._url,
                                        return_status_code = status_code,
                                        status_message=status_message
                                        )
        else:
            ret_val = None
            status_message = "Success, But No Data to download"
            if ret_val is None:
                detail = "MostCitedError: " + ERR_MSG_SEARCH_RETURNED_NONE
                logger.error(detail)
                raise HTTPException(
                    status_code=httpCodes.HTTP_400_BAD_REQUEST, 
                    detail = detail
                )           
            

    else:
        # if no special paramaters, then use the cache. It doesn't make sense otherwise.
        if cached and all(v is None for v in [pubperiod, citecount, author, title, sourcename,
                                               sourcecode, sourcename, sourcetype, abstract,
                                               stat, similarcount]):

            ret_val = mostcitedcache.get_most_cited(citeperiod=citeperiod,
                                                      limit=limit,
                                                      offset=0,
                                                      session_info=session_info,
                                                      forced_update=update_cache)
        else:
            # return documentList, much more limited document list if download==True
            ret_val, ret_status = opasAPISupportLib.database_get_most_cited( cited_in_period=citeperiod,
                                                                             cite_count=citecount,
                                                                             publication_period=pubperiod,
                                                                             author=author,
                                                                             title=title,
                                                                             source_name=sourcename, 
                                                                             source_code=sourcecode,
                                                                             source_type=sourcetype,  # see VALS_SOURCE_TYPE (norm_val applied in opasCenralDBLib)
                                                                             abstract_requested=abstract, 
                                                                             req_url=request.url, 
                                                                             limit=limit,
                                                                             offset=offset,
                                                                             download=False, 
                                                                             mlt_count=similarcount, 
                                                                             session_info=session_info,
                                                                             request=request
                                                                             )
    
            if isinstance(ret_val, models.ErrorReturn): 
                detail = ret_val.error + " - " + ret_val.error_description                
                logger.error(f"MostCitedError: {detail}")
                raise HTTPException(
                    status_code=ret_val.httpcode, 
                    detail = detail
                )
    
            if ret_val is None:
                detail = "MostCitedError: " + ERR_MSG_SEARCH_RETURNED_NONE + f" Status: {ret_status}"
                logger.error(detail)
                raise HTTPException(
                    status_code=httpCodes.HTTP_400_BAD_REQUEST, 
                    detail = detail
                )
                
            # Don't record in final build - (ok for now during testing)

    log_endpoint_time(request, ts=ts, level="debug")
    return ret_val

#-----------------------------------------------------------------------------
@app.get("/v2/Database/OpenURL/", response_model=Union[models.DocumentList, models.ErrorReturn], response_model_exclude_unset=True, tags=["Database"], summary=opasConfig.ENDPOINT_SUMMARY_OPENURL)
async def database_open_url(response: Response, 
                            request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),
                            #moreinfo: bool=Query(False, title=opasConfig.TITLE_MOREINFO, description=opasConfig.DESCRIPTION_MOREINFO),
                            client_id:int=Depends(get_client_id), 
                            client_session:str= Depends(get_client_session), 
                            #open_url arg reference: https://biblio.ugent.be/publication/760060/file/760063
                            issn: str=Query(None, title=opasConfig.TITLE_ISSN, description=opasConfig.DESCRIPTION_ISSN), 
                            eissn: str=Query(None, title=opasConfig.TITLE_ISSN, description=opasConfig.DESCRIPTION_EISSN), 
                            isbn: str=Query(None, title=opasConfig.TITLE_ISBN, description=opasConfig.DESCRIPTION_ISBN), 
                            title: str=Query(None, title=opasConfig.TITLE_SOURCENAME, description=opasConfig.DESCRIPTION_SOURCENAME, min_length=2),  
                            stitle: str=Query(None, title=opasConfig.TITLE_SOURCENAME, description=opasConfig.DESCRIPTION_SOURCENAME, min_length=2),  
                            atitle: str=Query(None, title=opasConfig.TITLE_TITLE, description=opasConfig.DESCRIPTION_TITLE),
                            aufirst: str=Query(None, title=opasConfig.TITLE_AUTHOR, description=opasConfig.DESCRIPTION_AUTHOR), 
                            aulast: str=Query(None, title=opasConfig.TITLE_AUTHOR, description=opasConfig.DESCRIPTION_AUTHOR), 
                            volume: str=Query(None, title=opasConfig.TITLE_VOLUMENUMBER, description=opasConfig.DESCRIPTION_VOLUMENUMBER), 
                            issue: str=Query(None, title=opasConfig.TITLE_ISSUE, description=opasConfig.DESCRIPTION_ISSUE),
                            spage: int=Query(None, title=opasConfig.TITLE_FIRST_PAGE, description=opasConfig.DESCRIPTION_FIRST_PAGE),
                            epage: int=Query(None, title=opasConfig.TITLE_FIRST_PAGE, description=opasConfig.DESCRIPTION_LAST_PAGE),
                            pages: str=Query(None, title=opasConfig.TITLE_PAGEREQUEST, description=opasConfig.DESCRIPTION_PAGEREQUEST),
                            artnum: str=Query(None, title=opasConfig.TITLE_DOCUMENT_ID, description=opasConfig.DESCRIPTION_DOCIDSINGLE), # return controls 
                            date: str=Query(None, title=opasConfig.TITLE_STARTYEAR, description=opasConfig.DESCRIPTION_STARTYEAR), 
                            sort: str=Query("score desc", title=opasConfig.TITLE_SORT, description=opasConfig.DESCRIPTION_SORT),
                            limit: int=Query(100, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                            offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET), 
                            ):
    """
    ## Function
       ### Search the database per OpenURL 0.1 paramaters.

    ## Return Type
       models.DocumentList

    ## Status
       Status: In Development

    ## Sample Call
         /v2/Database/OpenURL/

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    ts = time.time()
    caller_name = "[v2/Database/OpenURL]"
    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name} {client_session}: ")

    if aufirst is not None and aulast is not None:
        author = aufirst + " , " + aulast
    elif aufirst is not None:
        author = aufirst
    elif aulast is not None:
        author = aulast
    else:
        author = None
    
    sourcetype = "journal OR videostream"
    if issn is not None:
        smarttext = f"art_issn:{issn}"
    elif eissn is not None:
        smarttext = f"art_issn:{eissn}"
    elif isbn is not None:
        smarttext = f"art_isbn:{isbn}"
        sourcetype = "book"
    else:
        smarttext = None
        sourcetype = None
        
    if title is not None:
        sourcename = title
    elif stitle is not None:
        sourcename = stitle
    else:
        sourcename = None
        
    if atitle is not None:
        title = atitle
    else:
        title = None

    if date is not None:
        startyear = date
    else:
        startyear = None

    #page_range = opasQueryHelper.page_arg_parser(pgrg=pages, pgstart=spage, pgend=epage)
    
    #if page_range is not None and page_range != "":
        #if smarttext is None:
            #smarttext = f"art_pgrg:{page_range}"
        #else:
            #smarttext += f" && art_pgrg:{page_range}"
        
    opasDocPermissions.verify_header(request, "OpenURL") # for debugging client call
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")
    errors, mod_args = opasQueryHelper.check_search_args( author=author,
                                                          title=title,
                                                          startyear=startyear
                                                        )
    if errors:
        detail = ERR_MSG_QUERY_FRAGMENT # "Query had too few characters or was unbalanced."
        logger.error(f"OpenURLError: {detail}")
        raise HTTPException(
            status_code=opasConfig.httpCodes.HTTP_425_TOO_EARLY,
            detail=detail
        )
    
    ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)

    solr_query_spec = \
        opasQueryHelper.parse_search_query_parameters(solrQueryTermList=None,
                                                      source_name=sourcename,
                                                      source_type=sourcetype,
                                                      smarttext=smarttext,
                                                      document_id=artnum, 
                                                      vol=volume,
                                                      issue=issue,
                                                      pgrg=pages,
                                                      pgstart=spage,
                                                      pgend=epage, 
                                                      author=author,
                                                      title=title,
                                                      startyear=startyear,
                                                      limit=limit,
                                                      offset=offset,
                                                      sort=sort,
                                                      req_url = request.url._url
                                                      )

    ret_val, ret_status = search_text_qs(solr_query_spec, 
                                         extra_context_len=opasConfig.DEFAULT_KWIC_CONTENT_LENGTH,
                                         limit=limit,
                                         offset=offset,
                                         session_info=session_info, 
                                         request=request,
                                         caller_name=caller_name
                                         )

    #  if there's a Solr server error in the call, it returns a non-200 ret_status[0]
    if ret_status[0] != httpCodes.HTTP_200_OK:
        #  throw an exception rather than return an object (which will fail)
        try:
            detail = ERR_MSG_BAD_SEARCH_REQUEST + f" {ret_status[1].reason}:{ret_status[1].body}"
        except:
            detail = ERR_MSG_BAD_SEARCH_REQUEST # "Bad Solr Search Request"
            
        raise HTTPException(
            status_code=ret_status[0],
            detail=detail
        )

    if ret_val != {}:
        matches = len(ret_val.documentList.responseSet)
        # ret_val.documentList.responseInfo.request = request.url._url
    else:
        matches = 0

    logger.debug(f"....matches = {matches}")
    # fill in additional return structure status info
    statusMsg = f"{matches} hits"
    logger.debug("Done with search.")
    item_of_interest = opasAPISupportLib.get_query_item_of_interest(solrQuery=solr_query_spec.solrQuery)
    
    ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_DATABASE_OPENURL,
                                session_info=session_info,
                                item_of_interest=item_of_interest, 
                                params=request.url._url,
                                return_status_code = ret_status[0], 
                                status_message=statusMsg
                                )

    log_endpoint_time(request, ts=ts, level="debug")
    return ret_val

#-----------------------------------------------------------------------------
@app.get("/v2/Database/TermCounts/", response_model=Union[models.TermIndex, models.ErrorReturn], response_model_exclude_unset=True, tags=["Database"], summary=opasConfig.ENDPOINT_SUMMARY_TERM_COUNTS)  #  removed for now: response_model=models.DocumentList, 
async def database_term_counts(response: Response, 
                               request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),  
                               termlist: str=Query(None, title=opasConfig.TITLE_TERMLIST, description=opasConfig.DESCRIPTION_TERMLIST),
                               termfield: str=Query("text", title=opasConfig.TITLE_TERMFIELD, description=opasConfig.DESCRIPTION_TERMFIELD),
                               method: int=Query(0, title=opasConfig.TITLE_TERMCOUNT_METHOD, description=opasConfig.DESCRIPTION_TERMCOUNT_METHOD),
                               #client_id:int=Depends(get_client_id), 
                               #client_session:str= Depends(get_client_session)
                               ):
    """
    ## Function
       ### Get a list of term frequency counts (# of times term occurs across documents)

    Can specify a field per the docs schema to limit it. e.g.,

       - text - all text
       - title - within titles
       - author - within authors
       - author_bio_xml - in author bio
       - art_kwds - in keyword lists
       - art_lang - can look at frequency of en, fr, ... at article level
       - lang - can look at frequency of en, fr, ... at paragraph level
       - meta_xml - meta document data, including description of fixes, edits to documents

       Note: The field must be listed in the docs schema as stored, not just indexed.

    Note this call still uses SolyPy rather than PySolr underneath.

    ## Return Type
       models.TermIndex

    ## Status
       Status: Working as described above

    ## Sample Call
            
         /v2/Database/TermCounts/?termlist=dog%2C%20cat%2C%20mouse&termfield=text
            
        Response:
        {
             "termIndex": {
             "responseInfo": {
               "count": 3,
               "fullCountComplete": true,
               "listType": "termindex",
               "scopeQuery": [
                 "Terms: dog, cat, mouse"
               ],
               "request": "http://development.org:9100/v2/Database/TermCounts/?termlist=dog%2C%20cat%2C%20mouse&termfield=text",
               "dataSource": "OPAS.Local.2021-05-26"
             },
             "responseSet": [
               {
                 "field": "text",
                 "term": "dog",
                 "termCount": 4256
               },
               {
                 "field": "text",
                 "term": "cat",
                 "termCount": 2431
               },
               {
                 "field": "text",
                 "term": "mouse",
                 "termCount": 744
               }
             ]
           }
         }

    ## Example Calls
       N/A

    ## Notes
    
    **** IMPORTANT: See Potential errors below ***

    ## Potential Errors

    IMPORTANT: Note this API call still uses SolyPy rather than PySolr as the rest of the API does.  The error
               handling is different between the two libraries

    """
    caller_name = "[v2/Database/TermCounts]"
    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name} ")
    
    ts = time.time()
    # no logging, no session info for this call...speed, speed, speed
    term_index_items = []
    param_error = False
    if termfield is None:
        termfield = "text"
    else:
        field_ok, field_info = opasSchemaHelper.solr_field_check(SOLR_DOCS, termfield)
        if field_ok == False:
            param_error = True
            statusMsg = f"Bad Request: Field {termfield} underfined"

    if termlist is None:
        param_error = True
        statusMsg = f"Bad Request: No termlist specified"
        
    if param_error == False:
        results = {}  # results = {field1:{term:value, term:value, term:value}, field2:{term:value, term:value, term:value}}
        terms = [x.strip() for x in termlist.split(",")]
        for n in terms:
            try:
                # If specified as field:term
                nfield, nterms = n.split(":")
                result = opasSolrPyLib.get_term_count_list(nterms, term_field = nfield)
            except Exception as e:
                # just list of terms, use against termfield parameter
                nterms = n.strip("', ")
                try:
                    if method != 0:
                        result = opasSolrPyLib.get_term_count_list(nterms, term_field = termfield)
                    else: # default
                        result = opasPySolrLib.get_term_count_list(nterms, term_field = termfield)
                    for key, value in result.items():
                        try:
                            results[termfield][key] = value
                        except Exception as e:
                            results[termfield] = {}
                            results[termfield][key] = value
                except Exception as e:
                    detail = f"{e} - {result.error}. {result.error_description}"
                    logger.error(detail)
                    # Solr Error
                    raise HTTPException(
                        status_code=result.httpcode, 
                        detail=detail 
                    )
            else:
                try:
                    #a = results[nfield]
                    # exists, if we get here, so add it to the existing dict
                    for key, value in result.items():
                        results[nfield][key] = value
                except: #  new dict entry
                    results[nfield] = result

        response_info = models.ResponseInfo( listType="termindex", # this is a mistake in the GVPi API, should be termIndex
                                             scopeQuery=[f"Terms: {termlist}"],
                                             dataSource = opasConfig.DATA_SOURCE + database_update_date, 
                                             timestamp=datetime.utcfromtimestamp(time.time()).strftime(opasConfig.TIME_FORMAT_STR)
                                             )

        response_info.count = 0
        if results != {}:
            for field, termdict in results.items():
                for term, value in termdict.items():
                    if value > 0:
                        item = models.TermIndexItem(field = field, 
                                                    term = term, 
                                                    termCount = value,
                                                    ) 
                        term_index_items.append(item)
                        logger.debug("termIndexGetInfo: %s", item)

        response_info.count = len(term_index_items)
        response_info.fullCountComplete = True
        term_index_struct = models.TermIndexStruct( responseInfo = response_info, 
                                                    responseSet = term_index_items
                                                    )

        term_index = models.TermIndex(termIndex = term_index_struct)

        if response_info.count > 0:
            matches = response_info.count
            term_index.termIndex.responseInfo.request = request.url._url
        else:
            matches = 0

        statusMsg = f"{matches} hits"
        logger.debug(statusMsg)

    if param_error:
        logger.error(statusMsg)
        raise HTTPException(
            status_code=httpCodes.HTTP_400_BAD_REQUEST, 
            detail=statusMsg
        )

    log_endpoint_time(request, ts=ts, level="debug")                  
    return term_index

#---------------------------------------------------------------------------------------------------------
@app.get("/v2/Database/WhoCitedThis/", response_model=models.DocumentList, response_model_exclude_unset=True, tags=["Database"], summary=opasConfig.ENDPOINT_SUMMARY_WHO_CITED)
def database_who_cited_this(response: Response,
                            request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),  
                            citedid: str=Query(None, title=opasConfig.TITLE_CITED_ID, description=opasConfig.DESCRIPTION_CITEDID), 
                            pubperiod: int=Query(None, title=opasConfig.TITLE_PUBLICATION_PERIOD, description=opasConfig.DESCRIPTION_PUBLICATION_PERIOD),
                            sourcename: str=Query(None, title=opasConfig.TITLE_SOURCENAME, description=opasConfig.DESCRIPTION_SOURCENAME + " of citing document"),  
                            sourcecode: str=Query(None, title=opasConfig.TITLE_SOURCECODE, description=opasConfig.DESCRIPTION_SOURCECODE + " of citing document"), 
                            sourcetype: str=Query(None, title=opasConfig.TITLE_SOURCETYPE, description=opasConfig.DESCRIPTION_PARAM_SOURCETYPE + " of citing document"),
                            abstract:bool=Query(False, title=opasConfig.TITLE_RETURN_ABSTRACTS_BOOLEAN, description=opasConfig.DESCRIPTION_RETURN_ABSTRACTS),
                            similarcount: int=Query(0, title=opasConfig.TITLE_SIMILARCOUNT, description=opasConfig.DESCRIPTION_SIMILARCOUNT),
                            sort: str=Query("score desc", title=opasConfig.TITLE_SORT, description=opasConfig.DESCRIPTION_SORT),
                            limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                            offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET), 
                            client_id:int=Depends(get_client_id), 
                            client_session:str= Depends(get_client_session)
                            ):
    """
    ## Function
    ### Return a list of documents citing the document specified by citedid.

       If you don't request abstracts returned, document permissions will not be checked or returned.
       This is intended to speed up retrieval, especially for returning large numbers of
       articles (e.g., for downloads.)

       MoreLikeThese, controlled by similarcount, is set to be off by default (0)

    ## Return Type
       models.DocumentList

    ## Status
       This endpoint is working.

    ## Sample Call
         /v2/Database/WhoCitedThis/?citedid=IJP.077.0217A

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    caller_name = "[v2/Database/WhoCitedThis]"
    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name} {client_session}: ")

    ret_val = True
    ts = time.time()
    
    opasDocPermissions.verify_header(request, caller_name) # for debugging client call    
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")

    ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)

    # session_id = session_info.session_id 

    # return documentList, much more limited document list if download==True
    ret_val, ret_status = opasAPISupportLib.database_who_cited( cited_art_id=citedid,
                                                                publication_period=pubperiod,
                                                                source_name=sourcename, 
                                                                source_code=sourcecode,
                                                                source_type=sourcetype,  # see VALS_SOURCE_TYPE (norm_val applied in opasCenralDBLib)
                                                                abstract_requested=abstract, 
                                                                req_url=request.url,
                                                                sort=sort, 
                                                                limit=limit,
                                                                offset=offset,
                                                                download=False, 
                                                                mlt_count=similarcount, 
                                                                session_info=session_info,
                                                                request=request
                                                                )

    if isinstance(ret_val, models.ErrorReturn): 
        raise HTTPException(
            status_code=ret_val.httpcode, 
            detail = ret_val.error + " - " + ret_val.error_description
        )
    else:
        status_message = opasCentralDBLib.API_STATUS_SUCCESS
        status_code = httpCodes.HTTP_200_OK

    ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_DATABASE_WHOCITEDTHIS,
                                session_info=session_info, 
                                params=request.url._url,
                                item_of_interest=citedid, 
                                return_status_code = status_code,
                                status_message=status_message
                                )

    log_endpoint_time(request, ts=ts, level="debug")
    return ret_val

#---------------------------------------------------------------------------------------------------------
@app.get("/v2/Database/Biblio/{documentID}/", response_model=models.BiblioList, response_model_exclude_unset=True, tags=["Database"], summary=opasConfig.ENDPOINT_SUMMARY_WHO_CITED)
async def database_biblio(response: Response, 
                    request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),
                    documentID: str=Path(..., title=opasConfig.TITLE_DOCUMENT_ID, description=opasConfig.DESCRIPTION_DOCIDORPARTIAL),
                    documentYear: int=Query(None, title="Document year", description="Year of document publication"),
                    client_id:int=Depends(get_client_id), 
                    client_session:str= Depends(get_client_session)
                    ):
    """
    ## Function
    ### Return a bibliographic reference links for a specified document.

    This endpoint retrieves the document IDs for each bibliographic reference associated with a specific document,
    identified by its document ID.

    ## Return Type
    models.BiblioList

    ## Status
    This endpoint is working.

    ## Sample Call
        /v2/Database/Biblio/IJP.077.0217A/

    ## Notes
    The document ID should be a valid identifier in the database.

    ## Potential Errors
    Errors may include database access issues, or unexpected
    server errors.
    """

    caller_name = "[v2/Database/Biblio]"
    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name} {client_session}: ")

    ts = time.time()
    status_message = None
    status_code = None
    
    opasDocPermissions.verify_header(request, caller_name) # for debugging client call    
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")

    permit, check_val = opasDocPermissions.authserver_permission_check(
        client_session, documentID, documentYear
    )

    if not permit:
        raise HTTPException(
            status_code=check_val.StatusCode or 500,
            detail=check_val.ReasonStr
        )

    ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)

    try:
        ret_val = ocd.do_fetch_records("SELECT * FROM api_biblioxml2 where art_id = %(documentId)s;", {"documentId": documentID})
        status_message = opasCentralDBLib.API_STATUS_SUCCESS
        status_code = httpCodes.HTTP_200_OK
    except Exception as e:
        status_message = "An unexpected error occurred"
        status_code = 500
        raise HTTPException(
            status_code=status_code, 
            detail=f"{status_message}: {e}"
        )
    finally:
        ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_DATABASE_BIBLIO,
                                    session_info=session_info, 
                                    params=request.url._url,
                                    item_of_interest=documentID, 
                                    return_status_code=status_code,
                                    status_message=status_message
                                    )

    log_endpoint_time(request, ts=ts, level="debug")
    return ret_val

#---------------------------------------------------------------------------------------------------------
@app.get("/v2/Database/WhatsNew/", response_model=models.WhatsNewList, response_model_exclude_unset=True, tags=["Database"], summary=opasConfig.ENDPOINT_SUMMARY_WHATS_NEW)
def database_whatsnew(response: Response,
                      request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),  
                      days_back: int=Query(14, title=opasConfig.TITLE_DAYSBACK, description=opasConfig.DESCRIPTION_DAYSBACK),
                      limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_WHATS_NEW, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                      offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET), 
                      update_cache: bool=Query(False, title=opasConfig.TITLE_UPDATE_CACHE, description=opasConfig.DESCRIPTION_UPDATE_CACHE),
                      client_id:int=Depends(get_client_id), 
                      client_session:str= Depends(get_client_session)
                      ):  
    """
    ## Function
       ### Return a list of issues for journals modified in the days_back period).


    ## Return Type
       models.WhatsNewList

    ## Status
       This endpoint is working.

    ## Sample Call
         /v2/Database/WhatsNew/

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    ts = time.time()
    
    caller_name = "[v2/Database/WhatsNew]"
    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name} {client_session}: ")

    # for debugging client call
    #opasDocPermissions.verify_header(request, "WhatsNew")
    # (Don't log calls to this endpoint)
    # ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id)
    #log_endpoint(request, client_id=client_id, level="debug")
    
    try:
        # return whatsNewList
        ret_val = whatsnewdb.get_whats_new(limit=limit, 
                                           offset=offset,
                                           days_back=days_back, 
                                           req_url=request.url,
                                           forced_update=update_cache
                                           )

    except Exception as e:
        e = str(e)
        logger.error(f"Error in database_whatsnew: {e}. Raising HTTP_400_BAD_REQUEST.")
        raise HTTPException(status_code=httpCodes.HTTP_400_BAD_REQUEST,
                            detail="Error: {}".format(e.replace("'", "\\'"))
                            )
    else:
        # response.status_message = "Success"
        if ret_val is None:
            logger.error("whatsnewdb returned None")
        else:
            response.status_code = httpCodes.HTTP_200_OK
            ret_val.whatsNew.responseInfo.request = request.url._url

    log_endpoint_time(request, ts=ts, level="debug")
    return ret_val

#---------------------------------------------------------------------------------------------------------
@app.get("/v2/Database/WordWheel/", response_model=models.TermIndex, response_model_exclude_unset=True, tags=["Database"], summary=opasConfig.ENDPOINT_SUMMARY_WORD_WHEEL)
def database_word_wheel(response: Response,
                        request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),
                        word: str=Query(None, title=opasConfig.TITLE_WORD, description=opasConfig.DESCRIPTION_WORD),
                        field: str=Query("text", title=opasConfig.TITLE_WORDFIELD, description=opasConfig.DESCRIPTION_WORDFIELD),
                        core: str=Query("docs", title=opasConfig.TITLE_CORE, description=opasConfig.DESCRIPTION_CORE),
                        limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                        #offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET),
                        startat:str=Query(None, title="Start at this term/prefix (inconsistent at best)."), 
                        client_id:int=Depends(get_client_id), 
                        client_session:str= Depends(get_client_session)
                        ):
    """
    ## Function
       ### Return a list (termIndex) of words for the field matching the prefix.

       Implement a word wheel function in the calling app.

       The field must be a derivative of class solr.TextField. In the PEP implementation,  that includes:
       text, text_general, text_simple, text_general_syn
       string_ci (case insensitive string, TextField based)

       The field does not need to be one that's actually "stored".

       Examples of applicable fields from solr core docs for PEP:
             text - full-text document search
             para or art_para (deprecated) - full-text, all paragraphs (para is level 2, art_para is level 1 (deprecated))
             art_authors - string_ci, returns matching full names (authors is string, so it cannot be used)
             art_authors_xml - text, returns matching components of names
             quotes_spkr
             headings_xml - text
             capt
             authors - string_ci, full names
             terms_xml
             glossary_terms, glossary_group_terms - string_ci
             art_kwds - text
             art_kwds_str - string_ci 

       The default field text is in the docs core; currently docs and authors are the only cores supported.

       This endpoint return word counts for the words returned; it can also be used to check the number of instances of a search
       term, to determine if it's going to be effective by itself in limiting the results.


    ## Return Type
       models.TermIndex

    ## Status
       This endpoint is working.

    ## Sample Call
         http://localhost:9100/v2/WordWheel?phleb

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    ret_val = None

    caller_name = "[v2/Database/WordWheel]"
    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name} {client_session}: ")
    
    # ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id)

    if core in ["docs", "authors"] and word is not None:
        try:
            # returns models.TermIndex
            term_to_check = word.lower()  # work with lower case only, since Solr is case sensitive.
            ret_val = opasPySolrLib.get_term_index(term_to_check,
                                                   term_field=field,
                                                   core=core,
                                                   req_url=request.url, 
                                                   limit=limit,
                                                   start_at=startat)
        except ConnectionRefusedError as e:
            status_message = f"The server is not running or is currently not accepting connections: {e}"
            logger.error(f"WordWheelError: {status_message}")
            raise HTTPException(
                status_code=httpCodes.HTTP_503_SERVICE_UNAVAILABLE,
                detail=status_message
            )
        except Exception as e:
            status_message = f"InternalServerError: {e}"
            logger.error(status_message)
            raise HTTPException(
                status_code=httpCodes.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=status_message
            )
        else:
            status_message = opasCentralDBLib.API_STATUS_SUCCESS
            response.status_code = httpCodes.HTTP_200_OK
    else:
        if word is not None:
            status_message = f"WordWheelError: Unsupported Core: {core}"
        else:
            status_message = f"WordWheelError: No word supplied."
        logger.error(status_message)
        raise HTTPException(
            status_code=httpCodes.HTTP_400_BAD_REQUEST,
            detail=status_message
        )

    # for maximum speed since this is used for word_wheels, don't log the endpoint occurrences
    return ret_val  # Return author information or error

#-----------------------------------------------------------------------------
@app.get("/v2/Metadata/ArticleID/", response_model=ArticleID, response_model_exclude_unset=True, tags=["Metadata"], summary=opasConfig.ENDPOINT_SUMMARY_METADATA_ARTICLEID)
def metadata_articleid(response: Response,
                       request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),
                       articleID: str=Query("*", title=opasConfig.TITLE_SOURCECODE, description="ArticleID to evaluate"), 
                       diagnostics: bool=Query(False, title="Show all parsed fields", description="Show all information for article ID diagnostics"), 
                   ):
    """
    ## Function
       <b>Check if articleID is a valid articleID and break down the subinformation from it, returning in a the
          ArticleID model (which is defined in opasConfig).  This is defined and configured in opasConfig
          so it can be customized for variations in the ID.

    ## Return Type
       opasconfig.ArticleID

    ## Status
       This endpoint is working.

    ## Sample Call
         {url}/v2/Metadata/ArticleID/
         
         http://development.org:9100/v2/Metadata/ArticleID/&articleID=GW.004.0051A

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    caller_name = "[v2/Metadata/ArticleID]"
    if opasConfig.DEBUG_TRACE: print(caller_name)
    
    # api_id = opasCentralDBLib.API_METADATA_ARTICLEID
    
    # return the articleID model to the client to break it down for them
    ret_val = ArticleID(art_id=articleID, allInfo=diagnostics)
    
    return ret_val


#-----------------------------------------------------------------------------
@app.get("/v2/Metadata/Books/", response_model=models.SourceInfoList, response_model_exclude_unset=True, tags=["Metadata"], summary=opasConfig.ENDPOINT_SUMMARY_BOOK_NAMES)
def metadata_books(response: Response,
                   request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),  
                   sourcecode: str=Query("*", title=opasConfig.TITLE_SOURCECODE, description=opasConfig.DESCRIPTION_SOURCECODE_METADATA_BOOKS), 
                   sourcename: str=Query(None, title=opasConfig.TITLE_SOURCENAME, description=opasConfig.DESCRIPTION_SOURCENAME),  
                   limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_METADATA_LISTS, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                   offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET), 
                   client_id:int=Depends(get_client_id), 
                   client_session:str= Depends(get_client_session)
                   ):
    """
    ## Function
    ### Get a list of Book names equivalent to what is displayed on the original PEP-Web in the books tab.

       The data is pulled from the database api_productbase table.  Subvolumes of a book series (e.g., GW, SE) are not returned, nor is any volume
       marked with multivolumesubbok in the src_type_qualifier column.  This is exactly what's currently in PEP-Web's
       presentation today.
       
       To get metadata for a specific book of a series or subvolume of a multivolume book like GW, SE, NLP, IPL, or ZBK,
       use the first two parts of the source document ID (series and series volume) without the period separator used in
       the document ID. For example, to request metadata for SE volume 6, use SE006 in the sourcecode API parameter.
       Similarly, use NLP014 to get metadata on the book published as volume 14 of the NLP series.  While ZBK is a general
       series ID for classic books, the same is true...use ZBK047 to return metadata on that specific book.

    ## Return Type
       models.SourceInfoList (return label sourceInfo)

    ## Status
       This endpoint is working.

    ## Sample Call
         {url}/v2/Metadata/Books/?sourcecode=SE

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    caller_name = "[v2/Metadata/Books]"
    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name} {client_session}: ")

    ret_val = metadata_by_sourcetype_sourcecode(response,
                                                request,
                                                SourceType="Book",
                                                SourceCode=sourcecode,
                                                sourcename=sourcename, 
                                                limit=limit,
                                                offset=offset,
                                                client_id=client_id 
                                                #client_session=client_session
                                                )
    return ret_val

#-----------------------------------------------------------------------------
@app.get("/v2/Metadata/Contents/{SourceCode}/", response_model=models.DocumentList, response_model_exclude_unset=True, tags=["Metadata"], summary=opasConfig.ENDPOINT_SUMMARY_CONTENTS_SOURCE)
def metadata_contents_sourcecode(response: Response,
                                 request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),  
                                 SourceCode: str=Path(..., title=opasConfig.TITLE_SOURCECODE, description=opasConfig.DESCRIPTION_SOURCECODE), 
                                 year: str=Query("*", title=opasConfig.TITLE_YEAR, description=opasConfig.DESCRIPTION_YEAR),
                                 moreinfo:int=Query(0, title=opasConfig.TITLE_MOREINFO, description=opasConfig.DESCRIPTION_MOREINFO_CONTENTS), 
                                 limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_CONTENTS_LISTS, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                                 offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET), 
                                 client_id:int=Depends(get_client_id), 
                                 client_session:str= Depends(get_client_session)
                                 ):
    """
    ## Function
       ### Return a list of documents for a SourceCode (and optional year specified in query params).

       Note: The GVPi implementation does not appear to support the limit and offset parameter

    ## Return Type
       models.DocumentList

    ## Status
       This endpoint is working.

    ## Sample Call
         /v2/Metadata/Contents/IJP/

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    # ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id)
    caller_name = "[/v2/Metadata/Contents/{SourceCode}]"

    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name} {client_session}: ")
    
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")
    
    try:       
        ret_val = opasPySolrLib.metadata_get_contents(SourceCode,
                                                          year,
                                                          extra_info=moreinfo, 
                                                          limit=limit,
                                                          offset=offset)
        # fill in additional return structure status info
        # client_host = request.client.host
    except Exception as e:
        status_message = "Error: {}".format(e)
        raise HTTPException(
            status_code=httpCodes.HTTP_400_BAD_REQUEST,
            detail=status_message
        )
    else:
        # status_message = opasCentralDBLib.API_STATUS_SUCCESS
        # status_code = httpCodes.HTTP_200_OK
        ret_val.documentList.responseInfo.request = request.url._url

    #ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_METADATA_CONTENTS,
                                #session_info=session_info, 
                                #params=request.url._url,
                                #item_of_interest="{}.{}".format(SourceCode, year), 
                                #return_status_code = status_code,
                                #status_message=status_message
                                #)

    return ret_val # document_list

#-----------------------------------------------------------------------------
@app.get("/v2/Metadata/Contents/{SourceCode}/{SourceVolume}/", response_model=models.DocumentList, response_model_exclude_unset=True, tags=["Metadata"], summary=opasConfig.ENDPOINT_SUMMARY_CONTENTS_SOURCE_VOLUME)
def metadata_contents(SourceCode: str, 
                      SourceVolume: str, 
                      response: Response,
                      request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),  
                      year: str=Query("*", title=opasConfig.TITLE_YEAR, description=opasConfig.DESCRIPTION_YEAR),
                      moreinfo:int=Query(0, title=opasConfig.TITLE_MOREINFO, description=opasConfig.DESCRIPTION_MOREINFO_CONTENTS), 
                      limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_CONTENTS_LISTS, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                      offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET),
                      client_id:int=Depends(get_client_id), 
                      client_session:str= Depends(get_client_session)
                      ):
    """
    ## Function
       ### Return a list of documents for a SourceCode and Source Volume (required).

       Year can also be optionally specified in query params.  

    ## Return Type
       models.DocumentList

    ## Status
       This endpoint is working.

    ## Sample Call
         /v2/Metadata/Contents/IJP/77/
         http://development.org:9100/v2/Metadata/Contents/IJP/77/

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    # ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id)
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")

    caller_name = "[/v2/Metadata/Contents/{SourceCode}/{SourceVolume}]"

    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name} {client_session}: ")

    try:
        ret_val = documentList = opasPySolrLib.metadata_get_contents(SourceCode,
                                                                         year,
                                                                         vol=SourceVolume,
                                                                         extra_info=moreinfo, 
                                                                         limit=limit,
                                                                         offset=offset)
        # fill in additional return structure status info
        # client_host = request.client.host
    except Exception as e:
        status_message = f"{SourceCode} / {year} {SourceVolume} {moreinfo} MetadataError: {e}"
        logger.error(status_message)
        raise HTTPException(
            status_code=httpCodes.HTTP_400_BAD_REQUEST,
            detail=status_message
        )
    else:
        status_message = opasCentralDBLib.API_STATUS_SUCCESS
        # status_code = httpCodes.HTTP_200_OK
        ret_val.documentList.responseInfo.request = request.url._url

    # 2020-07-23 No need to log success for these, can be excessive.
    #ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_METADATA_CONTENTS_FOR_VOL,
                                #session_info=session_info, 
                                #item_of_interest="{}.{}".format(SourceCode, SourceVolume), 
                                #params=request.url._url,
                                #return_status_code = status_code,
                                #status_message=status_message
                                #)
    return ret_val

#-----------------------------------------------------------------------------
@app.get("/v2/Metadata/Journals/", response_model=models.JournalInfoList, response_model_exclude_unset=True, tags=["Metadata"], summary=opasConfig.ENDPOINT_SUMMARY_JOURNALS)
def metadata_journals(response: Response,
                      request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),  
                      #this param changed to sourcecode in v2 from journal in v1 (sourcecode is more accurately descriptive since this includes book series and video series)
                      sourcecode: str=Query("*", title=opasConfig.TITLE_SOURCECODE, description=opasConfig.DESCRIPTION_SOURCECODE_METADATA_JOURNALS), 
                      sourcename: str=Query(None, title=opasConfig.TITLE_SOURCENAME, description=opasConfig.DESCRIPTION_SOURCENAME),  
                      limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_METADATA_LISTS, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                      offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET),
                      client_id:int=Depends(get_client_id), 
                      client_session:str= Depends(get_client_session)
                      ):
    """
    ## Function
    ### Get a list of of journal names and metadata equivalent to what is displayed on the original PEP-Web in the journals tab.
    
    To get information on a specific journal, set the sourcecode query parameter to one of the standard PEP journal
    codes (the first part of the three part document ID for articles (e.g., AJRPP, IJP, PAQ).  Case is not treated signficantly.
    
    ## Return Type
       models.JournalInfoList (return label sourceInfo)

    ## Status
       This endpoint is working.

    ## Sample Call
         /v2/Metadata/Journals/

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")
    ret_val = metadata_by_sourcetype_sourcecode(response,
                                                request,
                                                SourceType="Journal",
                                                SourceCode=sourcecode,
                                                sourcename=sourcename, 
                                                limit=limit,
                                                offset=offset,
                                                client_id=client_id
                                                #client_session= client_session
                                                )
    return ret_val

#-----------------------------------------------------------------------------
@app.get("/v2/Metadata/Videos/", response_model=models.VideoInfoList, response_model_exclude_unset=True, tags=["Metadata"], summary=opasConfig.ENDPOINT_SUMMARY_VIDEOS)
def metadata_videos(response: Response,
                    request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),
                    sourcecode: str=Query("*", title=opasConfig.TITLE_SOURCECODE, description=opasConfig.DESCRIPTION_SOURCECODE_METADATA_VIDEOS),
                    sourcename: str=Query(None, title=opasConfig.TITLE_SOURCENAME, description=opasConfig.DESCRIPTION_SOURCENAME),
                    streams: bool=Query(True, description="Return videostreams (e.g., by publisher) rather than individual video information"),
                    limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_METADATA_LISTS, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                    offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET),
                    client_id:int=Depends(get_client_id), 
                    client_session:str= Depends(get_client_session)
                    ):
    """
    ## Function
    ### Get a complete list of video names

    Query parameter sourcecode is the short abbreviation of the videostream (by publisher) used as part of the DocumentIDs. (e.g., for PEP in 2020, this includes:
      IPSAVS, PEPVS, PEPTOPAUTHVS, BPSIVS, IJPVS, PCVS, SFCPVS, UCLVS, PEPGRANTVS, AFCVS, NYPSIVS, SPIVS).
      
    To get information on all, do not specify a sourcecode (* is the default)
    
    To get 

    ## Return Type
       models.VideoInfoList

    ## Status
       This endpoint is working.

    ## Sample Call
         /v2/Metadata/Videos/

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    if streams:
        source_type = "stream"
    else:
        source_type = "videos"
    
    ret_val = metadata_by_sourcetype_sourcecode(response,
                                                request,
                                                SourceType=source_type,
                                                SourceCode=sourcecode,
                                                sourcename=sourcename, 
                                                limit=limit,
                                                offset=offset,
                                                client_id=client_id, 
                                                client_session= client_session
                                                )
    return ret_val

#-----------------------------------------------------------------------------
@app.get("/v2/Metadata/Volumes/", response_model=models.VolumeList, response_model_exclude_unset=True, tags=["Metadata"], summary=opasConfig.ENDPOINT_SUMMARY_VOLUMES)
def metadata_volumes(response: Response,
                     request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),  
                     sourcetype: str=Query(None, title=opasConfig.TITLE_SOURCETYPE, description=opasConfig.DESCRIPTION_PARAM_SOURCETYPE),
                     sourcecode: str=Query(None, title=opasConfig.TITLE_SOURCECODE, description=opasConfig.DESCRIPTION_SOURCECODE), 
                     #limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_VOLUME_LISTS, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                     #offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET),
                     client_id:int=Depends(get_client_id), 
                     client_session:str= Depends(get_client_session)
                     ):
    """
    ## Function
    ### Return a list of volumes for a SourceCode (aka, PEPCode (e.g., IJP)) per the limit and offset parameters

    ## Return Type
       models.VolumeList

    ## Status
       This endpoint is working.

    ## Sample Call
         {url}/v2/Metadata/Volumes/?sourcecode=ijp

    ## Notes
      2020.11.17 Removed limit and offset from q params: they don't work here with the facet pivot used,
                 and moreover, don't make sense when you have more than one journal being returned

    ## Potential Errors
       N/A
       
    """
    ocd = opasCentralDBLib.opasCentralDB()
    log_endpoint(request, client_id=client_id, level="debug")

    # Solr is case sensitive, make sure arg is upper
    try:
        source_code = sourcecode.upper()
    except:
        source_code = "*"

    src_exists = ocd.get_sources(src_code=source_code)
    
    if not src_exists[0] and source_code != "*" and source_code not in ["ZBK", "NLP", "IPL"] and source_code is not None: # ZBK not in productbase table without booknum
        response.status_code = httpCodes.HTTP_400_BAD_REQUEST
        status_message = f"Failure: Bad SourceCode {source_code}"
        raise HTTPException(
            status_code=response.status_code,
            detail=status_message
        )
    else:
        try:
            ret_val = opasPySolrLib.metadata_get_volumes(source_code,
                                                         source_type=sourcetype,
                                                         req_url=request.url
                                                         #limit=limit,   # these don't work with facet pivot used here
                                                         #offset=offset  # at least with solrpy
                                                         )
        except Exception as e:
            response.status_code = httpCodes.HTTP_400_BAD_REQUEST,
            status_message = "Error: {}".format(e)
            raise HTTPException(
                status_code=response.status_code, 
                detail=status_message
            )
        else:
            response.status_code = httpCodes.HTTP_200_OK
            status_message = opasCentralDBLib.API_STATUS_SUCCESS

    return ret_val # returns volumeList

#-----------------------------------------------------------------------------
@app.get("/v1/Metadata/{SourceType}/{SourceCode}/", response_model=models.SourceInfoList, response_model_exclude_unset=True, tags=["Metadata"], summary=opasConfig.ENDPOINT_SUMMARY_SOURCE_NAMES)
def metadata_by_sourcetype_sourcecode(response: Response,
                                      request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),  
                                      SourceType: str=Path(..., title=opasConfig.TITLE_SOURCETYPE, description=opasConfig.DESCRIPTION_PATH_SOURCETYPE), 
                                      SourceCode: str=Path(..., title=opasConfig.TITLE_SOURCECODE, description=opasConfig.DESCRIPTION_SOURCECODE), 
                                      sourcename: str=Query(None, title=opasConfig.TITLE_SOURCENAME, description=opasConfig.DESCRIPTION_SOURCENAME),  
                                      limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_METADATA_LISTS, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                                      offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET),
                                      client_id:int=Depends(get_client_id), 
                                      client_session:str= Depends(get_client_session)
                                      ):
    """

    ## Function
       ### Return a list of information about a source type, e.g., journal names

       The data is pulled from the database ISSN table.  Subvolumes, of SE and GW are not returned, nor is any volume
       marked with multivolumesubbok in the src_type_qualifier column.  This is exactly what's currently in PEP-Web's
       presentation today.

    ## Return Type
       models.SourceInfoList

    ## Status
       This endpoint is working.  This is v1 only.  Deprecated as of v2, but still active.
       
       In v2, the endpoint uses a param for sourcecode rather than the path variable.

    ## Sample Call
         http://localhost:9100/v1/Metadata/Books/IPL

    ## Notes
        Depends on:
          vw_api_productbase_instance_counts

    ## Potential Errors
        N/A

    """
    opasDocPermissions.verify_header(request, "metadata_by_sourcetype_sourcecode") # for debugging client call
    log_endpoint(request, client_id=client_id, level="debug")

    #ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id)

    source_code = SourceCode.upper()
    try:    
        ret_val = source_info_list = opasAPISupportLib.metadata_get_source_info(src_type=SourceType,
                                                                                src_code=source_code,
                                                                                src_name=sourcename, 
                                                                                limit=limit,
                                                                                offset=offset)
    except Exception as e:
        status_message = f"MetadataError: {e}"
        response.status_code = httpCodes.HTTP_400_BAD_REQUEST
        logger.error(status_message)
        raise HTTPException(
            status_code=response.status_code, 
            detail=status_message
        )
    else:
        status_message = opasCentralDBLib.API_STATUS_SUCCESS

        response.status_code = httpCodes.HTTP_200_OK
        # fill in additional return structure status info
        ret_val.sourceInfo.responseInfo.request = request.url._url

    return ret_val

#-----------------------------------------------------------------------------
@app.get("/v2/Authors/Index/{authorNamePartial}/", response_model=models.AuthorIndex, response_model_exclude_unset=True, tags=["Authors"], summary=opasConfig.ENDPOINT_SUMMARY_AUTHOR_INDEX)
def authors_index(response: Response,
                  request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),  
                  authorNamePartial: str=Path(..., title=opasConfig.TITLE_AUTHORNAMEORPARTIAL, description=opasConfig.DESCRIPTION_AUTHORNAMEORPARTIAL), 
                  limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                  offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET),
                  client_id:int=Depends(get_client_id), 
                  client_session:str= Depends(get_client_session)
                  ):
    """
    ## Function
       ### Return a list (index) of authors.  The list shows the author IDs, which are a normalized form of an authors name.

    ## Return Type
       models.AuthorIndex

    ## Status
       This endpoint is working.

    ## Sample Call
         http://localhost:9100/v2/Authors/Index/Tuck/

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    ret_val = None
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")
    # ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id)

    try:
        # returns models.AuthorIndex
        author_name_to_check = authorNamePartial.lower()  # work with lower case only, since Solr is case sensitive.
        ret_val = opasPySolrLib.authors_get_author_info(author_name_to_check, limit=limit, offset=offset)
    except ConnectionRefusedError as e:
        status_message = f"AuthorsIndexError: The server is not running or is currently not accepting connections: {e}"
        logger.error(status_message)
        raise HTTPException(
            status_code=httpCodes.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=status_message
        )

    except Exception as e:
        status_message = f"AuthorsIndexError: {e}"
        logger.error(status_message)
        raise HTTPException(
            status_code=httpCodes.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=status_message
        )
    else:
        status_message = opasCentralDBLib.API_STATUS_SUCCESS

        response.status_code = httpCodes.HTTP_200_OK
        # fill in additional return structure status info
        ret_val.authorIndex.responseInfo.request = request.url._url

    # for speed since this is used for author pick lists, don't log the endpoint occurrences
    #ocd.recordSessionEndpoint(apiEndpointID=opasCentralDBLib.API_AUTHORS_INDEX,
                                        #params=request.url._url,
                                        #returnStatusCode = resp.status_code = ,
                                        #statusMessage=statusMessage
                                        #)

    return ret_val  # Return author information or error

#-----------------------------------------------------------------------------
@app.get("/v2/Authors/Publications/{authorNamePartial}/", response_model=models.AuthorPubList, response_model_exclude_unset=True, tags=["Authors"], summary=opasConfig.ENDPOINT_SUMMARY_AUTHOR_PUBLICATIONS)
def authors_publications(response: Response,
                         request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),  
                         authorNamePartial: str=Path(..., title=opasConfig.TITLE_AUTHORNAMEORPARTIAL, description=opasConfig.DESCRIPTION_AUTHORNAMEORPARTIAL), 
                         limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                         offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET),
                         client_id:int=Depends(get_client_id), 
                         client_session:str= Depends(get_client_session)
                         ):
    """
    ## Function
       ### Return a list of the author's publications.
       regex style wildcards are permitted.

    ## Return Type
       models.AuthorPubList

    ## Status
       This endpoint is working.

    ## Sample Call
         http://localhost:8000/v2/Authors/Publications/Tuck/
         http://localhost:8000/v2/Authors/Publications/maslow, a.*/

    ## Notes
       N/A
       
    ## Potential Errors
       N/A

    """
    caller_name = "[v2/Authors/Publications]"

    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name} {client_session}: ")
    
    # ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")

    try:
        author_name_to_check = authorNamePartial.lower()  # work with lower case only, since Solr is case sensitive.
        ret_val = opasPySolrLib.authors_get_author_publications(author_name_to_check, limit=limit, offset=offset)
    except Exception as e:
        response.status_code=httpCodes.HTTP_500_INTERNAL_SERVER_ERROR
        status_message = f"AuthorsPublicationsError: Internal Server Error: {e}"
        logger.error(status_message)
        raise HTTPException(
            status_code=response.status_code,
            detail=status_message
        )
    else:
        status_message = opasCentralDBLib.API_STATUS_SUCCESS
        ret_val.authorPubList.responseInfo.request = request.url._url
        response.status_code = httpCodes.HTTP_200_OK
        #ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_AUTHORS_PUBLICATIONS,
                                    #session_info=session_info, 
                                    #params=request.url._url,
                                    #item_of_interest=f"{authorNamePartial}", 
                                    #return_status_code = response.status_code,
                                    #status_message=status_message
                                    #)

    return ret_val

#-----------------------------------------------------------------------------
@app.get("/v2/Documents/Abstracts/{documentID}/", response_model=models.Documents, response_model_exclude_unset=True, tags=["Documents"], summary=opasConfig.ENDPOINT_SUMMARY_ABSTRACT_VIEW)
def documents_abstracts(response: Response,
                        request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),  
                        documentID: str=Path(..., title=opasConfig.TITLE_DOCUMENT_ID, description=opasConfig.DESCRIPTION_DOCIDORPARTIAL), 
                        return_format: str=Query("HTML", title=opasConfig.TITLE_RETURNFORMATS, description=opasConfig.DESCRIPTION_RETURNFORMATS),
                        similarcount: int=Query(0, title=opasConfig.TITLE_SIMILARCOUNT, description=opasConfig.DESCRIPTION_SIMILARCOUNT),
                        sort: str=Query("art_authors_citation asc", title=opasConfig.TITLE_SORT, description=opasConfig.DESCRIPTION_SORT),
                        limit: int=Query(opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, title=opasConfig.TITLE_LIMIT, description=opasConfig.DESCRIPTION_LIMIT),
                        offset: int=Query(0, title=opasConfig.TITLE_OFFSET, description=opasConfig.DESCRIPTION_OFFSET),
                        client_id:int=Depends(get_client_id), 
                        client_session:str= Depends(get_client_session)
                        ):
    """
    ## Function
       ### Return an abstract for the requested documentID (e.g., IJP.077.0001A, or multiple abstracts for a partial ID, e.g., IJP.077)

    ## Return Type
       models.Documents

    ## Status
       This endpoint is working.

    ## Sample Call
         http://localhost:9100/v2/Documents/Abstracts/IJP.001.0203A/

    ## Notes
       PEP Easy 1.03Beta expects HTML abstract return (it doesn't specify a format)

    ## Potential Errors
       N/A

    """
    caller_name = "[v2/Documents/Abstracts]"

    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name} {client_session}: ")
    
    opasDocPermissions.verify_header(request, caller_name)  # for debugging client call
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")
    ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)

    try:
        # authenticated = opasAPISupportLib.is_session_authenticated(request, response)
        # make sure it's upper case for consistency (added 2021-10-10)
        documentID = documentID.upper()
        ret_val = opasAPISupportLib.documents_get_abstracts(document_id=documentID,
                                                            ret_format=return_format,
                                                            #authenticated=authenticated,
                                                            similar_count=similarcount, 
                                                            req_url=request.url._url, 
                                                            limit=limit,
                                                            offset=offset,
                                                            sort=sort,
                                                            session_info=session_info,
                                                            request=request
                                                            )
    except Exception as e:
        response.status_code=httpCodes.HTTP_400_BAD_REQUEST
        status_message = f"AbstractsError: {response.status_code}: {e}"
        logger.error(status_message)
        # error, don't record
        #ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_DOCUMENTS_ABSTRACTS,
                                    #session_info=session_info, 
                                    #params=request.url._url,
                                    #item_of_interest=f"{documentID}", 
                                    #return_status_code = response.status_code,
                                    #status_message=status_message
                                    #)
        raise HTTPException(
            status_code=response.status_code,
            detail=status_message
        )
    else:
        status_message = opasCentralDBLib.API_STATUS_SUCCESS

        if ret_val.documents.responseInfo.count > 0:
            response.status_code = httpCodes.HTTP_200_OK
            #  record document view if found
            ocd.record_document_view(document_id=documentID,
                                     session_info=session_info,
                                     view_type="Abstract")
            
            if ret_val.documents.responseInfo.count == 1:
                if ret_val.documents.responseSet[0].accessChecked == True and ret_val.documents.responseSet[0].accessLimited == False:
                    document_list_item = ret_val.documents.responseSet[0]
                    if opasFileSupport.file_exists(document_id=document_list_item.documentID, 
                                                   year=document_list_item.year,
                                                   ext=localsecrets.PDF_ORIGINALS_EXTENSION, 
                                                   path=localsecrets.PDF_ORIGINALS_PATH):
                        document_list_item.pdfOriginalAvailable = True
                    else:
                        document_list_item.pdfOriginalAvailable = False
            
        else:
            # make sure we specify an error in the session log
            #  not sure this is the best return code, but for now...
            response.status_code = httpCodes.HTTP_404_NOT_FOUND

        ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_DOCUMENTS_ABSTRACTS,
                                    session_info=session_info, 
                                    params=request.url._url,
                                    item_of_interest=f"{documentID}", 
                                    return_status_code = response.status_code,
                                    status_message=status_message
                                    )
    return ret_val

#-----------------------------------------------------------------------------
@app.get("/v2/Documents/Concordance/", response_model=models.Documents, tags=["Documents"], summary=opasConfig.ENDPOINT_SUMMARY_CONCORDANCE, response_model_exclude_unset=True)  # the current PEP API
def documents_concordance(response: Response,
                          request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),
                          paralangid: str=Query(None, title=opasConfig.TITLE_DOCUMENT_CONCORDANCE_ID, description=opasConfig.DESCRIPTION_DOCUMENT_CONCORDANCE_ID), # return controls 
                          paralangrx: str=Query(None, title=opasConfig.TITLE_DOCUMENT_CONCORDANCE_RX, description=opasConfig.DESCRIPTION_DOCUMENT_CONCORDANCE_RX), # return controls 
                          return_format: str=Query("HTML", title=opasConfig.TITLE_RETURNFORMATS, description=opasConfig.DESCRIPTION_RETURNFORMATS),
                          search: str=Query(None, title=opasConfig.TITLE_SEARCHPARAM, description=opasConfig.DESCRIPTION_SEARCHPARAM),
                          client_id:int=Depends(get_client_id), 
                          client_session:str= Depends(get_client_session)
                          ):
    """
    ## Function
       ### Returns the translation paragraph identified by the unique para ID(s)

    ## Return Type
       models.Documents

    ## Status
       This endpoint is working.

    ## Sample Call
         http://localhost:9100/v2/Documents/Document/IJP.077.0217A/

    ## Notes
       N/A
       
    ## Potential Errors
       THE USER NEEDS TO BE AUTHENTICATED to return a para.

    """

    caller_name = "[v2/Documents/Concordance]"

    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name} {client_session}: ")

    ret_val = None
    item_of_interest = f"{paralangid}/{paralangrx}"

    opasDocPermissions.verify_header(request, caller_name) # for debugging client call
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")
    ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)

    try:
        ret_val = opasAPISupportLib.documents_get_concordance_paras( para_lang_id=paralangid,
                                                                     para_lang_rx=paralangrx, 
                                                                     ret_format=return_format,
                                                                     req_url=request.url._url, 
                                                                     session_info=session_info,
                                                                     request=request
                                                                     )
    except Exception as e:
        response.status_code=httpCodes.HTTP_400_BAD_REQUEST
        status_message = f"ConcordanceError: Para Fetch: {e}"
        logger.error(status_message)
        ret_val = None
        #ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_DOCUMENTS_CONCORDANCE,
                                    #session_info=session_info, 
                                    #params=request.url._url,
                                    #item_of_interest=item_of_interest, 
                                    #return_status_code = response.status_code,
                                    #status_message=status_message
                                    #)
        raise HTTPException(
            status_code=response.status_code,
            detail=status_message
        )
    else:
        if ret_val != {}:
            response.status_code = httpCodes.HTTP_200_OK
            status_message = opasCentralDBLib.API_STATUS_SUCCESS
        else:
            # make sure we specify an error in the session log
            # not sure this is the best return code, but for now...
            status_message = "Not Found"
            response.status_code = httpCodes.HTTP_404_NOT_FOUND

        ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_DOCUMENTS_CONCORDANCE,
                                    session_info=session_info, 
                                    params=request.url._url,
                                    item_of_interest=item_of_interest, 
                                    return_status_code = response.status_code,
                                    status_message=status_message
                                    )

        if ret_val == {}:
            raise HTTPException(
                status_code=response.status_code,
                detail=status_message
            )           

    return ret_val

#-----------------------------------------------------------------------------
@app.get("/v2/Documents/Document/{documentID}/", response_model=models.Documents, tags=["Documents"], summary=opasConfig.ENDPOINT_SUMMARY_DOCUMENT_VIEW, response_model_exclude_unset=True) # more consistent with the model grouping
def documents_document_fetch(response: Response,
                             request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),
                             documentID: str=Path(..., title=opasConfig.TITLE_DOCUMENT_ID, description=opasConfig.DESCRIPTION_DOCIDSINGLE), # return controls 
                             page:int=Query(None, title=opasConfig.TITLE_PAGEREQUEST, description=opasConfig.DESCRIPTION_PAGEREQUEST),
                             return_format: str=Query("HTML", title=opasConfig.TITLE_RETURNFORMATS, description=opasConfig.DESCRIPTION_RETURNFORMATS),
                             similarcount: int=Query(0, title=opasConfig.TITLE_SIMILARCOUNT, description=opasConfig.DESCRIPTION_SIMILARCOUNT),
                             translations: bool=Query(True, title=opasConfig.TITLE_TRANSLATIONS, description=opasConfig.DESCRIPTION_TRANSLATIONS),
                             search: str=Query(None, title=opasConfig.TITLE_SEARCHPARAM, description=opasConfig.DESCRIPTION_SEARCHPARAM),
                             pagelimit: int=Query(None,title=opasConfig.TITLE_PAGELIMIT, description=opasConfig.DESCRIPTION_PAGELIMIT),
                             pageoffset: int=Query(None, title=opasConfig.TITLE_PAGEOFFSET,description=opasConfig.DESCRIPTION_PAGEOFFSET),
                             specialoptions:int=Query(0, title=opasConfig.TITLE_SPECIALOPTIONS, description=opasConfig.DESCRIPTION_SPECIALOPTIONS), 
                             client_id:int=Depends(get_client_id), 
                             client_session:str= Depends(get_client_session)
                             ):
    """
    ## Function
       ### Returns the Document information, document summary (absract) and full-text - but conditionally.

       Returns only the summary (abstract) if non-authorized for that document via the authorization/license server (e.g., PaDS)
        
       Restricted to a single document return (partial documentID not permitted).

       To have hits marked in the full-text context, you can include a search, matching the /v2/Database/Search parameters,
       in the search parameter.  For example:
            search: fulltext1="philosophical differences"

       Parameter Additonal information
            - A Page component to the documentID, e.g., PCT.011.0171A.P0172 _is ignored_.
              This should be used by a client to jump to a page number instead.
            - limit, offset have been renamed to pagelimit, pageoffset to avoid confusion with the equivalent search parameters
            - page is an alternative to pageoffset, to use a page number from the document

    ## Return Type
       models.Documents

    ## Status
       This endpoint is working.
       
       EXPERIMENTAL:
          Use the specialoptions parameter, with an integer representing (flags) to turn on or off experimental features.
            If the feature is kept, they can be formally parameterized in a later edition.
            
            1 = Turn off glossary markup in documents
            2 = Return basic doclistinfo for any translations of the document (or original language) so
                they can be listed in a infocard or popup.  Note the original is returned in the list as well whenever there are
                translations.
                
            bitwise & these flags (full reference in opasConfig) together, e.g., 3 selects both options.  

    ## Sample Call
         http://localhost:9100/v2/Documents/Document/IJP.077.0217A/

    ## Notes
    
       Parameter "search" in the API Docs interface must include the variable name prefix "search=" to make it work
       so in the /Docs interface, the search edit field should be like:
           search=?fulltext1=adolescent
           rather than just ?fulltext1=adolescent
           
           But in the command line sent to the API it can be just:
           
                http://development.org:9100/v2/Documents/Document/ANRP.012.0129A/?return_format=xml&search=?fulltext1%3Dmultiple%20dimension&specialoptions=3

           or the longer form:
           
                http://development.org:9100/v2/Documents/Document/ANRP.012.0129A/?return_format=xml&search=?search%3D%26fulltext1%3Dmultiple%20dimension&specialoptions=3
           

    ## Potential Errors
       THE USER NEEDS TO BE AUTHENTICATED to return a document.  Otherwise an abstract/excerpt will be returned.
       Abstracts (and excerpts) are always returned in HTML

       If not authenticated:
          - The PEP developed server/API for V2 will return an abstract or summary if non-authenticated.
          - The GVPI V1 API returns an error if not authenticated

    """
    # NOTE: Calls the code for the Glossary endpoint via function view_a_glossary_entry)
    ret_val = None
    ts = time.time()
    caller_name = "[v2/Documents/Document]"
    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name} {client_session}: ")

    session_id = client_session
    opasDocPermissions.verify_header(request, "documents_fetch") # for debugging client call
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")
    ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)
    # for qualifying any errors:
    request_qualifier_text = f" Request: {documentID}. Session {session_info.session_id}."

    # check if this is a Glossary request, this is per API.v1.
    m = re.match("(ZBK\.069\..*?)?(?P<termid>(Y.0.*))", documentID)
    if m is not None:    
        # this is a glossary request, submit only the termID
        term_id = m.group("termid")
        #ret_val = view_a_glossary_entry(response, request, term_id=term_id, search=search, return_format=return_format)
        ret_val = documents_glossary_term(response, request, termIdentifier=term_id, return_format=return_format)
    else:       
        # Check to see if this is a valid source code, and if it is, that the document exists (or fix it)
        # new resilient documentID feature 2023-06-22/2023-07-02 (in progress)
        # 2023.0402/v2.3.008b Enabled
        if 1:
            doc_id = ArticleID(art_id=documentID)
            try:
                if doc_id is not None:
                    src_code = doc_id.src_code.upper()
                    if src_code in all_source_codes:
                        documentID = doc_id.exists_with_resilience(verbose=True, resilient=True)            
                    else:
                        #documentID = None
                        if src_code not in all_source_codes:
                            status_message = f"Src code {doc_id.src_code} not in {all_source_codes}.  But Letting pass though. "
                            logger.warning(status_message)
                            print (status_message)
                        else:
                            # for now, log and try anyway
                            status_message = f"Indication of Nonexistant document: {doc_id} Requestor: Client:{client_id}/Sess:{session_id}.  Letting pass though. "
                            logger.warning(status_message)
                            print (status_message)
            except Exception as e:
                logger.debug(f"Article ID resilience error ({e})")
            
            if not documentID:
                response.status_code=httpCodes.HTTP_404_NOT_FOUND
                status_message = f"Nonexistant document: {doc_id} Requestor: Client:{client_id}/Sess:{session_id} "
                logger.warning(status_message)
                ret_val = None
                raise HTTPException(
                    status_code=response.status_code,
                    detail=status_message
                )
                
            
        try:
            # documents_get_document handles the view authorization and returns abstract if not authenticated.
            req_url=urllib.parse.unquote(request.url._url)
            req_url_params = dict(parse.parse_qsl(parse.urlsplit(req_url).query))
            # param_search = req_url_params.get("search", None)
            
            ft1 = req_url_params.get("fulltext1")
            ft2 = req_url_params.get("smarttext")
            # search_old = search
            search = ""
            if ft2 is not None:
                search = f"&smarttext={ft2}"
            if ft1 is not None:
                search += f"&fulltext1={ft1}"
                
            solr_query_params = opasQueryHelper.parse_search_query_parameters(fulltext1=ft1, smarttext=ft2)
            
            # solr_query_params = opasQueryHelper.parse_search_query_parameters(**argdict)
            logger.debug("Document View Request: %s/%s/%s", solr_query_params, documentID, return_format)
            
            if translations == True:
                specialoptions = specialoptions | 2 # add flag to return translations

            # make sure it's upper case for consistency in logging (added 2021-10-10)
            documentID = documentID.upper()
            ret_val = opasAPISupportLib.documents_get_document( documentID, 
                                                                solr_query_params,
                                                                ret_format=return_format,
                                                                similar_count=similarcount,
                                                                page_offset=pageoffset, # starting page
                                                                page_limit=pagelimit, # number of pages
                                                                page=page, # specific page number request (rather than offset),
                                                                req_url=req_url, 
                                                                session_info=session_info,
                                                                option_flags=specialoptions,
                                                                request=request
                                                                )

            try:
                prev_art, match_art, next_art = opasPySolrLib.metadata_get_next_and_prev_articles(art_id=documentID)
                if prev_art != {}:
                    ret_val.documents.responseSet[0].sourcePrevious = prev_art["art_id"]
                if next_art != {}:
                    ret_val.documents.responseSet[0].sourceNext = next_art["art_id"]
            except Exception as e:
                logger.debug(f"No next/prev data to return ({e})")
        
        except Exception as e:
            response.status_code=httpCodes.HTTP_400_BAD_REQUEST
            status_message = f"DocumentFetchError: {client_id}/{session_id}: {e}"
            logger.error(status_message)
            ret_val = None
            raise HTTPException(
                status_code=response.status_code,
                detail=status_message
            )
        else:
            if ret_val is not None and ret_val != {}:
                response.status_code = httpCodes.HTTP_200_OK
                status_message = opasCentralDBLib.API_STATUS_SUCCESS
                try:
                    access = ret_val.documents.responseSet[0].accessChecked == True and ret_val.documents.responseSet[0].accessLimited == False
                    doc_len = len(ret_val.documents.responseSet[0].document)
                except Exception as e:
                    access = False
                    doc_len = 0
                    status_message = f"{client_id}:{session_id}: Document fetch Error {e}"
                    logger.info(status_message)
                else:
                    if access == False:
                        status_message = f"{client_id}:{session_id}: Document (Abstract only) fetch (access: {access}; doc length: {doc_len}"
                    else:
                        status_message = f"{client_id}:{session_id}: Document fetch (access: {access}; doc length: {doc_len}"
                    
                    logger.info(status_message)

            else:
                # make sure we specify an error in the session log
                # not sure this is the best return code, but for now...
                status_message = msgdb.get_user_message(opasConfig.ERROR_404_DOCUMENT_NOT_FOUND) + request_qualifier_text 
                response.status_code = httpCodes.HTTP_404_NOT_FOUND
                # record session endpoint in any case   

            ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_DOCUMENTS,
                                        session_info=session_info, 
                                        params=req_url,
                                        item_of_interest=documentID, 
                                        return_status_code = response.status_code,
                                        status_message=status_message
                                        )

            if ret_val is None or ret_val == {}:
                logger.error(status_message)
                raise HTTPException(
                    status_code=response.status_code,
                    detail=status_message
                )           
            else:
                ret_val.documents.responseInfo.request = req_url
                if access == False:
                    #  abstract returned...we don't count those currently.
                    logger.info("Full-text access not allowed--Abstract only." + request_qualifier_text)
                else:
                    if ret_val.documents.responseInfo.count > 0:
                        #  record document view if found
                        ocd.record_document_view(document_id=documentID,
                                                 session_info=session_info,
                                                 view_type="Document")
                    else:
                        logger.error("No document available." + request_qualifier_text)

    log_endpoint_time(request, ts=ts, level="debug")
    return ret_val

@app.get("/v2/Documents/Archival/{documentID}/", response_model=models.Documents, tags=["Documents"], summary=opasConfig.ENDPOINT_SUMMARY_DOCUMENT_VIEW, response_model_exclude_unset=True) # more consistent with the model grouping
def documents_archival_fetch(response: Response,
                             request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),
                             documentID: str=Path(..., title=opasConfig.TITLE_DOCUMENT_ID, description=opasConfig.DESCRIPTION_DOCIDSINGLE), # return controls 
                             specialoptions:int=Query(0, title=opasConfig.TITLE_SPECIALOPTIONS, description=opasConfig.DESCRIPTION_SPECIALOPTIONS), 
                             client_id:int=Depends(get_client_id), 
                             client_session:str= Depends(get_client_session)
                             ):
    """
    ## Function
       ### Returns the Document information, abstract and full-text, for ARCHIVAL documents only.

       Restricted to a single document return (partial documentID not permitted).
        
       Client session must be authenticated and have access permissions.

    ## Return Type
       models.Documents

    ## Status
       This endpoint is working but under review.

    ## Notes

    ## Potential Errors
       THE USER NEEDS TO BE AUTHENTICATED to return a document.  Otherwise an abstract/excerpt will be returned.

    """
    # NOTE: Calls the code for the Glossary endpoint via function view_a_glossary_entry)
    ret_val = None
    ts = time.time()
    caller_name = "[v2/Documents/Archival]"
    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name} {client_session}: ")

    session_id = client_session
    opasDocPermissions.verify_header(request, "archival_fetch") # for debugging client call
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")
    ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)
    # for qualifying any errors:
    request_qualifier_text = f" Request: {documentID}. Session {session_info.session_id}."

    if 1:    
        try:
            # documents_get_document handles the view authorization and returns abstract if not authenticated.
            req_url=urllib.parse.unquote(request.url._url)
            # req_url_params = dict(parse.parse_qsl(parse.urlsplit(req_url).query))
            # param_search = req_url_params.get("search", None)
            
            # let's see about loading it directly.
            # See if it's in the api_articles_removed table
            sqlSelect = f"SELECT * from api_articles_removed WHERE art_id='{documentID}'"
            rows = ocd.get_select_as_list_of_dicts(sqlSelect)
            # if not, it's an error
            if rows == []:
                logger.error("Archival Document not found")
            else:
                if len(rows) > 1: #this is an error
                    logger.error("There are more than one matching row")
                else:
                    document_record = rows[0]
                    # filename = document_record.get("filename", None)
                    fullfilename = document_record.get("fullfilename", None)
                    # Get the output file name
                    fullfilename = re.sub("\(b.*\)", opasConfig.DEFAULT_OUTPUT_BUILD, fullfilename)
                    # art_id = document_record.get("art_id", None)
                    # title = document_record.get("art_title", None)
                    ret_val = opasAPISupportLib.documents_get_document_from_file( documentID, 
                                                                                  req_url=req_url,
                                                                                  fullfilename=fullfilename, 
                                                                                  session_info=session_info,
                                                                                  option_flags=specialoptions,
                                                                                  request=request
                                                                                  )

                    try:
                        prev_art, match_art, next_art = opasPySolrLib.metadata_get_next_and_prev_articles(art_id=documentID)
                        if prev_art != {}:
                            ret_val.documents.responseSet[0].sourcePrevious = prev_art["art_id"]
                        if next_art != {}:
                            ret_val.documents.responseSet[0].sourceNext = next_art["art_id"]
                    except Exception as e:
                        logger.debug(f"No next/prev data to return ({e})")
          
        except Exception as e:
            response.status_code=httpCodes.HTTP_400_BAD_REQUEST
            status_message = f"DocumentFetchError: {client_id}/{session_id}: {e}"
            logger.error(status_message)
            ret_val = None
            raise HTTPException(
                status_code=response.status_code,
                detail=status_message
            )
        else:
            if ret_val is not None and ret_val != {}:
                response.status_code = httpCodes.HTTP_200_OK
                status_message = opasCentralDBLib.API_STATUS_SUCCESS
                try:
                    access = ret_val.documents.responseSet[0].accessChecked == True and ret_val.documents.responseSet[0].accessLimited == False
                    doc_len = len(ret_val.documents.responseSet[0].document)
                except Exception as e:
                    access = False
                    doc_len = 0
                    status_message = f"{client_id}:{session_id}: Document fetch Error {e}"
                    logger.info(status_message)
                else:
                    if access == False:
                        status_message = f"{client_id}:{session_id}: Document (Abstract only) fetch (access: {access}; doc length: {doc_len}"
                    else:
                        status_message = f"{client_id}:{session_id}: Document fetch (access: {access}; doc length: {doc_len}"
                    
                    logger.info(status_message)

            else:
                # make sure we specify an error in the session log
                # not sure this is the best return code, but for now...
                status_message = msgdb.get_user_message(opasConfig.ERROR_404_DOCUMENT_NOT_FOUND) + request_qualifier_text 
                response.status_code = httpCodes.HTTP_404_NOT_FOUND
                # record session endpoint in any case   

            ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_DOCUMENTS,
                                        session_info=session_info, 
                                        params=req_url,
                                        item_of_interest=documentID, 
                                        return_status_code = response.status_code,
                                        status_message=status_message
                                        )

            if ret_val is None or ret_val == {}:
                logger.error(status_message)
                raise HTTPException(
                    status_code=response.status_code,
                    detail=status_message
                )           
            else:
                ret_val.documents.responseInfo.request = req_url
                if access == False:
                    #  abstract returned...we don't count those currently.
                    logger.info("Full-text access not allowed--Abstract only." + request_qualifier_text)
                else:
                    if ret_val.documents.responseInfo.count > 0:
                        #  record document view if found
                        ocd.record_document_view(document_id=documentID,
                                                 session_info=session_info,
                                                 view_type="Document")
                    else:
                        logger.error("No document available." + request_qualifier_text)

    log_endpoint_time(request, ts=ts, level="debug")
    return ret_val

#-----------------------------------------------------------------------------
@app.get("/v2/Documents/Downloads/{retFormat}/{documentID}/", response_model_exclude_unset=True, tags=["Documents"], summary=opasConfig.ENDPOINT_SUMMARY_DOCUMENT_DOWNLOAD)
def documents_downloads(response: Response,
                        request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),  
                        documentID: str=Path(..., title=opasConfig.TITLE_DOCUMENT_ID, description=opasConfig.DESCRIPTION_DOCIDORPARTIAL), 
                        retFormat=Path(..., title=opasConfig.TITLE_RETURNFORMATS, description=opasConfig.DESCRIPTION_DOCDOWNLOADFORMAT),
                        page:int=Query(None, title=opasConfig.TITLE_PAGEREQUEST, description=opasConfig.DESCRIPTION_PAGEREQUEST),
                        pagelimit: int=Query(None,title=opasConfig.TITLE_PAGELIMIT, description=opasConfig.DESCRIPTION_PAGELIMIT),
                        pageoffset: int=Query(None, title=opasConfig.TITLE_PAGEOFFSET,description=opasConfig.DESCRIPTION_PAGEOFFSET),
                        client_id:int=Depends(get_client_id), 
                        client_session:str= Depends(get_client_session)
                        ):
    """
    ## Function
       ### Initiates download of the document in EPUB or PDF format (if authenticated)

    ## Return Type
       initiates a download of the requested document type

    ## Status
       This endpoint is working.
       There may be more work needed on the conversions:
          ePub conversion (links go nowhere in the tests I did' need to compare to GVPi version)
          PDF conversion (formatting is not great)

    ## Sample Call
         http://localhost:9100/v2/Documents/Downloads/EPUB/IJP.077.0217A/
         http://localhost:9100/v2/Documents/Downloads/PDF/IJP.077.0217A/
         http://localhost:9100/v2/Documents/Downloads/PDFORIG/IJP.077.0217A/

    ## Notes
        N/A
       
    ## Potential Errors
       USER NEEDS TO BE AUTHENTICATED to request a download.  Otherwise, returns error.
    """
    ts = time.time()
    caller_name = "[v2/Documents/Downloads]"
    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name} {client_session}: ")
    
    opasDocPermissions.verify_header(request, "documents_downloads") # for debugging client call
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug") # just for debug/info
    ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)
    user_name = session_info.username
    
    if client_id is None or client_session is None:
        logger.error(f"{caller_name}: Client {client_id} Session: {client_session} ")

    if retFormat.upper() == "PDF":
        file_format = 'PDF'
        media_type='application/pdf'
        endpoint = opasCentralDBLib.API_DOCUMENTS_PDF
    elif retFormat.upper() == "PDFORIG":  
        file_format = 'PDFORIG'
        media_type='application/pdf'
        endpoint = opasCentralDBLib.API_DOCUMENTS_PDFORIG
    else: # retFormat.upper() == "EPUB":
            file_format = 'EPUB'
            media_type='application/epub+zip'
            endpoint = opasCentralDBLib.API_DOCUMENTS_EPUB
    #else: # (no HTML download below!)
        #file_format = 'HTML'
        #media_type='application/xhtml+xml'
        #endpoint = opasCentralDBLib.API_DOCUMENTS_HTML

    #prep_document_download will check permissions for this user, and return abstract based file
    # so we need to check the PDF_ORIGINALS_PATH here first
    try:
        pdf_originals_path = localsecrets.PDF_ORIGINALS_PATH
    except Exception as e:
        pdf_originals_path = opasConfig.DEFAULT_PDF_ORIGINALS_PATH
        logger.error(f"PDF_ORIGINALS_PATH needs to be set ({e}) in localsecrets. Using opasConfig.DEFAULT_PDF_ORIGINALS_PATH {pdf_originals_path} for recovery") # added for setup error notice 2022-06-06
    
    #if there's no permission
    flex_fs = opasFileSupport.FlexFileSystem(key=localsecrets.S3_KEY,
                                             secret=localsecrets.S3_SECRET,
                                             root=pdf_originals_path) # important to use this path, not the XML one!

    # make sure it's upper case for consistency (added 2021-10-10)
    documentID = documentID.upper()
    filename, status = opasPySolrLib.prep_document_download( documentID,
                                                             ret_format=file_format,
                                                             base_filename="opasDoc",
                                                             session_info=session_info, 
                                                             flex_fs=flex_fs,
                                                             page=page, 
                                                             page_limit=pagelimit, 
                                                             page_offset=pageoffset, 
                                                            )    

    request_qualifier_text = f" Request: {documentID}. Session {session_info.session_id}."

    if filename is None:
        response.status_code = status.httpcode
        if status.httpcode == httpCodes.HTTP_401_UNAUTHORIZED:
            status_message = status.error_description # msgdb.get_user_message(opasConfig.ACCESS_SUMMARY_PERMISSION_DENIED) + request_qualifier_text + f" {status.error_description}" 
        elif status.httpcode == httpCodes.HTTP_422_UNPROCESSABLE_ENTITY:
            status_message = msgdb.get_user_message(opasConfig.ERROR_422_UNPROCESSABLE_ENTITY) + request_qualifier_text + f" {status.error_description}" 
        elif status.httpcode == httpCodes.HTTP_400_BAD_REQUEST:
            status_message = msgdb.get_user_message(opasConfig.ERROR_400_BAD_REQUEST) + request_qualifier_text + f" {status.error_description}" 
        elif status.httpcode == httpCodes.HTTP_404_NOT_FOUND:
            status_message = msgdb.get_user_message(opasConfig.ERROR_404_DOCUMENT_NOT_FOUND) + request_qualifier_text
        elif status.httpcode == httpCodes.HTTP_403_FORBIDDEN:
            status_message = status.error_description # status_message = msgdb.get_user_message(opasConfig.ERROR_403_DOWNLOAD_OR_PRINTING_RESTRICTED) + " " + request_qualifier_text
        elif status.httpcode == httpCodes.HTTP_500_INTERNAL_SERVER_ERROR:
            status_message = status.error_description # status_message = msgdb.get_user_message(opasConfig.ERROR_403_DOWNLOAD_OR_PRINTING_RESTRICTED) + " " + request_qualifier_text
        
        else:
            if status.error_description is not None and len(status.error_description) > 0:
                status_message = f"{status.error_description} ({status.httpcode}): {request_qualifier_text}"
            else:
                status_message = f"Unknown/Unexpected error ({status.httpcode}): {request_qualifier_text}"
            
        if status_message is not None:
            logger.error(status_message)

        raise HTTPException(status_code=response.status_code,
                            detail=status_message)
    else:
        if file_format == 'PDFORIG':
            # We need users name
            # user needs to have a name!
            if user_name is None or len(user_name) == 0:
                error_status_message = f"{caller_name}: Username must be assigned for download of originals"
                logger.error(error_status_message)
                response.status_code = httpCodes.HTTP_400_BAD_REQUEST 
                raise HTTPException(status_code=response.status_code,
                                    detail=error_status_message)
            else:
                try:
                    if flex_fs.key is not None:                    # S3
                        fileurl = flex_fs.fs.url(filename)
                        filename = wget.download(fileurl)
    
                    stamped_file = opasPDFStampCpyrght.stampcopyright(user_name, input_file=filename, suffix="original")
                    response.status_code = httpCodes.HTTP_200_OK
                    ret_val = FileResponse(path=stamped_file,
                                           status_code=response.status_code,
                                           filename=os.path.split(stamped_file)[1], 
                                           media_type=media_type)
    
                except Exception as e:
                    response.status_code = httpCodes.HTTP_404_NOT_FOUND 
                    status_message = msgdb.get_user_message(opasConfig.ACCESS_SUMMARY_PDFORIG_NOT_FOUND) + request_qualifier_text 
                    extended_status_message = f"{status_message}:{e}"
                    logger.error(extended_status_message)
                    raise HTTPException(status_code=response.status_code,
                                        detail=status_message)
                else:
                    status_message = opasCentralDBLib.API_STATUS_SUCCESS
                    logger.debug(status_message)
                    # success
                    ocd.record_document_view(document_id=documentID,
                                             session_info=session_info,
                                             view_type=file_format)
                    ocd.record_session_endpoint(api_endpoint_id=endpoint,
                                                session_info=session_info, 
                                                params=request.url._url,
                                                item_of_interest=f"{documentID}", 
                                                return_status_code = response.status_code,
                                                status_message=status_message
                                                )
        elif file_format == 'PDF':
            try:
                stamped_file = opasPDFStampCpyrght.stampcopyright(user_name, input_file=filename, suffix="pepweb")
                response.status_code = httpCodes.HTTP_200_OK
                ret_val = FileResponse(path=stamped_file,
                                       status_code=response.status_code,
                                       filename=os.path.split(stamped_file)[1], 
                                       media_type=media_type)

            except Exception as e:
                response.status_code = httpCodes.HTTP_404_NOT_FOUND # changed from 400 code on 2022-04-11 to match 404 error code below
                status_message = msgdb.get_user_message(opasConfig.ERROR_404_DOCUMENT_NOT_FOUND) + request_qualifier_text
                extended_status_message = f"{status_message}:{e}"
                logger.error(extended_status_message)
                raise HTTPException(status_code=response.status_code,
                                    detail=status_message)

            else: # success
                response.status_code = httpCodes.HTTP_200_OK
                status_message = opasCentralDBLib.API_STATUS_SUCCESS
                logger.debug(status_message)
                # success
                ocd.record_document_view(document_id=documentID,
                                         session_info=session_info,
                                         view_type=file_format)
                ocd.record_session_endpoint(api_endpoint_id=endpoint,
                                            session_info=session_info, 
                                            params=request.url._url,
                                            item_of_interest=f"{documentID}", 
                                            return_status_code = response.status_code,
                                            status_message=status_message
                                            )
        else: # file_format =='EPUB' (no HTML download)
            try:
                response.status_code = httpCodes.HTTP_200_OK
                ret_val = FileResponse(path=filename,
                                       status_code=response.status_code,
                                       filename=os.path.split(filename)[1], 
                                       media_type=media_type)

            except Exception as e:
                response.status_code = httpCodes.HTTP_400_BAD_REQUEST 
                status_message = f"{caller_name}: The requested document {filename} could not be returned."
                extended_status_message = f"{status_message}:{e}"
                logger.error(extended_status_message)
                # don't count--not successful (09/30/2021)
                #ocd.record_session_endpoint(api_endpoint_id=endpoint,
                                            #session_info=session_info, 
                                            #params=request.url._url,
                                            #item_of_interest=f"{documentID}", 
                                            #return_status_code = response.status_code,
                                            #status_message=extended_status_message
                                            #)
                raise HTTPException(status_code=response.status_code,
                                    detail=status_message)

            else: # success
                response.status_code = httpCodes.HTTP_200_OK
                status_message = opasCentralDBLib.API_STATUS_SUCCESS
                logger.debug(status_message)
                ocd.record_document_view(document_id=documentID,
                                         session_info=session_info,
                                         view_type=file_format)
                ocd.record_session_endpoint(api_endpoint_id=endpoint,
                                            session_info=session_info, 
                                            params=request.url._url,
                                            item_of_interest=f"{documentID}", 
                                            return_status_code = response.status_code,
                                            status_message=status_message
                                            )

    log_endpoint_time(request, ts=ts, level="debug")
    return ret_val

#-----------------------------------------------------------------------------
@app.get("/v2/Documents/Glossary/{termIdentifier}/", response_model=models.Documents, tags=["Documents"], summary=opasConfig.ENDPOINT_SUMMARY_GLOSSARY_VIEW, response_model_exclude_unset=True)  # the current PEP API
def documents_glossary_term(response: Response,
                            request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),  
                            termIdentifier: str=Path(..., title="Glossary Term ID or Partial ID", description=opasConfig.DESCRIPTION_GLOSSARYID),
                            termidtype: models.TermTypeIDEnum=Query(models.TermTypeIDEnum.termid, title="Type of term descriptor supplied", description=opasConfig.DESCRIPTION_TERMIDTYPE),
                            #search: str=Query(None, title="Document request from search results", description="This is a document request, including search parameters, to show hits"),
                            similarcount: int=Query(0, title=opasConfig.TITLE_SIMILARCOUNT, description=opasConfig.DESCRIPTION_SIMILARCOUNT),
                            recordperterm: bool=Query(False, title="Return a record per term in a group", description=opasConfig.DESCRIPTION_RETURNFORMATS),
                            return_format: str=Query("HTML", title=opasConfig.TITLE_RETURNFORMATS, description=opasConfig.DESCRIPTION_RETURNFORMATS),
                            client_id:int=Depends(get_client_id), 
                            client_session:str= Depends(get_client_session)
                            ): # Note this is called by the Document endpoint if it detects a term_id in the DocumentID
    """
    ## Function
       ### Return a glossary entry for the specified {termIdentifier} if authenticated with permission.  If not, returns error.

    ## Return Type
       models.Documents

    ## Status
       This endpoint is working.

    ## Sample Call
         /v2/Documents/Glossary/{termIdentifier}

    ## Potential Errors
       USER NEEDS TO BE AUTHENTICATED for glossary access at the term level.  Otherwise, returns error.

       Client apps should disable the glossary links when not authenticated.
    """
    caller_name = "[v2/Documents/Glossary]"
    if opasConfig.DEBUG_TRACE:
        print(f"{datetime.now().time().isoformat()}: {caller_name} {client_session}: ")
    
    ret_val = None

    opasDocPermissions.verify_header(request, "documents_glossary_term") # for debugging client call
    log_endpoint(request, client_id=client_id, session_id=client_session, level="debug")
    ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)

    try:
        # handle default passthrough, when the value becomes query.
        if not isinstance(termidtype, models.TermTypeIDEnum):
            termidtype = termidtype.default
        #  ok now look at the value
        if termidtype == "ID":
            try:
                term_parts = termIdentifier.split(".")
                if len(term_parts) == 4:
                    termIdentifier = term_parts[-2]
                elif len(term_parts) == 3:
                    termIdentifier = term_parts[-1]
                else:
                    pass
                logger.debug("Glossary View Request (termIdentifier/return_format): %s/%s", termIdentifier, return_format)
            except Exception as e:
                status_message = f"GlossaryViewError: Error splitting term: {e}"
                response.status_code = httpCodes.HTTP_400_BAD_REQUEST
                logger.error(status_message)
                raise HTTPException(
                    status_code=response.status_code,
                    detail=status_message
                )

        ret_val = opasPySolrLib.documents_get_glossary_entry(term_id=termIdentifier,
                                                             term_id_type=termidtype,
                                                             record_per_term=recordperterm,
                                                             retFormat=return_format,
                                                             session_info=session_info,
                                                             req_url=request.url._url,
                                                             request=request
                                                             )

        ret_val.documents.responseInfo.request = request.url._url

    except Exception as e:
        response.status_code = httpCodes.HTTP_400_BAD_REQUEST
        status_message = f"GlossaryViewTermError: {e}"
        logger.error(status_message)
        # don't count--not successful (09/30/2021)
        #ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_DOCUMENTS,
                                    #session_info=session_info, 
                                    #params=request.url._url,
                                    #item_of_interest=termIdentifier, 
                                    #return_status_code = response.status_code,
                                    #status_message=status_message
                                    #)
        raise HTTPException(
            status_code=response.status_code,
            detail=status_message
        )
    else:
        if ret_val.documents.responseInfo.count == 0:
            status_message = opasCentralDBLib.API_STATUS_SUCCESS
            response.status_code = httpCodes.HTTP_404_NOT_FOUND
            raise HTTPException(
                status_code=response.status_code,
                detail=status_message
            )
        else:
            status_message = opasCentralDBLib.API_STATUS_SUCCESS
            response.status_code = httpCodes.HTTP_200_OK
            ret_val.documents.responseInfo.request = request.url._url
            ocd.record_session_endpoint(api_endpoint_id=opasCentralDBLib.API_DOCUMENTS_GLOSSARY_TERM,
                                        session_info=session_info, 
                                        params=request.url._url,
                                        item_of_interest=termIdentifier, 
                                        return_status_code = response.status_code,
                                        status_message=status_message
                                        )
    return ret_val

#-----------------------------------------------------------------------------
@app.get("/v2/Documents/Image/{imageID}/", response_model_exclude_unset=True, tags=["Documents"], summary=opasConfig.ENDPOINT_SUMMARY_IMAGE_DOWNLOAD)
async def documents_image_fetch(response: Response,
                                request: Request=Query(None, title=opasConfig.TITLE_REQUEST, description=opasConfig.DESCRIPTION_REQUEST),  
                                imageID: str=Path(..., title=opasConfig.TITLE_IMAGEID, description=opasConfig.DESCRIPTION_IMAGEID),
                                download: int=Query(0, title="Return or download", description="0 returns the binary image, 1 downloads, 2 returns the article ID"),
                                insensitive: bool=Query(True, title="Filename case ignored"),  
                                #seed:str=Query(None, title="Seed String to help randomize daily expert pick", description="Use the date, for example, to avoid caching from a prev. date. "),
                                reselect:bool=Query(False, title="Force a new random image selection"),  
                                #client_id:int=Depends(get_client_id), 
                                #client_session:str= Depends(get_client_session)
                                client_id:int=Query(0, title="Client Id as Parameter"), 
                                client_session:str=Query(0, title="Client Session as Parameter")
                                ):
    """
    ## Function
       ### Returns image data - see return type for options
          
       Use * to return a random image each day.  The first fetch may take 7 or so seconds.  The rest of the day
         it will be instantaneous.

    ## Return Type
       If
         download=0: Returns Binary data
         download=1: Downloads the image
         download=2: Returns a pair of IDs "articleID", "figureID"

    ## Status
       This endpoint is working.

    ## Sample Call
         http://localhost:9100/v2/Documents/Image/AIM.036.0275A.FIG001/

    ## Notes
        N/A
        
    ## Potential Errors
       USER NEEDS TO BE AUTHENTICATED to request a download.  Otherwise, returns error.
    """
    
    def select_new_image(expert_picks_path):
        flex_fs = opasFileSupport.FlexFileSystem(key=localsecrets.S3_KEY,
                                                 secret=localsecrets.S3_SECRET,
                                                 root=expert_picks_path) 
        filenames = flex_fs.get_matching_filelist(path=expert_picks_path, filespec_regex=".*\.jpg", max_items=opasConfig.EXPERT_PICK_IMAGE_FILENAME_READ_LIMIT)
        if len(filenames) == 0:
            logger.warning(f"No filenames returned from get_matching_filelist for '.*\.jpg' and max_items={opasConfig.EXPERT_PICK_IMAGE_FILENAME_READ_LIMIT}")
            filename = opasConfig.EXPERT_PICKS_DEFAULT_IMAGE
            expert_pick_image[0] = today
            expert_pick_image[1] = filename
        else:
            status_message = f"Info: Expert Picks Image Count: {len(filenames)}"
            logger.info(status_message)
            filename = random.choice(filenames)
            filename = filename.basename
            expert_pick_image[0] = today
            expert_pick_image[1] = filename
            
        return filename

    caller_name = "[v2/Documents/Image]"

    ret_val = None
    filename = None

    if client_id is not None:
        try:
            a = int(client_id)
        except Exception as e:
            # 20230322 New switch to demote these froms errors to info for now...they dominate the logs.
            #  (and decided to change them to warning when not demoted.)
            msg = ERR_MSG_CALLER_IDENTIFICATION_ERROR + f" Client/Session {client_id}/{client_session}. URL: {request.url._url} Headers:{request.headers} {e}"
            if opasConfig.DEMOTE_PREVALENT_LOG_ENTRIES:
                logger.info(msg)
            else:
                logger.warning(msg)

            response.status_code = httpCodes.HTTP_400_BAD_REQUEST 
            status_message = ERR_MSG_CALLER_IDENTIFICATION_ERROR
            raise HTTPException(
                status_code=response.status_code,
                detail=status_message
            )

    try: # Verify PATH setting
        expert_picks_path = localsecrets.IMAGE_EXPERT_PICKS_PATH
    except Exception as e: # recover in case path in localsecrets is not set
        expert_picks_path = opasConfig.DEFAULT_IMAGE_EXPERT_PICKS_PATH
        logger.error(f"IMAGE_EXPERT_PICKS_PATH needs to be set in localsecrets ({e}). Using opasConfig.DEFAULT_IMAGE_EXPERT_PICKS_PATH {expert_picks_path} for recovery") # added for setup error notice 2022-06-06
   
    if imageID is not None:
        imageID = imageID.replace("+", " ")
        
    log_endpoint(request, client_id=client_id, level="debug")

    # find client_id and client_session in one of two ways
    if client_id == 0:
        client_id = request.query_params.get('client-id', 0)

    if client_session == 0:
        client_session = request.query_params.get('client-session', 0)
        
    if client_session == 0:
        client_id_from_header, client_session_from_header = opasDocPermissions.verify_header(request, "DocumentImage") # for debugging client call
        if client_session_from_header is not None:
            client_session = client_session_from_header
        
    if client_id == 0:
        client_id_from_header, client_session_from_header = opasDocPermissions.verify_header(request, "DocumentImage") # for debugging client call
        if client_id_from_header is not None:
            client_id = client_id_from_header       

    endpoint = opasCentralDBLib.API_DOCUMENTS_IMAGE
    if download != 0 and download != 2:
        # removed and put back in endpoint; I think the errors I'm seeing in production are due to this call
        #client_session = get_client_session(response, request, 
                                            #client_id=client_id) 
        # this is when we need a session id
            
        ocd, session_info = opasDocPermissions.get_session_info(request, response, session_id=client_session, client_id=client_id, caller_name=caller_name)

        # allow viewing, but not downloading if not logged in
        if not session_info.authenticated:
            response.status_code = httpCodes.HTTP_400_BAD_REQUEST 
            status_message = "Must be logged in and authorized to download an image."
            logger.error(status_message + f" Client/Session {client_id}/{client_session}.")
            raise HTTPException(
                status_code=response.status_code,
                detail=status_message
            )    

    # new failsafe check for this setting, 2022-10-17
    try:
        image_source_path = localsecrets.IMAGE_SOURCE_PATH
    except Exception as e: # in case IMAGE_SOURCE_PATH in localsecrets is not set
        image_source_path = opasConfig.DEFAULT_IMAGE_SOURCE_PATH 
        logger.error(f"IMAGE_SOURCE_PATH needs to be set in localsecrets ({e}).") # added for setup error notice 2022-06-06
   
    fs = opasFileSupport.FlexFileSystem(key=localsecrets.S3_KEY, secret=localsecrets.S3_SECRET, root=image_source_path)
    media_type='image/jpeg'
    if imageID != "*":
        filename = fs.get_image_filename(filespec=imageID, insensitive=insensitive, log_errors=False) # IMAGE_SOURCE_PATH set as root above, all that we need
        logger.debug(f"Random (*) expert pick image returns: {filename}.")       

    if download == 0 or download == 2:
        if imageID == "*":
            #  load a random image.  Load a new one each day
            try:
                today = datetime.today().strftime("%Y%m%d")
                if expert_pick_image[0] != today or reselect:
                    returned_filename = select_new_image(expert_picks_path) # saves new image to expert_pick_image as a side effect
                    logger.debug(f"select_new_image returns filename: {returned_filename}")
                    filename = expert_pick_image[1] 
                else:
                    filename = expert_pick_image[1]
            except Exception as e:
                logger.error(f"Error selecting a random expert pick image {filename}.  Error: {e}")
                # load the default image, so user doesn't see get a bad request.  But we'll need to watch for the error above.
                filename = opasConfig.EXPERT_PICKS_DEFAULT_IMAGE
            
        if filename is None:
            response.status_code = httpCodes.HTTP_400_BAD_REQUEST 
            status_message = f"Error: {imageID} not found or no filename specified. URL requested: {request.url}."
            logger.warning(status_message + f" Client/Session {client_id}/{client_session}.") # To check on the many calls for pseudo images IDs we get
            raise HTTPException(status_code=response.status_code,
                                detail=status_message)
        else:
            if opasConfig.DEBUG_TRACE:
                print(f"{datetime.now().time().isoformat()}: {caller_name} {client_id}/{client_session} Image:{imageID} Filename:{filename}")

            if download == 0:
                file_content = fs.get_image_binary(filename)
                try:
                    ret_val = response = Response(file_content, media_type=media_type)
    
                except Exception as e:
                    response.status_code = httpCodes.HTTP_400_BAD_REQUEST 
                    status_message = f" The requested image {filename} could not be returned {e}."
                    logger.warning(status_message + f" Client/Session {client_id}/{client_session}.") # To check on the many calls for pseudo images IDs we get
                    raise HTTPException(status_code=response.status_code,
                                        detail=status_message)
                else:
                    status_message = opasCentralDBLib.API_STATUS_SUCCESS
                    logger.debug(status_message)
            elif download == 2:
                # TODO - get article ID instead of filename (otherwise will need to remove those that aren't articleID based)
                try:
                    # TODO: Fix this routine to better deal with nonconforming names
                    #       and then we won't need the while.
                    doc_id = opasGenSupportLib.DocumentID(filename).document_id
                    counter = 0
                    while doc_id is None: # non-conforming image filename
                        logger.error(f"ImageFetchError: Nonconforming image filename {filename}, can't get article id from it")
                        counter += 1
                        filename = select_new_image(expert_picks_path)
                        doc_id = opasGenSupportLib.DocumentID(filename).document_id
                        if doc_id is not None:
                            break
                        
                        if counter > 10:
                            logger.error(f"ImageFetchError: {counter} nonconforming image filenames found in expert pick images.  Quitting.")
                            break # should never get that high
                        
                    graphic_item = models.GraphicItem(documentID = doc_id, graphic = filename)
                    ret_val = response = graphic_item
                    
                except Exception as e:
                    response.status_code = httpCodes.HTTP_400_BAD_REQUEST 
                    status_message = f"ImageFetchError: The requested image {filename} could not be returned {e}"
                    logger.warning(status_message + f" Client/Session {client_id}/{client_session}.") # To check on the many calls for pseudo images IDs we get
                    raise HTTPException(status_code=response.status_code,
                                        detail=status_message)
                    

    else: # download == 1
        try:
            response.status_code = httpCodes.HTTP_200_OK
            filename = fs.get_image_filename(filename)
            if fs.key is not None:
                fileurl = fs.fs.url(filename)
                fname = wget.download(fileurl)
                ret_val = FileResponse(path=fname,
                                       status_code=response.status_code,
                                       filename=os.path.split(fname)[1], 
                                       media_type=media_type)
            else:
                fileurl = filename
                ret_val = FileResponse(path=fileurl,
                                       status_code=response.status_code,
                                       filename=os.path.split(filename)[1], 
                                       media_type=media_type)


        except Exception as e:
            response.status_code = httpCodes.HTTP_400_BAD_REQUEST 
            status_message = f" The requested document {filename} could not be downloaded {e}"
            raise HTTPException(status_code=response.status_code,
                                detail=status_message)

        else:
            status_message = opasCentralDBLib.API_STATUS_SUCCESS

            logger.debug(status_message)
            ocd.record_document_view(document_id=imageID,
                                     session_info=session_info,
                                     view_type="file_format")
            ocd.record_session_endpoint(api_endpoint_id=endpoint,
                                        session_info=session_info, 
                                        params=request.url._url,
                                        item_of_interest=f"{imageID}", 
                                        return_status_code = response.status_code,
                                        status_message=status_message
                                        )

    return ret_val


if __name__ == "__main__":
    from localsecrets import CONFIG, DBNAME
    print(f"Server Running: ({localsecrets.BASEURL}:{localsecrets.API_PORT_MAIN})")
    print (f"Running in Python: {sys.version_info[0]}.{sys.version_info[1]}")
    print (f"Configuration used: {CONFIG}")
    print (f"Database Name: {DBNAME}")
    print (f"Version: {__version__}")
    import fastapi
    import pydantic
    print (f"FastAPI Version {fastapi.__version__}")
    print (f"Pydantic Version {pydantic.__version__}")
    
    try:
        if opasConfig.DEBUG_TRACE:
            print ("Debug on")
            uvicorn.run(app, host=localsecrets.BASEURL, port=localsecrets.API_PORT_MAIN, debug=True, log_level="warning")
        else:
            print ("Debug off")
            uvicorn.run(app, host=localsecrets.BASEURL, port=localsecrets.API_PORT_MAIN, debug=False, log_level="warning")
    except:
        print ("Debug off, DEBUG_TRACE not defined")
        uvicorn.run(app, host=localsecrets.BASEURL, port=localsecrets.API_PORT_MAIN, debug=False, log_level="warning")
        
    # uvicorn.run(app, host=localsecrets.BASEURL, port=9100, debug=True)
    print ("Now we're exiting...")