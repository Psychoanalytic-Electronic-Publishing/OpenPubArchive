#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=C0321,C0103,C0301,E1101,C0303,E1004,C0330,R0915,R0914,W0703,C0326

"""
opasAPISupportLib

This library is meant to hold the heart of the API based Solr queries and other support 
functions.  

2019.0614.1 - Python 3.7 compatible.  Work in progress.
2019.1203.1 - fixed authentication value error in show abstract call

"""
__author__      = "Neil R. Shapiro"
__copyright__   = "Copyright 2019, Psychoanalytic Electronic Publishing"
__license__     = "Apache 2.0"
__version__     = "2019.1203.1"
__status__      = "Development"

import os
import os.path
import sys
sys.path.append('./solrpy')
# print(os.getcwd())
import http.cookies
import re
import secrets
import socket, struct
from starlette.responses import JSONResponse, Response
from starlette.requests import Request
from starlette.responses import Response
import time
import datetime
from datetime import datetime, timedelta
from typing import Union, Optional, Tuple, List
from enum import Enum
# import pymysql

import opasConfig
import localsecrets
from localsecrets import BASEURL, SOLRURL, SOLRUSER, SOLRPW, DEBUG_DOCUMENTS, CONFIG, COOKIE_DOMAIN  
from opasConfig import OPASSESSIONID, OPASACCESSTOKEN, OPASEXPIRES 
from stdMessageLib import COPYRIGHT_PAGE_HTML  # copyright page text to be inserted in ePubs and PDFs


if (sys.version_info > (3, 0)):
    # Python 3 code in this block
    from io import StringIO
    pyVer = 3
else:
    # Python 2 code in this block
    pyVer = 2
    import StringIO
    
import solrpy as solr
import lxml
import logging
logger = logging.getLogger(__name__)

from lxml import etree
from pydantic import BaseModel
from pydantic import ValidationError

from ebooklib import epub              # for HTML 2 EPUB conversion
from xhtml2pdf import pisa             # for HTML 2 PDF conversion

# note: documents and documentList share the same internals, except the first level json label (documents vs documentlist)
import models

import opasXMLHelper as opasxmllib
import opasQueryHelper
from opasQueryHelper import QueryTextToSolr
import opasGenSupportLib as opasgenlib
import opasCentralDBLib
import sourceInfoDB as SourceInfoDB
    
sourceDB = opasCentralDBLib.SourceInfoDB()
count_anchors = 0

#from solrq import Q
import json

# Setup a Solr instance. The timeout is optional.
# solr = pysolr.Solr('http://localhost:8983/solr/pepwebproto', timeout=10)
# This is the old way -- should switch to class Solr per https://pythonhosted.org/solrpy/reference.html
# 
if SOLRUSER is not None:
    solr_docs = solr.SolrConnection(SOLRURL + opasConfig.SOLR_DOCS, http_user=SOLRUSER, http_pass=SOLRPW)
    solr_refs = solr.SolrConnection(SOLRURL + opasConfig.SOLR_REFS, http_user=SOLRUSER, http_pass=SOLRPW)
    solr_gloss = solr.SolrConnection(SOLRURL + opasConfig.SOLR_GLOSSARY, http_user=SOLRUSER, http_pass=SOLRPW)
    solr_authors = solr.SolrConnection(SOLRURL + opasConfig.SOLR_AUTHORS, http_user=SOLRUSER, http_pass=SOLRPW)
    solr_author_term_search = solr.SearchHandler(solr_authors, "/terms")

else:
    solr_docs = solr.SolrConnection(SOLRURL + opasConfig.SOLR_DOCS)
    solr_refs = solr.SolrConnection(SOLRURL + opasConfig.SOLR_REFS)
    solr_gloss = solr.SolrConnection(SOLRURL + opasConfig.SOLR_GLOSSARY)
    solr_authors = solr.SolrConnection(SOLRURL + opasConfig.SOLR_AUTHORS)
    solr_author_term_search = solr.SearchHandler(solr_authors, "/terms")

#API endpoints
documentURL = "/v1/Documents/"
TIME_FORMAT_STR = '%Y-%m-%dT%H:%M:%SZ'

#-----------------------------------------------------------------------------
def get_basecode(document_id):
    """
    Get basecode from document_id
    """
    ret_val = None
    try:
        parts = document_id.split(".")
        ret_val = parts[0]
    except Exception as e:
        logging.error(f"Bad document_id {document_id} to get_basecode. {e}")
    
    #TODO: later we might want to check for special book basecodes.
    
    return ret_val
    
#-----------------------------------------------------------------------------
def numbered_anchors(matchobj):
    """
    Called by re.sub on replacing anchor placeholders for HTML output.  This allows them to be numbered as they are replaced.
    """
    global count_anchors
    #JUMPTOPREVHIT = "<a onclick='hitCursor.prevHit();event.preventDefault();'>🡄</a>"
    #JUMPTONEXTHIT = "<a onclick='hitCursor.nextHit();event.preventDefault();'>🡆</a>"
    JUMPTOPREVHIT = f"""<a onclick='scrollToAnchor("hit{count_anchors}");event.preventDefault();'>🡄</a>"""
    JUMPTONEXTHIT = f"""<a onclick='scrollToAnchor("hit{count_anchors+1}");event.preventDefault();'>🡆</a>"""
    
    if matchobj.group(0) == opasConfig.HITMARKERSTART:
        count_anchors += 1
        if count_anchors > 1:
            #return f"<a name='hit{count_anchors}'><a href='hit{count_anchors-1}'>🡄</a>{opasConfig.HITMARKERSTART_OUTPUTHTML}"
            return f"<a name='hit{count_anchors}'>{JUMPTOPREVHIT}{opasConfig.HITMARKERSTART_OUTPUTHTML}"
        elif count_anchors <= 1:
            return f"<a name='hit{count_anchors}'> "
    if matchobj.group(0) == opasConfig.HITMARKEREND:
        #return f"{opasConfig.HITMARKEREND_OUTPUTHTML}<a href='hit{count_anchors+1}'>🡆</a>"
        return f"{opasConfig.HITMARKEREND_OUTPUTHTML}{JUMPTONEXTHIT}"
            
    else:
        return matchobj.group(0)

#-----------------------------------------------------------------------------
def get_max_age(keep_active=False):
    if keep_active:    
        ret_val = opasConfig.COOKIE_MAX_KEEP_TIME    
    else:
        ret_val = opasConfig.COOKIE_MIN_KEEP_TIME     
    return ret_val  # maxAge

#-----------------------------------------------------------------------------
def get_session_info(request: Request,
                     response: Response, 
                     access_token=None,
                     expires_time=None, 
                     keep_active=False,
                     force_new_session=False,
                     user=None):
    """
    Get session info from cookies, or create a new session if one doesn't exist.
    Return a sessionInfo object with all of that info, and a database handle
    
    """
    session_id = get_session_id(request)
    logger.debug("Get Session Info, Session ID via GetSessionID: %s", session_id)
    
    if session_id is None or session_id=='' or force_new_session:  # we need to set it
        # get new sessionID...even if they already had one, this call forces a new one
        logger.debug("session_id is none (or forcedNewSession).  We need to start a new session.")
        ocd, session_info = start_new_session(response, request, access_token, keep_active=keep_active, user=user)  
        
    else: # we already have a session_id, no need to recreate it.
        # see if an access_token is already in cookies
        access_token = get_access_token(request)
        expiration_time = get_expiration_time(request)
        logger.debug(f"session_id {session_id} is already set.")
        try:
            ocd = opasCentralDBLib.opasCentralDB(session_id, access_token, expiration_time)
            session_info = ocd.get_session_from_db(session_id)
            if session_info is None:
                # this is an error, and means there's no recorded session info.  Should we create a s
                #  session record, return an error, or ignore? #TODO
                # try creating a record
                username="NotLoggedIn"
                ret_val, session_info = ocd.save_session(session_id, 
                                                         userID=0,
                                                         userIP=request.client.host, 
                                                         connectedVia=request.headers["user-agent"],
                                                         username=username
                                                        )  # returns save status and a session object (matching what was sent to the db)

        except ValidationError as e:
            logger.error("Validation Error: %s", e.json())             
    
    logger.debug("getSessionInfo: %s", session_info)
    return ocd, session_info
    
def is_session_authenticated(request: Request, resp: Response):
    """
    Look to see if the session has been marked authenticated in the database
    """
    ocd, sessionInfo = get_session_info(request, resp)
    # sessionID = sessionInfo.session_id
    # is the user authenticated? if so, loggedIn is true
    ret_val = sessionInfo.authenticated
    return ret_val
    
def ip2long(ip):
    """
    Convert an IP string to long
    
    >>> ip2long("127.0.0.1")
    2130706433
    >>> socket.inet_ntoa(struct.pack('!L', 2130706433))
    '127.0.0.1'
    """
    packedIP = socket.inet_aton(ip)
    return struct.unpack("!L", packedIP)[0]    
    
def extract_html_fragment(html_str, xpath_to_extract="//div[@id='abs']"):
    # parse HTML
    htree = etree.HTML(html_str)
    ret_val = htree.xpath(xpath_to_extract)
    # make sure it's a string
    ret_val = force_string_return_from_various_return_types(ret_val)
    
    return ret_val

#-----------------------------------------------------------------------------
def start_new_session(resp: Response, request: Request, session_id=None, access_token=None, keep_active=None, user=None):
    """
    Create a new session record and set cookies with the session

    Returns database object, and the sessionInfo object
    
    If user is supplied, that means they've been authenticated.
      
    This should be the only place to generate and start a new session.
    """
    logger.debug("************** Starting a new SESSION!!!! *************")
    # session_start=datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    max_age = get_max_age(keep_active)
    token_expiration_time=datetime.utcfromtimestamp(time.time() + max_age) # .strftime('%Y-%m-%d %H:%M:%S')
    if session_id == None:
        session_id = secrets.token_urlsafe(16)
        logger.info("startNewSession assigning New Session ID: {}".format(session_id))

    # Try 
    # set_cookies(resp, session_id, access_token, token_expires_time=token_expiration_time)
    # get the database Object
    ocd = opasCentralDBLib.opasCentralDB()
    # save the session info
    if user:
        username=user.username
        ret_val, sessionInfo = ocd.save_session(session_id=session_id, 
                                                username=user.username,
                                                userID=user.user_id,
                                                expiresTime=token_expiration_time,
                                                userIP=request.client.host, 
                                                connectedVia=request.headers["user-agent"],
                                                accessToken = access_token
                                                )
    else:
        username="NotLoggedIn"
        ret_val, sessionInfo = ocd.save_session(session_id, 
                                                userID=0,
                                                expiresTime=token_expiration_time,
                                                userIP=request.client.host, 
                                                connectedVia=request.headers["user-agent"],
                                                username=username)  # returns save status and a session object (matching what was sent to the db)

    # return the object so the caller can get the details of the session
    return ocd, sessionInfo

#-----------------------------------------------------------------------------
def get_session_id(request):
    ret_val = request.cookies.get(OPASSESSIONID, None)
    
    #if ret_val is None:
        #cookie_dict = parse_cookies_from_header(request)
        #ret_val = cookie_dict.get(OPASSESSIONID, None)
        #if ret_val is not None:
            #logger.debug("getSessionID: Session cookie had to be retrieved from header: {}".format(ret_val))
    #else:
        #logger.debug ("getSessionID: Session cookie from client: {}".format(ret_val))
    return ret_val

#-----------------------------------------------------------------------------
def get_access_token(request):
    ret_val = request.cookies.get(opasConfig.OPASACCESSTOKEN, None)
    return ret_val

#-----------------------------------------------------------------------------
def get_expiration_time(request):
    ret_val = request.cookies.get(opasConfig.OPASEXPIRES, None)
    return ret_val
#-----------------------------------------------------------------------------
def check_solr_docs_connection():
    """
    Queries the solrDocs core (i.e., pepwebdocs) to see if the server is up and running.
    Solr also supports a ping, at the corename + "/ping", but that doesn't work through pysolr as far as I can tell,
    so it was more straightforward to just query the Core. 
    
    Note that this only checks one core, since it's only checking if the Solr server is running.
    
    >>> check_solr_docs_connection()
    True
    
    """
    if solr_docs is None:
        return False
    else:
        try:
            results = solr_docs.query(q = "art_id:{}".format("APA.009.0331A"),  fields = "art_id, art_vol, art_year")
        except Exception as e:
            logger.error("Solr Connection Error: {}".format(e))
            return False
        else:
            if len(results.results) == 0:
                return False
        return True


#-----------------------------------------------------------------------------
def document_get_info(document_id, fields="art_id, art_pepsourcetype, art_year, file_classification, art_pepsrccode"):
    """
    Gets key information about a single document for the specified fields.
    
    >>> document_get_info('PEPGRANTVS.001.0003A', fields='file_classification')
    {'file_classification': 'pepfree', 'score': 5.1908216}
    
    """
    ret_val = {}
    if solr_docs is not None:
        try:
            # PEP indexes field in upper case, but just in case caller sends lower case, convert.
            document_id = document_id.upper()
            results = solr_docs.query(q = f"art_id:{document_id}",  fields = fields)
        except Exception as e:
            logger.error(f"Solr Retrieval Error: {e}")
        else:
            if len(results.results) == 0:
                return ret_val
            else:
                try:
                    ret_val = results.results[0]
                except Exception as e:
                    logger.error(f"Solr Result Error: {e}")
                
    return ret_val

#-----------------------------------------------------------------------------
def force_string_return_from_various_return_types(text_str, min_length=5):
    """
    Sometimes the return isn't a string (it seems to often be "bytes") 
      and depending on the schema, from Solr it can be a list.  And when it
      involves lxml, it could even be an Element node or tree.
      
    This checks the type and returns a string, converting as necessary.
    
    >>> force_string_return_from_various_return_types(["this is really a list",], min_length=5)
    'this is really a list'

    """
    ret_val = None
    if text_str is not None:
        if isinstance(text_str, str):
            if len(text_str) > min_length:
                # we have an abstract
                ret_val = text_str
        elif isinstance(text_str, list):
            if text_str == []:
                ret_val = None
            else:
                ret_val = text_str[0]
                if ret_val == [] or ret_val == '[]':
                    ret_val = None
        else:
            logger.error("Type mismatch on Solr Data. forceStringReturn ERROR: %s", type(ret_val))

        try:
            if isinstance(ret_val, lxml.etree._Element):
                ret_val = etree.tostring(ret_val)
            
            if isinstance(ret_val, bytes) or isinstance(ret_val, bytearray):
                logger.error("Byte Data")
                ret_val = ret_val.decode("utf8")
        except Exception as e:
            err = "forceStringReturn Error forcing conversion to string: %s / %s" % (type(ret_val), e)
            logger.error(err)
            
    return ret_val        

#-----------------------------------------------------------------------------
def get_article_data_raw(article_id, fields=None):  # DEPRECATED??????? (at least, not used)
    """
    Fetch an article "Doc" from the Solr solrDocs core.  If fields is none, it fetches all fields.

    This returns a dictionary--the one returned by Solr 
      (hence why the function is Raw rather than Pydantic like getArticleData)
      
    >>> result = get_article_data_raw("APA.009.0331A")
    >>> result["art_id"]
    'APA.009.0331A'
    
    """
    ret_val = None
    if article_id != "":
        try:
            results = solr_docs.query(q = "art_id:{}".format(article_id),  fields = fields)
        except Exception as e:
            logger.error("Solr Error: {}".format(e))
            ret_val = None
        else:
            if results._numFound == 0:
                ret_val = None
            else:
                ret_val = results.results[0]

    return ret_val
                
#-----------------------------------------------------------------------------
def get_article_data(article_id, fields=None):  # DEPRECATED???????  (at least, not used, though its tested)
    """
    Fetch an article "Doc" from the Solr solrDocs core.  If fields is none, it fetches all fields.

    Returns the pydantic model object for a document in a regular documentListStruct

    >>> result = get_article_data("APA.009.0331A")
    >>> result.documentList.responseSet[0].documentID
    'APA.009.0331A'
    
    """
    ret_val = None
    if article_id != "":
        try:
            q = f"art_id:{article_id}"
            results = solr_docs.query(q, fields = fields)
        except Exception as e:
            logger.error(f"Solr Error: {e}")
            ret_val = None
        else:
            if results._numFound == 0:
                ret_val = None
            else:
                ret_val = results.results[0]

    limit = 5 # for now, we may later make this 1
    offset = 0
    response_info = models.ResponseInfo (count = len(results.results),
                                         fullCount = results._numFound,
                                         totalMatchCount = results._numFound,
                                         limit = limit,
                                         offset = offset,
                                         listType="documentlist",
                                         scopeQuery=[q],
                                         fullCountComplete = limit >= results._numFound,
                                         solrParams = results._params,
                                         timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)                     
                                       )

    document_item_list = []
    row_count = 0
    # row_offset = 0
    for result in results.results:
        author_ids = result.get("art_authors", None)
        if author_ids is None:
            authorMast = None
        else:
            authorMast = opasgenlib.deriveAuthorMast(author_ids)

        pgrg = result.get("art_pgrg", None)
        if pgrg is not None:
            pg_start, pg_end = opasgenlib.pgrg_splitter(pgrg)
         
        # TODO: Highlighting return is incomplete.  Return from non-highlighted results, and figure out workaround later.
        
        document_id = result.get("art_id", None)        
        #titleXml = results.highlighting[documentID].get("art_title_xml", None)
        title_xml = result.get("art_title_xml", None)
        title_xml = force_string_return_from_various_return_types(title_xml)
        #abstractsXml = results.highlighting[documentID].get("abstracts_xml", None)
        abstracts_xml = result.get("abstracts_xml", None)
        abstracts_xml  = force_string_return_from_various_return_types(abstracts_xml )
        #summariesXml = results.highlighting[documentID].get("abstracts_xml", None)
        summaries_xml = result.get("abstracts_xml", None)
        summaries_xml  = force_string_return_from_various_return_types(summaries_xml)
        #textXml = results.highlighting[documentID].get("text_xml", None)
        text_xml = result.get("text_xml", None)
        text_xml  = force_string_return_from_various_return_types(text_xml)
        kwic_list = []
        kwic = ""  # this has to be "" for PEP-Easy, or it hits an object error.  
    
        if DEBUG_DOCUMENTS != 1:
            if not user_logged_in or not full_text_requested:
                text_xml = get_excerpt_from_abs_sum_or_doc(xml_abstract=abstracts_xml,
                                                           xml_summary=summaries_xml,
                                                           xml_document=text_xml
                                                          )

        citeas = result.get("art_citeas_xml", None)
        citeas = force_string_return_from_various_return_types(citeas)
        
        try:
            item = models.DocumentListItem(PEPCode = result.get("art_pepsrccode", None), 
                                           year = result.get("art_year", None),
                                           vol = result.get("art_vol", None),
                                           pgRg = pgrg,
                                           pgStart = pg_start,
                                           pgEnd = pg_end,
                                           authorMast = authorMast,
                                           documentID = document_id,
                                           documentRefHTML = citeas,
                                           documentRef = opasxmllib.xml_elem_or_str_to_text(citeas, default_return=""),
                                           title = title_xml,
                                           abstract = abstracts_xml,
                                           documentText = None, #textXml,
                                           score = result.get("score", None), 
                                           )
        except ValidationError as e:
            logger.error(e.json())  
        else:
            row_count += 1
            # logger.debug("{}:{}".format(row_count, citeas))
            document_item_list.append(item)
            if row_count > limit:
                break

    response_info.count = len(document_item_list)
    
    document_list_struct = models.DocumentListStruct( responseInfo = response_info, 
                                                      responseSet = document_item_list
                                                    )
    
    document_list = models.DocumentList(documentList = document_list_struct)
    
    ret_val = document_list
    
    return ret_val

#-----------------------------------------------------------------------------
def database_get_most_downloaded(period: str="all",
                                document_type: str="journals",
                                author: str=None,
                                title: str=None,
                                journal_name: str=None,
                                limit: int=5,
                                offset=0):
    """
    Return the most downloaded (viewed) journal articles duing the prior period years.
    
    Args:
        period (int or str, optional): Look only at articles this many years back to current.  Defaults to 5.
        documentType (str, optional): The type of document, enumerated set: journals, books, videos, or all.  Defaults to "journals"
        author (str, optional): Filter, include matching author names per string .  Defaults to None (no filter).
        title (str, optional): Filter, include only titles that match.  Defaults to None (no filter).
        journalName (str, optional): Filter, include only journals matching this name.  Defaults to None (no filter).
        limit (int, optional): Paging mechanism, return is limited to this number of items.
        offset (int, optional): Paging mechanism, start with this item in limited return set, 0 is first item.

    Returns:
        models.DocumentList: Pydantic structure (dict) for DocumentList.  See models.py

    Docstring Tests:
    
    >>> result = database_get_most_downloaded()
    >>> result.documentList.responseSet[0].documentID
    'ijp.030.0069a'

    """
    if period.lower() not in ['5', '10', '20', 'all']:
        period = '5'

    ocd = opasCentralDBLib.opasCentralDB()
    count, most_downloaded = ocd.get_most_downloaded( view_period=period, 
                                                      document_type=document_type, 
                                                      author=author, 
                                                      title=title, 
                                                      journal_name=journal_name, 
                                                      limit=limit, offset=offset
                                                    )  # (most viewed)
    
    response_info = models.ResponseInfo( count = count,
                                         fullCount = count,
                                         limit = limit,
                                         offset = offset,
                                         listType="mostviewed",
                                         fullCountComplete = limit >= count,  # technically, inaccurate, but there's no point
                                         timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)                     
                                       )

    
    document_list_items = []
    row_count = 0

    for download in most_downloaded:
        hdg_author = download.get("hdgauthor", None)
        hdg_title = download.get("hdgtitle", None)
        src_title = download.get("srctitleseries", None)
        volume = download.get("vol", None)
        issue = download.get("issue", "")
        year = download.get("pubyear", None)
        pgrg = download.get("pgrg", None)
        pg_start, pg_end = opasgenlib.pgrg_splitter(pgrg)
        xmlref = download.get("xmlref", None)
        citeas = opasxmllib.get_html_citeas( authors_bib_style=hdg_author, 
                                              art_year=year,
                                              art_title=hdg_title, 
                                              art_pep_sourcetitle_full=src_title, 
                                              art_vol=volume, 
                                              art_pgrg=pgrg
                                            )

        item = models.DocumentListItem( documentID = download.get("document_id", None), # 11/24 database sync fix
                                        instanceCount = download.get("last12months", None),
                                        title = download.get("hdgtitle", None), # 11/24 database sync fix
                                        sourceTitle = download.get("srctitleseries", None), # 11/24 database sync fix
                                        PEPCode = download.get("jrnlcode", None), 
                                        authorMast = download.get("authorMast", None),
                                        year = download.get("pubyear", None),
                                        vol = download.get("vol", None),
                                        pgRg = download.get("pgrg", None),
                                        issue = issue,
                                        pgStart = pg_start,
                                        pgEnd = pg_end,
                                        count1 = download.get("lastweek", None),
                                        count2 = download.get("lastmonth", None),
                                        count3 = download.get("last6months", None),
                                        count4 = download.get("last12months", None),
                                        count5 = download.get("lastcalyear", None),
                                        documentRefHTML = citeas,
                                        documentRef = opasxmllib.xml_elem_or_str_to_text(xmlref, default_return=None),
                                     ) 
        row_count += 1
        logger.debug(item)
        document_list_items.append(item)
        if row_count > limit:
            break

    # Not sure why it doesn't come back sorted...so we sort it here.
    #ret_val2 = sorted(ret_val, key=lambda x: x[1], reverse=True)
    
    response_info.count = len(document_list_items)
    
    document_list_struct = models.DocumentListStruct( responseInfo = response_info, 
                                                      responseSet = document_list_items
                                                    )
    
    document_list = models.DocumentList(documentList = document_list_struct)
    
    ret_val = document_list
    
    return ret_val   


#-----------------------------------------------------------------------------
def database_get_most_cited(period: models.TimePeriod='5',
                            more_than: int=25, # if they only want the top 100 or so, a large number here speeds the query
                            limit: int=10,
                            offset: int=0):
    """
    Return the most cited journal articles duing the prior period years.
    
    period must be either '5', 10, '20', or 'all'
    
    args:
      limit: the number of records you want to return
      more_than: setting more_than to a large number speeds the query because the set to be sorted is smaller.
                 just set it so it's not so high you still get "limit" records back.
    
    >>> result = database_get_most_cited()
    >>> result.documentList.responseSet[0].documentID
    'PAQ.073.0005A'

    """
    if str(period).lower() not in models.TimePeriod._value2member_map_:
        period = '5'
    
    results = solr_docs.query( q = f"art_cited_{period}:[{more_than} TO *]",  
                               fl = f"art_id, title, art_vol, art_iss, art_year,  art_pepsrccode, \
                                     art_cited_5, art_cited_10, art_cited_20, art_cited_all, timestamp, art_pepsrccode, \
                                     art_pepsourcetype, art_pepsourcetitleabbr, art_pgrg, \
                                     art_citeas_xml, art_authors_mast, abstract_xml, text_xml",
                               fq = "art_pepsourcetype: journal",
                               sort = f"art_cited_{period} desc",
                               rows = limit, offset = offset
                              )

    logger.debug("databaseGetMostCited Number found: %s", results._numFound)
    
    response_info = models.ResponseInfo( count = len(results.results),
                                         fullCount = results._numFound,
                                         limit = limit,
                                         offset = offset,
                                         listType ="mostcited",
                                         fullCountComplete = limit >= results._numFound,
                                         timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR) 
                                       )

    
    document_list_items = []
    row_count = 0
    # row_offset = 0

    for result in results:
        PEPCode = result.get("art_pepsrccode", None)
        # volume = result.get("art_vol", None)
        # issue = result.get("art_iss", "")
        # year = result.get("art_year", None)
        # abbrev = result.get("art_pepsourcetitleabbr", "")
        # updated = result.get("timestamp", None)
        # updated = updated.strftime('%Y-%m-%d')
        pgrg = result.get("art_pgrg", None)
        pg_start, pg_end = opasgenlib.pgrg_splitter(pgrg)
        
        #displayTitle = abbrev + " v%s.%s (%s) (Added: %s)" % (volume, issue, year, updated)
        #volumeURL = "/v1/Metadata/Contents/%s/%s" % (PEPCode, issue)
        
        citeas = result.get("art_citeas_xml", None)
        art_abstract = result.get("art_abstract", None)
        
        item = models.DocumentListItem( documentID = result.get("art_id", None),
                                        instanceCount = result.get(f"art_cited_{period}", None),
                                        title = result.get("art_pepsourcetitlefull", ""),
                                        PEPCode = PEPCode, 
                                        authorMast = result.get("art_authors_mast", None),
                                        year = result.get("art_year", None),
                                        vol = result.get("art_vol", None),
                                        issue = result.get("art_iss", ""),
                                        pgRg = pgrg,
                                        pgStart = pg_start,
                                        pgEnd = pg_end,
                                        documentRefHTML = citeas,
                                        documentRef = opasxmllib.xml_elem_or_str_to_text(citeas, default_return=None),
                                        abstract = art_abstract
                                      ) 
        row_count += 1
        document_list_items.append(item)
        if row_count > limit:
            break

    # Not sure why it doesn't come back sorted...so we sort it here.
    #ret_val2 = sorted(ret_val, key=lambda x: x[1], reverse=True)
    
    response_info.count = len(document_list_items)
    
    document_list_struct = models.DocumentListStruct( responseInfo = response_info, 
                                                      responseSet = document_list_items
                                                    )
    
    document_list = models.DocumentList(documentList = document_list_struct)
    
    ret_val = document_list
    
    return ret_val   

#-----------------------------------------------------------------------------
def database_get_whats_new(days_back=7, limit=opasConfig.DEFAULT_LIMIT_FOR_WHATS_NEW, offset=0):
    """
    Return a what's been updated in the last week
    
    >>> result = database_get_whats_new()
    
    """    
    
    try:
        results = solr_docs.query(q = f"timestamp:[NOW-{days_back}DAYS TO NOW]",  
                                 fl = "art_id, title, art_vol, art_iss, art_pepsrccode, timestamp, art_pepsourcetype",
                                 fq = "{!collapse field=art_pepsrccode max=art_year_int}",
                                 sort="timestamp", sort_order="desc",
                                 rows=limit, offset=0,
                                 )
    
        logger.debug("databaseWhatsNew Number found: %s", results._numFound)
    except Exception as e:
        logger.error(f"Solr Search Exception: {e}")
    
    if results._numFound == 0:
        try:
            results = solr_docs.query( q = "art_pepsourcetype:journal",  
                                       fl = "art_id, title, art_vol, art_iss, art_pepsrccode, timestamp, art_pepsourcetype",
                                       fq = "{!collapse field=art_pepsrccode max=art_year_int}",
                                       sort="timestamp", sort_order="desc",
                                       rows=limit, offset=0,
                                     )
    
            logger.debug("databaseWhatsNew Expanded search to most recent...Number found: %s", results._numFound)

        except Exception as e:
            logger.error(f"Solr Search Exception: {e}")
    
    response_info = models.ResponseInfo( count = len(results.results),
                                         fullCount = results._numFound,
                                         limit = limit,
                                         offset = offset,
                                         listType="newlist",
                                         fullCountComplete = limit >= results._numFound,
                                         timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)                     
                                       )

    
    whats_new_list_items = []
    row_count = 0
    already_seen = []
    for result in results:
        PEPCode = result.get("art_pepsrccode", None)
        #if PEPCode is None or PEPCode in ["SE", "GW", "ZBK", "IPL"]:  # no books
            #continue
        src_type = result.get("art_pepsourcetype", None)
        if src_type != "journal":
            continue
            
        volume = result.get("art_vol", None)
        issue = result.get("art_iss", "")
        year = result.get("art_year", None)
        abbrev = sourceDB.sourceData[PEPCode].get("sourcetitleabbr", "")
        updated = result.get("timestamp", None)
        updated = updated.strftime('%Y-%m-%d')
        display_title = abbrev + " v%s.%s (%s) " % (volume, issue, year)
        if display_title in already_seen:
            continue
        else:
            already_seen.append(display_title)
        volume_url = "/v1/Metadata/Contents/%s/%s" % (PEPCode, issue)
        src_title = sourceDB.sourceData[PEPCode].get("sourcetitlefull", "")
            
        item = models.WhatsNewListItem( documentID = result.get("art_id", None),
                                        displayTitle = display_title,
                                        abbrev = abbrev,
                                        volume = volume,
                                        issue = issue,
                                        year = year,
                                        PEPCode = PEPCode, 
                                        srcTitle = src_title,
                                        volumeURL = volume_url,
                                        updated = updated
                                     ) 
        whats_new_list_items.append(item)
        row_count += 1
        if row_count > limit:
            break

    response_info.count = len(whats_new_list_items)
    
    whats_new_list_struct = models.WhatsNewListStruct( responseInfo = response_info, 
                                                       responseSet = whats_new_list_items
                                                     )
    
    ret_val = models.WhatsNewList(whatsNew = whats_new_list_struct)
    
    return ret_val   # WhatsNewList

#-----------------------------------------------------------------------------
#def search_like_the_pep_api():
    #pass  # later

#-----------------------------------------------------------------------------
def metadata_get_volumes(pep_code, year="*", limit=opasConfig.DEFAULT_LIMIT_FOR_VOLUME_LISTS, offset=0):
    """
    Get a list of volumes for this pep_code.
    
    #TODO: Not currently used in OPAS server though.  Deprecate?
    
    """
    ret_val = []
           
    results = solr_docs.query( q = "art_pepsrccode:%s && art_year:%s" % (pep_code, year),  
                               fields = "art_vol, art_year",
                               sort="art_year", sort_order="asc",
                               fq="{!collapse field=art_vol}",
                               rows=limit, start=offset
                             )

    logger.debug("metadataGetVolumes Number found: %s", results._numFound)
    response_info = models.ResponseInfo( count = len(results.results),
                                         fullCount = results._numFound,
                                         limit = limit,
                                         offset = offset,
                                         listType="volumelist",
                                         fullCountComplete = limit >= results._numFound,
                                         timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)                     
                                       )

    volume_item_list = []
    for result in results.results:
        item = models.VolumeListItem( PEPCode = pep_code, 
                                      year = result.get("art_year", None),
                                      vol = result.get("art_vol", None),
                                      score = result.get("score", None)
                                    )
    
        #logger.debug(item)
        volume_item_list.append(item)
       
    response_info.count = len(volume_item_list)
    
    volume_list_struct = models.VolumeListStruct( responseInfo = response_info, 
                                                  responseSet = volume_item_list
                                                )
    
    volume_list = models.VolumeList(volumeList = volume_list_struct)
    
    ret_val = volume_list
    return ret_val

#-----------------------------------------------------------------------------
def metadata_get_contents(pep_code, #  e.g., IJP, PAQ, CPS
                          year="*",
                          vol="*",
                          limit=opasConfig.DEFAULT_LIMIT_FOR_CONTENTS_LISTS, offset=0):
    """
    Return a jounals contents
    
    >>> metadata_get_contents("IJP", "1993", limit=5, offset=0)
    <DocumentList documentList=<DocumentListStruct responseInfo=<ResponseInfo count=5 limit=5 offset=0 page=No…>
    >>> metadata_get_contents("IJP", "1993", limit=5, offset=5)
    <DocumentList documentList=<DocumentListStruct responseInfo=<ResponseInfo count=5 limit=5 offset=5 page=No…>
    """
    ret_val = []
    if year == "*" and vol != "*":
        # specified only volume
        field="art_vol"
        search_val = vol
    else:  #Just do year
        field="art_year"
        search_val = "*"
        
    results = solr_docs.query(q = "art_pepsrccode:{} && {}:{}".format(pep_code, field, search_val),  
                             fields = "art_id, art_vol, art_year, art_iss, art_iss_title, art_newsecnm, art_pgrg, art_title, art_author_id, art_citeas_xml",
                             sort="art_year, art_pgrg", sort_order="asc",
                             rows=limit, start=offset
                             )

    response_info = models.ResponseInfo( count = len(results.results),
                                         fullCount = results._numFound,
                                         limit = limit,
                                         offset = offset,
                                         listType="documentlist",
                                         fullCountComplete = limit >= results._numFound,
                                         timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)                     
                                       )

    document_item_list = []
    for result in results.results:
        # transform authorID list to authorMast
        authorIDs = result.get("art_author_id", None)
        if authorIDs is None:
            authorMast = None
        else:
            authorMast = opasgenlib.deriveAuthorMast(authorIDs)
        
        pgRg = result.get("art_pgrg", None)
        pgStart, pgEnd = opasgenlib.pgrg_splitter(pgRg)
        citeAs = result.get("art_citeas_xml", None)  
        citeAs = force_string_return_from_various_return_types(citeAs)
        
        item = models.DocumentListItem(PEPCode = pep_code, 
                                year = result.get("art_year", None),
                                vol = result.get("art_vol", None),
                                pgRg = result.get("art_pgrg", None),
                                pgStart = pgStart,
                                pgEnd = pgEnd,
                                authorMast = authorMast,
                                documentID = result.get("art_id", None),
                                documentRef = opasxmllib.xml_elem_or_str_to_text(citeAs, default_return=""),
                                documentRefHTML = citeAs,
                                score = result.get("score", None)
                                )
        #logger.debug(item)
        document_item_list.append(item)

    response_info.count = len(document_item_list)
    
    document_list_struct = models.DocumentListStruct( responseInfo = response_info, 
                                                      responseSet=document_item_list
                                                    )
    
    document_list = models.DocumentList(documentList = document_list_struct)
    
    ret_val = document_list
    
    return ret_val

#-----------------------------------------------------------------------------
def metadata_get_videos(src_type=None, pep_code=None, limit=opasConfig.DEFAULT_LIMIT_FOR_METADATA_LISTS, offset=0):
    """
    Fill out a sourceInfoDBList which can be used for a getSources return, but return individual 
      videos, as is done for books.  This provides more information than the 
      original API which returned video "journals" names.  
      
    """
    
    if pep_code != None:
        query = "art_pepsourcetype:video* AND art_pepsrccode:{}".format(pep_code)
    else:
        query = "art_pepsourcetype:video*"
    try:
        srcList = solr_docs.query(q = query,  
                                  fields = "art_id, art_issn, art_pepsrccode, art_authors, title, \
                                            art_pepsourcetitlefull, art_pepsourcetitleabbr, art_vol, \
                                            art_year, art_citeas_xml, art_lang, art_pgrg",
                                  sort = "art_citeas_xml",
                                  sort_order = "asc",
                                  rows=limit, start=offset
                                 )
    except Exception as e:
        logger.error("metadataGetVideos Error: {}".format(e))

    source_info_dblist = []
    # count = len(srcList.results)
    total_count = int(srcList.results.numFound)
    
    for result in srcList.results:
        source_info_record = {}
        authors = result.get("art_authors")
        if authors is None:
            source_info_record["author"] = None
        elif len(authors) > 1:
            source_info_record["author"] = "; ".join(authors)
        else:    
            source_info_record["author"] = authors[0]
            
        source_info_record["src_code"] = result.get("art_pepsrccode")
        source_info_record["ISSN"] = result.get("art_issn")
        source_info_record["documentID"] = result.get("art_id")
        try:
            source_info_record["title"] = result.get("title")[0]
        except:
            source_info_record["title"] = ""
            
        source_info_record["art_citeas"] = result.get("art_citeas_xml")
        source_info_record["pub_year"] = result.get("art_year")
        source_info_record["bib_abbrev"] = result.get("art_year")
        try:
            source_info_record["language"] = result.get("art_lang")[0]
        except:
            source_info_record["language"] = "EN"

        logger.debug("metadataGetVideos: %s", source_info_record)
        source_info_dblist.append(source_info_record)

    return total_count, source_info_dblist

#-----------------------------------------------------------------------------
def metadata_get_source_by_type(src_type=None, src_code=None, limit=opasConfig.DEFAULT_LIMIT_FOR_METADATA_LISTS, offset=0):
    """
    Return a list of source metadata, by type (e.g., journal, video, etc.).
    
    No attempt here to map to the correct structure, just checking what field/data items we have in sourceInfoDB.
    
    >>> metadata_get_source_by_type(src_type="journal", limit=3)
    <SourceInfoList sourceInfo=<SourceInfoStruct responseInfo=<ResponseInfo count=3 limit=3 offset=0 page=None…>
    >>> metadata_get_source_by_type(src_type="book", limit=3)
    <SourceInfoList sourceInfo=<SourceInfoStruct responseInfo=<ResponseInfo count=3 limit=3 offset=0 page=None…>
    >>> metadata_get_source_by_type(src_type="journals", limit=5, offset=0)
    <SourceInfoList sourceInfo=<SourceInfoStruct responseInfo=<ResponseInfo count=5 limit=5 offset=0 page=None…>
    >>> metadata_get_source_by_type(src_type="journals", limit=5, offset=6)
    <SourceInfoList sourceInfo=<SourceInfoStruct responseInfo=<ResponseInfo count=5 limit=5 offset=6 page=None…>
    """
    ret_val = []
    source_info_dblist = []
    ocd = opasCentralDBLib.opasCentralDB()
    # standardize Source type, allow plural, different cases, but code below this part accepts only those three.
    src_type = src_type.lower()
    if src_type not in ["journal", "book"]:
        if re.match("videos.*", src_type, re.IGNORECASE):
            src_type = "videos"
        elif re.match("video", src_type, re.IGNORECASE):
            src_type = "videostream"
        elif re.match("boo.*", src_type, re.IGNORECASE):
            src_type = "book"
        else: # default
            src_type = "journal"
   
    # This is not part of the original API, it brings back individual videos rather than the videostreams
    # but here in case we need it.  In that case, your source must be videos.*, like videostream, in order
    # to load individual videos rather than the video journals
    if src_type == "videos":
        #  gets count of videos and a list of them (from Solr database)
        total_count, source_info_dblist = metadata_get_videos(src_type, src_code, limit, offset)
        count = len(source_info_dblist)
    else: # get from mySQL
        try:
            if src_code != "*":
                total_count, sourceData = ocd.get_sources(src_type = src_type, source=src_code, limit=limit, offset=offset)
            else:
                total_count, sourceData = ocd.get_sources(src_type = src_type, limit=limit, offset=offset)
                
            for sourceInfoDict in sourceData:
                if sourceInfoDict["product_type"] == src_type:
                    # match
                    source_info_dblist.append(sourceInfoDict)
            if limit < total_count:
                count = limit
            else:
                count = total_count
            logger.debug("MetadataGetSourceByType: Number found: %s", count)
        except Exception as e:
            errMsg = "MetadataGetSourceByType: Error getting source information.  {}".format(e)
            count = 0
            logger.error(errMsg)

    response_info = models.ResponseInfo( count = count,
                                         fullCount = total_count,
                                         fullCountComplete = count == total_count,
                                         limit = limit,
                                         offset = offset,
                                         listLabel = "{} List".format(src_type),
                                         listType = "sourceinfolist",
                                         scopeQuery = [src_type, src_code],
                                         timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)                     
                                       )

    source_info_listitems = []
    counter = 0
    for source in source_info_dblist:
        counter += 1
        if counter < offset:
            continue
        if counter > limit:
            break
        try:
            title = source.get("title")
            authors = source.get("author")
            pub_year = source.get("pub_year")
            publisher = source.get("publisher")
            bookCode = None
            if src_type == "book":
                bookCode = source.get("pepcode")
                m = re.match("(?P<code>[a-z]+)(?P<num>[0-9]+)", bookCode, re.IGNORECASE)
                if m is not None:
                    code = m.group("code")
                    num = m.group("num")
                    bookCode = code + "." + num
                
                art_citeas = u"""<p class="citeas"><span class="authors">%s</span> (<span class="year">%s</span>) <span class="title">%s</span>. <span class="publisher">%s</span>.""" \
                    %                   (authors,
                                         source.get("pub_year"),
                                         title,
                                         publisher
                                        )
            elif src_type == "video":
                art_citeas = source.get("art_citeas")
            else:
                art_citeas = title # journals just should show display title


            try:
                item = models.SourceInfoListItem( sourceType = src_type,
                                                  PEPCode = source.get("basecode"),
                                                  authors = authors,
                                                  pub_year = pub_year,
                                                  documentID = source.get("art_id"),
                                                  displayTitle = art_citeas,
                                                  title = title,
                                                  srcTitle = title,  # v1 Deprecated for future
                                                  bookCode = bookCode,
                                                  abbrev = source.get("bibabbrev"),
                                                  bannerURL = f"http://{BASEURL}/{opasConfig.IMAGES}/banner{source.get('basecode')}.logo.gif",
                                                  language = source.get("language"),
                                                  ISSN = source.get("ISSN"),
                                                  ISBN10 = source.get("ISBN-10"),
                                                  ISBN13 = source.get("ISBN-13"),
                                                  yearFirst = source.get("start_year"),
                                                  yearLast = source.get("end_year"),
                                                  embargoYears = source.get("embargo")
                                                ) 
                #logger.debug("metadataGetSourceByType SourceInfoListItem: %s", item)
            except ValidationError as e:
                logger.error("metadataGetSourceByType SourceInfoListItem Validation Error:")
                logger.error(e.json())        

        except Exception as e:
                logger.error("metadataGetSourceByType: %s", e)        
            

        source_info_listitems.append(item)
        
    try:
        source_info_struct = models.SourceInfoStruct( responseInfo = response_info, 
                                                      responseSet = source_info_listitems
                                                     )
    except ValidationError as e:
        logger.error("models.SourceInfoStruct Validation Error:")
        logger.error(e.json())        
    
    try:
        source_info_list = models.SourceInfoList(sourceInfo = source_info_struct)
    except ValidationError as e:
        logger.error("SourceInfoList Validation Error:")
        logger.error(e.json())        
    
    ret_val = source_info_list

    return ret_val

#-----------------------------------------------------------------------------
def metadata_get_source_by_code(src_code=None, limit=opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, offset=0):
    """
    Rather than get this from Solr, where there's no 1:1 records about this, we will get this from the sourceInfoDB instance.
    
    No attempt here to map to the correct structure, just checking what field/data items we have in sourceInfoDB.
    
    The sourceType is listed as part of the endpoint path, but I wonder if we should really do this 
    since it isn't needed, the pepCodes are unique.
    
    curl -X GET "http://stage.pep.gvpi.net/api/v1/Metadata/Journals/AJP/" -H "accept: application/json"
    
    >>> metadata_get_source_by_code(src_code="APA")
    <SourceInfoList sourceInfo=<SourceInfoStruct responseInfo=<ResponseInfo count=1 limit=10 offset=0 page=Non…>
    >>> metadata_get_source_by_code()
    <SourceInfoList sourceInfo=<SourceInfoStruct responseInfo=<ResponseInfo count=192 limit=10 offset=0 page=N…>
    
    """
    ret_val = []
    ocd = opasCentralDBLib.opasCentralDB()
    
    # would need to add URL for the banner
    if src_code is not None:
        total_count, source_info_dblist = ocd.get_sources(src_code)    #sourceDB.sourceData[pepCode]
        #sourceType = sourceInfoDBList.get("src_type", None)
    else:
        total_count, source_info_dblist = ocd.get_sources(src_code)    #sourceDB.sourceData
        #sourceType = "All"
            
    count = len(source_info_dblist)
    logger.debug("metadataGetSourceByCode: Number found: %s", count)

    response_info = models.ResponseInfo( count = count,
                                         fullCount = total_count,
                                         limit = limit,
                                         offset = offset,
                                         #listLabel = "{} List".format(sourceType),
                                         listType = "sourceinfolist",
                                         scopeQuery = [src_code],
                                         fullCountComplete = True,
                                         timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)                     
                                       )

    source_info_list_items = []
    counter = 0
    for source in source_info_dblist:
        counter += 1
        if counter < offset:
            continue
        if counter > limit:
            break
        try:
            # remove leading and trailing spaces from strings in response.
            source = {k:v.strip() if isinstance(v, str) else v for k, v in source.items()}
            item = models.SourceInfoListItem( ISSN = source.get("ISSN"),
                                              PEPCode = source.get("src_code"),
                                              abbrev = source.get("bib_abbrev"),
                                              bannerURL = f"http://{BASEURL}/{opasConfig.IMAGES}/banner{source.get('src_code')}.logo.gif",
                                              displayTitle = source.get("title"),
                                              language = source.get("language"),
                                              yearFirst = source.get("start_year"),
                                              yearLast = source.get("end_year"),
                                              sourceType = source.get("src_type"),
                                              title = source.get("title")
                                            ) 
        except ValidationError as e:
            logger.info("metadataGetSourceByCode: SourceInfoListItem Validation Error:")
            logger.error(e.json())

        source_info_list_items.append(item)
        
    try:
        source_info_struct = models.SourceInfoStruct( responseInfo = response_info, 
                                                      responseSet = source_info_list_items
                                                    )
    except ValidationError as e:
        logger.info("metadataGetSourceByCode: SourceInfoStruct Validation Error:")
        logger.error(e.json())
    
    try:
        source_info_list = models.SourceInfoList(sourceInfo = source_info_struct)
    
    except ValidationError as e:
        logger.info("metadataGetSourceByCode: SourceInfoList Validation Error:")
        logger.error(e.json())
    
    ret_val = source_info_list
    return ret_val

#-----------------------------------------------------------------------------
def authors_get_author_info(author_partial, limit=opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, offset=0, author_order="index"):
    """
    Returns a list of matching names (per authors last name), and the number of articles in PEP found by that author.
    
    Args:
        author_partial (str): String prefix of author names to return.
        limit (int, optional): Paging mechanism, return is limited to this number of items.
        offset (int, optional): Paging mechanism, start with this item in limited return set, 0 is first item.
        author_order (str, optional): Return the list in this order, per Solr documentation.  Defaults to "index", which is the Solr determined indexing order.

    Returns:
        models.DocumentList: Pydantic structure (dict) for DocumentList.  See models.py

    Docstring Tests:    
        >>> resp = authors_get_author_info("Tuck")
        >>> resp.authorIndex.responseInfo.count
        8
        >>> resp = authors_get_author_info("Levins.*", limit=5)
        >>> resp.authorIndex.responseInfo.count
        5
    """
    ret_val = {}
    method = 2
    
    if method == 1:
        query = "art_author_id:/%s.*/" % (author_partial)
        results = solr_authors.query( q=query,
                                      fields="authors, art_author_id",
                                      facet_field="art_author_id",
                                      facet="on",
                                      facet_sort="index",
                                      facet_prefix="%s" % author_partial,
                                      facet_limit=limit,
                                      facet_offset=offset,
                                      rows=0
                                    )       

    if method == 2:
        # should be faster way, but about the same measuring tuck (method1) vs tuck.* (method2) both about 2 query time.  However, allowing regex here.
        if "*" in author_partial or "?" in author_partial or "." in author_partial:
            results = solr_author_term_search( terms_fl="art_author_id",
                                               terms_limit=limit,  # this causes many regex expressions to fail
                                               terms_regex=author_partial.lower() + ".*",
                                               terms_sort=author_order  # index or count
                                              )           
        else:
            results = solr_author_term_search( terms_fl="art_author_id",
                                               terms_prefix=author_partial.lower(),
                                               terms_sort=author_order,  # index or count
                                               terms_limit=limit
                                             )
    
    response_info = models.ResponseInfo( limit=limit,
                                         offset=offset,
                                         listType="authorindex",
                                         scopeQuery=[f"Terms: {author_partial}"],
                                         solrParams=results._params,
                                         timeStamp=datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)
                                       )
    
    author_index_items = []
    if method == 1:
        for key, value in results.facet_counts["facet_fields"]["art_author_id"].items():
            if value > 0:
                item = models.AuthorIndexItem(authorID = key, 
                                              publicationsURL = "/v1/Authors/Publications/{}/".format(key),
                                              publicationsCount = value,
                                             ) 
                author_index_items.append(item)
                logger.debug ("authorsGetAuthorInfo", item)

    if method == 2:  # faster way
        for key, value in results.terms["art_author_id"].items():
            if value > 0:
                item = models.AuthorIndexItem(authorID = key, 
                                              publicationsURL = "/v1/Authors/Publications/{}/".format(key),
                                              publicationsCount = value,
                                             ) 
                author_index_items.append(item)
                logger.debug("authorsGetAuthorInfo: %s", item)
       
    response_info.count = len(author_index_items)
    response_info.fullCountComplete = limit >= response_info.count
        
    author_index_struct = models.AuthorIndexStruct( responseInfo = response_info, 
                                                    responseSet = author_index_items
                                                  )
    
    author_index = models.AuthorIndex(authorIndex = author_index_struct)
    
    ret_val = author_index
    return ret_val

#-----------------------------------------------------------------------------
def authors_get_author_publications(author_partial, limit=opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, offset=0):
    """
    Returns a list of publications (per authors partial name), and the number of articles by that author.
    
    >>> authors_get_author_publications(author_partial="Tuck")
    <AuthorPubList authorPubList=<AuthorPubListStruct responseInfo=<ResponseInfo count=10 limit=10 offset=0 page…>
    >>> authors_get_author_publications(author_partial="Fonag")
    <AuthorPubList authorPubList=<AuthorPubListStruct responseInfo=<ResponseInfo count=10 limit=10 offset=0 page…>
    >>> authors_get_author_publications(author_partial="Levinson, Nadine A.")
    <AuthorPubList authorPubList=<AuthorPubListStruct responseInfo=<ResponseInfo count=8 limit=10 offset=0 page=…>
    """
    ret_val = {}
    query = "art_author_id:/{}/".format(author_partial)
    aut_fields = "art_author_id, art_year_int, art_id, art_auth_pos_int, art_author_role, art_author_bio, art_citeas_xml"
    # wildcard in case nothing found for #1
    results = solr_authors.query( q = "{}".format(query),   
                                  fields = aut_fields,
                                  sort="art_author_id, art_year_int", sort_order="asc",
                                  rows=limit, start=offset
                                )

    logger.debug("Author Publications: Number found: %s", results._numFound)
    
    if results._numFound == 0:
        logger.debug("Author Publications: Query didn't work - %s", query)
        query = "art_author_id:/{}[ ]?.*/".format(author_partial)
        logger.debug("Author Publications: trying again - %s", query)
        results = solr_authors.query( q = "{}".format(query),  
                                      fields = aut_fields,
                                      sort="art_author_id, art_year_int", sort_order="asc",
                                      rows=limit, start=offset
                                    )

        logger.debug("Author Publications: Number found: %s", results._numFound)
        if results._numFound == 0:
            query = "art_author_id:/(.*[ ])?{}[ ]?.*/".format(author_partial)
            logger.debug("Author Publications: trying again - %s", query)
            results = solr_authors.query( q = "{}".format(query),  
                                          fields = aut_fields,
                                          sort="art_author_id, art_year_int", sort_order="asc",
                                          rows=limit, start=offset
                                        )
    
    response_info = models.ResponseInfo( count = len(results.results),
                                         fullCount = results._numFound,
                                         limit = limit,
                                         offset = offset,
                                         listType="authorpublist",
                                         scopeQuery=[query],
                                         solrParams = results._params,
                                         fullCountComplete = limit >= results._numFound,
                                         timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)                     
                                       )

    author_pub_list_items = []
    for result in results.results:
        citeas = result.get("art_citeas_xml", None)
        citeas = force_string_return_from_various_return_types(citeas)
        
        item = models.AuthorPubListItem( authorID = result.get("art_author_id", None), 
                                         documentID = result.get("art_id", None),
                                         documentRefHTML = citeas,
                                         documentRef = opasxmllib.xml_elem_or_str_to_text(citeas, default_return=""),
                                         documentURL = documentURL + result.get("art_id", None),
                                         year = result.get("art_year", None),
                                         score = result.get("score", 0)
                                        ) 

        author_pub_list_items.append(item)
       
    response_info.count = len(author_pub_list_items)
    
    author_pub_list_struct = models.AuthorPubListStruct( responseInfo = response_info, 
                                           responseSet = author_pub_list_items
                                           )
    
    author_pub_list = models.AuthorPubList(authorPubList = author_pub_list_struct)
    
    ret_val = author_pub_list
    return ret_val

#-----------------------------------------------------------------------------
def get_excerpt_from_abs_sum_or_doc(xml_abstract, xml_summary, xml_document):
   
    ret_val = None
    # see if there's an abstract
    ret_val = force_string_return_from_various_return_types(xml_abstract)
    if ret_val is None:
        # try the summary
        ret_val = force_string_return_from_various_return_types(xml_summary)
        if ret_val is None:
            # get excerpt from the document
            if xml_document is None:
                # we fail.  Return None
                logger.warning("No excerpt can be found or generated.")
            else:
                # extract the first 10 paras
                ret_val = force_string_return_from_various_return_types(xml_document)
                ret_val = opasxmllib.remove_encoding_string(ret_val)
                # deal with potentially broken XML excerpts
                parser = lxml.etree.XMLParser(encoding='utf-8', recover=True)                
                #root = etree.parse(StringIO(ret_val), parser)
                root = etree.fromstring(ret_val, parser)
                body = root.xpath("//*[self::h1 or self::p or self::p2 or self::pb]")
                ret_val = ""
                count = 0
                for elem in body:
                    if elem.tag == "pb" or count > 10:
                        # we're done.
                        ret_val = "%s%s%s" % ("<abs><unit type='excerpt'>", ret_val, "</unit></abs>")
                        break
                    else:
                        ret_val  += etree.tostring(elem, encoding='utf8').decode('utf8')

    return ret_val
    
#-----------------------------------------------------------------------------
def documents_get_abstracts(document_id, ret_format="TEXTONLY", authenticated=False, limit=opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, offset=0):
    """
    Returns an abstract or summary for the specified document
    If part of a documentID is supplied, multiple abstracts will be returned.
    
    The endpoint reminds me that we should be using documentID instead of "art" for article perhaps.
      Not thrilled about the prospect of changing it, but probably the right thing to do.
      
    >>> documents_get_abstracts("IJP.075")
    <Documents documents=<DocumentListStruct responseInfo=<ResponseInfo count=10 limit=10 offset=0 page=…>
    >>> documents_get_abstracts("AIM.038.0279A")  # no abstract on this one
    <Documents documents=<DocumentListStruct responseInfo=<ResponseInfo count=1 limit=10 offset=0 page=N…>
    >>> documents_get_abstracts("AIM.040.0311A")
    <Documents documents=<DocumentListStruct responseInfo=<ResponseInfo count=1 limit=10 offset=0 page=N…>
      
    """
    ret_val = None
    if document_id is not None:
        try:
            document_id = document_id.upper()
        except Exception as e:
            logger.warning("Bad argument {document_id} to get_abstract(Error:{e})")
            return ret_val
            
        results = solr_docs.query(q = "art_id:%s*" % (document_id),  
                                    fields = "art_id, art_pepsourcetitlefull, art_vol, art_year, art_citeas_xml, art_pgrg, art_title_xml, art_authors, abstracts_xml, summaries_xml, text_xml",
                                    sort="art_year, art_pgrg", sort_order="asc",
                                    rows=limit, start=offset
                                 )
        
        matches = len(results.results)
        cwd = os.getcwd()    
        # print ("GetAbstract: Current Directory {}".format(cwd))
        logger.debug ("%s document matches for getAbstracts", matches)
        
        response_info = models.ResponseInfo( count = len(results.results),
                                             fullCount = results._numFound,
                                             limit = limit,
                                             offset = offset,
                                             listType="documentlist",
                                             fullCountComplete = limit >= results._numFound,
                                             timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)                     
                                           )
        
        document_item_list = []
        for result in results:
            if matches > 0:
                try:
                    xml_abstract = result["abstracts_xml"]
                except KeyError as e:
                    xml_abstract = None
                    logger.info("No abstract for document ID: %s", document_id)
            
                try:
                    xml_summary = result["summaries_xml"]
                except KeyError as e:
                    xml_summary = None
                    logger.info("No summary for document ID: %s", document_id)
            
                try:
                    xml_document = result["text_xml"]
                except KeyError as e:
                    xml_document = None
                    logger.error("No content matched document ID for: %s", document_id)
    
                author_ids = result.get("art_authors", None)
                if author_ids is None:
                    author_mast = None
                else:
                    author_mast = opasgenlib.deriveAuthorMast(author_ids)
    
                pgrg = result.get("art_pgrg", None)
                pg_start, pg_end = opasgenlib.pgrg_splitter(pgrg)
                
                source_title = result.get("art_pepsourcetitlefull", None)
                title = result.get("art_title_xml", "")  # name is misleading, it's not xml.
                art_year = result.get("art_year", None)
                art_vol = result.get("art_vol", None)
    
                citeas = result.get("art_citeas_xml", None)
                citeas = force_string_return_from_various_return_types(citeas)
    
                abstract = get_excerpt_from_abs_sum_or_doc(xml_abstract, xml_summary, xml_document)
                if abstract == "[]":
                    abstract = None
                elif ret_format == "TEXTONLY":
                    abstract = opasxmllib.xml_elem_or_str_to_text(abstract)
                elif ret_format == "HTML":
                    abstractHTML = opasxmllib.xml_str_to_html(abstract)
                    # try to extract just the abstract.  Not sure why this used to work and now (20191111) doesn't for some articles.  Maybe sampling, or
                    #   the style sheet application changed.
                    abstract = extract_html_fragment(abstractHTML, "//div[@id='abs']")
                    if abstract == None:
                        abstract = abstractHTML
    
                abstract = opasxmllib.add_headings_to_abstract_html(abstract=abstract, 
                                                                source_title=source_title,
                                                                pub_year=art_year,
                                                                vol=art_vol, 
                                                                pgrg=pgrg, 
                                                                citeas=citeas, 
                                                                title=title,
                                                                author_mast=author_mast,
                                                                ret_format=ret_format)
    
                item = models.DocumentListItem(year = art_year,
                                        vol = art_vol,
                                        sourceTitle = source_title,
                                        pgRg = pgrg,
                                        pgStart = pg_start,
                                        pgEnd = pg_end,
                                        authorMast = author_mast,
                                        documentID = result.get("art_id", None),
                                        documentRefHTML = citeas,
                                        documentRef = opasxmllib.xml_elem_or_str_to_text(citeas, default_return=""),
                                        accessLimited = authenticated,
                                        abstract = abstract,
                                        score = result.get("score", None)
                                        )
            
                document_item_list.append(item)
    
        response_info.count = len(document_item_list)
        
        document_list_struct = models.DocumentListStruct( responseInfo = response_info, 
                                                          responseSet=document_item_list
                                                          )
        
        documents = models.Documents(documents = document_list_struct)
            
        ret_val = documents

    return ret_val


#-----------------------------------------------------------------------------
def documents_get_document(document_id,
                           solr_query_params=None,
                           ret_format="XML",
                           authenticated=True,
                           file_classification=None, 
                           page_offset=None,
                           page_limit=None,
                           page=None
                           ):
    """
   For non-authenticated users, this endpoint returns only Document summary information (summary/abstract)
   For authenticated users, it returns with the document itself
   
    >> resp = documents_get_document("AIM.038.0279A", ret_format="html") 
    
    >> resp = documents_get_document("AIM.038.0279A") 
    
    >> resp = documents_get_document("AIM.040.0311A")
    

    """
    ret_val = {}
    
    if not authenticated and file_classification != opasConfig.DOCUMENT_ACCESS_FREE:
        #if user is not authenticated, effectively do endpoint for getDocumentAbstracts
        logger.info("documentsGetDocument: User not authenticated...fetching abstracts instead")
        ret_val = document_list_struct = documents_get_abstracts(document_id, authenticated=authenticated, ret_format=ret_format, limit=1)
        return ret_val

    try: # Solr match against art_id is case sensitive
        document_id = document_id.upper()
    except Exception as e:
        logger.warning("Bad argument {document_id} to documents_get_document(Error:{e})")
        return ret_val
    else:
        if solr_query_params is not None:
            # repeat the query that the user had done when retrieving the document
            query = "art_id:{} && {}".format(document_id, solr_query_params.searchQ)
            document_list, ret_status = search_text(query, 
                                        filter_query = solr_query_params.filterQ,
                                        full_text_requested=True,
                                        format_requested = ret_format,
                                        authenticated=authenticated,
                                        file_classification=file_classification, 
                                        query_debug = False,
                                        dis_max = solr_query_params.solrMax,
                                        limit=1, # document call returns only one document.  limit and offset used for something else
                                        #offset=offset
                                        page_offset=page_offset, #  e.g., start with the 5th page
                                        page_limit=page_limit,    #        return limit pages
                                        page=page # start page specified
                                      )
        
        if document_list == None or document_list.documentList.responseInfo.count == 0:
            #sometimes the query is still sent back, even though the document was an independent selection.  So treat it as a simple doc fetch
            
            query = "art_id:{}".format(document_id)
            #summaryFields = "art_id, art_vol, art_year, art_citeas_xml, art_pgrg, art_title, art_author_id, abstracts_xml, summaries_xml, text_xml"
           
            document_list, ret_status = search_text(query,
                                                    full_text_requested=True,
                                                    format_requested = ret_format,
                                                    authenticated=authenticated,
                                                    query_debug = False,
                                                    limit=1,   # document call returns only one document.  limit and offset used for something else
                                                    #offset=offset
                                                    page_offset=page_offset, #  e.g., start with the 5th page
                                                    page_limit=page_limit    #        return limit pages
                                                    )
    
        try:
            matches = document_list.documentList.responseInfo.count
            logger.debug("documentsGetDocument %s document matches", matches)
            full_count = document_list.documentList.responseInfo.fullCount
            full_count_complete = document_list.documentList.responseInfo.fullCountComplete
            document_list_item = document_list.documentList.responseSet[0]
        except Exception as e:
            logger.info("No matches or error: %s", e)
        else:
            if page_limit is None:
                page_limit = 0
            if page_offset is None:
                page_offset = 0
                
            response_info = models.ResponseInfo( count = matches,
                                                 fullCount = full_count,
                                                 page=page, 
                                                 limit = page_limit,
                                                 offset = page_offset,
                                                 listType="documentlist",
                                                 fullCountComplete = full_count_complete,
                                                 solrParams = solr_query_params.dict(), 
                                                 timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)
                                               )
            
            if matches >= 1:       
                document_list_struct = models.DocumentListStruct( responseInfo = response_info, 
                                                                  responseSet = [document_list_item]
                                                                )
                    
                documents = models.Documents(documents = document_list_struct)
                        
                ret_val = documents
    
    return ret_val

#-----------------------------------------------------------------------------
def documents_get_glossary_entry(term_id,
                                 solrQueryParams=None,
                                 retFormat="XML",
                                 authenticated=True,
                                 limit=opasConfig.DEFAULT_LIMIT_FOR_DOCUMENT_RETURNS, offset=0):
    """
    For non-authenticated users, this endpoint should return an error (#TODO)
    
    For authenticated users, it returns with the glossary itself
   
    IMPORTANT NOTE: At least the way the database is currently populated, for a group, the textual part (text) is the complete group, 
      and thus the same for all entries.  This is best for PEP-Easy now, otherwise, it would need to concatenate all the result entries.
   
    >> resp = documentsGetGlossaryEntry("ZBK.069.0001o.YN0019667860580", retFormat="html") 
    
    >> resp = documentsGetGlossaryEntry("ZBK.069.0001o.YN0004676559070") 
    
    >> resp = documentsGetGlossaryEntry("ZBK.069.0001e.YN0005656557260")
    

    """
    ret_val = {}
    term_id = term_id.upper()
    
    if not authenticated:
        #if user is not authenticated, effectively do endpoint for getDocumentAbstracts
        documents_get_abstracts(term_id, limit=1)
    else:
        results = solr_gloss.query(q = f"term_id:{term_id} || group_id:{term_id}",  
                                  fields = "term_id, group_id, term_type, term_source, group_term_count, art_id, text"
                                 )
        document_item_list = []
        count = 0
        try:
            for result in results:
                try:
                    document = result.get("text", None)
                    if retFormat == "HTML":
                        document = opasxmllib.xml_str_to_html(document)
                    else:
                        document = document
                    item = models.DocumentListItem(PEPCode = "ZBK", 
                                                   documentID = result.get("art_id", None), 
                                                   title = result.get("term_source", None),
                                                   abstract = None,
                                                   document = document,
                                                   score = result.get("score", None)
                                            )
                except ValidationError as e:
                    logger.error(e.json())  
                else:
                    document_item_list.append(item)
                    count = len(document_item_list)

        except IndexError as e:
            logger.warning("No matching glossary entry for %s.  Error: %s", (term_id, e))
        except KeyError as e:
            logger.warning("No content or abstract found for %s.  Error: %s", (term_id, e))
        else:
            response_info = models.ResponseInfo( count = count,
                                                 fullCount = count,
                                                 limit = limit,
                                                 offset = offset,
                                                 listType="documentlist",
                                                 fullCountComplete = True,
                                                 timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)                     
                                               )
            
            document_list_struct = models.DocumentListStruct( responseInfo = response_info, 
                                                              responseSet = document_item_list
                                                            )
                
            documents = models.Documents(documents = document_list_struct)
                        
            ret_val = documents
        
        return ret_val

#-----------------------------------------------------------------------------
def prep_document_download(document_id, ret_format="HTML", authenticated=True, base_filename="opasDoc"):
    """
   For non-authenticated users, this endpoint returns only Document summary information (summary/abstract)
   For authenticated users, it returns with the document itself
   
    >>> a = prep_document_download("IJP.051.0175A", ret_format="html") 
    
    >> a = prep_document_download("IJP.051.0175A", ret_format="epub") 
    

    """
    def add_epub_elements(str):
        # for now, just return
        return str
        
    ret_val = None
    
    if authenticated:
        results = solr_docs.query( q = "art_id:%s" % (document_id),  
                                   fields = "art_id, art_citeas_xml, text_xml"
                                 )
        try:
            ret_val = results.results[0]["text_xml"]
        except IndexError as e:
            logger.warning("No matching document for %s.  Error: %s", document_id, e)
        except KeyError as e:
            logger.warning("No content or abstract found for %s.  Error: %s", document_id, e)
        else:
            try:    
                if isinstance(ret_val, list):
                    ret_val = ret_val[0]
            except Exception as e:
                logger.warning("Empty return: %s", e)
            else:
                try:    
                    if ret_format.upper() == "HTML":
                        ret_val = opasxmllib.remove_encoding_string(ret_val)
                        filename = convert_xml_to_html_file(ret_val, output_filename=document_id + ".html")  # returns filename
                        ret_val = filename
                    elif ret_format.upper() == "PDFORIG":
                        ret_val = find(document_id + ".PDF", opasConfig.PDFORIGDIR)
                    elif ret_format.upper() == "PDF":
                        ret_val = opasxmllib.remove_encoding_string(ret_val)
                        html_string = opasxmllib.xml_str_to_html(ret_val)
                        # open output file for writing (truncated binary)
                        filename = document_id + ".PDF" 
                        result_file = open(filename, "w+b")
                        # convert HTML to PDF
                        pisaStatus = pisa.CreatePDF(html_string,                # the HTML to convert
                                                    dest=result_file)           # file handle to recieve result
                        # close output file
                        result_file.close()                 # close output file
                        # return True on success and False on errors
                        ret_val = filename
                    elif ret_format.upper() == "EPUB":
                        ret_val = opasxmllib.remove_encoding_string(ret_val)
                        html_string = opasxmllib.xml_str_to_html(ret_val)
                        html_string = add_epub_elements(html_string)
                        filename = opasxmllib.html_to_epub(html_string, document_id, document_id)
                        ret_val = filename
                    else:
                        logger.warning(f"Format {ret_format} not supported")
                        
                except Exception as e:
                    logger.warning("Can't convert data: %s", e)
        
    return ret_val

#-----------------------------------------------------------------------------
def find(name, path):
    """
    Find the file name in the selected path
    """
    for root, dirs, files in os.walk(path):
        if name.lower() in [x.lower() for x in files]:
            return os.path.join(root, name)

#-----------------------------------------------------------------------------
def convert_xml_to_html_file(xmltext_str, xslt_file=r"./styles/pepkbd3-html.xslt", output_filename=None):
    if output_filename is None:
        basename = "opasDoc"
        suffix = datetime.datetime.now().strftime("%y%m%d_%H%M%S")
        filename_base = "_".join([basename, suffix]) # e.g. 'mylogfile_120508_171442'        
        output_filename = filename_base + ".html"

    htmlString = opasxmllib.xml_str_to_html(xmltext_str, xslt_file=xslt_file)
    fo = open(output_filename, "w", encoding="utf-8")
    fo.write(str(htmlString))
    fo.close()
    
    return output_filename

#-----------------------------------------------------------------------------
def get_image_filename(image_id):
    """
    Return the file name given the image id, if it exists
    
    >>> get_image_binary("AIM.036.0275A.FIG001")

    >>> get_image_binary("JCPTX.032.0329A.F0003g")
    
    """
    image_filename = None
    image_source_path = localsecrets.API_BINARY_IMAGE_SOURCE_PATH
    ext = os.path.splitext(image_id)[-1].lower()
    if ext in (".jpg", ".tif", ".gif"):
        image_filename = os.path.join(image_source_path, image_id)
        exists = os.path.isfile(image_filename)
        if not exists:
            image_filename = None
    else:
        image_filename = os.path.join(image_source_path, image_id + ".jpg")
        exists = os.path.isfile(image_filename)
        if not exists:
            image_filename = os.path.join(image_source_path, image_id + ".gif")
            exists = os.path.isfile(image_filename)
            if not exists:
                image_filename = os.path.join(image_source_path, image_id + ".tif")
                exists = os.path.isfile(image_filename)
                if not exists:
                    image_filename = None

    return image_filename

#-----------------------------------------------------------------------------
def get_image_binary(image_id):
    """
    Return a binary object of the image, e.g.,
   
    >> get_image_binary("NOTEXISTS.032.0329A.F0003g")

    >>> get_image_binary("AIM.036.0275A.FIG001")

    >>> get_image_binary("JCPTX.032.0329A.F0003g")
    
    Note: the current server requires the extension, but it should not.  The server should check
    for the file per the following extension hierarchy: .jpg then .gif then .tif
    
    However, if the extension is supplied, that should be accepted.

    The current API implements this:
    
    curl -X GET "http://stage.pep.gvpi.net/api/v1/Documents/Downloads/Images/aim.036.0275a.fig001.jpg" -H "accept: image/jpeg" -H "Authorization: Basic cC5lLnAuYS5OZWlsUlNoYXBpcm86amFDayFsZWdhcmQhNQ=="
    
    and returns a binary object.
        
    """

    
    # these won't be in the Solr database, needs to be brought back by a file
    # the file ID should match a file name
    ret_val = None
    image_filename = get_image_filename(image_id)
    if image_filename is not None:
        try:
            f = open(image_filename, "rb")
            image_bytes = f.read()
            f.close()    
        except OSError as e:
            logger.error("getImageBinary: File Open Error: %s", e)
        except Exception as e:
            logger.error("getImageBinary: Error: %s", e)
        else:
            ret_val = image_bytes
    else:
        logger.error("Image File ID %s not found", image_id)
  
    return ret_val

#-----------------------------------------------------------------------------
def get_kwic_list(marked_up_text, 
                  extra_context_len=opasConfig.DEFAULT_KWIC_CONTENT_LENGTH, 
                  solr_start_hit_tag=opasConfig.HITMARKERSTART, # supply whatever the start marker that solr was told to use
                  solr_end_hit_tag=opasConfig.HITMARKEREND,     # supply whatever the end marker that solr was told to use
                  output_start_hit_tag_marker=opasConfig.HITMARKERSTART_OUTPUTHTML, # the default output marker, in HTML
                  output_end_hit_tag_marker=opasConfig.HITMARKEREND_OUTPUTHTML,
                  limit=opasConfig.DEFAULT_MAX_KWIC_RETURNS):
    """
    Find all nonoverlapping matches, using Solr's return.  Limit the number.
    
    (See git version history for an earlier -- and different version)
    """
    
    ret_val = []
    em_marks = re.compile("(.{0,%s}%s.*%s.{0,%s})" % (extra_context_len, solr_start_hit_tag, solr_end_hit_tag, extra_context_len))
    marked_up = re.compile(".*(%s.*%s).*" % (solr_start_hit_tag, solr_end_hit_tag))
    marked_up_text = opasxmllib.xml_string_to_text(marked_up_text) # remove markup except match tags which shouldn't be XML

    match_text_pattern = "({{.*?}})"
    pat_compiled = re.compile(match_text_pattern)
    word_list = pat_compiled.split(marked_up_text) # split all the words
    index = 0
    count = 0
    #TODO may have problems with adjacent matches!
    skip_next = False
    for n in word_list:
        if pat_compiled.match(n) and skip_next == False:
            # we have a match
            try:
                text_before = word_list[index-1]
                text_before_words = text_before.split(" ")[-extra_context_len:]
                text_before_phrase = " ".join(text_before_words)
            except:
                text_before = ""
            try:
                text_after = word_list[index+1]
                text_after_words = text_after.split(" ")[:extra_context_len]
                text_after_phrase = " ".join(text_after_words)
                if pat_compiled.search(text_after_phrase):
                    skip_next = True
            except:
                text_after = ""

            # change the tags the user told Solr to use to the final output tags they want
            #   this is done to use non-xml-html hit tags, then convert to that after stripping the other xml-html tags
            match = re.sub(solr_start_hit_tag, output_start_hit_tag_marker, n)
            match = re.sub(solr_end_hit_tag, output_end_hit_tag_marker, match)

            context_phrase = text_before_phrase + match + text_after_phrase

            ret_val.append(context_phrase)

            try:
                logger.info("getKwicList Match: '...{}...'".format(context_phrase))
            except Exception as e:
                logger.error("getKwicList Error printing or logging matches. %s", e)
            
            index += 1
            count += 1
            if count >= limit:
                break
        else:
            skip_next = False
            index += 1
        
    # matchCount = len(ret_val)
    
    return ret_val    
#-----------------------------------------------------------------------------
def year_arg_parser(year_arg):
    """
    Look for fulll start/end year ranges submitted in a single field.
    Returns with Solr field name and proper syntax
    
    For example:
        >1977
        <1990
        1980-1990
        1970

    >>> year_arg_parser("1970")
    '&& art_year_int:1970 '
    >>> year_arg_parser(">1977")
    '&& art_year_int:[1977 TO *] '
    >>> year_arg_parser("<1990")
    '&& art_year_int:[* TO 1990] '
    >>> year_arg_parser("1980-1990")
    '&& art_year_int:[1980 TO 1990] '
    """
    ret_val = None
    year_query = re.match("[ ]*(?P<option>[\>\^\<\=])?[ ]*(?P<start>[12][0-9]{3,3})?[ ]*(?P<separator>([-]|TO))*[ ]*(?P<end>[12][0-9]{3,3})?[ ]*", year_arg, re.IGNORECASE)            
    if year_query is None:
        logger.warning("Search - StartYear bad argument {}".format(year_arg))
    else:
        option = year_query.group("option")
        start = year_query.group("start")
        end = year_query.group("end")
        separator = year_query.group("separator")
        if start is None and end is None:
            logger.warning("Search - StartYear bad argument {}".format(year_arg))
        else:
            if option == "^":
                # between
                # find endyear by parsing
                if start is None:
                    start = end # they put > in start rather than end.
                elif end is None:
                    end = start # they put < in start rather than end.
                search_clause = "&& art_year_int:[{} TO {}] ".format(start, end)
            elif option == ">":
                # greater
                if start is None:
                    start = end # they put > in start rather than end.
                search_clause = "&& art_year_int:[{} TO {}] ".format(start, "*")
            elif option == "<":
                # less than
                if end is None:
                    end = start # they put < in start rather than end.
                search_clause = "&& art_year_int:[{} TO {}] ".format("*", end)
            else: # on
                if start is not None and end is not None:
                    # they specified a range anyway
                    search_clause = "&& art_year_int:[{} TO {}] ".format(start, end)
                elif start is None and end is not None:
                    # they specified '- endyear' without the start, so less than
                    search_clause = "&& art_year_int:[{} TO {}] ".format("*", end)
                elif start is not None and separator is not None:
                    # they mean greater than
                    search_clause = "&& art_year_int:[{} TO {}] ".format(start, "*")
                else: # they mean on
                    search_clause = "&& art_year_int:{} ".format(year_arg)

            ret_val = search_clause

    return ret_val
                   
#---------------------------------------------------------------------------------------------------------
# this function lets various endpoints like search, searchanalysis, and document, share this large parameter set.
def parse_search_query_parameters(search=None,
                                  journal_name=None,  # full name of journal or wildcarded
                                  journal=None,       # journal code or list of codes
                                  fulltext1=None,     # term, phrases, and boolean connectors for full-text search
                                  fulltext2=None,     # term, phrases, and boolean connectors for full-text search
                                  vol=None,           # match only this volume (integer)
                                  issue=None,         # match only this issue (integer)
                                  author=None,        # author last name, optional first, middle.  Wildcards permitted
                                  title=None,         
                                  datetype=None,  # not implemented
                                  startyear=None, # can contain complete range syntax
                                  endyear=None,   # year only.
                                  dreams=None,
                                  quotes=None,
                                  abstracts=None,
                                  dialogs=None,
                                  references=None,
                                  citecount=None, 
                                  viewcount=None, 
                                  viewed_within=None, 
                                  solrQ=None, 
                                  disMax=None, 
                                  edisMax=None, 
                                  quick_search=None, 
                                  sort=None, 
                                  ):
    """
    >>> search = parse_search_query_parameters(journal="IJP", vol=57, author="Tuckett")
    >>> search.analyzeThis
    'art_authors_ngrm:Tuckett '
    
    <QueryParameters analyzeThis='art_authors_ngrm:Tuckett ' searchQ='*:* ' filterQ='art_pepsrccode:IJP && art_vol:57  && art_authors_ngrm:Tuckett ' searchAnalysisTermList=['art_pepsrccode:IJP ', 'art_authors_ngrm:Tuckett '] solrMax=None solrSortBy=None urlRequest=''>    
    """
                
        
        
        # convert to upper case

    # initialize accumulated variables
    search_q = "*:* "
    filter_q = "*:* "
    analyze_this = ""
    solr_max = None
    search_analysis_term_list = []
    # used to remove prefix && added to queries.  
    # Could make it global to save a couple of CPU cycles, but I suspect it doesn't matter
    # and the function is cleaner this way.
    pat_prefix_amps = re.compile("^\s*&& ")
    qparse = opasQueryHelper.QueryTextToSolr()
    
    if sort is not None:  # not sure why this seems to have a slash, but remove it
        sort = re.sub("\/", "", sort)

    if title is not None:
        title = qparse.markup(title, "art_title_xml")
        analyze_this = f"&& {title} "
        filter_q += analyze_this
        search_analysis_term_list.append(analyze_this)  

    if journal_name is not None:
        # accepts a journal name and optional wildcard
        analyze_this = f"&& art_pepsourcetitle_fulltext:{journal_name} "
        filter_q += analyze_this
        search_analysis_term_list.append(analyze_this)  

    if journal is not None:
        # accepts a journal code (no wildcards) or a list of journal codes
        # ALSO can accept a single journal name or partial name with an optional wildcard.  But
        #   that's really what argument journal_name is for, so this is just extra and may be later removed.
        code_for_query = ""
        analyze_this = ""
        # journal_code_list_pattern = "((?P<namelist>[A-z0-9]*[ ]*\+or\+[ ]*)+|(?P<namelist>[A-z0-9]))"
        journal_wildcard_pattern = r".*\*[ ]*"  # see if it ends in a * (wildcard)
        if re.match(journal_wildcard_pattern, journal):
            # it's a wildcard pattern
            code_for_query = journal
            analyze_this = f"&& art_pepsourcetitlefull:{code_for_query} "
            filter_q += analyze_this
        else:
            journal_code_list = journal.split(" or ")
            # convert to upper case
            journal_code_list = [f"art_pepsrccode:{x.upper()}" for x in journal_code_list]
            if len(journal_code_list) > 1:
                # it was a list.
                code_for_query = " OR ".join(journal_code_list)
                analyze_this = f"&& ({code_for_query}) "
                filter_q += analyze_this
            else:
                sourceInfo = sourceDB.lookupSourceCode(journal.upper())
                if sourceInfo is not None:
                    # it's a single source code
                    code_for_query = journal.upper()
                    analyze_this = f"&& art_pepsrccode:{code_for_query} "
                    filter_q += analyze_this
                else: # not a pattern, or a code, or a list of codes.
                    # must be a name
                    code_for_query = journal
                    analyze_this = f"&& art_pepsourcetitlefull:{code_for_query} "
                    filter_q += analyze_this

        search_analysis_term_list.append(analyze_this)
        # or it could be an abbreviation #TODO
        # or it counld be a complete name #TODO

    if vol is not None:
        analyze_this = f"&& art_vol:{vol} "
        filter_q += analyze_this
        #searchAnalysisTermList.append(analyzeThis)  # Not collecting this!

    if issue is not None:
        analyze_this = f"&& art_iss:{issue} "
        filter_q += analyze_this
        #searchAnalysisTermList.append(analyzeThis)  # Not collecting this!

    if author is not None:
        author = termlist_to_doubleamp_query(author, field="art_authors_text")
        # add a && to the start to add to existng filter_q 
        analyze_this = f" && {author} "
        filter_q += analyze_this
        search_analysis_term_list.append(analyze_this)  

    if datetype is not None:
        #TODO for now, lets see if we need this. (We might)
        pass

    if startyear is not None and endyear is None:
        # put this in the filter query
        # parse startYear
        parsed_year_search = year_arg_parser(startyear)
        if parsed_year_search is not None:
            filter_q += parsed_year_search
            search_analysis_term_list.append(parsed_year_search)  
        else:
            logger.info(f"Search - StartYear bad argument {startyear}")

    if startyear is not None and endyear is not None:
        # put this in the filter query
        # should check to see if they are each dates
        if re.match("[12][0-9]{3,3}", startyear) is None or re.match("[12][0-9]{3,3}", endyear) is None:
            logger.info("Search - StartYear {} /Endyear {} bad arguments".format(startyear, endyear))
        else:
            analyze_this = f"&& art_year_int:[{startyear} TO {endyear}] "
            filter_q += analyze_this
            search_analysis_term_list.append(analyze_this)

    if startyear is None and endyear is not None:
        if re.match("[12][0-9]{3,3}", endyear) is None:
            logger.info(f"Search - Endyear {endyear} bad argument")
        else:
            analyze_this = f"&& art_year_int:[* TO {endyear}] "
            filter_q += analyze_this
            search_analysis_term_list.append(analyze_this)

    if citecount is not None:
        # This is the only query handled by GVPi and the current API.  But
        # the Solr database is set up so this could be easily extended to
        # the 10, 20, and "all" periods.  Here we add syntax to the 
        # citecount field, to allow the user to say:
        #  25 in 10 
        # which means 25 citations in 10 years
        # or 
        #  400 in ALL
        # which means 400 in all years. 
        # 'in' is required along with a space in front of it and after it
        # when specifying the period.
        # the default period is 5 years.
        # citecount = citecount.strip()
        val = None
        match_ptn = "\s*(?P<nbr>[0-9]+)(\s+TO\s+(?P<endnbr>[0-9]+))?(\s+IN\s+(?P<period>(5|10|20|All)))?\s*"
        m = re.match(match_ptn, citecount, re.IGNORECASE)
        if m is not None:
            val = m.group("nbr")
            val_end = m.group("endnbr")
            if val_end == None:
                val_end = "*"
            period = m.group("period")

        if val is None:
            val = 1
        if period is None:
            period = '5'

        analyze_this = f"&& art_cited_{period.lower()}:[{val} TO {val_end}] "
        filter_q += analyze_this
        search_analysis_term_list.append(analyze_this)

    if fulltext1 is not None:
        fulltext1 = qparse.markup(fulltext1, "text_xml")
        analyze_this = f"&& {fulltext1} "
        search_q += analyze_this
        search_analysis_term_list.append(analyze_this)

    if fulltext2 is not None:
        # we should use this for thesaurus OFF later
        fulltext2 = qparse.markup(fulltext2, "text_xml")
        analyze_this = f"&& {fulltext2} "
        search_q += analyze_this
        search_analysis_term_list.append(analyze_this)

    if dreams is not None:
        dreams = qparse.markup(dreams, "dreams_xml")
        analyze_this = f"&& {dreams} "
        search_q += analyze_this
        search_analysis_term_list.append(analyze_this)

    if quotes is not None:
        quotes = qparse.markup(quotes, "quotes_xml")
        analyze_this = f"&& {quotes} "
        search_q += analyze_this
        search_analysis_term_list.append(analyze_this)

    if abstracts is not None:
        abstracts = qparse.markup(abstracts, "abstracts_xml")
        analyze_this = f"&& {abstracts} "
        search_q += analyze_this
        search_analysis_term_list.append(analyze_this)

    if dialogs is not None:
        dialogs = qparse.markup(dialogs, "dialogs_xml")
        analyze_this = f"&& {dialogs} "
        search_q += analyze_this
        search_analysis_term_list.append(analyze_this)

    if references is not None:
        references = qparse.markup(references, "references_xml")
        analyze_this = f"&& {references} "
        search_q += analyze_this
        search_analysis_term_list.append(analyze_this)

    if solrQ is not None:
        search_q = solrQ # (overrides fields) # search = solrQ
        search_analysis_term_list = [solrQ]

    if disMax is not None:
        search_q = disMax # (overrides fields) # search = solrQ
        solr_max = "disMax"

    if edisMax is not None:
        search_q = edisMax # (overrides fields) # search = solrQ
        solr_max = "edisMax"

    if quick_search is not None: #TODO - might want to change this to match PEP-Web best
        search_q = quick_search # (overrides fields) # search = solrQ
        solr_max = "edisMax"
        
    # now clean up the final components.
    if search_q is not None:
        # no need to start with '*:* && '.  Remove it.
        search_q = search_q.replace("*:* && ", "")

    if filter_q is not None:
        # no need to start with '*:* && '.  Remove it.
        filter_q = filter_q.replace("*:* && ", "")

    if analyze_this is not None:
        # no need to start with '&& '.  Remove it.
        analyze_this = pat_prefix_amps.sub("", analyze_this)
    
    if search_analysis_term_list is not []:
        search_analysis_term_list = [pat_prefix_amps.sub("", x) for x in search_analysis_term_list]

    ret_val = models.QueryParameters(analyzeThis = analyze_this,
                                     searchQ = search_q,
                                     filterQ = filter_q,
                                     solrMax = solr_max,
                                     searchAnalysisTermList = search_analysis_term_list,
                                     solrSortBy = sort
    )

    return ret_val
                       
#-----------------------------------------------------------------------------
def search_analysis(query_list, 
                    filter_query = None,
                    more_like_these = False,
                    query_analysis = False,
                    dis_max = None,
                    # summaryFields="art_id, art_pepsrccode, art_vol, art_year, art_iss, 
                        # art_iss_title, art_newsecnm, art_pgrg, art_title, art_author_id, art_citeas_xml", 
                    summary_fields="art_id",                    
                    # highlightFields='art_title_xml, abstracts_xml, summaries_xml, art_authors_xml, text_xml', 
                    full_text_requested=False, 
                    user_logged_in=False,
                    limit=opasConfig.DEFAULT_MAX_KWIC_RETURNS
                   ):
    """
    Analyze the search clauses in the query list
	"""
    ret_val = {}
    document_item_list = []
    rowCount = 0
    for n in query_list:
        n = n[3:]
        n = n.strip(" ")
        if n == "" or n is None:
            continue

        results = solr_docs.query(n,
                                 disMax = dis_max,
                                 queryAnalysis = True,
                                 fields = summary_fields,
                                 rows = 1,
                                 start = 0)
    
        termField, termValue = n.split(":")
        if termField == "art_author_xml":
            term = termValue + " ( in author)"
        elif termField == "text_xml":
            term = termValue + " ( in text)"
            
        logger.debug("Analysis: Term %s, matches %s", n, results._numFound)
        item = models.DocumentListItem(term = n, 
                                termCount = results._numFound
                                )
        document_item_list.append(item)
        rowCount += 1

    if rowCount > 0:
        numFound = 0
        item = models.DocumentListItem(term = "combined",
                                termCount = numFound
                                )
        document_item_list.append(item)
        rowCount += 1
        print ("Analysis: Term %s, matches %s" % ("combined: ", numFound))

    response_info = models.ResponseInfo(count = rowCount,
                                        fullCount = rowCount,
                                        listType = "srclist",
                                        fullCountComplete = True,
                                        timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)
                                        )
    
    response_info.count = len(document_item_list)
    
    document_list_struct = models.DocumentListStruct( responseInfo = response_info, 
                                                      responseSet = document_item_list
                                                  )
    
    ret_val = models.DocumentList(documentList = document_list_struct)
    
    return ret_val

#================================================================================================================
# SEARCHTEXT
#================================================================================================================
def search_text(query, 
               filter_query = None,
               query_debug = False,
               more_like_these = False,
               full_text_requested = False,
               file_classification=None, 
               format_requested = "HTML",
               dis_max = None,
               # bring text_xml back in summary fields in case it's missing in highlights! I documented a case where this happens!
               # summary_fields = "art_id, art_pepsrccode, art_vol, art_year, art_iss, art_iss_title, art_newsecnm, art_pgrg, art_title, art_author_id, art_citeas_xml, text_xml", 
               # highlight_fields = 'art_title_xml, abstracts_xml, summaries_xml, art_authors_xml, text_xml', 
               summary_fields = "art_id, art_pepsrccode, art_vol, art_year, art_iss, art_iss_title, art_pepsourcetitleabbr, art_newsecnm, art_pgrg, abstracts_xml, art_title, art_author_id, art_citeas_xml, text_xml", 
               highlight_fields = 'text_xml', 
               sort_by="score desc",
               authenticated = None, 
               extra_context_len = opasConfig.DEFAULT_KWIC_CONTENT_LENGTH,
               maxKWICReturns = opasConfig.DEFAULT_MAX_KWIC_RETURNS,
               limit=opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, 
               offset=0,
               page_offset=None,
               page_limit=None,
               page=None
               ):
    """
    Full-text search, via the Solr server api.
    
    Returns a pair of values: ret_val, ret_status.  The double return value is important in case the Solr server isn't running or it returns an HTTP error.  The 
       ret_val = a DocumentList model object
       ret_status = a status tuple, consisting of a HTTP status code and a status mesage. Default (HTTP_200_OK, "OK")

    >>> search_text(query="art_title_xml:'ego identity'", limit=10, offset=0, full_text_requested=False)
    (<DocumentList documentList=<DocumentListStruct responseInfo=<ResponseInfo count=10 limit=10 offset=0 page=…>, (200, 'OK'))
    
        Original Parameters in API
        Original API return model example, needs to be supported:
    
                "authormast": "Ringstrom, P.A.",
				"documentID": "IJPSP.005.0257A",
				"documentRef": "Ringstrom, P.A. (2010). Commentary on Donna Orange's, &#8220;Recognition as: Intersubjective Vulnerability in the Psychoanalytic Dialogue&#8221;. Int. J. Psychoanal. Self Psychol., 5(3):257-273.",
				"issue": "3",
				"PEPCode": "IJPSP",
				"pgStart": "257",
				"pgEnd": "273",
				"title": "Commentary on Donna Orange's, &#8220;Recognition as: Intersubjective Vulnerability in the Psychoanalytic Dialogue&#8221;",
				"vol": "5",
				"year": "2010",
				"rank": "100",
				"citeCount5": "1",
				"citeCount10": "3",
				"citeCount20": "3",
				"citeCountAll": "3",
				"kwic": ". . . \r\n        
   
    """
    ret_val = {}
    ret_status = (200, "OK") # default is like HTTP_200_OK
    global count_anchors
    
    if more_like_these:
        mlt_fl = "text_xml, headings_xml, terms_xml, references_xml"
        mlt = "true"
        mlt_minwl = 8
    else:
        mlt_fl = None
        mlt = "false"
        mlt_minwl = None
    
    if query_debug:
        query_debug = "on"
    else:
        query_debug = "off"
        
    if full_text_requested:
        fragSize = opasConfig.SOLR_HIGHLIGHT_RETURN_FRAGMENT_SIZE 
    else:
        fragSize = extra_context_len

    if filter_query is not None:
        # for logging/debug
        filter_query = filter_query.replace("*:* && ", "")
        logger.debug("Solr FilterQ: %s", filter_query)
    else:
        filter_query = "*:*"

    if query is not None:
        query = query.replace("*:* && ", "")
        logger.debug("Solr Query: %s", query)

    try:
        results = solr_docs.query(query,  
                                 fq = filter_query,
                                 debugQuery = query_debug,
                                 disMax = dis_max,
                                 fields = summary_fields,
                                 hl='true', 
                                 hl_fragsize = fragSize, 
                                 hl_multiterm = 'true',
                                 hl_fl = highlight_fields,
                                 hl_usePhraseHighlighter = 'true',
                                 hl_snippets = maxKWICReturns,
                                 hl_maxAnalyzedChars=opasConfig.SOLR_HIGHLIGHT_RETURN_FRAGMENT_SIZE, 
                                 #hl_method="unified",  # these don't work
                                 #hl_encoder="HTML",
                                 mlt = mlt,
                                 mlt_fl = mlt_fl,
                                 mlt_count = 2,
                                 mlt_minwl = mlt_minwl,
                                 rows = limit,
                                 start = offset,
                                 sort=sort_by,
                                 hl_simple_pre = opasConfig.HITMARKERSTART,
                                 hl_simple_post = opasConfig.HITMARKEREND)
    except solr.SolrException as e:
        logger.error(f"Solr Runtime Search Error: {e}")
        ret_status = (400, e) # e has type <class 'solrpy.core.SolrException'>, with useful elements of httpcode, reason, and body, e.g.,
                              #  (I added the 400 first element, because then I have a known quantity to catch)
                              #  httpcode: 400
                              #  reason: 'Bad Request'
                              #  body: b'<?xml version="1.0" encoding="UTF-8"?>\n<response>\n\n<lst name="responseHeader">\n  <int name="status">400</int>\n  <int name="QTime">0</int>\n  <lst name="params">\n    <str name="hl">true</str>\n    <str name="fl">art_id, art_pepsrccode, art_vol, art_year, art_iss, art_iss_title, art_newsecnm, art_pgrg, abstracts_xml, art_title, art_author_id, art_citeas_xml, text_xml,score</str>\n    <str name="hl.fragsize">200</str>\n    <str name="hl.usePhraseHighlighter">true</str>\n    <str name="start">0</str>\n    <str name="fq">*:* </str>\n    <str name="mlt.minwl">None</str>\n    <str name="sort">rank asc</str>\n    <str name="rows">15</str>\n    <str name="hl.multiterm">true</str>\n    <str name="mlt.count">2</str>\n    <str name="version">2.2</str>\n    <str name="hl.simple.pre">%##</str>\n    <str name="hl.snippets">5</str>\n    <str name="q">*:* &amp;&amp; text:depression &amp;&amp; text:"passive withdrawal" </str>\n    <str name="mlt">false</str>\n    <str name="hl.simple.post">##%</str>\n    <str name="disMax">None</str>\n    <str name="mlt.fl">None</str>\n    <str name="hl.fl">text_xml</str>\n    <str name="wt">xml</str>\n    <str name="debugQuery">off</str>\n  </lst>\n</lst>\n<lst name="error">\n  <lst name="metadata">\n    <str name="error-class">org.apache.solr.common.SolrException</str>\n    <str name="root-error-class">org.apache.solr.common.SolrException</str>\n  </lst>\n  <str name="msg">sort param field can\'t be found: rank</str>\n  <int name="code">400</int>\n</lst>\n</response>\n'

    else: #  search was ok
        logger.debug("Search Performed: %s", query)
        logger.debug("The Filtering: %s", filter_query)
        logger.debug("Result  Set Size: %s", results._numFound)
        logger.debug("Return set limit: %s", limit)
        scopeofquery = [query, filter_query]

        if ret_status[0] == 200: 
            documentItemList = []
            rowCount = 0
            rowOffset = 0
            if full_text_requested:
                # if we're not authenticated, then turn off the full-text request and behave as if we didn't try
                if not authenticated and full_text_requested and file_classification != opasConfig.DOCUMENT_ACCESS_FREE:
                    # can't bring back full-text
                    logger.warning("Fulltext requested--by API--but not authenticated and not open access document.")
                    full_text_requested = False
                
            for result in results.results:
                # reset anchor counts for full-text markup re.sub
                count_anchors = 0
                authorIDs = result.get("art_author_id", None)
                if authorIDs is None:
                    authorMast = None
                else:
                    authorMast = opasgenlib.deriveAuthorMast(authorIDs)
        
                pgRg = result.get("art_pgrg", None)
                if pgRg is not None:
                    pgStart, pgEnd = opasgenlib.pgrg_splitter(pgRg)
                    
                documentID = result.get("art_id", None)
                art_year = result.get("art_year", None)
                art_vol = result.get("art_vol", None)
                art_issue = result.get("art_iss", None)
                text_xml = results.highlighting[documentID].get("text_xml", None)
                # no kwic list when full-text is requested.
                if text_xml is not None and not full_text_requested:
                    #kwicList = getKwicList(textXml, extraContextLen=extraContextLen)  # returning context matches as a list, making it easier for clients to work with
                    kwic_list = []
                    for n in text_xml:
                        # strip all tags
                        match = opasxmllib.xml_string_to_text(n)
                        # change the tags the user told Solr to use to the final output tags they want
                        #   this is done to use non-xml-html hit tags, then convert to that after stripping the other xml-html tags
                        match = re.sub(opasConfig.HITMARKERSTART, opasConfig.HITMARKERSTART_OUTPUTHTML, match)
                        match = re.sub(opasConfig.HITMARKEREND, opasConfig.HITMARKEREND_OUTPUTHTML, match)
                        kwic_list.append(match)
                        
                    kwic = " . . . ".join(kwic_list)  # how its done at GVPi, for compatibility (as used by PEPEasy)
                    text_xml = None
                    #print ("Document Length: {}; Matches to show: {}".format(len(textXml), len(kwicList)))
                else: # either fulltext requested, or no document
                    kwic_list = []
                    kwic = ""  # this has to be "" for PEP-Easy, or it hits an object error.  
                
                if full_text_requested:
                    fullText = result.get("text_xml", None)
                    text_xml = force_string_return_from_various_return_types(text_xml)
                    if text_xml is None:  # no highlights, so get it from the main area
                        try:
                            text_xml = fullText
                        except:
                            text_xml = None
     
                    elif len(fullText) > len(text_xml):
                        logger.warning("Warning: text with highlighting is smaller than full-text area.  Returning without hit highlighting.")
                        text_xml = fullText
                        
                    if text_xml is not None:
                        reduce = False
                        # see if an excerpt was requested.
                        if page is not None and page <= int(pgEnd) and page >= int(pgStart):
                            # use page to grab the starting page
                            # we've already done the search, so set page offset and limit these so they are returned as offset and limit per V1 API
                            offset = page - int(pgStart)
                            reduce = True
                        # Only use supplied offset if page parameter is out of range, or not supplied
                        if reduce == False and page_offset is not None: 
                            offset = page_offset
                            reduce = True

                        if page_limit is not None:
                            limit = page_limit
                            
                        if reduce == True or page_limit is not None:
                            # extract the requested pages
                            try:
                                text_xml = opasxmllib.xml_get_pages(text_xml, page_offset, page_limit, inside="body", env="body")
                                text_xml = text_xml[0]
                            except Exception as e:
                                logging.error(f"Page extraction from document failed. Error: {e}")
                                                    
                    if format_requested == "HTML":
                        # Convert to HTML
                        source_title = result.get("art_pepsourcetitleabbr", "")
                        heading = opasxmllib.get_running_head(source_title=source_title, pub_year=art_year, vol=art_vol, issue=art_issue, pgrg=pgRg, ret_format="HTML")
                        text_xml = opasxmllib.xml_str_to_html(text_xml, xslt_file=opasConfig.XSLT_XMLTOHTML)  #  e.g, r"./libs/styles/pepkbd3-html.xslt"
                        text_xml = re.sub(f"{opasConfig.HITMARKERSTART}|{opasConfig.HITMARKEREND}", numbered_anchors, text_xml)
                        text_xml = re.sub("\[\[RunningHead\]\]", f"{heading}", text_xml, count=1)
                        #text_xml = re.sub(opasConfig.HITMARKERSTART, opasConfig.HITMARKERSTART_OUTPUTHTML, text_xml)
                        #text_xml = re.sub(opasConfig.HITMARKEREND, opasConfig.HITMARKEREND_OUTPUTHTML, text_xml)
                    elif format_requested == "TEXTONLY":
                        # strip tags
                        text_xml = opasxmllib.xml_elem_or_str_to_text(text_xml, default_return=text_xml)
                    elif format_requested == "XML":
                        text_xml = re.sub(f"{opasConfig.HITMARKERSTART}|{opasConfig.HITMARKEREND}", numbered_anchors, text_xml)
                        #text_xml = re.sub(opasConfig.HITMARKERSTART, opasConfig.HITMARKERSTART_OUTPUTHTML, text_xml)
                        #text_xml = re.sub(opasConfig.HITMARKEREND, opasConfig.HITMARKEREND_OUTPUTHTML, text_xml)
        
                #  shouldn't need this anymore...per above where we turned off full_text_requested when not authenticated.  But leave for now.
                #if full_text_requested and not authenticated: # don't do this when textXml is a fragment from kwiclist!
                    #try:
                        #abstracts_xml = results.highlighting[documentID].get("abstracts_xml", None)
                        #abstracts_xml  = force_string_return_from_various_return_types(abstracts_xml )
     
                        #summaries_xml = results.highlighting[documentID].get("abstracts_xml", None)
                        #summaries_xml  = force_string_return_from_various_return_types(summaries_xml)
     
                        #text_xml = get_excerpt_from_abs_sum_or_doc(xml_abstract=abstracts_xml,
                                                                   #xml_summary=summaries_xml,
                                                                   #xml_document=text_xml)
                    #except:
                        #text_xml = None
        
                citeAs = result.get("art_citeas_xml", None)
                citeAs = force_string_return_from_various_return_types(citeAs)
                
                if more_like_these:
                    similarDocs = results.moreLikeThis[documentID]
                    similarMaxScore = results.moreLikeThis[documentID].maxScore
                    similarNumFound = results.moreLikeThis[documentID].numFound
                else:
                    similarDocs = None
                    similarMaxScore = None
                    similarNumFound = None
                
                abstract = force_string_return_from_various_return_types(result.get("abstracts_xml", None)) # these were highlight versions, not needed
                if format_requested == "HTML":
                    # Convert to HTML
                    abstract = opasxmllib.xml_str_to_html(abstract, xslt_file=opasConfig.XSLT_XMLTOHTML)  #  e.g, r"./libs/styles/pepkbd3-html.xslt"
                elif format_requested == "TEXTONLY":
                    # strip tags
                    abstract = opasxmllib.xml_elem_or_str_to_text(abstract, default_return=abstract)
                
                try:
                    item = models.DocumentListItem(PEPCode = result.get("art_pepsrccode", None), 
                                            year = result.get("art_year", None),
                                            vol = result.get("art_vol", None),
                                            pgRg = pgRg,
                                            pgStart = pgStart,
                                            pgEnd = pgEnd,
                                            authorMast = authorMast,
                                            documentID = documentID,
                                            documentRefHTML = citeAs,
                                            documentRef = opasxmllib.xml_elem_or_str_to_text(citeAs, default_return=""),
                                            kwic = kwic,
                                            kwicList = kwic_list,
                                            title = result.get("art_title", None),
                                            abstract = abstract, 
                                            document = text_xml,
                                            score = result.get("score", None), 
                                            rank = rowCount + 1,
                                            similarDocs = similarDocs,
                                            similarMaxScore = similarMaxScore,
                                            similarNumFound = similarNumFound
                                            )
                except ValidationError as e:
                    logger.error(e.json())  
                else:
                    rowCount += 1
                    # logger.info("{}:{}".format(rowCount, citeAs.decode("utf8")))
                    documentItemList.append(item)
                    if rowCount > limit:
                        break
        
        # Moved this down here, so we can fill in the Limit, Page and Offset fields based on whether there
        #  was a full-text request with a page offset and limit
        # Solr search was ok
        responseInfo = models.ResponseInfo(
                         count = len(results.results),
                         fullCount = results._numFound,
                         totalMatchCount = results._numFound,
                         limit = limit,
                         offset = offset,
                         page = page, 
                         listType="documentlist",
                         scopeQuery=[scopeofquery], 
                         fullCountComplete = limit >= results._numFound,
                         solrParams = results._params,
                         timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)                     
                       )
        
        responseInfo.count = len(documentItemList)
        
        documentListStruct = models.DocumentListStruct( responseInfo = responseInfo, 
                                                        responseSet = documentItemList
                                                      )
        
        documentList = models.DocumentList(documentList = documentListStruct)
 
        ret_val = documentList
    
    return ret_val, ret_status

#-----------------------------------------------------------------------------
def termlist_to_doubleamp_query(termlist_str, field=None):
    """
    Take a comma separated term list and change to a
    (double ampersand) type query term (e.g., for solr)
    
    >>> a = "tuckett, dav"
    >>> termlist_to_doubleamp_query(a)
    'tuckett && dav'
    >>> termlist_to_doubleamp_query(a, field="art_authors_ngrm")
    'art_authors_ngrm:tuckett && art_authors_ngrm:dav'

    """
    # in case it's in quotes in the string
    termlist_str = termlist_str.replace('"', '')
    # split it
    name_list = re.split("\W+", termlist_str)
    # if a field or function is supplied, use it
    if field is not None:
        name_list = [f"art_authors_ngrm:{x}"
                     for x in name_list if len(x) > 0]
    else:
        name_list = [f"{x}" for x in name_list]
        
    ret_val = " && ".join(name_list)
    return ret_val

def main():

    print (40*"*", "opasAPISupportLib Tests", 40*"*")
    print ("Fini")

# -------------------------------------------------------------------------------------------------------
# run it!

if __name__ == "__main__":
    print ("Running in Python %s" % sys.version_info[0])
    
    sys.path.append(r'E:/usr3/GitHub/openpubarchive/app')
    sys.path.append(r'E:/usr3/GitHub/openpubarchive/app/config')
    sys.path.append(r'E:/usr3/GitHub/openpubarchive/app/libs')
    for n in sys.path:
        print (n)

    # Spot testing during Development
    #metadataGetContents("IJP", "1993")
    #getAuthorInfo("Tuck")
    #metadataGetVolumes("IJP")
    #authorsGetAuthorInfo("Tuck")
    #authorsGetAuthorPublications("Tuck", limit=40, offset=0)    
    #databaseGetMostCited(limit=10, offset=0)
    #getArticleData("PAQ.073.0005A")
    #databaseWhatsNew()
    # docstring tests
    # get_list_of_most_downloaded()
    # sys.exit(0)
    logger = logging.getLogger(__name__)
    # extra logging for standalong mode 
    logger.setLevel(logging.WARN)
    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # create formatter
    formatter = logging.Formatter('%(asctime)s %(name)s %(lineno)d - %(levelname)s %(message)s')    
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)
    
    document_get_info("PEPGRANTVS.001.0003A", fields="file_classification")
    
    import doctest
    doctest.testmod()    
    main()
