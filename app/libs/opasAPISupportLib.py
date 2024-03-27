#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=C0321,C0103,C0301,E1101,C0303,E1004,C0330,R0915,R0914,W0703,C0326
# doctest_ellipsis.py

"""
opasAPISupportLib

This library is meant to hold the 'meat' of the API based Solr queries and other support 
functions to keep the FastAPI definitions smaller, easier to read, and more manageable.

Also, some of these functions are reused in different API calls.

"""
    #2020.0530.1 Updated getmostviewed routine and all support for it to use the new in place updated art_view count fields in Solr
                # rather than using the values from the database, as before.  Moving the data to Solr allows these values to be
                # integrated with a solr query.
    #2021.0109.1 Removed commented out code
                 # used new documentID class from opasGenSupportLib rather than local checks and handling of document_od

__author__      = "Neil R. Shapiro"
__copyright__   = "Copyright 2019-2021, Psychoanalytic Electronic Publishing"
__license__     = "Apache 2.0"
__version__     = "2021.0109.1"
__status__      = "Development"

import os
import os.path
import sys
# import shlex
#import json
#import copy
#import string

sys.path.append('./solrpy')
sys.path.append('./libs/configLib')

# import http.cookies
import re
# import secrets
# import socket, struct
# import urllib
# from urllib.parse import unquote
# from urllib.error import HTTPError
# import json
# from xml.sax import SAXParseException

from starlette.responses import Response
from starlette.requests import Request
from starlette.responses import Response
# import starlette.status as httpCodes
from starlette.exceptions import HTTPException

import time
# used this name because later we needed to refer to the module, and datetime is also the name
#  of the import from datetime.
import datetime as dtime 
from datetime import datetime
# from datetime import datetime as dt # to avoid python's confusion with datetime.timedelta

import opasConfig
import localsecrets
import opasXMLHelper as opasxmllib

import opasFileSupport
from lxml import etree
parser = etree.XMLParser(encoding='utf-8', recover=True, resolve_entities=True, load_dtd=True)

import PEPGlossaryRecognitionEngine
glossEngine = PEPGlossaryRecognitionEngine.GlossaryRecognitionEngine(gather=False)

# Removed support for Py2, only Py3 supported now
pyVer = 3
import logging
logger = logging.getLogger(__name__)

from lxml import etree
# from pydantic import BaseModel
from pydantic import ValidationError

# note: documents and documentList share the same internals, except the first level json label (documents vs documentlist)
import models

# import opasXMLHelper as opasxmllib
import opasQueryHelper
import opasGenSupportLib as opasgenlib
import opasCentralDBLib
# import opasDocPermissions as opasDocPerm
import opasPySolrLib
from opasPySolrLib import search_text, search_text_qs
# import opasProductLib
import opasArticleIDSupport

from opasMetadataCache import metadata_cache
cached_metadata = metadata_cache.get_cached_data()

# count_anchors = 0

def has_data(str):
    ret_val = True
    if str is None or str == "":
        ret_val = False

    return ret_val

def set_log_level(level_int):
    logger = logging.getLogger()
    logger.debug(f"Log Level: {logger.level}")
    # see https://stackoverflow.com/questions/37703609/using-python-logging-with-aws-lambda
    if logger.hasHandlers:
        # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
        # `.basicConfig` does not execute. Thus we set the level directly.
        logger.setLevel(level_int)
    else:
        logging.basicConfig(level=level_int)
        
    ret_val = logger.level
    logger.debug(f"Log Level: {logger.level}")
    return ret_val
    
def get_query_item_of_interest(solrQuery):
    """
    Give a solrQuery, use that to derive a string to be logged in the endpoint_session
      record column item_of_interest
    """
    ret_val = None
    try:
        try:
            fq = re.sub("art_level:1(\s\&\&\s)?", "", solrQuery.filterQ, re.I)
            if fq != '':
                ret_val = f"f:'{fq}'"
                spacer = " "
            else:
                ret_val = ""
                spacer = ""
        except:
            fq = ""
            ret_val = ""
            spacer = ""
    
        try:
            q = re.sub("\*:\*|{!parent\swhich=\'art_level:1\'}\sart_level:2\s&&\s", "", solrQuery.searchQ, re.I)
            if q != '':
                col_width_remaining = opasConfig.DB_ITEM_OF_INTEREST_WIDTH - len(fq) # don't exceed column width for logging 
                ret_val += f"{spacer}q:'{q[:col_width_remaining]}'"
        except Exception as e:
            pass
    except:
        ret_val = None

    return ret_val

def remove_leading_zeros(numeric_string):
    """
        >>> remove_leading_zeros("0033")
        '33'
        
    """
    ret_val = ""
    for n in numeric_string:
        if n != "0":
            ret_val += n
    
    return ret_val
            
def split_article_id(article_id):
    """
    >>> split_article_id("gap.005.0199a")
    ('GAP', None, '5', '199A', None)

    >>> split_article_id("rfp.075.0017a")
    ('RFP', None, '75', '17A', None)

    >>> split_article_id(None)
    (None, None, None, None, None)

    """
    journal = year = vol = page = page_id = None
    if article_id is not None:
        article_id = article_id.upper()
        
        try:
            journal, vol, page = article_id.split(".")
        except Exception as e:
            try:
                a,b,c,d = article_id.split(".")
                if d[0] == "P":
                    journal, vol, page, page_id = a, b, c, d
                elif b[:2] in [19, 20]:
                    journal, year, vol, page = a, b, c, d
            except Exception as e:
                logger.error(f"SplitArticleIDError: can not split ID {article_id} ({e})")
        else:
            vol = remove_leading_zeros(vol)
            page = remove_leading_zeros(page)
        
            if journal is not None:
                journal = journal.upper()
        
            if year is not None:
                year = page.upper()
                
            if page is not None:
                page = page.upper()
                
            if page_id is not None:
                page_id = page_id.upper()
       
    return journal, year, vol, page, page_id
    
#-----------------------------------------------------------------------------
def get_max_age(keep_active=False):
    if keep_active:    
        ret_val = opasConfig.COOKIE_MAX_KEEP_TIME    
    else:
        ret_val = opasConfig.COOKIE_MIN_KEEP_TIME     
    return ret_val  # maxAge

#-----------------------------------------------------------------------------
def extract_abstract_from_html(html_str, xpath_to_extract=opasConfig.HTML_XPATH_ABSTRACT): # xpath example: "//div[@id='abs']"
    # parse HTML
    htree = etree.HTML(html_str)
    ret_val = htree.xpath(xpath_to_extract)
    # TOC's should be passed through "whole" (well, just the body, since headers will be added.)
    if ret_val == []: # see if it's a TOC
        ret_val = htree.xpath(opasConfig.HTML_XPATH_TOC_INSTANCE) # e.g, "//div[@data-arttype='TOC']"
        if ret_val != []:
            ret_val = htree.xpath(opasConfig.HTML_XPATH_DOC_BODY) # e.g., "//div[@class='body']"
    # make sure it's a string
    ret_val = opasgenlib.force_string_return_from_various_return_types(ret_val)

    return ret_val

# REMOVED COMMENTED CODE 20210109
#-----------------------------------------------------------------------------
#def get_session_id(request):
    ## this will be a sesson ID from the client--must be in every call!
    #ret_val = request.cookies.get(opasConfig.OPASSESSIONID, None)
    #if ret_val is None:
        #ret_val = request.headers.get(opasConfig.CLIENTSESSIONID, None)

    #return ret_val
#-----------------------------------------------------------------------------
#def get_access_token(request):
    #ret_val = request.cookies.get(opasConfig.OPASACCESSTOKEN, None)
    #return ret_val

#-----------------------------------------------------------------------------
def get_expiration_time(request):
    ret_val = request.cookies.get(opasConfig.OPASEXPIRES, None)
    return ret_val
#-----------------------------------------------------------------------------
def database_get_most_cited( publication_period: int=None,   # Limit the considered pubs to only those published in these years
                             cited_in_period: str='5',  # 
                             cite_count: int=0,              # Has to be cited more than this number of times, a large nbr speeds the query
                             author: str=None,
                             title: str=None,
                             source_name: str=None,  
                             source_code: str=None,
                             source_type: str=None,
                             abstract_requested: bool=True, 
                             req_url: str=None, 
                             stat:bool=False, 
                             limit: int=None,
                             offset: int=0,
                             mlt_count:int=None, 
                             sort:str=None,
                             download:bool=None, 
                             session_info=None,
                             request=None
                             ):
    """
    Return the most cited journal articles duing the prior period years.

    period must be either '5', 10, '20', or 'all'

    args:
      limit: the number of records you want to return
      more_than: setting more_than to a large number speeds the query because the set to be sorted is smaller.
                 just set it so it's not so high you still get "limit" records back.

    >>> result, status = database_get_most_cited()
    >>> len(result.documentList.responseSet)>=10
    True

    """
    ret_val = {}
    ret_status = (200, "OK") # default is like HTTP_200_OK
    caller_name = "database_get_most_cited"
    
    cited_in_period = opasConfig.normalize_val(cited_in_period, opasConfig.VALS_YEAROPTIONS, default='5')
    #if str(period).lower() not in models.TimePeriod._value2member_map_:
        #period = '5'

    if sort is None:
        sort = f"art_cited_{cited_in_period} desc"

    start_year = dtime.date.today().year
    if publication_period is None:
        start_year = None
    else:
        start_year -= publication_period
        start_year = f">{start_year}"

    cite_count_predicate = f"{cite_count} in {cited_in_period}"
    if stat:
        field_set = "STAT"
    else:
        field_set = None
    
    solr_query_spec = \
        opasQueryHelper.parse_search_query_parameters(citecount=cite_count_predicate, 
                                                      source_name=source_name,
                                                      source_code=source_code,
                                                      source_type=source_type, 
                                                      author=author,
                                                      title=title,
                                                      startyear=start_year,
                                                      highlightlimit=0, 
                                                      return_field_set=field_set, 
                                                      abstract_requested=abstract_requested,
                                                      similar_count=mlt_count, 
                                                      sort = sort,
                                                      req_url = req_url
                                                    )

    if download: # much more limited document list if download==True
        ret_val, ret_status = opasPySolrLib.search_stats_for_download(solr_query_spec, 
                                                                      session_info=session_info
                                                                     )
    else:
        try:
            ret_val, ret_status = opasPySolrLib.search_text_qs(solr_query_spec, 
                                                               limit=limit,
                                                               offset=offset,
                                                               #mlt_count=mlt_count, 
                                                               session_info=session_info, 
                                                               req_url = req_url,
                                                               request = request,
                                                               caller_name=caller_name
                                                              )
        except Exception as e:
            logger.error(f"Search error {e}")
        
    return ret_val, ret_status   

#-----------------------------------------------------------------------------
def database_who_cited( publication_period: int=None,   # Limit the considered pubs to only those published in these years
                        cited_in_period: str='5',  # 
                        cited_art_id: str=None, # search rx for this
                        author: str=None,
                        title: str=None,
                        source_name: str=None,  
                        source_code: str=None,
                        source_type: str=None,
                        abstract_requested: bool=True, 
                        req_url: str=None, 
                        stat:bool=False, 
                        limit: int=None,
                        offset: int=0,
                        mlt_count:int=None, 
                        sort:str=None,
                        download:bool=None, 
                        session_info=None,
                        request=None
                        ):
    """
    Return the list of documents that cited this journal article.

    period must be either '5', 10, '20', or 'all'

    args:
      limit: the number of records you want to return

    >>> result, status = database_who_cited()
    >>> len(result.documentList.responseSet)>=10
    True

    """
    
    cited_in_period = opasConfig.normalize_val(cited_in_period, opasConfig.VALS_YEAROPTIONS, default='5')
    caller_name = "database_who_cited"

    if sort is None:
        sort = f"art_cited_{cited_in_period} desc"

    start_year = dtime.date.today().year
    if publication_period is None:
        start_year = None
    else:
        start_year -= publication_period
        start_year = f">{start_year}"

    try:
        solr_query_spec = \
            opasQueryHelper.parse_search_query_parameters(cited_art_id=cited_art_id, 
                                                          source_name=source_name,
                                                          source_code=source_code,
                                                          source_type=source_type, 
                                                          author=author,
                                                          title=title,
                                                          startyear=start_year,
                                                          highlightlimit=0, 
                                                          abstract_requested=abstract_requested,
                                                          similar_count=mlt_count, 
                                                          sort = sort,
                                                          req_url = req_url
                                                        )

        ret_val, ret_status = opasPySolrLib.search_text_qs(solr_query_spec, 
                                             limit=limit,
                                             offset=offset,
                                             #mlt_count=mlt_count, 
                                             session_info=session_info, 
                                             req_url = req_url, 
                                             request = request,
                                             caller_name=caller_name
                                            )
    except Exception as e:
        ret_status_msg = f"Who Cited Search error {e}"
        ret_val = {}
        ret_status = (200, ret_status_msg) 
        logger.error(ret_status_msg)
        
    return ret_val, ret_status   

#-----------------------------------------------------------------------------
def metadata_get_source_info(src_type=None, # opasConfig.VALS_PRODUCT_TYPES
                             src_code=None,
                             src_name=None, 
                             req_url: str=None, 
                             limit=opasConfig.DEFAULT_LIMIT_FOR_METADATA_LISTS,
                             offset=0):
    """
    Return a list of source metadata, by type (e.g., journal, book, video, stream.)

    NOTE: Stream is equiv to journal for videos. Video is more like books, individual videos).

    No attempt here to map to the correct structure, just checking what field/data items we have in sourceInfoDB.

    >>> results = metadata_get_source_info(src_code="APA")
    >>> results.sourceInfo.responseInfo.count == 1
    True
    >>> results = metadata_get_source_info(src_type="journal", limit=3)
    >>> results.sourceInfo.responseInfo.count >= 3
    True
    >>> results = metadata_get_source_info(src_type="book", limit=10)
    >>> results.sourceInfo.responseInfo.count >= 10
    True
    >>> results = metadata_get_source_info(src_type="journals", limit=10, offset=0)
    >>> results.sourceInfo.responseInfo.count >= 10
    True
    >>> results = metadata_get_source_info(src_type="journals", limit=10, offset=6)
    >>> results.sourceInfo.responseInfo.count >= 10
    True

    >>> results = metadata_get_source_info()
    >>> results.sourceInfo.responseInfo.fullCount
    191
    
    """
    ret_val = []
    source_info_dblist = []
    err = None
    ocd = opasCentralDBLib.opasCentralDB()
    # standardize Source type, allow plural, different cases, but code below this part accepts only those three.
    if src_type is not None and src_type != "*":
        src_type_in = src_type # save it for logging
        src_type = opasConfig.normalize_val(src_type, opasConfig.VALS_PRODUCT_TYPES)
        if src_type is None:
            err = f"SourceTypeError: {src_type_in}"
            logger.error(err)
            raise Exception(err)

    if src_type == "videos":
        # sort by title like other media types
        # IMPORTANT NOTE: Sort must be a string field to work!
        total_count, source_info_dblist, ret_val, return_status = opasPySolrLib.metadata_get_videos(src_type=src_type, pep_code=src_code, limit=limit, offset=offset, sort_field="title_str")
        count = len(source_info_dblist)
        if return_status != (200, "OK"):
            raise HTTPException(
                status_code=return_status[0],
                detail=return_status[1]
            )                
        
    else: # get from mySQL
        try:
            # note...this sorts by title as only current option
            total_count, source_info_dblist = ocd.get_sources(src_type = src_type, src_code=src_code, src_name=src_name, limit=limit, offset=offset)
            if source_info_dblist is not None:
                count = len(source_info_dblist)
            else:
                count = 0
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
                                         request=f"{req_url}",
                                         timeStamp = datetime.utcfromtimestamp(time.time()).strftime(opasConfig.TIME_FORMAT_STR)                     
                                         )

    source_info_listitems = []
    counter = 0
    if count > 0:
        for source in source_info_dblist:
            counter += 1
            err = 0
            if counter < offset:
                continue
            if counter > limit:
                break
            try:
                # first capture fields needed for art_citeas
                base_code = source.get("basecode")
                documentID = source.get("documentID", source.get("articleID"))
                title = source.get("title")
                publisher_name = source.get("publisher", "Psychoanalytic Electronic Publishing")
                biblio_abbrev = source.get("bib_abbrev", source.get("bibabbrev")) # videos and books

                # for books (from productbase)
                authors = source.get("author")
                pub_year = source.get("pub_year") 
                instance_count = source.get("instances", 1)
                book_code = None

                # src_type = source.get("product_type")

                # for journals (from productbase)
                start_year = source.get("start_year")
                end_year = source.get("end_year")
                if start_year is None:
                    start_year = pub_year
                if end_year is None:
                    end_year = pub_year

                # general
                # active = source.get("active")
                pub_class = source.get("accessClassification")
                # use standardized version of class
                pep_release = source.get("pepversion")
                pub_source_url = source.get("landing_page")
                art_citeas = ""
                
                if src_type == "book":
                    book_code = source.get("pepcode")
                    if book_code is None:
                        logger.warning(f"Book code (pepcode) information missing for requested basecode {base_code} in productbase")
                    else:
                        m = re.match("(?P<code>[a-z]+)(?P<num>[0-9]+)", book_code, re.IGNORECASE)
                        if m is not None:
                            code = m.group("code")
                            num = m.group("num")
                            book_code = code + "." + num
    
                        art_citeas = u"""<p class="citeas"><span class="authors">%s</span> (<span class="year">%s</span>) <span class="title">%s</span>. <span class="publisher">%s</span>.""" \
                            %                   (authors,
                                                 pub_year,
                                                 title,
                                                 biblio_abbrev
                                                 )
                elif src_type == "videos":
                    art_citeas = source.get("art_citeas")
                    base_code = source.get("src_code")
                else:
                    art_citeas = title # journals just should show display title
    
                try:
                    item = models.SourceInfoListItem( sourceType = src_type,
                                                      PEPCode = base_code,
                                                      accessClassification=pub_class, 
                                                      pubSourceURL=pub_source_url,
                                                      PEPRelease=pep_release, 
                                                      authors = authors,
                                                      documentID = documentID,
                                                      displayTitle = art_citeas,
                                                      title = title,
                                                      srcTitle = title,  # v1 Deprecated for future
                                                      bookCode = book_code,
                                                      abbrev = source.get("bibabbrev"),
                                                      bannerURL = f"{localsecrets.APIURL}/{opasConfig.IMAGES}/banner{source.get('basecode')}Logo.gif",
                                                      publisher = publisher_name, 
                                                      language = source.get("language"),
                                                      ISSN = source.get("ISSN"),
                                                      ISBN10 = source.get("ISBN-10"),
                                                      ISBN13 = source.get("ISBN-13"),
                                                      pub_year = pub_year, # added back 2022-01-22 (comes from productbase, for books -- not needed in solr)
                                                      yearFirst = start_year,
                                                      yearLast = end_year,
                                                      instanceCount = instance_count, 
                                                      embargoYears = source.get("embargo")
                                                      ) 
                except ValidationError as e:
                    logger.error(f"ValidationError: metadataGetSourceByType SourceInfoListItem: {e.json()}")
                    #logger.error(e.json())
                    err = 1
    
            except Exception as e:
                logger.error(f"MetadataGetSourceInfoError: {e}")
                err = 1
    
            if err == 0:
                source_info_listitems.append(item)
    else:
        # book series workaround...any code not enabled in the database, i.e., SE/GW should not ever be getting do
        if src_code in cached_metadata["BOOK_CODES_ALL"]:
            #print (f"Book Source code workaround--not enabled in DB: {src_code}")
            try:
                item = models.SourceInfoListItem( sourceType = "book series",
                                                  PEPCode = src_code,
                                                  #srcTitle = title,  # v1 Deprecated for future
                                                  #bookCode = book_code,
                                                  #abbrev = source.get("bibabbrev"),
                                                  bannerURL = f"{localsecrets.APIURL}/{opasConfig.IMAGES}/banner{src_code}Logo.gif",
                                                  #language = source.get("language"),
                                                  #ISSN = source.get("ISSN"),
                                                  #ISBN10 = source.get("ISBN-10"),
                                                  #ISBN13 = source.get("ISBN-13"),
                                                  #yearFirst = start_year,
                                                  #yearLast = end_year,
                                                  #instanceCount = instance_count, 
                                                  #embargoYears = source.get("embargo")
                                                  ) 
    
                source_info_listitems.append(item)
                response_info.count = 1
    
            except ValidationError as e:
                logger.error(f"MetadataGetSourceValidationError: {e.json()}")
                #logger.error(e.json())
                err = 1
        
        elif src_code in cached_metadata["VIDEOSTREAM_CODES_ALL"]:  # workaround for getting codes 
            try:
                item = models.SourceInfoListItem( sourceType = "videostream series",
                                                  PEPCode = src_code,
                                                  #srcTitle = title,  # v1 Deprecated for future
                                                  #bookCode = book_code,
                                                  #abbrev = source.get("bibabbrev"),
                                                  bannerURL = f"{localsecrets.APIURL}/{opasConfig.IMAGES}/banner{src_code}Logo.gif",
                                                  #language = source.get("language"),
                                                  #ISSN = source.get("ISSN"),
                                                  #ISBN10 = source.get("ISBN-10"),
                                                  #ISBN13 = source.get("ISBN-13"),
                                                  #yearFirst = start_year,
                                                  #yearLast = end_year,
                                                  #instanceCount = instance_count, 
                                                  #embargoYears = source.get("embargo")
                                                  ) 
    
                source_info_listitems.append(item)
                response_info.count = 1
    
            except ValidationError as e:
                logger.error(f"MetadataGetSourceValidationError: Video Validation Error {e.json()}")
                #logger.error(e.json())
                err = 1

    try:
        source_info_struct = models.SourceInfoStruct( responseInfo = response_info, 
                                                      responseSet = source_info_listitems
                                                      )
    except ValidationError as e:
        logger.error(f"MetadataGetSourceValidationError: models.SourceInfoStruct {e.json()}")
        #logger.error(e.json())        

    try:
        source_info_list = models.SourceInfoList(sourceInfo = source_info_struct)
    except ValidationError as e:
        logger.error("MetadataGetSourceValidationError:")
        logger.error(e.json())        

    ret_val = source_info_list

    return ret_val

#-----------------------------------------------------------------------------
def documents_get_abstracts(document_id,
                            ret_format="TEXTONLY",
                            authenticated=False,
                            similar_count=0,
                            sort: str=None, 
                            limit=opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS,
                            req_url: str=None, 
                            offset=0,
                            session_info=None,
                            request=None
                            ):
    """
    Returns an abstract or summary for the specified document
    If part of a documentID is supplied, multiple abstracts will be returned.

    The endpoint reminds me that we should be using documentID instead of "art" for article perhaps.
      Not thrilled about the prospect of changing it, but probably the right thing to do.

    >>> results = documents_get_abstracts("IJP.075")
    >>> results.documents.responseInfo.count == 15
    True
    >>> results = documents_get_abstracts("AIM.038.0279A")  # no abstract on this one
    >>> results.documents.responseInfo.count == 1
    True
    >>> results = documents_get_abstracts("AIM.040.0311A")
    >>> results.documents.responseInfo.count == 1
    True

    """
    ret_val = None

    if document_id is not None:
        # new document ID object provides precision and case normalization
        if re.search("[\?\*]", document_id) is None:
            # fix it, or add a wildcard if needed
            document_id_obj = opasgenlib.DocumentID(document_id)
            document_id = document_id_obj.document_id
            if document_id is None:
                document_id = document_id_obj.jrnlvol_id
                if document_id is None:
                    document_id = document_id_obj.journal_code          
        #else:
            #document_id = document_id # leave it alone, it has wildcards

        if sort is None:
            sort="art_citeas_xml asc"

        document_list, ret_status = search_text( query="art_id:%s*" % (document_id),
                                                 full_text_requested=False,
                                                 abstract_requested=True, 
                                                 format_requested = ret_format,
                                                 #authenticated=authenticated,
                                                 similar_count=similar_count,
                                                 facet_fields=opasConfig.DOCUMENT_VIEW_FACET_LIST,
                                                 facet_mincount=1,
                                                 sort=sort, 
                                                 limit=limit,   
                                                 offset=offset, 
                                                 req_url=req_url,
                                                 session_info=session_info,
                                                 request=request
                                                 )

        try:
            if opasFileSupport.file_exists(document_id=document_id, 
                                           year=document_list.documentList.responseSet[0].year,
                                           ext=localsecrets.PDF_ORIGINALS_EXTENSION, 
                                           path=localsecrets.PDF_ORIGINALS_PATH):
                document_list.documentList.responseSet[0].pdfOriginalAvailable = True
            else:
                document_list.documentList.responseSet[0].pdfOriginalAvailable = False
        except Exception as e:
            logger.debug(f"pdfOriginalAvailable check error: {e}")
            #default is false
            #document_list.documentList.responseSet[0].pdfOriginalAvailable = False          
        
        if not isinstance(document_list, models.ErrorReturn):
            documents = models.Documents(documents = document_list.documentList)
        else:
            err = document_list
            logger.error(f"DocumentGetAbstractError: {err.error_description}")
            raise HTTPException(
                status_code=err.error,
                detail=err.error_description
            )                
        
        ret_val = documents

    return ret_val

#-----------------------------------------------------------------------------
def documents_get_document_from_file(document_id,
                                     ret_format="XML",
                                     req_url:str=None,
                                     fullfilename=None, 
                                     authenticated=True,
                                     session_info=None, 
                                     option_flags=0,
                                     request=None
                                     ):
    """
    Load an article into the document_list_item directly from a file in order to return
      **archived** articles (such as used by IJPOPen) which have been removed from Solr
    """
    caller_name = "documents_get_document_from_file"
    ret_val = None
    # document_list = None
    
    # new document ID object provides precision and case normalization
    document_id_obj = opasgenlib.DocumentID(document_id)
    document_id = document_id_obj.document_id
    filenamebase = os.path.basename(fullfilename)
    fs = opasFileSupport.FlexFileSystem(key=localsecrets.S3_KEY, secret=localsecrets.S3_SECRET, root=localsecrets.FILESYSTEM_ROOT)
    #temp for testing
    #if not fs.exists(fullfilename, localsecrets.FILESYSTEM_ROOT):
        #fullfilename = fs.find(fullfilename, path_root="X:\AWS_S3\AWS PEP-Web-Live-Data\_PEPTests")
    
    fileXMLContents, input_fileinfo = fs.get_file_contents(fullfilename)
    parsed_xml = etree.fromstring(opasxmllib.remove_encoding_string(fileXMLContents), parser)
    title = opasxmllib.xml_xpath_return_textsingleton(parsed_xml, '//arttitle', default_return=None)
    abstract = opasxmllib.xml_xpath_return_textsingleton(parsed_xml, '//abs', default_return=None)
    pgrg = opasxmllib.xml_xpath_return_textsingleton(parsed_xml, '//artpgrg', default_return=None)
    # save common document (article) field values into artInfo instance for both databases
    #sourceDB = opasProductLib.SourceInfoDB()
    artInfo = opasArticleIDSupport.ArticleInfo(parsed_xml=parsed_xml, art_id=document_id, filename_base=filenamebase, fullfilename=fullfilename, logger=logger)
    generic_document_id = document_id[:-1] + "?"
    # query the latest version of this document to get the documentListItem info
    #result = documents_get_document(generic_document_id, session_info)
    # have to call abstracts to get whatever matching root document is in solr
    result = documents_get_abstracts(generic_document_id, 
                                     ret_format=ret_format,
                                     req_url=req_url, 
                                     session_info=session_info,
                                     request=request
                                     )

    if result.documents.responseInfo.count == 1:
        # change fields as needed
        document_list_item = result.documents.responseSet[0]
        document_list_item.documentID = document_id
        document_list_item.title = title
        document_list_item.abstract = abstract
        document_list_item.pgRg = pgrg
        document_list_item.pgStart = artInfo.art_pgstart
        document_list_item.pgEnd = artInfo.art_pgend
        document_list_item.stat = None
        document_list_item.documentRef = artInfo.art_citeas_text 
        # for reference, art_citeas_xml is both legal xml and html
        document_list_item.documentRefXML = artInfo.art_citeas_xml 
        document_list_item.documentRefHTML = artInfo.art_citeas_xml
        document_list_item.documentMetaXML = opasxmllib.xml_xpath_return_textsingleton(parsed_xml, '//meta', default_return=None)
        
        if result.documents.responseSet[0].accessChecked and result.documents.responseSet[0].accessLimited == False:
            document_list_item.document = fileXMLContents
            # replace facet_counts with new dict
            try:
                matched_word_count, term_dict = glossEngine.getGlossaryLists(fileXMLContents, art_id=document_id, verbose=False)
                result.documents.responseInfo.facetCounts = {"facet_fields": {"glossary_group_terms": term_dict}}
            except Exception as e:
                status_message = f"{caller_name}: {e}"
                logger.error(status_message)
            
        ret_val = result

    return ret_val

#-----------------------------------------------------------------------------
def documents_get_document(document_id,
                           solr_query_spec=None,
                           ret_format="XML",
                           similar_count=0,
                           #file_classification=None,
                           req_url:str=None, 
                           page_offset=None,
                           page_limit=None,
                           page=None, 
                           authenticated=True,
                           session_info=None, 
                           option_flags=0,
                           request=None
                           ):
    """
    For non-authenticated users, this endpoint returns only Document summary information (summary/abstract)
    For authenticated users, it returns with the document itself

    >> resp = documents_get_document("AIM.038.0279A", ret_format="html") 

    >> resp = documents_get_document("AIM.038.0279A") 

    >> resp = documents_get_document("AIM.040.0311A")


    """
    caller_name = "documents_get_document"
    ret_val = None
    document_list = None
    search_context = None
    
    ext = localsecrets.PDF_ORIGINALS_EXTENSION #  PDF originals extension
    # search_text_qs handles the authentication verification

    # new document ID object provides precision and case normalization
    document_id_obj = opasgenlib.DocumentID(document_id)
    document_id = document_id_obj.document_id

    #m = re.match("(?P<docid>(?P<scode>[A-Z]+)\.(?P<svol>[0-9]{3,3})\.(?P<spage>[0-9]{4,4}[A-Z]))(\.P0{0,3}(?P<pagejump>[0-9]{1,4}))?", document_id)
    #if m is not None:
        #if m.group("pagejump") is not None:
            #document_id = m.group("docid")
            ## only if they haven't directly specified page
            #if page is None:
                #page = m.group("pagejump")
    # just to be sure
    query = "*:*"
    if solr_query_spec is not None:
        solr_query_params = solr_query_spec.solrQuery
        # repeat the query that the user had done when retrieving the document
        query = f"{solr_query_params.searchQ}"
        if query == "":
            query = "*:*"
            search_context = None
        else:
            search_context = query
        filterQ = f"art_id:{document_id} && {solr_query_params.filterQ}"
        # solrParams = solr_query_params.dict() 
    else:
        query = f"art_id:{document_id}"  # TODO - Set this to accept with or without issue letter if one is supplied (seen in error logs - nrs)
        filterQ = None
        #solrMax = None
        # solrParams = None

    solr_query_spec = \
            opasQueryHelper.parse_to_query_spec(query = query,
                                                filter_query = filterQ,
                                                similar_count=similar_count, 
                                                full_text_requested=True,
                                                abstract_requested=True,
                                                format_requested=ret_format,
                                                #return_field_set=return_field_set, 
                                                #summary_fields = summary_fields,  # deprecate?
                                                highlight_fields = "text_xml, art_title",
                                                facet_fields=opasConfig.DOCUMENT_VIEW_FACET_LIST,
                                                facet_mincount=1,
                                                extra_context_len=opasConfig.SOLR_HIGHLIGHT_RETURN_FRAGMENT_SIZE, 
                                                limit = 1,
                                                page_offset = page_offset,
                                                page_limit = page_limit,
                                                page = page,
                                                req_url = req_url, 
                                                option_flags=option_flags # dictionary of return options
                                                )

    document_list, ret_status = opasPySolrLib.search_text_qs(solr_query_spec,
                                                             session_info=session_info,
                                                             get_full_text=True, 
                                                             request=request,
                                                             caller_name=caller_name
                                                             )

    try:
        matches = document_list.documentList.responseInfo.count
        if matches > 0:
            # get the first document item only
            document_list_item = document_list.documentList.responseSet[0]
            # is user authorized?
            if document_list_item.accessChecked == False or document_list_item.accessLimited == True or document_list_item.accessLimited is None:
                document_list_item.document = document_list_item.abstract
            else:
                if opasFileSupport.file_exists(document_id=document_list_item.documentID, 
                                               year=document_list_item.year,
                                               ext=ext, 
                                               path=localsecrets.PDF_ORIGINALS_PATH):
                    document_list_item.pdfOriginalAvailable = True
                else:
                    document_list_item.pdfOriginalAvailable = False

        elif search_context is not None:
            # failed to retrieve, get it without the search qualifier from last time.
            solr_query_spec.solrQuery.searchQ = "*:*"
            document_list, ret_status = opasPySolrLib.search_text_qs(solr_query_spec,
                                                                     session_info=session_info,
                                                                     request=request,
                                                                     caller_name=caller_name
                                                                     )
            matches = document_list.documentList.responseInfo.count
            if matches > 0:
                # get the first document item only
                document_list_item = document_list.documentList.responseSet[0]
                # is user authorized?
                if document_list_item.accessChecked == False or document_list_item.accessLimited == True or document_list_item.accessLimited is None:
                    document_list_item.document = document_list_item.abstract
                else:
                    if opasFileSupport.file_exists(document_id=document_list_item.documentID, 
                                                   year=document_list_item.year,
                                                   ext=ext, 
                                                   path=localsecrets.PDF_ORIGINALS_PATH):
                        document_list_item.pdfOriginalAvailable = True
                    else:
                        document_list_item.pdfOriginalAvailable = False
                        
                document_list.documentList.responseSet[0].term = f'SearchHits({search_context})'
                document_list.documentList.responseSet[0].termCount = 0
            else:
                logger.error(f"{caller_name}: No matches for: {solr_query_spec.solrQuery}") # added 2022-02-06 to help diagnose document not found errors in logs

    except Exception as e:
        logger.error("get_document exception: No matches or another error: %s", e)
        # return None
    else:
        if page_limit is None:
            page_limit = 100 # TODO - Check this (was 0 before)
        if page_offset is None:
            page_offset = 0
            
        response_info = document_list.documentList.responseInfo
        response_info.page=page
        response_info.limit = page_limit # use for page_limit, rather than the usual limit
        response_info.offset = page_offset # use for page_offset, rather than the usual limit
        # response_info.solrParams = solrParams
        # response_info.request = req_url
        
        if matches >= 1:
            # experimental options: uses option_flags to turn on
            if option_flags is not None:
                if option_flags & opasConfig.OPTION_2_RETURN_TRANSLATION_SET:
                    # get document translations of the first document (note this also includes the original)
                    if document_list_item.origrx is not None:
                        translationSet, count = opasPySolrLib.quick_docmeta_docsearch(q_str=f"art_origrx:{document_list_item.origrx} OR art_id:{document_list_item.origrx}", req_url=req_url)
                        if translationSet is not None:
                            # set translationSet to a list, just like 
                            document_list_item.translationSet = translationSet
            
            # is user authorized?
            if document_list.documentList.responseSet[0].accessLimited or document_list.documentList.responseSet[0].accessChecked == False or document_list.documentList.responseSet[0].accessLimited is None:
                document_list.documentList.responseSet[0].document = document_list.documentList.responseSet[0].abstract
            else: # yes
                # replace facet_counts with new dict
                try:
                    pepxml = document_list.documentList.responseSet[0].document
                    matched_word_count, term_dict = glossEngine.getGlossaryLists(pepxml, art_id=document_id, verbose=False)
                    response_info.facetCounts = {"facet_fields": {"glossary_group_terms": term_dict}}
                except Exception as e:
                    logger.error(f"{caller_name}: Error replacing term_dict {e}")
                
            document_list_struct = models.DocumentListStruct( responseInfo = response_info, 
                                                              responseSet = [document_list_item]
                                                              )

            documents = models.Documents(documents = document_list_struct)

            ret_val = documents

    return ret_val

#-----------------------------------------------------------------------------
def documents_get_concordance_paras(para_lang_id,
                                    para_lang_rx=None, 
                                    solr_query_spec=None,
                                    ret_format="XML",
                                    req_url:str=None, 
                                    session_info=None,
                                    request=None
                                    ):
    """
    For non-authenticated users, this endpoint returns only Document summary information (summary/abstract)
    For authenticated users, it returns with the document itself

    >> resp = documents_get_concordance_paras("SEXixa5", ret_format="html") 

    >> resp = documents_get_concordance_paras("SEXixa5") 


    """
    caller_name = "documents_get_concordance_paras"
    ret_val = {}
    document_list = None
    if para_lang_rx is not None:
        paraLangFilterQ = f'({re.sub(",", " || ", para_lang_rx)})'
    else:
        paraLangFilterQ = f"{para_lang_id}"
        
    query = "*:*"
    filterQ = f"para_lgrid:{paraLangFilterQ}"
    try:
        solr_query_spec = \
                opasQueryHelper.parse_to_query_spec(solr_query_spec=solr_query_spec, 
                                                    query=query,
                                                    filter_query=filterQ,
                                                    full_text_requested=True,
                                                    format_requested=ret_format,
                                                    return_field_set="CONCORDANCE", 
                                                    highlight_fields="para",
                                                    extra_context_len=opasConfig.SOLR_HIGHLIGHT_RETURN_FRAGMENT_SIZE, 
                                                    #limit=10,
                                                    req_url=req_url
                                                    )

        document_list, ret_status = search_text_qs(solr_query_spec,
                                                   session_info=session_info,
                                                   request=request,
                                                   get_full_text=False, 
                                                   get_child_text_only=True, 
                                                   caller_name=caller_name
                                                   )

        #matches = document_list.documentList.responseInfo.count
        #if matches >= 1:
            #for count in range(0, matches-1):
                #document_list_item = document_list.documentList.responseSet[count]
                ## is user authorized?
                #if document_list.documentList.responseSet[count].accessLimited and 0:
                    ## Should we require it's authorized?
                    #document_list.documentList.responseSet[count].document = document_list.documentList.responseSet[count].abstract
                ##else:
                    ### All set
        #else:
            #logger.info(f"get_para_trans: No matches: {filterQ}")
    except Exception as e:
        logger.error(f"get_para_trans: No matches or error: {e}")
    else:
        document_list_struct = models.DocumentListStruct( responseInfo = document_list.documentList.responseInfo, 
                                                          responseSet = document_list.documentList.responseSet
                                                          )

        documents = models.Documents(documents = document_list_struct)

        ret_val = documents

    return ret_val

#================================================================================================================
def save_opas_session_cookie(request: Request, response: Response, session_id):
    ret_val = False
    already_set = False
    if session_id is not None and session_id is not 'None':
        already_set = False
        try:
            opasSession = [x for x in response.raw_headers[0] if b"opasSessionID" in x]
            already_set = b"opasSessionID" in opasSession[0][0:13]
        except Exception as e:
            logger.info(f"Exception, but Ok, opasSessionID cookie not in response {e}")

    if already_set == False and session_id is not None:
        try:
            logger.debug("Saved OpasSessionID Cookie")
            response.set_cookie(
                opasConfig.OPASSESSIONID,
                value=f"{session_id}",
                domain=localsecrets.COOKIE_DOMAIN
            )
            ret_val = True
        except Exception as e:
            logger.error(f"Can't save opas session-id cookie: {e}")
            ret_val = False
            
    return ret_val
    
# -------------------------------------------------------------------------------------------------------
# run it (tests)!

if __name__ == "__main__":
    sys.path.append('./config') 

    print (40*"*", "opasAPISupportLib Tests", 40*"*")
    print ("Running in Python %s" % sys.version_info[0])
    logger = logging.getLogger(__name__)
    # extra logging for standalong mode 
    logger.setLevel(logging.WARN)
    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.WARN)
    # create formatter
    formatter = logging.Formatter('%(asctime)s %(name)s %(lineno)d - %(levelname)s %(message)s')    
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)

    import doctest
    doctest.testmod(optionflags=doctest.ELLIPSIS|doctest.NORMALIZE_WHITESPACE)
    print ("All tests complete!")
    print ("Fini")
