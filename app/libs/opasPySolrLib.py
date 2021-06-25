#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=C0321,C0103,C0301,E1101,C0303,E1004,C0330,R0915,R0914,W0703,C0326

"""
opasPySolrLib

This library is meant to support query to Solr

"""
__author__      = "Neil R. Shapiro"
__copyright__   = "Copyright 2020-2021, Psychoanalytic Electronic Publishing"
__license__     = "Apache 2.0"
__version__     = "2020.1118.1"
__status__      = "Development"

import re
import os
import tempfile
import logging
logger = logging.getLogger(__name__)
import time
from fastapi import HTTPException
from errorMessages import *
from datetime import datetime
# import datetime as dtime
# from operator import itemgetter

import sys
sys.path.append('./solrpy')
from xml.sax import SAXParseException
# import lxml

import localsecrets
from localsecrets import TIME_FORMAT_STR

# from localsecrets import BASEURL, SOLRURL, SOLRUSER, SOLRPW, DEBUG_DOCUMENTS, SOLR_DEBUG, CONFIG, COOKIE_DOMAIN  
import starlette.status as httpCodes
from configLib.opasCoreConfig import solr_docs2, solr_authors2, solr_gloss2
import opasConfig 
from opasConfig import KEY_SEARCH_FIELD, KEY_SEARCH_SMARTSEARCH, KEY_SEARCH_VALUE
from configLib.opasCoreConfig import EXTENDED_CORES
from stdMessageLib import COPYRIGHT_PAGE_HTML  # copyright page text to be inserted in ePubs and PDFs

import models
import opasCentralDBLib
import schemaMap
import opasGenSupportLib as opasgenlib
import opasXMLHelper as opasxmllib
import opasDocPermissions as opasDocPerm
import smartsearch
import opasQueryHelper
from xhtml2pdf import pisa             # for HTML 2 PDF conversion

import pysolr
# still using a function in solpy
import solrpy as solr

# logging.getLogger('pysolr').setLevel(logging.INFO)
sourceDB = opasCentralDBLib.SourceInfoDB()
ocd = opasCentralDBLib.opasCentralDB()
pat_prefix_amps = re.compile("^\s*&& ")

rx_nuisance_words = f"""{opasConfig.HITMARKERSTART}(?P<word>i\.e|e\.g|a|am|an|are|as|at|be|because|been|before|but|by|can|cannot|could|did|do|does|doing|down|each|for|from|further|had|has|have|having|he|her|here|hers
|herself|him|himself|his|how|i|if|in|into|is|it|its|itself|me|more|most|my|myself|no|nor|not|of|off|on|once|only|or|other|ought
|our|ours|ourselves|out|over|own|same|she|should|so|some|such|than|that|the|their|theirs|them|then|there|these|they|this|those|to|too|under|until|up|very
|was|we|were|what|when|where|which|while|who|whom|why|with|would|you|your|yours|yourself|yourselves){opasConfig.HITMARKEREND}"""

rcx_remove_nuisance_words = re.compile(rx_nuisance_words, flags=re.IGNORECASE)

def pysolrerror_processing(e):
    error = "pySolr.SolrError"
    error_num = 400
    error_description=f"There's an error in your input (no reason supplied)"
    ret_val = models.ErrorReturn(httpcode=400, error=error, error_description=error_description)

    try:
        if e is None:
            pass # take defaults
        elif e.args is not None:
            # defaults, before trying to decode error
            error = 400
            try:
                err = e.args
                error_set = err[0].split(":", 1)
                error = error_set[0]
                error = error.replace('Solr ', 'Search engine ')
                ret_val.error = error_set[1]
                ret_val.error_description = error_description.strip(" []")
                m = re.search("HTTP (?P<err>[0-9]{3,3})", error)
                if m is not None:
                    http_error = m.group("err")
                    http_error_num = int(http_error)
                    ret_val.httpcode = http_error_num
            except Exception as e:
                logger.error(f"PySolrError: Exception {e} Parsing error {e.args}")
            else:
                ret_val = models.ErrorReturn(httpcode=http_error_num, error=error, error_description=error_description)
    except Exception as e2:
        logger.error(f"PySolrError: {e} Processing exception {e2}")

    return ret_val    

#-----------------------------------------------------------------------------
def remove_nuisance_word_hits(result_str):
    """
    >>> a = '#@@@the@@@# cat #@@@in@@@# #@@@the@@@# hat #@@@is@@@# #@@@so@@@# smart'
    >>> remove_nuisance_word_hits(a)
    
    """
    ret_val = rcx_remove_nuisance_words.sub("\g<word>", result_str)
    return ret_val 
    
#-----------------------------------------------------------------------------
def list_all_matches(search_result):
    """
    Not currently used.
    """
    # makes it easier to see matches in a large result
    ret_val = re.findall(f"{opasConfig.HITMARKERSTART}.*{opasConfig.HITMARKEREND}", search_result)
    return ret_val

#-----------------------------------------------------------------------------
def list_all_matches_with_loc(search_result):
    # makes it easier to see matches in a large result
    ret_val = []
    for m in re.compile(f"{opasConfig.HITMARKERSTART}.*{opasConfig.HITMARKEREND}").finditer(search_result):
        start_char = max(m.start()-20, 0)
        end_char = m.end()+30
        ret_val.append(search_result[start_char:end_char])

    return ret_val

#-----------------------------------------------------------------------------
def numbered_anchors(matchobj):
    """
    Called by re.sub on replacing anchor placeholders for HTML output.  This allows them to be numbered as they are replaced.
    """
    global count_anchors
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
        return f"{opasConfig.HITMARKEREND_OUTPUTHTML}{JUMPTONEXTHIT}"

    else:
        return matchobj.group(0)

#-----------------------------------------------------------------------------
def cleanNullTerms(dictionary):
    # one liner comprehension to clean Nones from dict:
    # from https://medium.com/better-programming/how-to-remove-null-none-values-from-a-dictionary-in-python-1bedf1aab5e4
    return {
        k:v
        for k, v in dictionary.items()
        if v is not None
    }

#-----------------------------------------------------------------------------
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
         
#----------------------------------------------------------------------------
def facet_processing(facets):
    """
    Convert PySolr facet return to regular dictionary form
    
    """
   
    facet_fields_dict = {}
    for facet_field, facet_data in facets.items():
        facet_pairs = zip(facet_data[::2], facet_data[1::2])
        facet_dict = {}
        for key, value in facet_pairs:
            facet_dict[key] = value
        facet_fields_dict[facet_field] = facet_dict
    
    return facet_fields_dict
        
#-----------------------------------------------------------------------------
def get_base_article_info_by_id(art_id):
    """
    Not currently used.
    """

    documentList, ret_status = search_text(query=f"art_id:{art_id}", 
                                           limit=1,
                                           abstract_requested=False,
                                           full_text_requested=False
                                           )

    try:
        ret_val = documentListItem = documentList.documentList.responseSet[0]
    except Exception as e:
        logger.warning(f"Error getting article {art_id} by id: {e}")
        ret_val = None
        
    return ret_val

#-----------------------------------------------------------------------------
def get_translated_article_info_by_origrx_id(art_id):
    """
    Not currently used.
    """
    
    documentList, ret_status = search_text(query=f"art_origrx:{art_id}", 
                                           limit=10,
                                           abstract_requested=False,
                                           full_text_requested=False
                                           )

    try:
        ret_val = documentListItem = documentList.documentList.responseSet[0]
    except Exception as e:
        logger.warning(f"Error getting article {art_id} by id: {e}")
        ret_val = None
        
    return ret_val

#-----------------------------------------------------------------------------
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
def authors_get_author_info(author_partial,
                            req_url:str=None, 
                            limit=opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, offset=0, author_order="index"):
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
        >>> resp.authorIndex.responseInfo.count >= 7
        True
        >>> resp = authors_get_author_info("Levins", limit=5)
        >>> resp.authorIndex.responseInfo.count
        5
    """
    ret_val = {}
    method = 1

    if method == 1:
        query = "art_author_id:/%s.*/" % (author_partial)
        args = {
        "fl": "authors, art_author_id",
        "facet": "on",
        "facet.field": "art_author_id",
        "facet.sort": author_order + " asc",
        "facet.prefix" : "%s" % author_partial.lower(),
        "facet.limit": limit,
        "facet.mincount": 1,
        "facet.offset": offset,
        "rows": 1,
        }
        args = cleanNullTerms(args)
        
        results = solr_authors2.search( q=query, **args)

        response_info = models.ResponseInfo( limit=limit,
                                             offset=offset,
                                             listType="authorindex",
                                             scopeQuery=[f"{author_partial}"],
                                             solrParams= None, #results._params,
                                             request=f"{req_url}",
                                             timeStamp=datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)
                                             )

        author_index_items = []

        facets = results.facets["facet_fields"]["art_author_id"]
        facet_pairs = zip(facets[::2], facets[1::2])
        for key, value in facet_pairs:
            if value > 0:
                item = models.AuthorIndexItem(authorID = key, 
                                              publicationsURL = "/v1/Authors/Publications/{}/".format(key),
                                              publicationsCount = value,
                                              ) 
                author_index_items.append(item)
                logger.debug ("authorsGetAuthorInfo", item)

    response_info.count = len(author_index_items)
    response_info.fullCountComplete = limit >= response_info.count

    author_index_struct = models.AuthorIndexStruct( responseInfo = response_info, 
                                                    responseSet = author_index_items
                                                    )

    author_index = models.AuthorIndex(authorIndex = author_index_struct)

    ret_val = author_index
    return ret_val
#-----------------------------------------------------------------------------
def authors_get_author_publications(author_partial,
                                    req_url:str=None, 
                                    limit=opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS,
                                    offset=0):
    """
    Returns a list of publications (per authors partial name), and the number of articles by that author.

    >>> ret_val =authors_get_author_publications(author_partial="Tuck") # doctest: +ELLIPSIS
    >>> type(ret_val)
    <class 'models.AuthorPubList'>
    >>> print (f"{ret_val}"[0:68])
    authorPubList=AuthorPubListStruct(responseInfo=ResponseInfo(count=15
    >>> ret_val=authors_get_author_publications(author_partial="Fonag")
    >>> print (f"{ret_val}"[0:68])
    authorPubList=AuthorPubListStruct(responseInfo=ResponseInfo(count=15
    >>> ret_val=authors_get_author_publications(author_partial="Levinson, Nadine A.")
    >>> print (f"{ret_val}"[0:67])
    authorPubList=AuthorPubListStruct(responseInfo=ResponseInfo(count=8
    """
    ret_val = {}
    query = "art_author_id:/{}/".format(author_partial)
    aut_fields = "art_author_id, art_year_int, art_id, art_auth_pos_int, art_author_role, art_author_bio, art_citeas_xml"
    # wildcard in case nothing found for #1
    args = {
    "fl": aut_fields,
    "rows": limit,
    "start": offset,
    "sort": "art_author_id asc, art_year_int asc"
    }
    args = cleanNullTerms(args)
    results = solr_authors2.search( q=query, **args)
    logger.debug("Author Publications: Number found: %s", results.hits)

    if results.hits == 0:
        query = "art_author_id:/{}[ ]?.*/".format(author_partial)
        logger.debug("Author Publications: trying again - %s", query)
        args = cleanNullTerms(args)
        results = solr_authors2.search( q=query, **args)

        logger.debug("Author Publications: Number found: %s", results.hits)
        if results.hits == 0:
            query = "art_author_id:/(.*[ ])?{}[ ]?.*/".format(author_partial)
            logger.debug("Author Publications: trying again - %s", query)
            results = solr_authors2.search( q=query, **args)

    response_info = models.ResponseInfo( count = len(results.docs),
                                         fullCount = results.hits,
                                         limit = limit,
                                         offset = offset,
                                         listType="authorpublist",
                                         scopeQuery=[query],
                                         solrParams = None, #results._params,
                                         fullCountComplete = limit >= results.hits,
                                         request=f"{req_url}",
                                         timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)                     
                                         )

    author_pub_list_items = []
    for result in results.docs:
        citeas = result.get("art_citeas_xml", None)
        citeas = opasQueryHelper.force_string_return_from_various_return_types(citeas)

        item = models.AuthorPubListItem( authorID = result.get("art_author_id", None), 
                                         documentID = result.get("art_id", None),
                                         documentRefHTML = citeas,
                                         documentRef = opasxmllib.xml_elem_or_str_to_text(citeas, default_return=""),
                                         documentURL = opasConfig.API_URL_DOCUMENTURL + result.get("art_id", None),
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
def check_solr_docs_connection():
    """
    Queries the solrDocs core (i.e., pepwebdocs) to see if the server is up and running.
    Solr also supports a ping, at the corename + "/ping", but that doesn't work through pysolr as far as I can tell,
    so it was more straightforward to just query the Core. 

    Note that this only checks one core, since it's only checking if the Solr server is running.

    >>> check_solr_docs_connection()
    True

    """
    ret_val = False

    if solr_docs2 is not None:
        args = {
                   'fl':"art_id, art_vol, art_year",
                   'rows':5,
               }
    
        query = f"art_id:APA.009.0331A"
        try:
            results = solr_docs2.search(query, **args)
        except Exception as e:
            logger.error(f"SolrConnectionError: {e}")
        else:
            if len(results.docs) > 0:
                ret_val = True
            #else
                # ret_val = False (default)

        return ret_val

#-----------------------------------------------------------------------------
def document_get_info(document_id, fields="art_id, art_sourcetype, art_year, file_classification, art_sourcecode, score"):
    """
    Gets key information about a single document for the specified fields.
    
    Currently unused (except in tests)

    Note: Careful about letting the caller specify fields in an endpoint,
       or they could get full-text

    >>> document_get_info('PEPGRANTVS.001.0003A', fields='art_id, art_year, file_classification, score')
    {'art_year': '2015', 'art_id': 'PEPGRANTVS.001.0003A', 'file_classification': 'free', 'score': ...}

    """
    ret_val = {}
    if solr_docs2 is not None:
        try:
            # PEP indexes field in upper case, but just in case caller sends lower case, convert.
            args = {
                       'fl':fields,
                       'rows':5,
                   }

            document_id = document_id.upper()
            query = f"art_id:{document_id}"
            logger.info(f"Solr Query: q={query}")
            results = solr_docs2.search(query, **args)
        except Exception as e:
            logger.error(f"SolrRetrievalError: {e}")
        else:
            if len(results.docs) == 0:
                return ret_val
            else:
                try:
                    ret_val = results.docs[0]
                except Exception as e:
                    logger.error(f"SolrResultError: {e}")

    return ret_val

#-----------------------------------------------------------------------------
def get_term_index(term_partial,
                   term_field="text",
                   core="docs",
                   req_url:str=None, 
                   limit=opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS,
                   offset=0,
                   start_at=None, # a particular term to start with
                   order="index"):
    """
    Returns a list of matching terms from an arbitrary field in the Solr database,
      either for core "authors" or "docs" per parameter core.
      
    IMPORTANT NOTE: : The offset is not supported by the PySolr solr library,
                      just simulated by this call, which does not save time or memory.
                      Instead, use start_at to specify the term to start with, using the start_at
    
    You can specify more than one field at once, using a tuple, e.g.,
          resp = get_term_index("love", term_field=('title','art_kwds_str'), limit=5)
          
    Typical fields (see docstring tests for examples):
       docs:
          art_kwds_str
          title
          text
          

    Args:
        term_partial (str): String prefix of author names to return.
        term_field (str): Where to look for term
        limit (int, optional): Paging mechanism, return is limited to this number of items.
        offset (int, optional): Paging mechanism, start with this item in limited return set, 0 is first item.
        order (str, optional): Return the list in this order, per Solr documentation.  Defaults to "index", which is the Solr determined indexing order.

    Returns:
        models.termIndex: Pydantic structure (dict) for termIndex.  See models.py

    Docstring Tests:    
        >>> resp = get_term_index("david", core="authors", term_field="authors", limit=15)
        >>> resp.termIndex.responseInfo.count > 0
        True
        >>> resp = get_term_index("love", term_field='text', limit=2)
        >>> resp.termIndex.responseInfo.count == 2
        True
        >>> resp = get_term_index("love", term_field='text', limit=20, offset=5)
        >>> resp.termIndex.responseSet[0].term
        'lovers'
        >>> resp = get_term_index("love", term_field='text', limit=20, start_at='lovet')
        >>> resp.termIndex.responseSet[0].term
        'lovett'
        >>> resp = get_term_index("love", term_field=('title','art_kwds_str'), limit=5)
        >>> resp.termIndex.responseInfo.count > 0
        True
        >>> resp = get_term_index("love", term_field="art_kwds_str", limit=5)
        >>> resp.termIndex.responseInfo.count > 0
        True
        >>> resp = get_term_index("love", term_field="title", limit=5)
        >>> resp.termIndex.responseInfo.count > 0
        True
        >>> resp = get_term_index("bion", term_field="art_kwds", limit=5)
        >>> resp.termIndex.responseInfo.count > 0
        True
        >>> resp = get_term_index("will", term_field="text", limit=5)
        >>> resp.termIndex.responseInfo.count > 0
        True
        >>> resp = get_term_index("david", term_field="art_authors_mast", limit=5)
        >>> resp.termIndex.responseInfo.count > 0
        True
        >>> resp = get_term_index("Inter.*", term_field="art_sourcetitlefull", limit=5)
        >>> resp.termIndex.responseInfo.count == 0
        True
        >>> resp = get_term_index("pand", limit=20)
        >>> resp.termIndex.responseInfo.count > 0
        True
        >>> resp = get_term_index("pand.*", limit=5)
        >>> resp.termIndex.responseInfo.count == 0
        True
    """
    ret_val = {}

    def load_from_term_field(results, term_field, limit=9999, offset=0):
        """
        to use one code base for loading from a list of term_fields in main function
        """
        term_index_items = []
        count = 0
        for key, value in results[term_field]: # tuples
            count += 1
            if offset != 0:
                if count < offset:
                    continue
    
            if value > 0:
                item = models.TermIndexItem(term = key, 
                                            field = term_field,
                                            termCount = value,
                                            ) 
                term_index_items.append(item)
                logger.debug ("TermIndexInfo", item)
        return term_index_items
        
    core_term_indexers = {
        "docs": solr_docs2,
        "authors": solr_authors2,
    }

    try:
        # select core
        term_index = core_term_indexers[core]
        args = {
            "terms.limit": limit,
            "terms.lower": start_at,
            "terms.lower.incl": 'true'
        }
        args = cleanNullTerms(args)
        # get index data
        results = term_index.suggest_terms(fields=term_field,
                                           prefix=term_partial.lower(),
                                           handler='terms',
                                           **args
                                          )
    except Exception as e:
        # error
        logger.error(f"TermIndexError: Specified core does not have a term index configured ({e})")

    else:
        response_info = models.ResponseInfo( limit=limit,
                                             offset=offset,
                                             listType="termindex",
                                             core=core, 
                                             scopeQuery=[f"Terms: {term_partial}"],
                                             #solrParams=None, 
                                             request=f"{req_url}",
                                             timeStamp=datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)
                                             )

        term_index_items = []
        if isinstance(term_field, (list, tuple)):
            for term_field_member in term_field:
                term_index_items.extend(load_from_term_field(results, term_field_member, limit=limit, offset=offset))
        else:
            term_index_items.extend(load_from_term_field(results, term_field, limit=limit, offset=offset))

        response_info.count = len(term_index_items)
        response_info.fullCountComplete = limit >= response_info.count

        term_index_struct = models.TermIndexStruct( responseInfo = response_info, 
                                                    responseSet = term_index_items
                                                    )

        term_index = models.TermIndex(termIndex = term_index_struct)

        ret_val = term_index

    return ret_val

def get_match_count(solrcore, query="*:*", qf="*:*"):
    """
    Return a count of matching records.
    """
    try:
        results = solrcore.search(query, fl="art_id, file_name, file_last_modified, timestamp", rows=1)
    except Exception as e:
        msg = f"SolrQueryError: {e}"
        logger.error(msg)
        # let me know whatever the logging is!
        if opasConfig.LOCAL_TRACE: print (msg)
    else:
        ret_val = results.hits
    
    return ret_val

def get_term_count_list(term, term_field="text_xml", limit=opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, offset=0, term_order="index", wildcard_match_limit=4):
    """
    Returns a list of matching terms, and the number of articles with that term.

    Args:
        term (str): Term or comma separated list of terms to return data on.
        term_field (str): the text field to look in
        limit (int, optional): Paging mechanism, return is limited to this number of items.
        offset (int, optional): Paging mechanism, start with this item in limited return set, 0 is first item.
        term_order (str, optional): Return the list in this order, per Solr documentation.  Defaults to "index", which is the Solr determined indexing order.

    Returns:
        list of dicts with term, count and status var ret_status

        return ret_val, ret_status

    Docstring Tests:    
        >>> resp = get_term_count_list("Jealousy")

    """
    ret_val = {}

    # make sure it's a list (if string, convert to list)
    if not isinstance(term, list):
        terms = term.split(', ')
    else:
        terms = term
    
    for term in terms:
        regexp = False
        if term[-1] == "*":
            org_term = term
            term = term[:-2]
            regexp = True

        termindex = get_term_index(term,
                                   term_field=term_field,
                                   offset=offset,
                                   limit=limit, 
                                   core="docs", 
                                   order=term_order)

        tindexlist = termindex.termIndex.responseSet

        # Note: we need an exact match here.
        if len(tindexlist) > 1:
            total = 0
            for n in tindexlist:
                if regexp:
                    if re.match(term, n.term, flags=re.I):
                        mt = f'{n.term}({org_term})'
                        ret_val[mt] = n.termCount
                        total += n.termCount
                else:
                    # find matching term
                    if n.term == term:
                        ret_val[term] = n.termCount
                        break

            if regexp:
                ret_val[f"Total({org_term})="] = total

    return ret_val

def search(query,
           summaryField,
           highlightFields='art_authors_xml, art_title_xml, text_xml',
           returnStartAt=0,
           returnLimit=10,
           sort="id ASC"):

    args = {
               # 'fl':summaryField,
               # 'q':'tuck*',
               'hl': 'true',
               'hl.fragsize': 125,
               'hl.fl':highlightFields,
               'start': returnStartAt, 
               'rows':returnLimit,
               "sort": f"{sort}", 
               'hl.simple.pre': opasConfig.HITMARKERSTART, # '<em>',
               'hl.simple.post': opasConfig.HITMARKEREND # '</em>'
           }

    #solr = pysolr.Solr('http://localhost:8983/solr/pepwebdocs', timeout=10)
    results = solr_docs2.search(query, **args)
    return results

#-----------------------------------------------------------------------------
def search_analysis( query_list, 
                     filter_query = None,
                     #more_like_these = False,
                     #query_analysis = False,
                     def_type = None,
                     # summaryFields="art_id, art_sourcecode, art_vol, art_year, art_iss, 
                     # art_iss_title, art_newsecnm, art_pgrg, art_title, art_author_id, art_citeas_xml", 
                     summary_fields="art_id",                    
                     # highlightFields='art_title_xml, abstract_xml, summaries_xml, art_authors_xml, text_xml', 
                     full_text_requested=False, 
                     user_logged_in=False,
                     req_url:str=None, 
                     limit=opasConfig.DEFAULT_MAX_KWIC_RETURNS,
                     api_version="v2"
                     ):
    """
    Analyze the search clauses in the query list
    """
    ret_val = {}
    return_item_list = []
    rowCount = 0
    term_field = None
    # save classes to neutral names so we can change between documentList and termIndex
    if 0: # api_version == "v1":
        RetItem = models.DocumentListItem
        RetStruct = models.DocumentListStruct
        RetList = models.DocumentList
    else:
        RetItem = models.TermIndexItem
        RetStruct = models.TermIndexStruct
        RetList = models.TermIndex

    for query_item in query_list:
        try:
            # remove outer parens added during query parsing
            query_item = opasQueryHelper.remove_outer_parens(query_item)
            logger.info(f"Solr Query: q={query_item}")
            args = {
                "defType": def_type,
                "q.op": "AND",
                "fl": summary_fields,
                "queryAnalysis": "true",
                "rows": 1,
                "start": 0,
            }
            args = cleanNullTerms(args)
            results = solr_docs2.search(query_item, **args)
            
        except Exception as e:
            # try to return an error message for now.
            return models.ErrorReturn(error="Search syntax error", error_description=f"There's an error in your input {e}")

        if "!parent" in query_item:
            term = query_item
            try:
                query_item = query_item.replace("parent_tag:(p_body || p_summaries || p_appxs)", "parent_tag:(doc)")
                query_parsed = re.split("(&&|\|\|) \(", query_item)
                del(query_parsed[0])
                for i in range(len(query_parsed)):
                    if query_parsed[i] in ["&&", "||"]:
                        continue
                    if "parent_tag" in query_parsed[i]:
                        m = re.match(".*parent_tag:\((?P<parent_tag>.*)\).*?(?P<field>[A-z_]+)\:\((?P<terms>.*)\)\)?\)?", query_parsed[i])
                        if m is not None:
                            query_parsed[i] = m.groupdict(default="")
                            query_parsed[i]['parent_tag'] = schemaMap.solrparent2user(query_parsed[i]['parent_tag'])
                            query_parsed[i]['terms'] = query_parsed[i]['terms'].strip("()")

            except Exception as e:
                pass

            by_parent = {}
            connector = ""
            for n in query_parsed:
                if n == '&&':
                    connector = " AND "
                    continue
                elif n == '||':
                    connector = " OR "
                    continue

                try:
                    by_parent[n["parent_tag"]] += f"{connector}{n['terms']}"
                except KeyError as e:
                    by_parent[n["parent_tag"]] = f"{n['terms']}"
                except Exception as e:
                    logger.warning(f"Error saving term clause: {e}")


            for key, value in by_parent.items():
                term = value
                term_field = f"in same paragraph in {key}"

        else:
            term = query_item
            if ":" in query_item:
                try:
                    term_field, term_value = query_item.split(":")
                except:
                    # pat = "((?P<parent>parent_tag\:\([a-z\s\(\)]\))\s+(AND|&&)\s+(?P<term>[A-z]+\:[\(\)A-Z]+))+"
                    # too complex
                    pass
                else:
                    term_value = opasQueryHelper.strip_outer_matching_chars(term_value, ")")
                    term = f"{term_value} (in {schemaMap.FIELD2USER_MAP.get(term_field, term_field)})"
            else:
                term = opasQueryHelper.strip_outer_matching_chars(term, ")")
                term = f"{query_item} (in text)"

        #logger.debug("Analysis: Term %s, matches %s", field_clause, results._numFound)
        item = RetItem(term = term, 
                       termCount = results.hits, 
                       field=term_field
                       )
        return_item_list.append(item)
        rowCount += 1

    response_info = models.ResponseInfo(count = rowCount,
                                        fullCount = rowCount,
                                        listType = "srclist",
                                        fullCountComplete = True,
                                        request=f"{req_url}",
                                        timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)
                                        )

    response_info.count = len(return_item_list)

    return_list_struct = RetStruct( responseInfo = response_info, 
                                    responseSet = return_item_list
                                    )
    if 0: # api_version == "v1":
        ret_val = RetList(documentList = return_list_struct)
    else:
        ret_val = RetList(termIndex = return_list_struct)


    return ret_val

#================================================================================================================
def search_text(query, 
                filter_query = None,
                query_debug = False,
                similar_count = 0,
                full_text_requested = False,
                abstract_requested = False, 
                format_requested = "HTML",
                def_type = None, # edisMax, disMax, or None
                # bring text_xml back in summary fields in case it's missing in highlights! I documented a case where this happens!
                return_field_set=None, 
                summary_fields=None, 
                highlight_fields = 'text_xml',
                facet_fields = None,
                facet_mincount = 1, 
                extra_context_len = None,
                highlightlimit = opasConfig.DEFAULT_MAX_KWIC_RETURNS,
                sort="score desc",
                limit=opasConfig.DEFAULT_LIMIT_FOR_SOLR_RETURNS, 
                offset = 0,
                page_offset = None,
                page_limit = None,
                page = None,
                req_url:str = None,
                core = None,
                #authenticated = None,
                session_info = None, 
                option_flags=0
                ):
    """
    Full-text search, via the Solr server api.
    
    Mapped now (8/2020) to use search_text_qs which works with the query spec directly,
      so this first calls and builds the querySpec, and then calls
      search_text_qs.

    Returns a pair of values: ret_val, ret_status.  The double return value is important in case the Solr server isn't running or it returns an HTTP error.  The 
        ret_val = a DocumentList model object
        ret_status = a status tuple, consisting of a HTTP status code and a status mesage. Default (HTTP_200_OK, "OK")

    >>> resp, status = search_text(query="art_title_xml:'ego identity'", limit=10, offset=0, full_text_requested=False)
    >>> resp.documentList.responseInfo.count >= 10
    True
    """

    solr_query_spec = \
            opasQueryHelper.parse_to_query_spec(query = query,
                                                filter_query = filter_query,
                                                similar_count=similar_count, 
                                                full_text_requested=full_text_requested,
                                                abstract_requested=abstract_requested,
                                                format_requested=format_requested,
                                                def_type = def_type, # edisMax, disMax, or None
                                                return_field_set=return_field_set, 
                                                summary_fields = summary_fields,  # deprecate?
                                                highlight_fields = highlight_fields,
                                                facet_fields = facet_fields,
                                                facet_mincount=facet_mincount,
                                                extra_context_len=extra_context_len, 
                                                highlightlimit=highlightlimit,
                                                sort = sort,
                                                limit = limit,
                                                offset = offset,
                                                page_offset = page_offset,
                                                page_limit = page_limit,
                                                page = page,
                                                core=core, 
                                                req_url = req_url,
                                                option_flags=option_flags
                                                )

    ret_val, ret_status = search_text_qs(solr_query_spec,
                                         limit=limit,
                                         offset=offset, 
                                         req_url=req_url, 
                                         #authenticated=authenticated,
                                         session_info=session_info
                                         )

    return ret_val, ret_status

#================================================================================================================
def search_text_qs(solr_query_spec: models.SolrQuerySpec,
                   extra_context_len=None,
                   req_url: str=None,
                   facet_limit=None,
                   facet_offset=None, 
                   limit=None,
                   offset=None,
                   mlt_count=None, # 0 turns off defaults for mlt, number overrides defaults, setting solr_query_spec is top priority
                   sort=None, 
                   session_info=None,
                   solr_core="pepwebdocs", 
                   request=None #pass around request object, needed for ip auth
                   ):
    """
    Full-text search, via the Solr server api.

    Returns a pair of values: ret_val, ret_status.  The double return value is important in case the Solr server isn't running or it returns an HTTP error.  The 
       ret_val = a DocumentList model object
       ret_status = a status tuple, consisting of a HTTP status code and a status mesage. Default (HTTP_200_OK, "OK")

    """
    ret_val = {}
    ret_status = (200, "OK") # default is like HTTP_200_OK
    # count_anchors = 0

    if 1: # just to allow folding
        if solr_query_spec.solrQueryOpts is None: # initialize a new model
            solr_query_spec.solrQueryOpts = models.SolrQueryOpts()
    
        if solr_query_spec.solrQuery is None: # initialize a new model
            solr_query_spec.solrQuery = models.SolrQuery()
    
        if extra_context_len is not None:
            solr_query_spec.solrQueryOpts.hlFragsize = extra_context_len
        elif solr_query_spec.solrQueryOpts.hlFragsize is None or solr_query_spec.solrQueryOpts.hlFragsize < opasConfig.DEFAULT_KWIC_CONTENT_LENGTH:
            solr_query_spec.solrQueryOpts.hlFragsize = opasConfig.DEFAULT_KWIC_CONTENT_LENGTH
        #else: # for debug only
            #print (f"Fragment Size: {solr_query_spec.solrQueryOpts.hlFragsize}")
    
        if solr_query_spec.solrQueryOpts.moreLikeThisCount > 0: # this (arg based) is the priority value
            mlt = "true"
            mlt_count = solr_query_spec.solrQueryOpts.moreLikeThisCount
            if solr_query_spec.solrQueryOpts.moreLikeThisFields is None:
                mlt_fl = opasConfig.DEFAULT_MORE_LIKE_THIS_FIELDS # Later: solr_query_spec.solrQueryOpts.moreLikeThisFields
            mlt_minwl = 4
        elif mlt_count is not None and mlt_count > 0:
            # mlt_count None means "don't care, use default", mlt_count > 0: override default
            mlt = "true"
            #  use default fields though
            mlt_fl = opasConfig.DEFAULT_MORE_LIKE_THIS_FIELDS # Later: solr_query_spec.solrQueryOpts.moreLikeThisFields
            mlt_minwl = 4
        elif (opasConfig.DEFAULT_MORE_LIKE_THIS_COUNT > 0 and mlt_count is None): # if caller doesn't care (None) and default is on
            # mlt_count None means "don't care, use default", mlt_count > 0: override default
            mlt = "true"
            if mlt_count is None: # otherwise it's more than 0 so overrides the default
                mlt_count = opasConfig.DEFAULT_MORE_LIKE_THIS_COUNT
            #  use default fields though
            mlt_fl = opasConfig.DEFAULT_MORE_LIKE_THIS_FIELDS # Later: solr_query_spec.solrQueryOpts.moreLikeThisFields
            mlt_minwl = 4
        else: # otherwise no MLT, mlt_count may be intentionally set to 0, or default is off and caller didn't say
            mlt_fl = None
            mlt = "false"
            mlt_minwl = None
            mlt_count = 0
    
        if solr_query_spec.facetFields is not None:
            facet = "on"
        else:
            facet = "off"
    
        try:
            if solr_query_spec.solrQueryOpts.hlMaxAnalyzedChars != 0: # let caller configure, but not 0!
                if solr_query_spec.fullReturn:
                    solr_query_spec.solrQueryOpts.hlMaxAnalyzedChars = opasConfig.SOLR_KWIC_MAX_ANALYZED_CHARS 
                else:
                    solr_query_spec.solrQueryOpts.hlMaxAnalyzedChars = opasConfig.SOLR_FULL_TEXT_MAX_ANALYZED_CHARS 
            else: # solr default
                solr_query_spec.solrQueryOpts.hlMaxAnalyzedChars = None # will be removed from args, giving solr default of 51200
        except:
            solr_query_spec.solrQueryOpts.hlMaxAnalyzedChars = opasConfig.SOLR_KWIC_MAX_ANALYZED_CHARS # opasConfig.SOLR_HIGHLIGHT_RETURN_FRAGMENT_SIZE
        #else: OK, leave it be!
    
        try: # must have value
            if solr_query_spec.solrQueryOpts.hlFragsize < opasConfig.DEFAULT_KWIC_CONTENT_LENGTH:
                solr_query_spec.solrQueryOpts.hlFragsize = opasConfig.DEFAULT_KWIC_CONTENT_LENGTH
        except:
            solr_query_spec.solrQueryOpts.hlFragsize = opasConfig.DEFAULT_KWIC_CONTENT_LENGTH
        else:
            pass # else, it's ok
    
        # let this be None, if no limit is set.
        if limit is not None:
            if limit < 0: # unlimited return, to bypass default
                solr_query_spec.limit = opasConfig.MAX_DOCUMENT_RECORDS_RETURNED_AT_ONCE
            else:
                solr_query_spec.limit = limit
    
        if offset is not None:
            solr_query_spec.offset = offset
    
        if sort is not None:
            solr_query_spec.solrQuery.sort = sort
    
        # q must be part of any query; this appears to be the cause of the many solr syntax errors seen. 
        if solr_query_spec.solrQuery.searchQ is None or solr_query_spec.solrQuery.searchQ == "":
            logger.error(f"QuerySpecificationError: searchQ is {solr_query_spec.solrQuery.searchQ}.  Filter: {solr_query_spec.solrQuery.filterQ} Endpoint was: {req_url}")
            solr_query_spec.solrQuery.searchQ = "*.*"
        
        # one last cleaning
        #solr_query_spec.solrQuery.searchQ = solr_query_spec.solrQuery.searchQ.replace(" && *:*", "")
        #solr_query_spec.solrQuery.filterQ = solr_query_spec.solrQuery.filterQ.replace(" && *:*", "")

    try:
        query = solr_query_spec.solrQuery.searchQ
        # set up return fields including abstract and full-text if requested
        return_fields = solr_query_spec.returnFields
        if solr_query_spec.fullReturn: #and session_info.XXXauthenticated:
            # NOTE: we add this here, but in return data, access by document will be checked.
            if "text_xml" not in solr_query_spec.returnFields:
                return_fields = return_fields + ", text_xml, para" #, art_excerpt, art_excerpt_xml
        
        if solr_query_spec.abstractReturn:
            if "abstract_xml" not in solr_query_spec.returnFields:
                return_fields = return_fields + ", abstract_xml"
            if "art_excerpt" not in solr_query_spec.returnFields:
                return_fields = return_fields + ", art_excerpt, art_excerpt_xml"
            if "summaries_xml" not in solr_query_spec.returnFields:
                return_fields = return_fields + ", summaries_xml"

        if type(solr_query_spec.solrQuery.facetQ) == str: # sometimes coming in as Query(None)
            if solr_query_spec.solrQuery.facetQ is not None:
                filterQ = solr_query_spec.solrQuery.filterQ + " && (" + solr_query_spec.solrQuery.facetQ + ")"
            else:
                filterQ = solr_query_spec.solrQuery.filterQ
        else:
            filterQ = solr_query_spec.solrQuery.filterQ
            
        # extend related documents search (art_qual) to unmarked documents that are explicitly referenced in ID
        # TODO: (Possible) Should this also do this in the query param?
        if "art_qual:" in filterQ:
            filterQ = re.sub('art_qual:\(\"?(?P<tgtid>[^\"]*?)\"?\)', '(art_qual:(\g<tgtid>) || art_id:(\g<tgtid>))', filterQ)
            
        solr_param_dict = { 
                            # "q": solr_query_spec.solrQuery.searchQ,
                            "fq": filterQ,
                            "q.op": solr_query_spec.solrQueryOpts.qOper, 
                            # "debugQuery": solr_query_spec.solrQueryOpts.queryDebug or localsecrets.SOLR_DEBUG,
                            # "defType" : solr_query_spec.solrQueryOpts.defType,
                            "fl" : return_fields,         
                            "facet" : facet,
                            "facet.field" : solr_query_spec.facetFields, #["art_lang", "art_authors"],
                            "facet.mincount" : solr_query_spec.facetMinCount,
                            "mlt" : mlt,
                            "mlt.fl" : mlt_fl,
                            "mlt.count" : mlt_count,
                            "mlt.minwl" : mlt_minwl,
                            "mlt.interestingTerms" : "list",
                            "rows" : solr_query_spec.limit,
                            "start" : solr_query_spec.offset,
                            "sort" : solr_query_spec.solrQuery.sort,
                            "hl" : solr_query_spec.solrQueryOpts.hl, 
                            "hl.multiterm" : solr_query_spec.solrQueryOpts.hlMultiterm,
                            "hl.fl" : solr_query_spec.solrQueryOpts.hlFields,
                            "hl.usePhraseHighlighter" : solr_query_spec.solrQueryOpts.hlUsePhraseHighlighter, 
                            "hl.snippets" : solr_query_spec.solrQueryOpts.hlMaxKWICReturns,
                            "hl.fragsize" : solr_query_spec.solrQueryOpts.hlFragsize, 
                            "hl.maxAnalyzedChars" : solr_query_spec.solrQueryOpts.hlMaxAnalyzedChars,
                            # for unified method, use hl.tag.pre and hl.tag.post NOTE: This tags illegally in XML
                            # for original method, use hl.simple.pre and hl.simple.post
                            "hl.method": "unified",
                            # "hl_encoder":"HTML",
                            "hl.tag.pre" : opasConfig.HITMARKERSTART,
                            "hl.tag.post" : opasConfig.HITMARKEREND        
        }

    except Exception as e:
        logger.error(f"SolrParamError: {e}")

    #allow core parameter here
    if solr_core is None:
        if solr_query_spec.core is not None:
            try:
                solr_core = EXTENDED_CORES.get(solr_query_spec.core, None)
            except Exception as e:
                detail=f"CoreSpecificationError: Bad Extended Request. {e}"
                logger.error(detail)
                ret_val = models.ErrorReturn(httpcode=400, error="Core specification error", error_description=detail)
            else:
                if solr_core is None:
                    detail=f"Bad Extended Request. Unknown core specified."
                    logger.warning(detail)
                    ret_val = models.ErrorReturn(httpcode=400, error="Core specification error", error_description=detail)
        else:
            solr_query_spec.core = "pepwebdocs"
            solr_core = solr_docs2
    else:
        try:
            solr_core = EXTENDED_CORES.get(solr_core, None)
        except Exception as e:
            detail=f"CoreSpecificationError: Bad Extended Request. {e}"
            logger.error(detail)
            ret_val = models.ErrorReturn(httpcode=400, error="Core specification error", error_description=detail)
        else:
            if solr_core is None:
                detail=f"CoreSpecificationError: Bad Extended Request. No core specified."
                logger.warning(detail)
                ret_val = models.ErrorReturn(httpcode=400, error="Core specification error", error_description=detail)

    try:
        # PySolr does not like None's, so clean them
        solr_param_dict = cleanNullTerms(solr_param_dict)

        if opasConfig.LOCAL_TRACE:
            print (f"+****Solr Query: q:{query}, fq:{filterQ}")
            #print (f"+****Solr facets:{solr_param_dict.get('facet.field', 'No facets to return')}" )
            print (f"+****Solr Facet Query: q:{solr_query_spec.solrQuery.facetQ}")
                       
        # ####################################################################################
        # THE SEARCH!
        results = solr_docs2.search(query, **solr_param_dict)
        # ####################################################################################
       
    except SAXParseException as e:
        ret_val = models.ErrorReturn(httpcode=httpCodes.HTTP_400_BAD_REQUEST, error="Search syntax error", error_description=f"{e.getMessage()}")
        ret_status = (httpCodes.HTTP_400_BAD_REQUEST, e) 
        logger.error(f"SolrSAXParseExceptionError: Search Error (parse): {ret_val}. Params sent: {solr_param_dict}")

    except AttributeError as e:
        logger.error(f"SolrAttributeExceptionError: Attribute Error: {e}")
           
    except pysolr.SolrError as e:
        error = "pySolr.SolrError"
        error_num = 400
        error_description=f"PySolrError: There's an error in your input ({e})"
        # {ret_status[1].reason}:{ret_status[1].body}
        ret_status = (error_num, {"reason": error, "body": error_description})
        ret_val = models.ErrorReturn(httpcode=400, error=error, error_description=error_description)

        if e is None:
            pass # take defaults
        elif e.args is not None:
            # defaults, before trying to decode error
            error_description = "PySolrError: Search Error"
            error = 400
            try:
                err = e.args
                error_set = err[0].split(":", 1)
                error = error_set[0]
                error = error.replace('Solr ', 'Search engine ')
                error_description = error_set[1]
                error_description = error_description.strip(" []")
                m = re.search("HTTP (?P<err>[0-9]{3,3})", error)
                if m is not None:
                    http_error = m.group("err")
                    http_error_num = int(http_error)
            except Exception as e:
                logger.error(f"PySolrError: Error parsing Solr error {e.args}")
                ret_status = (error_num, e.args)
            else:
                ret_val = models.ErrorReturn(httpcode=http_error_num, error=error, error_description=error_description)
                ret_status = (error_num, {"reason": error, "body": error_description})

        logger.error(f"PySolrError: Syntax: {ret_status}. Params sent: {solr_param_dict}")
        
    except Exception as e:
        try:
            tb = sys.exc_info()[2]
            raise ValueError(...).with_traceback(tb)
        except Exception as e2:
            error_code = 500
            ret_status = (httpCodes.HTTP_500_INTERNAL_SERVER_ERROR, None)
        else:
            ret_status = (httpCodes.HTTP_400_BAD_REQUEST, e) # e has type <class 'solrpy.core.SolrException'>, with useful elements of httpcode, reason, and body, e.g.,
        finally:
            ret_val = models.ErrorReturn(httpcode=error_code, error="Search syntax error", error_description=f"There's an error in your input (no reason supplied)")
            logger.error(f"PySolrError: Syntax: {ret_status}. Params sent: {solr_param_dict}")
                                
    else: #  search was ok
        try:
            logger.info(f"Search Ok. Result Size:{results.hits}; Search:{solr_query_spec.solrQuery.searchQ}; Filter:{solr_query_spec.solrQuery.filterQ}")
            scopeofquery = solr_query_spec.solrQuery # [solr_query_spec.solrQuery.searchQ, solr_query_spec.solrQuery.filterQ, solr_query_spec.solrQuery.facetQ]
    
            if ret_status[0] == 200: 
                documentItemList = []
                rowCount = 0
                # rowOffset = 0
                #if solr_query_spec.fullReturn:
                    ## if we're not authenticated, then turn off the full-text request and behave as if we didn't try
                    #if not authenticated: # and file_classification != opasConfig.DOCUMENT_ACCESS_FREE:
                        ## can't bring back full-text
                        #logger.warning("Fulltext requested--by API--but not authenticated.")
                        #solr_query_spec.fullReturn = False
                        
                # try checking PaDS for authenticated; if false, no need to check permits
                try:
                    if session_info is not None:
                        if session_info.authenticated == False:
                            logger.debug("User is not authenticated.  Permit optimization enabled.")
                        else:
                            logger.debug("User is authenticated.  Permit optimization disabled.")
                    else: # no session info provided.  Set it to defaults, non-authenticated
                        logger.debug("No session info object provided.")
                        
                except Exception as e:
                    #  no session info...what to do?
                    logger.debug(f"No session info to perform optimizations {e}")
                    
                for result in results.docs:
                    # reset anchor counts for full-text markup re.sub
                    # count_anchors = 0
                    record_count = len(results.docs)
                    # authorIDs = result.get("art_authors", None)
                    documentListItem = models.DocumentListItem()
                    documentListItem = opasQueryHelper.get_base_article_info_from_search_result(result, documentListItem)
                    documentID = documentListItem.documentID
                    if documentID is None:
                        # there's a problem with this records
                        logger.error(f"DocumentError: Incomplete record, skipping. Possible corrupt solr database: {result}")
                        continue
                    # sometimes, we don't need to check permissions
                    # Always check if fullReturn is selected
                    # Don't check when it's not and a large number of records are requested (but if fullreturn is requested, must check)
                    if record_count < opasConfig.MAX_RECORDS_FOR_ACCESS_INFO_RETURN or solr_query_spec.fullReturn:
                        access = opasDocPerm.get_access_limitations( doc_id=documentListItem.documentID, 
                                                                     classification=documentListItem.accessClassification, 
                                                                     year=documentListItem.year,
                                                                     doi=documentListItem.doi, 
                                                                     session_info=session_info, 
                                                                     documentListItem=documentListItem,
                                                                     fulltext_request=solr_query_spec.fullReturn,
                                                                     request=request
                                                                    ) # will updated accessLimited fields in documentListItem
                        
                        if access is not None: # copy all the access info returned
                            documentListItem.accessLimited = access.accessLimited   
                            documentListItem.accessLimitedCode = access.accessLimitedCode
                            documentListItem.accessLimitedClassifiedAsCurrentContent = access.accessLimitedClassifiedAsCurrentContent
                            documentListItem.accessLimitedReason = access.accessLimitedReason
                            documentListItem.accessLimitedDescription = access.accessLimitedDescription
                            documentListItem.accessLimitedPubLink = access.accessLimitedPubLink

                    documentListItem.score = result.get("score", None)               
                    try:
                        text_xml = results.highlighting[documentID].get("text_xml", None)
                        if text_xml == []:
                            text_xml = None
                    except:
                        text_xml = None
    
                    if text_xml is None: # try getting it from para
                        try:
                            text_xml = results.highlighting[documentID].get("para", None)
                        except:
                            try:
                                text_xml = result["text_xml"]
                            except:
                                text_xml = result.get("para", None)
                    
                    #if text_xml is None: # PySolrLib doesn't put text in highlight unless there was a term search, so get it here.
                        #text_xml = result.get("text_xml", None)
    
                    if text_xml is not None and type(text_xml) != list:
                        text_xml = [text_xml]
                       
                    # do this before we potentially clear text_xml if no full text requested below
                    if solr_query_spec.abstractReturn:
                        # we don't want to ever do this (Google!) 2021-04-29 (delete later, if the urge passes)
                        #omit_abstract = False
                        #if opasConfig.ACCESS_ABSTRACT_RESTRICTION and documentListItem.accessLimited: # if restriction is on, AND this is restricted content
                            #if session_info is not None:
                                #if not session_info.authenticated:
                                    ## experimental - remove abstract if not authenticated, per DT's requirement
                                    #omit_abstract = True
                            #else: # no session info, omit abstract
                                #omit_abstract = True

                        # this would print a message about logging in and not display an abstract if omit_abstract were true,
                        # but then Google could not index
                        documentListItem = opasQueryHelper.get_excerpt_from_search_result(result,
                                                                                          documentListItem,
                                                                                          solr_query_spec.returnFormat,
                                                                                          omit_abstract=False)
    
                    documentListItem.kwic = "" # need this, so it doesn't default to None
                    documentListItem.kwicList = []
                    # no kwic list when full-text is requested.
                    kwic_list = []
                    kwic = ""  # this has to be "" for PEP-Easy, or it hits an object error.                      if text_xml is not None and not solr_query_spec.fullReturn and solr_query_spec.solrQueryOpts.hl == 'true':
                        #kwicList = getKwicList(textXml, extraContextLen=extraContextLen)  # returning context matches as a list, making it easier for clients to work with
                        kwic_list = []
                        for n in text_xml:
                            # strip all tags
                            try:
                                match = opasxmllib.xml_string_to_text(n)
                                # change the tags the user told Solr to use to the final output tags they want
                                #   this is done to use non-xml-html hit tags, then convert to that after stripping the other xml-html tags
                                # this function changes variable count_anchors with the count of changes
                                match = re.sub(opasConfig.HITMARKERSTART, opasConfig.HITMARKERSTART_OUTPUTHTML, match)
                                match = re.sub(opasConfig.HITMARKEREND, opasConfig.HITMARKEREND_OUTPUTHTML, match)
                                # watch for Doctype which isn't removed if partial or part of a tag (2021-04-05)
                                match = re.sub("(\<?DOCTYPE[^>]+?\>)|(^[^\<]{0,25}?>)", "", match)
                                match = match.lstrip(". ")
                            except Exception as e:
                                logger.warn(f"Error in processing hitlist entry: {e}")
                            else:
                                kwic_list.append(match)
    
                        kwic = " . . . ".join(kwic_list)  # how its done at GVPi, for compatibility (as used by PEPEasy)
                        # we don't need fulltext
                        text_xml = None
    
                    if kwic != "": documentListItem.kwic = kwic
                    if kwic_list != []: documentListItem.kwicList = kwic_list
    
                    # see if this article is an offsite article
                    offsite = result.get("art_offsite", False)
                    # ########################################################################
                    # This is the room where where full-text return HAPPENS
                    # ########################################################################
                    if solr_query_spec.fullReturn and not documentListItem.accessLimited and not offsite:
                        documentListItem.term = f"SearchHits({solr_query_spec.solrQuery.searchQ})"
                        documentListItem = get_fulltext_from_search_results(result=result,
                                                                            text_xml=text_xml,
                                                                            format_requested=solr_query_spec.returnFormat,
                                                                            return_options=solr_query_spec.returnOptions, 
                                                                            page=solr_query_spec.page,
                                                                            page_offset=solr_query_spec.page_offset,
                                                                            page_limit=solr_query_spec.page_limit,
                                                                            documentListItem=documentListItem)

                        # test remove glossary..for my tests, not for stage/production code.
                        # Note: the question mark before the first field in search= matters
                        #  e.g., http://development.org:9100/v2/Documents/Document/JCP.001.0246A/?return_format=XML&search=%27?fulltext1="Evenly%20Suspended%20Attention"~25&limit=10&facetmincount=1&facetlimit=15&sort=score%20desc%27
                        # documentListItem.document = opasxmllib.xml_remove_tags_from_xmlstr(documentListItem.document,['impx'])
                        if documentListItem.document == None:
                            errmsg = f"DocumentError: Fetch failed! ({solr_query_spec.solrQuery.searchQ}"
                            logger.error(errmsg)
                            documentListItem.termCount = 0
                        
                    else: # by virtue of not calling that...
                        # no full-text if accessLimited or offsite article
                        # free up some memory, since it may be large
                        result["text_xml"] = None                   
    
                    stat = {}
                    count_all = result.get("art_cited_all", None)
                    if count_all is not None:
                        stat["art_cited_5"] = result.get("art_cited_5", None)
                        stat["art_cited_10"] = result.get("art_cited_10", None)
                        stat["art_cited_20"] = result.get("art_cited_20", None)
                        stat["art_cited_all"] = count_all
    
                    count0 = result.get("art_views_lastcalyear", 0)
                    count1 = result.get("art_views_lastweek", 0)
                    count2 = result.get("art_views_last1mos", 0)
                    count3 = result.get("art_views_last6mos", 0)
                    count4 = result.get("art_views_last12mos", 0)
    
                    if count0 + count1 + count2 + count3+ count4 > 0:
                        stat["art_views_lastcalyear"] = count0
                        stat["art_views_lastweek"] = count1
                        stat["art_views_last1mos"] = count2
                        stat["art_views_last6mos"] = count3
                        stat["art_views_last12mos"] = count4
    
                    # count fields (relatively new, 2021)
                    stat["reference_count"] = result.get("art_ref_count", 0)
                    stat["art_fig_count"] = result.get("art_fig_count", 0)
                    stat["art_tbl_count"] = result.get("art_tbl_count", 0)
                    stat["art_kwds_count"] = result.get("art_kwds_count", 0)
                    stat["art_words_count"] = result.get("art_words_count", 0)
                    stat["art_citations_count"] = result.get("art_citations_count", 0)
                    stat["art_ftns_count"] = result.get("art_ftns_count", 0)
                    stat["art_notes_count"] = result.get("art_notes_count", 0)
                    stat["art_dreams_count"] = result.get("art_dreams_count", 0)

                    if stat == {}:
                        stat = None
    
                    documentListItem.stat = stat
    
                    similarityMatch = None
                    if mlt_count > 0:
                        if results.raw_response["moreLikeThis"][documentID] is not None:
                            similarityMatch = {}
                            # remove text
                            similarityMatch["similarDocs"] = {}
                            similarityMatch["similarDocs"][documentID] = []
                            for n in results.raw_response["moreLikeThis"][documentID]["docs"]:
                                likeThisListItem = models.DocumentListItem()
                                #n["text_xml"] = None
                                n = opasQueryHelper.get_base_article_info_from_search_result(n, likeThisListItem)                    
                                similarityMatch["similarDocs"][documentID].append(n)
    
                            similarityMatch["similarMaxScore"] = results.raw_response["moreLikeThis"][documentID]["maxScore"]
                            similarityMatch["similarNumFound"] = results.raw_response["moreLikeThis"][documentID]["numFound"]
                            # documentListItem.moreLikeThis = results.moreLikeThis[documentID]
    
                    if similarityMatch is not None: documentListItem.similarityMatch = similarityMatch
                    
                    #parent_tag = result.get("parent_tag", None)
                    #if parent_tag is not None:
                        #documentListItem.docChild = {}
                        #documentListItem.docChild["id"] = result.get("id", None)
                        #documentListItem.docChild["parent_tag"] = parent_tag
                        #documentListItem.docChild["para"] = result.get("para", None)
                        #documentListItem.docChild["lang"] = result.get("lang", None)
                        #documentListItem.docChild["para_art_id"] = result.get("para_art_id", None)
                    #else:
                        #documentListItem.docChild = None
    
                    sort_field = None
                    if solr_query_spec.solrQuery.sort is not None:
                        try:
                            sortby = re.search("(?P<field>[a-z_]+[1-9][0-9]?)[ ]*?", solr_query_spec.solrQuery.sort)
                        except Exception as e:
                            sort_field = None
                        else:
                            if sortby is not None:
                                sort_field = sortby.group("field")
    
                    documentListItem.score = result.get("score", None)
                    documentListItem.rank = rowCount + 1
                    if sort_field is not None:
                        if sort_field == "art_cited_all":
                            documentListItem.rank = result.get("art_cited_all", None) 
                        elif sort_field == "score":
                            documentListItem.rank = result.get("score", None)
                        else:
                            documentListItem.rank = result.get(sort_field, None)
                            
                            
                    rowCount += 1
                    # add it to the set!
                    documentItemList.append(documentListItem)
                    #TODO - we probably don't need this.
                    if solr_query_spec.limit is not None:
                        if rowCount > solr_query_spec.limit:
                            break
    
                try:
                    facet_counts = {}
                    facets = results.facets["facet_fields"]
                    facet_counts["facet_fields"] = facet_processing(facets)
                    
                except:
                    facet_counts = None
    
            if req_url is None:
                req_url = solr_query_spec.urlRequest
    
            # Moved this down here, so we can fill in the Limit, Page and Offset fields based on whether there
            #  was a full-text request with a page offset and limit
            # Solr search was ok
            responseInfo = models.ResponseInfo(count = len(results.docs),
                                               fullCount = results.hits,
                                               totalMatchCount = results.hits,
                                               description=solr_query_spec.solrQuery.semanticDescription, 
                                               limit = solr_query_spec.limit,
                                               offset = solr_query_spec.offset,
                                               page = solr_query_spec.page, 
                                               listType="documentlist",
                                               scopeQuery=[scopeofquery], 
                                               fullCountComplete = solr_query_spec.limit >= results.hits,
                                               solrParams = None, # results._params,
                                               facetCounts=facet_counts,
                                               #authenticated=authenticated, 
                                               request=f"{req_url}",
                                               core=solr_query_spec.core, 
                                               timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)                     
                                               )
   
            # responseInfo.count = len(documentItemList)
    
            documentListStruct = models.DocumentListStruct( responseInfo = responseInfo, 
                                                            responseSet = documentItemList
                                                            )
    
            documentList = models.DocumentList(documentList = documentListStruct)
    
            ret_val = documentList
            
        except Exception as e:
            logger.error(f"QueryResultsError: Problem processing results {e}")
            

    return ret_val, ret_status

#-----------------------------------------------------------------------------
def metadata_get_videos(src_type=None, pep_code=None, limit=opasConfig.DEFAULT_LIMIT_FOR_METADATA_LISTS, offset=0, sort_field="art_citeas_xml"):
    """
    Fill out a sourceInfoDBList which can be used for a getSources return, but return individual 
      videos, as is done for books.  This provides more information than the 
      original API which returned video "journals" names.
      
    Authorizations are not checked or returned (thus no session id is needed)

    """
    source_info_dblist = []
    total_count = 0
    ret_val = {}
    return_status = (200, "OK")

    if pep_code is not None:
        query = "art_sourcetype:video* AND art_sourcecode:{}".format(pep_code)
    else:
        query = "art_sourcetype:video*"
        
    try:
        logger.info(f"Solr Query: q={query}")
        args = {
                   'fl':opasConfig.DOCUMENT_ITEM_VIDEO_FIELDS,
                   # 'q':'tuck*',
                   'rows':limit,
                   'start': offset,
                   'sort':f"{sort_field} asc",
                   #'sort.order':'asc'
               }

        srcList = solr_docs2.search(query, **args)

    except Exception as e:
        #logger.error(f"metadataGetVideosError: {e}")
        ret_val = models.ErrorReturn(httpcode=e.httpcode, error="Search syntax error", error_description=f"There's an error in your input (no reason supplied)")
        return_status = (httpCodes.HTTP_400_BAD_REQUEST, e) # e has type <class 'solrpy.core.SolrException'>, with useful elements of httpcode, reason, and body, e.g.,
        logger.error(f"metadataGetVideosError: {e.httpcode}. Params sent: {solr_param_dict} Body: {e.body}")
    else:
        # count = len(srcList.results)
        total_count = srcList.raw_response['response']['numFound']
    
        for result in srcList.docs:
            source_info_record = {}
            authors = result.get("art_authors")
            if authors is None:
                source_info_record["author"] = None
            elif len(authors) > 1:
                source_info_record["author"] = "; ".join(authors)
            else:    
                source_info_record["author"] = authors[0]
                
            source_info_record["src_code"] = result.get("art_sourcecode")
            source_info_record["ISSN"] = result.get("art_issn")
            source_info_record["documentID"] = result.get("art_id")
            try:
                source_info_record["title"] = result.get("title")
            except:
                source_info_record["title"] = ""
    
            source_info_record["art_citeas"] = result.get("art_citeas_xml")
            source_info_record["pub_year"] = result.get("art_year")
            source_info_record["bib_abbrev"] = result.get("art_sourcetitleabbr")  # error in get field, fixed 2019.12.19
            try:
                source_info_record["language"] = result.get("art_lang")
            except:
                source_info_record["language"] = "EN"
    
            logger.debug("metadataGetVideos: %s", source_info_record)
            source_info_dblist.append(source_info_record)

    return total_count, source_info_dblist, ret_val, return_status

#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------
def metadata_get_contents(pep_code, #  e.g., IJP, PAQ, CPS
                          year="*",
                          vol="*",
                          req_url: str=None,
                          extra_info:int=0, # since this requires an extra query of the DB
                          limit=opasConfig.DEFAULT_LIMIT_FOR_CONTENTS_LISTS,
                          offset=0):
    """
    Return a source's contents

    >>> results = metadata_get_contents("IJP", "1993", limit=5, offset=0)
    >>> results.documentList.responseInfo.count == 5
    True
    >>> results = metadata_get_contents("IJP", "1993", limit=5, offset=5)
    >>> results.documentList.responseInfo.count == 5
    True
    """
    ret_val = []
    if year == "*" and vol != "*":
        # specified only volume
        field="art_vol"
        search_val = vol
    else:  #Just do year
        field="art_year"
        search_val = year  #  was "*", thats an error, fixed 2019-12-19

    try:
        code = pep_code.upper()
    except:
        logger.warning(f"Illegal PEP Code or None supplied to metadata_get_contents: {pep_code}")
    else:
        pep_code = code

    query = f"art_sourcecode:{pep_code} && {field}:{search_val}"
    logger.info(f"Solr Query: q:{query}")
    
    fields = """art_id,
                art_vol,
                art_year,
                art_iss,
                art_iss_title,
                art_iss_seqnbr,
                art_newsecnm,
                art_pgrg,
                title,
                art_authors,
                art_authors_mast,
                art_citeas_xml,
                art_info_xml"""
    
    args = {
               'fl':fields,
               'rows':limit,
               'start': offset,
               'sort':"art_id asc",
           }

    results = solr_docs2.search(query, **args)
    
    document_item_list = []
    for result in results.docs:
        # transform authorID list to authorMast
        author_ids = result.get("art_authors", None)
        if author_ids is None:
            # try this, instead of abberrant behavior in alpha of display None!
            authorMast = result.get("art_authors_mast", "")
        else:
            authorMast = opasgenlib.derive_author_mast(author_ids)

        pgRg = result.get("art_pgrg", None)
        pgStart, pgEnd = opasgenlib.pgrg_splitter(pgRg)
        citeAs = result.get("art_citeas_xml", None)  
        citeAs = opasQueryHelper.force_string_return_from_various_return_types(citeAs)
        vol = result.get("art_vol", None)
        issue = result.get("art_iss", None)
        issue_title = result.get("art_iss_title", None)
        issue_seqnbr = result.get("art_iss_seqnbr", None)
        if issue is not None:
            if issue_title is None:
                if issue_seqnbr is None:
                    issue_title = f"Issue {issue}"
                else:
                    issue_title = f"No. {issue_seqnbr}"

        item = models.DocumentListItem(PEPCode = pep_code, 
                                       year = result.get("art_year", None),
                                       vol = vol,
                                       issue = issue,
                                       issueTitle = issue_title,
                                       issueSeqNbr = issue_seqnbr, 
                                       newSectionName = result.get("art_newsecnm", None),
                                       pgRg = result.get("art_pgrg", None),
                                       pgStart = pgStart,
                                       pgEnd = pgEnd,
                                       title = result.get("title", None), 
                                       authorMast = authorMast,
                                       documentID = result.get("art_id", None),
                                       documentRef = opasxmllib.xml_elem_or_str_to_text(citeAs, default_return=""),
                                       documentRefHTML = citeAs,
                                       documentInfoXML=result.get("art_info_xml", None), 
                                       score = result.get("score", None)
                                       )
        #logger.debug(item)
        document_item_list.append(item)

    # two options 2020-11-17 for extra info (lets see timing for each...)
    suppinfo = None
    if extra_info == 1 and search_val != "*" and pep_code != "*" and len(results.docs) > 0:
        ocd = opasCentralDBLib.opasCentralDB()
        suppinfo = ocd.get_min_max_volumes(source_code=pep_code)

    if extra_info == 2 and search_val != "*" and pep_code != "*" and len(results.docs) > 0:
        prev_vol, match_vol, next_vol = metadata_get_next_and_prev_vols(source_code=pep_code,
                                                                        source_vol=vol,
                                                                        req_url=req_url
                                                                        )
        suppinfo = {"infosource": "volumes_adjacent",
                    "prev_vol": prev_vol,
                    "matched_vol": match_vol,
                    "next_vol": next_vol}

    num_found = results.hits

    response_info = models.ResponseInfo( count = len(results.docs),
                                         fullCount = num_found,
                                         limit = limit,
                                         offset = offset,
                                         listType="documentlist",
                                         fullCountComplete = limit >= num_found,
                                         supplementalInfo=suppinfo, 
                                         request=f"{req_url}",
                                         timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)                     
                                         )

    document_list_struct = models.DocumentListStruct( responseInfo = response_info, 
                                                      responseSet=document_item_list
                                                      )

    document_list = models.DocumentList(documentList = document_list_struct)

    ret_val = document_list

    return ret_val
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
def database_get_whats_new(days_back=14,
                           limit=opasConfig.DEFAULT_LIMIT_FOR_WHATS_NEW,
                           req_url:str=None,
                           source_type="journal",
                           offset=0,
                           session_info=None):
    """
    Return what JOURNALS have been updated in the last week

    >>> result = database_get_whats_new()

    """    
    field_list = "art_id, title, art_vol, art_iss, art_year, art_sourcecode, art_sourcetitlefull, art_sourcetitleabbr, file_last_modified, timestamp, art_sourcetype"
    sort_by = "file_last_modified desc"
    ret_val = None
    new_articles = ocd.get_articles_newer_than(days_back)

    # two ways to get date, slightly different meaning: timestamp:[NOW-{days_back}DAYS TO NOW] AND file_last_modified:[NOW-{days_back}DAYS TO NOW]
    try:
        query = f"file_last_modified:[NOW-{days_back}DAYS TO NOW] AND art_sourcetype:{source_type}"
        logger.info(f"Solr Query: q={query}")

        args = {
            "fl": field_list,
            "fq": "{!collapse field=art_sourcecode max=art_year_int}",
            "sort": sort_by,
            "rows": opasConfig.MAX_WHATSNEW_ARTICLES_TO_CONSIDER,
            "start": offset
        }

        results = solr_docs2.search(query, **args)

    except Exception as e:
        logger.error(f"WhatsNewError: {e}")
        response_info = models.ResponseInfo( count = 0,
                                             fullCount = 0,
                                             limit = limit,
                                             offset = offset,
                                             listType="newlist",
                                             fullCountComplete = False,
                                             request=f"{req_url}",
                                             timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)                     
                                             )
        response_info.count = 0
        whats_new_list_items = []
        
        whats_new_list_struct = models.WhatsNewListStruct( responseInfo = response_info, 
                                                           responseSet = whats_new_list_items
                                                           )
        ret_val = models.WhatsNewList(whatsNew = whats_new_list_struct)
    else:
        num_found = results.raw_response['response']['numFound']
        whats_new_list_items = []
        row_count = 0
        eligible_entry_full_count = 0
        already_seen = []
        for result in results.docs:
            document_id = result.get("art_id", None)
            updated = result.get("file_last_modified", None)
            updated = datetime.strptime(updated,'%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d')
            if document_id not in new_articles:
                logger.debug(f"File {document_id} updated {updated}")
                continue
            
            PEPCode = result.get("art_sourcecode", None)
            #if PEPCode is None or PEPCode in ["SE", "GW", "ZBK", "IPL"]:  # no books
                #continue
            src_type = result.get("art_sourcetype", None)
            if src_type != "journal":
                continue
            
            # see if this is already been in the article tracker
                
            volume = result.get("art_vol", None)
            issue = result.get("art_iss", "")
            year = result.get("art_year", None)
            abbrev = result.get("art_sourcetitleabbr", None)
            src_title = result.get("art_sourcetitlefull", None)
            
            if abbrev is None:
                abbrev = src_title
            display_title = abbrev + " v%s.%s (%s) " % (volume, issue, year)
            if display_title in already_seen:
                continue
            else:
                already_seen.append(display_title)

            eligible_entry_full_count += 1
            if row_count > limit:
                continue

            volume_url = "/v1/Metadata/Contents/%s/%s" % (PEPCode, issue)
    
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
            
            row_count += 1 # number of rows added
            #if row_count > limit:
                #break
    
        whats_new_list_items = sorted(whats_new_list_items, key=lambda x: x.displayTitle, reverse = False)    

        if limit != None:
            if offset is None:
                offset = 0
            limited_whats_new_list = whats_new_list_items[offset:offset+limit]
        else:
            limited_whats_new_list = whats_new_list_items
        
        response_info = models.ResponseInfo( count = len(results.docs),
                                             fullCount = eligible_entry_full_count,
                                             limit = limit,
                                             offset = offset,
                                             listType="newlist",
                                             fullCountComplete = limit >= eligible_entry_full_count,
                                             request=f"{req_url}",
                                             timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)                     
                                             )

        response_info.count = len(whats_new_list_items)
    
        whats_new_list_struct = models.WhatsNewListStruct( responseInfo = response_info, 
                                                           responseSet = limited_whats_new_list
                                                           )
    
        ret_val = models.WhatsNewList(whatsNew = whats_new_list_struct)

    return ret_val   # WhatsNewList
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
def search_stats_for_download(solr_query_spec: models.SolrQuerySpec,
                              limit=None,
                              offset=None,
                              sort=None, 
                              session_info=None,
                              solr_core="pepwebdocs"
                              ):
    """
    SPECIAL - do the search for the purpose of downloading stat...could be many records.
    
    """
    ret_val = {}
    ret_status = (200, "OK") # default is like HTTP_200_OK
    
    if solr_query_spec.solrQueryOpts is None: # initialize a new model
        solr_query_spec.solrQueryOpts = models.SolrQueryOpts()

    if solr_query_spec.solrQuery is None: # initialize a new model
        solr_query_spec.solrQuery = models.SolrQuery()

    solr_query_spec.solrQueryOpts.hlMaxAnalyzedChars = 200
    # let this be None, if no limit is set.
    if offset is not None:
        solr_query_spec.offset = offset

    if limit is not None:
        solr_query_spec.limit = min(limit, opasConfig.MAX_DOCUMENT_RECORDS_RETURNED_AT_ONCE) 
    else:
        solr_query_spec.limit = 99000 # opasConfig.MAX_DOCUMENT_RECORDS_RETURNED_AT_ONCE

    if sort is not None:
        solr_query_spec.solrQuery.sort = sort

    # q must be part of any query; this appears to be the cause of the many solr syntax errors seen. 
    if solr_query_spec.solrQuery.searchQ is None or solr_query_spec.solrQuery.searchQ == "":
        logger.error(f"SearchStatDownloadError: searchQ: {solr_query_spec.solrQuery.searchQ}.  Filter: {solr_query_spec.solrQuery.filterQ} Endpoint: {req_url}")
        solr_query_spec.solrQuery.searchQ = "*.*"

    query = solr_query_spec.solrQuery.searchQ
    try:
        solr_param_dict = { 
                            "fq": solr_query_spec.solrQuery.filterQ,
                            "q.op": solr_query_spec.solrQueryOpts.qOper, 
                            "debugQuery": solr_query_spec.solrQueryOpts.queryDebug or localsecrets.SOLR_DEBUG,
                            "fl" : opasConfig.DOCUMENT_ITEM_STAT_FIELDS, 
                            "rows" : solr_query_spec.limit,
                            "start" : solr_query_spec.offset,
                            "sort" : solr_query_spec.solrQuery.sort
        }

        # PySolr does not like None's, so clean them
        solr_param_dict = cleanNullTerms(solr_param_dict)
        
    except Exception as e:
        logger.error(f"SolrParamAssignmentError: {e}")

    #allow core parameter here
    solr_query_spec.core = "pepwebdocs"
    solr_core = solr_docs2 # by specing this it's always solrpy docs2, no effect of core choice

    # ############################################################################
    # SOLR Download Query
    # ############################################################################
    try:
        start_time = time.time()
        results = solr_core.search(query, **solr_param_dict)
        total_time = time.time() - start_time
        
    except pysolr.SolrError as e:
        ret_status = pysolrerror_processing(e)

    except solr.SolrException as e:
        if e is None:
            ret_val = models.ErrorReturn(httpcode=httpCodes.HTTP_400_BAD_REQUEST, error="Solr engine returned an unknown error", error_description=f"Solr engine returned error without a reason")
            logger.error(f"SolrRuntimeError: {e.reason} Body: {e.body}")
            # logger.error(e.body)
        elif e.reason is not None:
            ret_val = models.ErrorReturn(httpcode=e.httpcode, error="Solr engine returned an unknown error", error_description=f"Solr engine returned error {e.httpcode} - {e.reason}")
            logger.error(f"SolrRuntimeError: {e.reason} Body: {e.body}")
            # logger.error(e.body)
        else:
            ret_val = models.ErrorReturn(httpcode=e.httpcode, error="Search syntax error", error_description=f"There's an error in your input (no reason supplied)")
            logger.error(f"SolrRuntimeError: {e.httpcode} Body: {e.body}")
            # logger.error(e.body)
        
        ret_status = (e.httpcode, e) # e has type <class 'solrpy.core.SolrException'>, with useful elements of httpcode, reason, and body, e.g.,

    else: #  search was ok
        try:
            logger.info("Download Search Performed: %s", solr_query_spec.solrQuery.searchQ)
            logger.info("The Filtering: %s", solr_query_spec.solrQuery.filterQ)
            logger.info("Result  Set Size: %s", results.hits)
            logger.info("Return set limit: %s", solr_query_spec.limit)
            logger.info(f"Download Stats Solr Search Time: {total_time}")
            scopeofquery = [solr_query_spec.solrQuery.searchQ, solr_query_spec.solrQuery.filterQ]
    
            if ret_status[0] == 200: 
                documentItemList = []
                rowCount = 0
                for result in results.docs:
                    documentListItem = models.DocumentListItem()
                    #documentListItem = get_base_article_info_from_search_result(result, documentListItem)
                    citeas = result.get("art_citeas_xml", None)
                    citeas = opasQueryHelper.force_string_return_from_various_return_types(citeas)
                    
                    documentListItem.score = result.get("score", None)               
                    # see if this article is an offsite article
                    result["text_xml"] = None                   
                    stat = {}
                    count_all = result.get("art_cited_all", None)
                    if count_all is not None:
                        stat["art_cited_5"] = result.get("art_cited_5", None)
                        stat["art_cited_10"] = result.get("art_cited_10", None)
                        stat["art_cited_20"] = result.get("art_cited_20", None)
                        stat["art_cited_all"] = count_all
    
                    count0 = result.get("art_views_lastcalyear", 0)
                    count1 = result.get("art_views_lastweek", 0)
                    count2 = result.get("art_views_last1mos", 0)
                    count3 = result.get("art_views_last6mos", 0)
                    count4 = result.get("art_views_last12mos", 0)
    
                    if count0 + count1 + count2 + count3+ count4 > 0:
                        stat["art_views_lastcalyear"] = count0
                        stat["art_views_lastweek"] = count1
                        stat["art_views_last1mos"] = count2
                        stat["art_views_last6mos"] = count3
                        stat["art_views_last12mos"] = count4
    
                    if stat == {}:
                        stat = None
    
                    documentListItem.stat = stat
                    documentListItem.docLevel = result.get("art_level", None)
                    rowCount += 1
                    # add it to the set!
                    documentItemList.append(documentListItem)

            responseInfo = models.ResponseInfo(
                                               count = len(results.docs),
                                               fullCount = results.hits,
                                               totalMatchCount = results.hits,
                                               limit = solr_query_spec.limit,
                                               offset = solr_query_spec.offset,
                                               listType="documentlist",
                                               scopeQuery=[scopeofquery], 
                                               fullCountComplete = solr_query_spec.limit >= results.hits,
                                               solrParams = results.raw_response["responseHeader"]["params"],
                                               request=f"{solr_query_spec.urlRequest}",
                                               core=solr_query_spec.core, 
                                               timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)                     
            )
    
            documentListStruct = models.DocumentListStruct( responseInfo = responseInfo, 
                                                            responseSet = documentItemList
                                                            )
    
            documentList = models.DocumentList(documentList = documentListStruct)
    
            ret_val = documentList
            
        except Exception as e:
            logger.error(f"SolrResultsError: Exception: {e}")
            
    logger.info(f"Download Stats Document Return Time: {time.time() - start_time}")
    return ret_val, ret_status
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
def metadata_get_next_and_prev_articles(art_id=None, 
                                        req_url: str=None 
                                       ):
    """
    Return the previous, matching and next article, assuming they all exist.
    The intent is to be able to have next and previous arrows on the articles.
    
    >>> prev, match, next = metadata_get_next_and_prev_articles(art_id="APA.066.0159A")
    >>> prev.get("art_id", None), match.get("art_id", None), next.get("art_id", None)
    ('APA.066.0149A', 'APA.066.0159A', 'APA.066.0167A')
    
    >>> prev, match, next = metadata_get_next_and_prev_articles(art_id="GW.016.0274A")
    >>> prev.get("art_id", None), match.get("art_id", None), next.get("art_id", None)
    ('GW.016.0273A', 'GW.016.0274A', 'GW.016.0276A')
    
    >>> metadata_get_next_and_prev_articles(art_id="GW.016")
    ({}, {}, {})
    
    New: 2020-11-17      
    """
    # returns multiple gw's and se's, 139 unique volumes counting those (at least in 2020)
    # works for journal, videostreams have more than one year per vol.
    # works for books, videostream vol numbers
    
    source_code, source_year, source_vol, source_page, source_page_id = split_article_id(art_id)
    distinct_return = "art_sourcecode, art_year, art_vol, art_id"
    next_art = {}
    prev_art = {}
    match_art = {}
    
    query = "art_level:1 "
    if source_code is not None and source_code.isalpha():
        query += f" && art_sourcecode:{source_code}"

    if source_vol is not None and source_vol.isalnum():
        query += f" && art_vol:{source_vol}"
        
    if source_year is not None and source_year.isalnum():
        query += f" && art_year:{source_year}"
        
    try:
        logger.info(f"Solr Query: q={query}")
        args = {
            "fl": distinct_return,
            "fq": "*:*",
            "sort": "art_id asc",
            "rows": 200
        }

        results = solr_docs2.search(query, **args)

    except Exception as e:
        logger.error(f"MetadataGetArtError: {e}")
    else:
        # find the doc
        count = 0
        for n in results.docs:
            if n["art_id"] == art_id:
                # we found it
                match_art = n
                try:
                    prev_art = results.docs[count-1]
                except:
                    prev_art = {}
                try:
                    next_art = results.docs[count+1]
                except:
                    next_art = {}
            else:
                count += 1
                continue
    
    return prev_art, match_art, next_art
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
def metadata_get_next_and_prev_vols(source_code=None,
                                    source_vol=None,
                                    req_url: str=None 
                                   ):
    """
    Return previous, matched, and next volume for the source code and year.
    New: 2020-11-17

    >>> metadata_get_next_and_prev_vols(source_code="APA", source_vol="66")
    ({'value': '65', 'count': 89, 'year': '2017'}, {'value': '66', 'count': 95, 'year': '2018'}, {'value': '67', 'count': 88, 'year': 'APA'})
    
    >>> metadata_get_next_and_prev_vols(source_code="GW", source_vol="16")
    ({'value': '15', 'count': 1, 'year': '1933'}, {'value': '16', 'count': 1, 'year': '1993'}, None)
    
    >>> metadata_get_next_and_prev_vols(source_code="GW")
    (None, None, None)
    
    >>> metadata_get_next_and_prev_vols(source_vol="66")
    (None, None, None)
    
    >>> metadata_get_next_and_prev_vols(source_code="GW", source_vol=16)
    ({'value': '15', 'count': 1, 'year': '1933'}, {'value': '16', 'count': 1, 'year': '1993'}, None)

    """  
    distinct_return = "art_sourcecode, art_year, art_vol"
    next_vol = None
    prev_vol = None
    match_vol = None
    
    query = "bk_subdoc:false"
    if source_code is None:
        logger.error("MetadataGetVolsError: No source code (e.g., journal code) provided;")
    else:
        query += f" && art_sourcecode:{source_code}"

        if source_vol is None:
            logger.error("MetadataGetVolsError: No vol number provided;")
        else:
            source_vol_int = int(source_vol)
            next_source_vol_int = source_vol_int + 1
            prev_source_vol_int = source_vol_int - 1
            try:
                logger.info(f"Solr Query: q={query}")
                facet_fields = ["art_vol", "art_sourcecode"]
                facet_pivot_fields = "art_sourcecode,art_year,art_vol" # important ...no spaces!
                query += f" && art_vol:({source_vol} || {next_source_vol_int} || {prev_source_vol_int})"
        
                args = {
                    "fl": distinct_return,
                    "fq": "*:*",
                    "sort": "art_sourcecode asc, art_year asc",
                    "facet": "on", 
                    "facet.fields" : facet_fields, 
                    "facet.pivot" : facet_pivot_fields,
                    "facet.mincount" : 1,
                    "facet.sort" : "art_year asc", 
                    #"rows": limit,
                    #"start": offset
                }
        
                results = solr_docs2.search(query, **args)
                logger.info(f"Solr Query: q={query}")
                facet_pivot = results.facets["facet_pivot"][facet_pivot_fields]
                #ret_val = [(piv['value'], [n["value"] for n in piv["pivot"]]) for piv in facet_pivot]
            except Exception as e:
                logger.error(f"MetadataGetVolsError: Exception: {e}")
            else:
                prev_vol = None
                match_vol = None
                next_vol = None
                if facet_pivot != []:
                    next_vol_idx = None
                    prev_vol_idx = None
                    match_vol_idx = None
                    pivot_len = len(facet_pivot[0]['pivot'])
                    counter = 0
                    for n in facet_pivot[0]['pivot']:
                        if n['pivot'][0]['value'] == str(source_vol):
                            match_vol_idx = counter
                            match_vol = n['pivot'][0]
                            match_vol_year = n['value']
                            match_vol['year'] = match_vol_year
                        counter += 1
        
                    if match_vol_idx is not None:
                        if match_vol_idx > 0:
                            prev_vol_idx = match_vol_idx - 1
                            prev_vol = facet_pivot[0]['pivot'][prev_vol_idx]
                            prev_vol_year = prev_vol['value']
                            prev_vol = prev_vol['pivot'][0]
                            prev_vol['year'] = prev_vol_year
                            
                        if match_vol_idx < pivot_len - 1:
                            next_vol_idx = match_vol_idx + 1
                            next_vol = facet_pivot[0]['pivot'][next_vol_idx]
                            next_vol_year = facet_pivot[0]['value']
                            next_vol = next_vol['pivot'][0]
                            next_vol['year'] = next_vol_year
                    else:
                        logger.warning(f"No volume to assess: {match_vol_idx} ")
                        
                try:
                    del(match_vol['field'])
                except:
                    pass
                    
                try:
                    del(prev_vol['field'])
                except:
                    pass
                    
                try:
                    del(next_vol['field'])
                except:
                    pass
    
    return prev_vol, match_vol, next_vol
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
def metadata_get_volumes(source_code=None,
                         source_type=None,
                         req_url: str=None, 
                         limit: int=1000,
                         #offset=0
                        ):
    """
    Return a list of volumes
      - for a specific source_code (code),
      - OR for a specific source_type (e.g. journal)
      - OR if source_code and source_type are not specified, bring back them all
      
    This is a new version (08/2020) using Solr pivoting rather than the OCD database.
      
    """
    # returns multiple gw's and se's, 139 unique volumes counting those (at least in 2020)
    # works for journal, videostreams have more than one year per vol.
    # works for books, videostream vol numbers
    #results = solr_docs.query( q = f"art_sourcecode:{pep_code} && art_year:{year}",  
                                #fields = "art_sourcecode, art_vol, art_year",
                                #sort="art_sourcecode, art_year", sort_order="asc",
                                #fq="{!collapse field=art_vol}",
                                #rows=limit, start=offset
                                #)
    
    distinct_return = "art_sourcecode, art_vol, art_year, art_sourcetype"
    row_limit = 6 # small number, since we don't care about the rows, we care about the facet limit.
    facet_limit = limit, 
    count = 0
    ret_val = None
    # normalize source type
    if source_type is not None: # none is ok
        source_type = opasConfig.normalize_val(source_type, opasConfig.VALS_SOURCE_TYPE, None)
    
    q_str = "bk_subdoc:false"
    if source_code is not None:
        q_str += f" && art_sourcecode:{source_code}"
    if source_type is not None:
        q_str += f" && art_sourcetype:{source_type}"
    facet_fields = ["art_vol", "art_sourcecode"]
    facet_pivot = "art_sourcecode,art_year,art_vol" # important ...no spaces!
    try:
        logger.info(f"Solr Query: q={q_str} facet='on'")
        args = {"fq":"*:*", 
                "fields" : distinct_return,
                "sort":"art_sourcecode ASC, art_year ASC",
                "facet":"on", 
                "facet.fields" : facet_fields, 
                "facet.pivot" : facet_pivot,
                "facet.mincount":1,
                "facet.sort":"art_year asc",
                "facet.limit": facet_limit,
                "rows":row_limit, 
                #"start":offset
              }

        results = solr_docs2.search(q_str, **args)
        
        facet_pivot = results.facets["facet_pivot"][facet_pivot]
        #ret_val = [(piv['value'], [n["value"] for n in piv["pivot"]]) for piv in facet_pivot]

        response_info = models.ResponseInfo( count = count,
                                             fullCount = count,
                                             #limit = limit,
                                             #offset = offset,
                                             listType="volumelist",
                                             fullCountComplete = (limit == 0 or limit >= count),
                                             request=f"{req_url}",
                                             timeStamp = datetime.utcfromtimestamp(time.time()).strftime(TIME_FORMAT_STR)                     
                                             )

        
        volume_item_list = []
        volume_dup_check = {}
        for m1 in facet_pivot:
            journal_code = m1["value"] # pepcode
            seclevel = m1["pivot"]
            for m2 in seclevel:
                secfield = m2["field"] # year
                secval = m2["value"]
                thirdlevel = m2["pivot"]
                for m3 in thirdlevel:
                    thirdfield = m3["field"] # vol
                    thirdval = m3["value"]
                    PEPCode = journal_code
                    year = secval
                    vol = thirdval
                    count = m3["count"]
                    pep_code_vol = PEPCode + vol
                    # if it's a journal, Supplements are not a separate vol, they are an issue.
                    if pep_code_vol[-1] == "S" and journal_code not in opasConfig.BOOK_CODES_ALL:
                        pep_code_vol = pep_code_vol[:-1]
                    cur_code = volume_dup_check.get(pep_code_vol)
                    if cur_code is None:
                        volume_dup_check[pep_code_vol] = [year]
                        volume_list_item = models.VolumeListItem(PEPCode=PEPCode,
                                                                 vol=vol,
                                                                 year=year,
                                                                 years=[year],
                                                                 count=count
                        )
                        volume_item_list.append(volume_list_item)
                    else:
                        volume_dup_check[pep_code_vol].append(year)
                        if year not in volume_list_item.years:
                            volume_list_item.years.append(year)
                        volume_list_item.count += count

                
    except Exception as e:
        logger.error(f"MetadataGetVolsError: {e}")
    else:
        response_info.count = len(volume_item_list)
        response_info.fullCount = len(volume_item_list)
    
        volume_list_struct = models.VolumeListStruct( responseInfo = response_info, 
                                                      responseSet = volume_item_list
                                                      )
    
        volume_list = models.VolumeList(volumeList = volume_list_struct)
    
        ret_val = volume_list
        
    return ret_val
#-----------------------------------------------------------------------------
def prep_document_download(document_id,
                           session_info=None, 
                           ret_format="HTML",
                           base_filename="opasDoc",
                           flex_fs=None):
    """
    Preps a file in the right format for download.  Returns the filename of the prepared file and the status.
    Note:
       Checks access with the auth server via opasDocPerm.get_access_limitations
           - If access not permitted, this returns an error (and None for the filename)
           - If access allowed, it returns with the document itself

    >>> a = prep_document_download("IJP.051.0175A", ret_format="html") 

    >>> a = prep_document_download("IJP.051.0175A", ret_format="epub") 


    """
    def add_epub_elements(str):
        # for now, just return
        return str

    ret_val = None
    status = models.ErrorReturn(httpcode=httpCodes.HTTP_200_OK) # no error

    query = "art_id:%s" % (document_id)
    args = {
             "fl": """art_id, art_citeas_xml, text_xml, art_excerpt, art_sourcetype, art_year,
                      art_sourcetitleabbr, art_vol, art_iss, art_pgrg, art_doi, art_title, art_authors, art_authors_mast, art_lang,
                      art_issn, file_classification"""
    }

    try:
        results = solr_docs2.search(query, **args)
    except Exception as e:
        logger.error(f"PrepDownloadError: Solr Search Exception: {e}")
    else:
        try:
            art_info = results.docs[0]
            docs = art_info.get("text_xml", art_info.get("art_excerpt", None))
        except IndexError as e:
            logger.error("Download Request: No matching document for %s.  Error: %s", document_id, e)
        except KeyError as e:
            logger.error("Download Request: No full-text content found for %s.  Error: %s", document_id, e)
        else:
            try:    
                if isinstance(docs, list):
                    doc = docs[0]
                else:
                    doc = docs
            except Exception as e:
                logger.error("PrepDownloadError: Empty return: %s", e)
            else:
                doi = art_info.get("art_doi", None)
                pub_year = art_info.get("art_year", None)
                art_title = art_info.get("art_title", None)
                art_lang = art_info.get("art_lang", "en")
                art_citeas_xml = art_info.get("art_citeas_xml", None)
                art_authors_mast = art_info.get("art_authors_mast", "PEP")
                art_authors = art_authors_mast
                    
                if art_citeas_xml is not None:
                    art_citeas = opasxmllib.xml_elem_or_str_to_text(art_citeas_xml)
                else:
                    art_citeas = ""
                    
                file_classification = art_info.get("file_classification", None)
                
                access = opasDocPerm.get_access_limitations( doc_id=document_id,
                                                             classification=file_classification,
                                                             session_info=session_info,
                                                             year=pub_year,
                                                             doi=doi,
                                                             fulltext_request=True
                                                            )
                if access.accessLimited != True:
                    try:
                        heading = opasxmllib.get_running_head( source_title=art_info.get("art_sourcetitleabbr", ""),
                                                               pub_year=pub_year,
                                                               vol=art_info.get("art_vol", ""),
                                                               issue=art_info.get("art_iss", ""),
                                                               pgrg=art_info.get("art_pgrg", ""),
                                                               ret_format="HTML"
                                                               )
        
                        if ret_format.upper() == "HTML":
                            html = opasxmllib.remove_encoding_string(doc)
                            filename = opasxmllib.convert_xml_to_html_file(html, output_filename=document_id + ".html")  # returns filename
                            ret_val = filename
                        elif ret_format.upper() == "PDFORIG":
                            # setup so can include year in path (folder names) in AWS, helpful.
                            if flex_fs is not None:
                                pub_year = art_info.get("art_year", None)
                                filename = flex_fs.get_download_filename(filespec=document_id, path=localsecrets.PDF_ORIGINALS_PATH, year=pub_year, ext=".pdf")    
                                ret_val = filename
                            else:
                                err_msg = "Prep PDFORIG for Download: File system setup error."
                                #logger.error(err_msg) # eliminate double log? 2021-06-02
                                status = models.ErrorReturn( httpcode=httpCodes.HTTP_400_BAD_REQUEST,
                                                             error_description=err_msg
                                                           )
                                ret_val = None
                        elif ret_format.upper() == "PDF":
                            html_string = opasxmllib.xml_str_to_html(doc)
                            html_string = re.sub("\[\[RunningHead\]\]", f"{heading}", html_string, count=1)
                            html_string = re.sub("()", f"", html_string, count=1) # in running head, missing issue
                            copyright_page = COPYRIGHT_PAGE_HTML.replace("[[username]]", session_info.username)
                            html_string = re.sub("</html>", f"{copyright_page}</html>", html_string, count=1)
                            if art_lang == "zh":
                                # add some spaces in the chinese text to permit wrapping:
                                html_string = re.sub('\。', '。 ', html_string)
                                html_string = re.sub('\，', '， ', html_string)
                                html_string = re.sub('\“', ' “', html_string)
                                #if 0: #old way
                                    #lines = html_string.split("\n")
                                    #new_str = ""
                                    #for line in lines:
                                        #line = opasgenlib.split_long_lines(line, max_len, '\。', "。 \n")
                                        #line = opasgenlib.split_long_lines(line, max_len, '\，', "， \n")
                                        #line = opasgenlib.split_long_lines(line, max_len, '\“', "\n “")
                                        #new_str += line + "\n"
                                    #html_string = new_str
                                html_string = html_string.replace("</head>", opasConfig.PDF_CHINESE_STYLE + "</head>")
                            else:
                                # PDF Font to support Turkish and English (Extended Character Font)
                                html_string = html_string.replace("</head>", opasConfig.PDF_OTHER_STYLE + "</head>")
                                
                            # html_string.encode("UTF-8")
                            filename = document_id + ".PDF" 
                            output_filename = os.path.join(tempfile.gettempdir(), filename)
                            pisa.showLogging() # debug only
                            #print (f"In Print Module.  Folder {os.getcwd()}")
                            #print (f"{opasConfig.PDF_EXTENDED_FONT}")
                            # doc = opasxmllib.remove_encoding_string(doc)
                            # open output file for writing (truncated binary)
                            result_file = open(output_filename, "w+b")
                            # Need to fix links for graphics, e.g., see https://xhtml2pdf.readthedocs.io/en/latest/usage.html#using-xhtml2pdf-in-django
                            pisaStatus = pisa.CreatePDF(src=html_string,            # the HTML to convert
                                                        dest=result_file,
                                                        encoding="UTF-8",
                                                        link_callback=opasConfig.fetch_resources)           # file handle to receive result
                            # close output file
                            result_file.close()                 
                            # return True on success and False on errors
                            ret_val = output_filename
                                
                        elif ret_format.upper() == "EPUB":
                            doc = opasxmllib.remove_encoding_string(doc)
                            html_string = opasxmllib.xml_str_to_html(doc)
                            html_string = re.sub("\[\[RunningHead\]\]", f"{heading}", html_string, count=1)
                            html_string = add_epub_elements(html_string)
                            filename = opasxmllib.html_to_epub(htmlstr=html_string,
                                                               output_filename_base=document_id,
                                                               art_id=document_id,
                                                               lang=art_lang, 
                                                               authors=art_authors, 
                                                               html_title=art_title,
                                                               citeas=art_citeas, 
                                                               session_info=session_info)
                            ret_val = filename
                        else:
                            err_msg = f"Download format {ret_format} not supported"
                            #logger.warning(err_msg) # eliminate double log? 2021-06-02
                            ret_val = None
                            status = models.ErrorReturn( httpcode=httpCodes.HTTP_400_BAD_REQUEST,
                                                         error_description=err_msg
                                                       )
        
                    except Exception as e:
                        err_msg = f"Prep for Download: Can't convert data: {e}"
                        ret_val = None
                        #raise HTTPException(
                            #status_code=httpCodes.HTTP_422_UNPROCESSABLE_ENTITY,
                            #detail=err_msg # or use ERR_MSG_PROBLEM_CONVERTING_TO_PDF
                        #)
                        status = models.ErrorReturn( httpcode=httpCodes.HTTP_422_UNPROCESSABLE_ENTITY,
                                                     error_description=err_msg
                                                   )
                else: # access is limited
                    err_msg = f"No permission to download document {document_id} for user {session_info}"
                    #logger.warning(err_msg) # eliminate double log? 2021-06-02
                    status = models.ErrorReturn( httpcode=httpCodes.HTTP_401_UNAUTHORIZED,
                                                 error_description=err_msg
                                               )
                    ret_val = None
    
    return ret_val, status

#-----------------------------------------------------------------------------
def get_fulltext_from_search_results(result,
                                     text_xml,
                                     page,
                                     page_offset,
                                     page_limit,
                                     documentListItem: models.DocumentListItem,
                                     format_requested="HTML",
                                     return_options=None):

    child_xml = None
    offset = 0
    if documentListItem.sourceTitle is None:
        documentListItem = opasQueryHelper.get_base_article_info_from_search_result(result, documentListItem)
        
    #if page_limit is None:
        #page_limit = opasConfig.DEFAULT_PAGE_LIMIT

    documentListItem.docPagingInfo = {}    
    documentListItem.docPagingInfo["page"] = page
    documentListItem.docPagingInfo["page_limit"] = page_limit
    documentListItem.docPagingInfo["page_offset"] = page_offset

    fullText = result.get("text_xml", None)
    text_xml = opasQueryHelper.force_string_return_from_various_return_types(text_xml)
    if text_xml is None:  # no highlights, so get it from the main area
        try:
            text_xml = fullText
        except:
            text_xml = None

    elif fullText is not None:
        if len(fullText) > len(text_xml):
            logger.warning("Warning: text with highlighting is smaller than full-text area.  Returning without hit highlighting.")
            text_xml = fullText

    if text_xml is not None:
        reduce = False
        # see if an excerpt was requested.
        if page is not None and page <= int(documentListItem.pgEnd) and page > int(documentListItem.pgStart):
            # use page to grab the starting page
            # we've already done the search, so set page offset and limit these so they are returned as offset and limit per V1 API
            offset = page - int(documentListItem.pgStart)
            reduce = True
        # Only use supplied offset if page parameter is out of range, or not supplied
        if reduce == False and page_offset is not None and page_offset != 0: 
            offset = page_offset
            reduce = True

        if reduce == True or page_limit is not None:
            # extract the requested pages
            try:
                temp_xml = opasxmllib.xml_get_pages(xmlstr=text_xml,
                                                    offset=offset,
                                                    limit=page_limit,
                                                    pagebrk="pb",
                                                    inside="body",
                                                    env="body")
                temp_xml = temp_xml[0]
                
            except Exception as e:
                logger.error(f"GetFulltextError: Page extraction from document failed. Error: {e}.  Keeping entire document.")
            else: # ok
                text_xml = temp_xml
    
        if return_options is not None:
            if return_options.get("Glossary", None) == False:
                # remove glossary markup
                text_xml = opasxmllib.remove_glossary_impx(text_xml)   
    
    try:
        format_requested_ci = format_requested.lower() # just in case someone passes in a wrong type
    except:
        format_requested_ci = "html"

    if documentListItem.docChild != {} and documentListItem.docChild is not None:
        child_xml = documentListItem.docChild["para"]
    else:
        child_xml = None
    
    if text_xml is None and child_xml is not None:
        text_xml = child_xml
        
    try:
        #ret_val.documents.responseSet[0].hitCriteria = urllib.parse.unquote(search) 
        # remove nuisance stop words from matches
        text_xml = remove_nuisance_word_hits(text_xml)
    except Exception as e:
        logger.error(f"GetFulltextError: Error removing nuisance hits: {e}")

    try:
        documentListItem.hitList = list_all_matches_with_loc(text_xml)
        documentListItem.hitCount = len(documentListItem.hitList)
    except Exception as e:
        logger.error(f"GetFulltextError: Error saving hits and count: {e}")
    
    try:
        matches = re.findall(f"class='searchhit'|{opasConfig.HITMARKERSTART}", text_xml)
    except Exception as e:
        logger.warning(f"Exception.  Could not count matches. {e}")
        documentListItem.termCount = 0
    else:
        documentListItem.termCount = len(matches)

    if format_requested_ci == "html":
        # Convert to HTML
        heading = opasxmllib.get_running_head( source_title=documentListItem.sourceTitle,
                                               pub_year=documentListItem.year,
                                               vol=documentListItem.vol,
                                               issue=documentListItem.issue,
                                               pgrg=documentListItem.pgRg,
                                               ret_format="HTML"
                                               )
        try:
            text_xml = opasxmllib.xml_str_to_html(text_xml)  #  e.g, r"./libs/styles/pepkbd3-html.xslt"
        except Exception as e:
            logger.error(f"GetFulltextError: Could not convert to HTML {e}; returning native format")
            text_xml = re.sub(f"{opasConfig.HITMARKERSTART}|{opasConfig.HITMARKEREND}", numbered_anchors, text_xml)
        else:
            try:
                global count_anchors
                count_anchors = 0
                text_xml = re.sub(f"{opasConfig.HITMARKERSTART}|{opasConfig.HITMARKEREND}", numbered_anchors, text_xml)
                text_xml = re.sub("\[\[RunningHead\]\]", f"{heading}", text_xml, count=1)
            except Exception as e:
                logger.error(f"GetFulltextError: Could not do anchor substitution {e}")

        if child_xml is not None:
            child_xml = opasxmllib.xml_str_to_html(child_xml)
                
    elif format_requested_ci == "textonly":
        # strip tags
        text_xml = opasxmllib.xml_elem_or_str_to_text(text_xml, default_return=text_xml)
        if child_xml is not None:
            child_xml = opasxmllib.xml_elem_or_str_to_text(child_xml, default_return=text_xml)
    elif format_requested_ci == "xml":
        # don't do this for XML
        pass
        # text_xml = re.sub(f"{opasConfig.HITMARKERSTART}|{opasConfig.HITMARKEREND}", numbered_anchors, text_xml)
        # child_xml = child_xml

    documentListItem.document = text_xml
                
    if child_xml is not None:
        # return child para in requested format
        documentListItem.docChild['para'] = child_xml

    return documentListItem

#-----------------------------------------------------------------------------
def quick_docmeta_docsearch(q_str,
                            fields=None,
                            req_url=None, 
                            limit=10,
                            offset=0):
    """
    Searches per query string and returns a document List
    """
    ret_val = None
    count = 0
    if fields is None:
        fields = opasConfig.DOCUMENT_ITEM_SUMMARY_FIELDS
        
    args = {
               'fl':fields,
               'rows':limit,
               'start': offset,
               'sort':"art_id asc",
           }
    
    results = solr_docs2.search(q=q_str, **args)
    document_item_list = []
    count = len(results)
    try:
        for result in results:
            documentListItem = models.DocumentListItem()
            documentListItem = opasQueryHelper.get_base_article_info_from_search_result(result, documentListItem)
            document_item_list.append(documentListItem)
    except IndexError as e:
        logger.warning("No matching entry for %s.  Error: %s", (q_str, e))
    except KeyError as e:
        logger.warning("No content found for %s.  Error: %s", (q_str, e))
    else:
        ret_val = document_item_list

    return ret_val, count

#-----------------------------------------------------------------------------
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
