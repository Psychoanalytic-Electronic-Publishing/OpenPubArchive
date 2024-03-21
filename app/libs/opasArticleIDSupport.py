#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__      = "Neil R. Shapiro"
__copyright__   = "Copyright 2019-2021, Psychoanalytic Electronic Publishing"
__license__     = "Apache 2.0"

import re
import os
import logging
logger = logging.getLogger(__name__)
from loggingDebugStream import log_everywhere_if

import string
import opasGenSupportLib as opasgenlib
import time
from typing import Optional # List, Generic, TypeVar
from datetime import datetime
import opasXMLHelper as opasxmllib
import opasLocator
import html
import json
# The following four functions moved from opasConfig - 2022-06-05
from pydantic.main import BaseModel, Field # ValidationError, validator, Extra

import lxml
from lxml import etree
parser = lxml.etree.XMLParser(encoding='utf-8', recover=True, resolve_entities=True, load_dtd=True)

import opasConfig
from configLib.opasIJPConfig import IJPOPENISSUES
from configLib.opasCoreConfig import EXTENDED_CORES
import opasFileSupport
import localsecrets
# import smartsearchLib # not needed without newer exists_with_resilience, now commented out

fs = opasFileSupport.FlexFileSystem(key=localsecrets.S3_KEY, secret=localsecrets.S3_SECRET, root=localsecrets.XML_ORIGINALS_PATH)

SUPPLEMENT_ISSUE_SEARCH_STR = "Supplement" # this is what will be searched in "art_iss" for supplements

import opasProductLib
sourceDB = opasProductLib.SourceInfoDB()

gDbg2 = True

def parse_glossary_terms_dict(glossary_terms_dict_str, verbose=False):
    
    m = re.search("<!--.*?(?P<dict_str>\{.*\}).*?-->", glossary_terms_dict_str)
    dict_str = m.group("dict_str")
    glossary_terms_dict = json.loads(dict_str)
    
    ret_val = glossary_terms_dict
       
    return ret_val

def parse_volume_code(vol_code: str, source_code: str=None): 
    """
    PEP Volume numbers in IDS can be numbers or suffixed by an issue code--we use them after a volume number
    when a journal repeats pagination from issue to issue or starts the pagination over in a Supplement.
    
    >>> parse_volume_code("34S")
    ('34', 'S')
    >>> parse_volume_code("101C")
    ('101', 'C')
    >>> parse_volume_code("130")
    ('130', None)
       
    """
    ret_val = ("*", None)
    if vol_code is not None:
        m = re.match("\(*(?P<vol>[0-9]+)(?P<issuecode>[A-z]+)?\)*", vol_code)
        if m is not None:
            vol = m.group("vol")
            vol = vol.lstrip("0")
            issuecode = m.group("issuecode") 
            ret_val = vol, issuecode

    return ret_val    

def parse_issue_code(issue_code: str, source_code=None, vol=None): 
    """
    Issue codes are PEP unique--we use them after a volume number when a journal repeats pagination
    from issue to issue or starts the pagination over in a Supplement.
    
    Source code and volume can be used to handle sources that are "exceptions" to rules (unfortunately)
    
    """
    ret_val = "*"
    if issue_code is not None and issue_code.isalpha():
        issue_code = issue_code.upper()
        if issue_code[0] != "S" or (source_code == "FA" and vol == 1):
            ret_val = string.ascii_uppercase.index(issue_code[0]) # A==0, B==1
            ret_val += 1 # now A==1
            ret_val = str(ret_val)
        elif issue_code[0] == 'S':
            ret_val = SUPPLEMENT_ISSUE_SEARCH_STR # supplement
        else:
            ret_val = "*" # not recognized, allow any issue
            
    elif issue_code.isdecimal():
        if type(issue_code) == "int":
            ret_val = str(issue_code)
        else:
            ret_val = issue_code
    return ret_val    

def parse_artid_from_filename(filename):
    """    
    >>> nm = "RPP-CS.019A.0072A(bKBD3).xml"
    >>> parse_artid_from_filename(nm)
    'RPP-CS.019A.0072A'
    >>> nm = r"X:\AWS_S3\AWS PEP-Web-Live-Data\_PEPArchive\RPP-CS\019.2017\RPP-CS.019A.0027A(bEXP_ARCH1).xml"
    >>> parse_artid_from_filename(nm)
    'RPP-CS.019A.0027A'
    
    >>> nm = r"X:/AWS_S3/AWS PEP-Web-Live-Data/_PEPArchive/RPP-CS/019.2017/RPP-CS.019A.9027A(bKBD3).xml"
    >>> parse_artid_from_filename(nm)
    'RPP-CS.019A.9027A'
    """
    ret_val = None
    try:
        if filename is not None:
            base = os.path.basename(filename)
            artID = os.path.splitext(base)[0]
            m = re.match(r"([^ ]*).*\(.*\)", artID)
            artID = m.group(1)
            artID = artID.upper()
            artID = artID.replace(".EMBARGOED", "")
            ret_val = artID
    except Exception as e:
        log_everywhere_if(1, "warning", f"Error parsing artid from filename {filename} - {e}")

    return ret_val    

class ArticleID(BaseModel):
    """
    This is a pydantic model for OPAS Article IDs
    
    Article IDs (document IDs) are at the core of the system.  In PEP's design, article IDs are meaningful,
    and can be broken apart to learn about the content metadata.
    
    But when designed as such, the structure of the article IDs may be different in different systems, so it needs to be configurable as possible.
    This routine is a start of allowing that to be defined as part of the customization. 

    >>> a = ArticleID(art_id="PSYCHE.025L.R0007A", checks="R", verbose=False)
    >>> print (a.art_id)
    PSYCHE.025L.R0007
    
    >>> a = ArticleID(art_id="ipl.055.0001a", checks="R", verbose=False)
    >>> print (a.art_id)
    IPL.055.0000A
    
    >>> a = ArticleID(art_id="FA.004.0455A", checks="R", verbose=False)
    >>> print (a.art_id)
    FA.004C.0455A
    
    >>> a = ArticleID(art_id="FA.004C.0455", checks="R", verbose=False)
    >>> print (a.art_id)
    FA.004C.0455A
    
    >>> a = ArticleID(art_id="APA.065E.0819A")
    >>> print (a.art_issue_alpha_code)
    E
    >>> print (a.alt_no_vol_suffix)
    APA.065.0819A

    >>> art_id="AJRPP.004(1).R0007A"
    >>> a = ArticleID(art_id=art_id)
    >>> print (f"Input: {art_id} Result: {a.art_issue_int}")
    Input: AJRPP.004(1).R0007A Result: 1
    >>> print (a.art_id)
    AJRPP.004.R0007A   
    >>> a.standardized
    'AJRPP.004.R0007A'

    >>> a = ArticleID(art_id="AJRPP.004S.R0007A")
    >>> print (a.art_issue_alpha_code)
    S
    >>> a.art_vol_int
    4
    >>> a.is_roman
    True
    >>> print (a.art_id)
    AJRPP.004S.R0007A
    >>> a.art_pgstart_int
    7
    >>> a.standardized
    'AJRPP.004S.R0007A'

    >>> a = ArticleID(art_id="FA.001A.0005A")
    >>> print (a.art_issue_alpha_code)
    A
    >>> print (a.art_issue_int)
    1

    >>> a = ArticleID(art_id="AJRPP.004.0007A:0.99")
    >>> print (a.articleidinfo)
    {'source_code': 'AJRPP', 'vol_str': '004', 'vol_numeric': '004', 'vol_suffix': '', 'vol_wildcard': '', 'issue_nbr': '', 'page': '0007A', 'special_section': '', 'roman': '', 'page_numeric': '0007', 'page_suffix': 'A', 'page_wildcard': '', 'match_probability': '0.99'}
    >>> print (a.standardized)
    AJRPP.004.0007A

    >>> a = ArticleID(art_id="AJRPP.004.0007A")
    >>> print (a.articleidinfo)
    {'source_code': 'AJRPP', 'vol_str': '004', 'vol_numeric': '004', 'vol_suffix': '', 'vol_wildcard': '', 'issue_nbr': '', 'page': '0007A', 'special_section': '', 'roman': '', 'page_numeric': '0007', 'page_suffix': 'A', 'page_wildcard': '', 'match_probability': ''}

    >>> a = ArticleID(art_id="MPSA.043.0117A")
    >>> print (a.alt_standard) # NOTE: not inserting wildcard ? for vol suffix any more as of 2023 (was MPSA.043?.0117A)
    MPSA.043.0117A
    
    >>> a = ArticleID(art_id="AJRPP.004A.0007A")
    >>> print (a.art_vol_str)
    004A

    >>> a = ArticleID(art_id="AJRPP.*.*")
    >>> a.standardized
    'AJRPP.*.*'

    >>> a = ArticleID(art_id="IJP.034.*")
    >>> a.standardized
    'IJP.034.*'

    >>> a = ArticleID(art_id="IJP.*.0001A")
    >>> a.standardized
    'IJP.*.*'

    >>> a = ArticleID(art_id="BADSTUFF")
    >>> a.is_ArticleID
    False
    >>> print (a)
    BADSTUFF

    Handle Special Naming
    >>> a = ArticleID(art_id="APA.062.NP0016A(bKBD3).xml")
    >>> print (a.art_id)
    APA.062.NP0016A

    >>> a = ArticleID(art_id="IJP.001.E0001A")
    >>> print (a.art_id)
    IJP.001.E0001A


    """
    #************************************************************************************
    # pydantic model - object definitions for ArticleID       
    #************************************************************************************
    art_id: str = Field(None, title="As submitted ID, if it's a valid ID")
    art_id_exists: bool = Field(False, title="True if article id exists in Solr")
    art_id_replaced: str = Field(None, title="Validated version of submitted ID was this before")
    articleidinfo: dict = Field(None, title="Regex result scanning input articleID")
    standardized: str = Field(None, title="Standard form of article (document) ID, volume suffix included if volume includes repeat page #s or for supplements")
    alt_standard: str = Field(None, title="Alternate form of article (document) ID from 2020 (most without volume suffix)")
    alt_wild_standard: str = Field(None, title="Alternate form of article (document) ID from 2020 (fix for missing volume suffix)")
    alt_no_vol_suffix: str = Field(None, title="Alternate form of article (document) ID without volume suffix (issue letter code)")
    is_ArticleID: bool = Field(False, title="True if initialized value is an article (document) ID")
    src_code: str = Field(None, title="Source material assigned code (e.g., journal, book, or video source code)")
    # volumeStr: str = Field(None, title="")
    art_vol_suffix: str = Field(None, title="")
    # volumeWildcardOverride: str = Field(None, title="")
    art_vol_int: int = Field(0, title="")
    art_vol_str: str = Field(None, title="Volume number padded to 3 digits and issuecode if repeating pages or supplement")
    art_issue_alpha_code: str = Field(None, title="Suffix after volume indicating issue number alphabetically")
    is_supplement: bool = Field(False, title="")
    art_issue: str = Field(None, title="")
    art_issue_int: Optional[int] = Field(default=None, title="")
    art_pgstart: str = Field(None, title="")
    art_pgstart_int: int = Field(default=0, title="")
    # pageWildcard: str = Field(None, title="")
    match_probability: float = Field(None, title="For rx link article ids, there may be a probability associated with it, this is it, from 0-1")
    roman_prefix: str = Field("", title="R if start page number in ID is roman")
    is_roman: bool = Field(False, title="")
    page_suffix: str = Field(None, title="")
    special_section_prefix: str = Field("", title="")
    is_special_section: bool = Field(False, title="")
    checks: str = Field(None, title="None for no checks, E for simple exists check, R for repair/resilience")
    # allInfo: bool = Field(False, title="Show all captured information, e.g. for diagnostics")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        regex_article_id = "(?P<source_code>[A-Z\-]{2,13})\.(?P<vol_str>(((?P<vol_numeric>[0-9]{3,4})(?P<vol_suffix>[A-Z]?))|(?P<vol_wildcard>\*)))(\((?P<issue_nbr>[0-9]{1,3})\))?\.(?P<page>((?P<special_section>(NP|E|C|I)?)(?P<roman>R?)(((?P<page_numeric>([0-9]{4,4}))(?P<page_suffix>[A-Z]?))|(?P<page_wildcard>\*))))(:(?P<match_probability>[01]\.[0-9]{0,9}))?"
        volumeWildcardOverride = ''
        m = re.match(regex_article_id, self.art_id, flags=re.IGNORECASE)
        if m is not None:
            self.articleidinfo = m.groupdict("")
            match_probability =self.articleidinfo.get("match_probability")
            if match_probability:
                self.match_probability = match_probability
            else:
                self.match_probability = None
            self.src_code = self.articleidinfo.get("source_code")
            # See if it has issue number numerically in ()
            self.art_issue = self.articleidinfo.get("issue_nbr") # default for groupdict is ''
            self.art_issue_int = opasgenlib.str_to_int(self.art_issue, default=None)
            if self.art_issue == "0":
                self.art_issue = ""
            self.art_vol_suffix = self.articleidinfo.get("vol_suffix", "")
            altVolSuffix = ""
            if self.art_vol_suffix != "":
                self.art_issue_alpha_code  = self.art_vol_suffix[0]  # sometimes it says supplement!
            else:
                self.art_issue_alpha_code = ""
                if self.art_issue_int is not None:
                    if self.art_issue_int > 0:
                        altVolSuffix = string.ascii_uppercase[self.art_issue_int-1]

            if self.art_vol_suffix == "?":
                self.art_vol_suffix = ""

            if self.art_issue_alpha_code != "":
                # an issue code was specified (but not supplement or "S")
                converted = parse_issue_code(self.art_issue_alpha_code, source_code=self.src_code, vol=self.art_vol_int)
                if converted.isdecimal() and self.art_issue_int is None:
                    self.art_issue = converted
                    self.art_issue_int = int(converted)

            self.art_vol_int = self.articleidinfo.get("vol_numeric") 
            if self.art_vol_int != '': # default for groupdict is ''
                self.art_vol_int = int(self.art_vol_int)
                # make sure str is at least 3 places via zero fill
                self.art_vol_str = format(self.art_vol_int, '03')
                if self.art_issue_alpha_code != "":
                    self.art_vol_str += self.art_issue_alpha_code # covers journals with repeating pages
            else:
                self.art_vol_int = 0

            volumeWildcardOverride = self.articleidinfo.get("vol_wildcard")
            if volumeWildcardOverride != '':
                self.art_vol_str = volumeWildcardOverride
                
            self.is_supplement = self.art_issue_alpha_code == "S" and self.src_code != "FA"
            if self.is_supplement:
                # supplements do not have an issue int!
                self.art_issue_int = None
                    
            # page info
            self.art_pgstart = self.articleidinfo.get("page_numeric")
            self.art_pgstart_int = self.art_pgstart 
            if self.art_pgstart_int != '':
                self.art_pgstart_int = int(self.art_pgstart_int)
                self.art_pgstart = format(self.art_pgstart_int, '04')
            else:
                self.art_pgstart_int = 0
                
            pageWildcard = self.articleidinfo.get("page_wildcard")
            if pageWildcard != '':
                self.art_pgstart = pageWildcard
            
            self.special_section_prefix = self.articleidinfo.get("special_section", "")
            self.is_special_section = not opasgenlib.is_empty(self.special_section_prefix)
            
            self.roman_prefix = self.articleidinfo.get("roman", "")  
            self.is_roman = self.roman_prefix.upper() == "R"
            self.page_suffix = self.articleidinfo.get("page_suffix", "")
            
            if not self.art_vol_str[-1].isalpha() and self.art_issue_alpha_code != "":
                self.standardized = f"{self.src_code}.{self.art_vol_str}{self.art_issue_alpha_code}"
            else:
                self.standardized = f"{self.src_code}.{self.art_vol_str}"
                
            self.alt_standard = f"{self.src_code}.{self.art_vol_str}"
            if not self.art_issue_alpha_code:
                # there's no alpha issue code in the standard one. Try adding one:
                if altVolSuffix != "" and altVolSuffix != "?" and not self.art_vol_str[-1].isalpha():
                    self.alt_standard = f"{self.src_code}.{self.art_vol_str}{altVolSuffix}"
            else:
                self.alt_no_vol_suffix = f"{self.src_code}.{self.art_vol_str[:-1]}"
            
            
            if self.alt_wild_standard is None and not self.art_vol_str[-1].isalpha():
                altVolWildSuffix = "?"
                self.alt_wild_standard = f"{self.src_code}.{self.art_vol_str}{altVolWildSuffix}"
                
            if volumeWildcardOverride == '':
                if pageWildcard == '':
                    page_addon = f".{self.special_section_prefix}{self.roman_prefix}{self.art_pgstart}{self.page_suffix}"
                    self.standardized += page_addon
                    self.alt_standard += page_addon
                    if self.alt_no_vol_suffix is not None:
                        self.alt_no_vol_suffix += page_addon
                    if self.alt_wild_standard is not None:
                        self.alt_wild_standard += page_addon
                    #self.standardizedPlusIssueCode += f".{self.roman_prefix}{self.pageNbrStr}{self.pageSuffix}"
                else:
                    self.standardized += f".*"
                    self.alt_standard += f".*"
                    #self.standardizedPlusIssueCode += f".*"
            else:
                self.standardized += f".*"
                self.alt_standard += f".*"
                #self.standardizedPlusIssueCode += f".*"
            
            # always should be uppercase
            self.standardized = self.standardized.upper()
            self.is_ArticleID = True
            self.art_id = self.standardized
            if self.checks:
                if self.checks[0].upper() == "R":
                    self.exists_with_resilience(resilient=True)
                else:
                    self.exists_with_resilience(resilient=False)
            pass
        else:
            self.is_ArticleID = False   
    
    def __str__(self):
        return str(self.art_id)


    def exists_with_resilience(self, solrcon=None, resilient=False, verbose=False):
        """
        Search Solr for the article ID, if not found, try some common variations:
          - the ArticleID alt_standard
          or
          - the simple missing page suffix 'A'
          or
          - one of the volume variant letters (issue)
          or
          - Page 0 vs 1 to handle split book variation
        
        Returns:
            - the current art_id exists
            - None if it doesn't
            - OR a heuristically modified existant art_id if resilient=True and
              a close match could be resolved
              
        Side Effects:
            - art_id changed if resilient id found
            - art_id_replaced stores the replaced id
            
        """
        ret_val = None
        if self.art_id_exists:
            ret_val = self.art_id
        elif resilient:
            doc_id = self.art_id
            if solrcon is None:
                solrcon = EXTENDED_CORES.get("pepwebdocs")
            
            try:
                results = solrcon.search(q = f"art_id:{doc_id}")
                if results.raw_response["response"]["numFound"] > 0:
                    self.art_id_exists = True
                    ret_val = doc_id
                else: # try variations
                    # TryAlternateID:
                    alt_id = self.alt_standard
                    results = solrcon.search(q = f"art_id:{alt_id}")
                    if results.raw_response["response"]["numFound"] > 0:
                        log_everywhere_if(verbose, "debug", f"Document ID {doc_id} not in Solr.  The correct ID seems to be {alt_id}. Using that instead!")
                        self.art_id_replaced = doc_id
                        self.art_id_exists = True
                        self.art_id = alt_id
                        ret_val = alt_id
                        
                    if not ret_val and doc_id[-1].isnumeric():
                        # missing page variant?
                        alt_id = doc_id + "A"
                        results = solrcon.search(q = f"art_id:{alt_id}")
                        if results.raw_response["response"]["numFound"] == 1:  # only accept alternative if there's only one match (otherwise, not known which)
                            # odds are good this is what was cited.
                            log_everywhere_if(verbose, "debug", f"Document ID {doc_id} not in Solr.  The correct ID seems to be {alt_id}. Using that instead!")
                            self.art_id_replaced = doc_id
                            self.art_id_exists = True
                            self.art_id = alt_id
                            ret_val = alt_id
                    
                    if not ret_val and doc_id[-1].isalpha():
                        # try without page variant?
                        alt_id = doc_id[:-1]
                        results = solrcon.search(q = f"art_id:{alt_id}")
                        if results.raw_response["response"]["numFound"] == 1:  # only accept alternative if there's only one match (otherwise, not known which)
                            # odds are good this is what was cited.
                            log_everywhere_if(verbose, "debug", f"Document ID {doc_id} not in Solr.  The correct ID seems to be {alt_id}. Using that instead!")
                            self.art_id_replaced = doc_id
                            self.art_id_exists = True
                            self.art_id = alt_id
                            ret_val = alt_id
                    
                    if not ret_val:
                        # match volume variant?
                        alt_id = self.alt_wild_standard
                        if alt_id is not None:
                            results = solrcon.search(q = f"art_id:{alt_id}")
                            count = results.raw_response["response"]["numFound"]
                            if count == 1:  # only accept alternative if there's only one match (otherwise, not known which)
                                # odds are good this is what was cited.
                                alt_id = results.docs[0]['art_id']
                                log_everywhere_if(verbose, "debug", f"Document ID {doc_id} not in Solr.  The correct ID seems to be {alt_id}. Using that instead!")
                                self.art_id_replaced = doc_id
                                self.art_id_exists = True
                                ret_val = self.art_id = alt_id
                            elif count > 1:
                                alt_id = results.docs[0]['art_id']
                                log_everywhere_if(verbose, "debug", f"Found {count} matches for volume variant (issue). Using the first.")
                                self.art_id_replaced = doc_id
                                self.art_id_exists = True
                                self.art_id = alt_id
                                ret_val = self.art_id = alt_id

                    if not ret_val:
                        # match without volume variant?
                        alt_id = self.alt_no_vol_suffix
                        if self.alt_no_vol_suffix is not None:
                            results = solrcon.search(q = f"art_id:{alt_id}")
                            count = results.raw_response["response"]["numFound"]
                            if count == 1:  # only accept alternative if there's only one match (otherwise, not known which)
                                # odds are good this is what was cited.
                                alt_id = results.docs[0]['art_id']
                                log_everywhere_if(verbose, "debug", f"Document ID {doc_id} not in Solr.  The correct ID seems to be {alt_id}. Using that instead!")
                                self.art_id_replaced = doc_id
                                self.art_id_exists = True
                                ret_val = self.art_id = alt_id

                    if not ret_val:
                        # page before or after?
                        for n in (-1, 1):
                            newloc = opasLocator.Locator(doc_id)
                            if newloc.validate():
                                try:
                                    newloc.pgStart += n
                                except Exception as e:
                                    log_everywhere_if(verbose, "warning", f"Bad locator {loc_str}. Except: {e}")
                                else:
                                    revised_doc_id = newloc.articleID()
                            
                            results = solrcon.search(q = f"art_id:{revised_doc_id}")
                            count = results.raw_response["response"]["numFound"]
                            if count == 1:  # only accept alternative if there's only one match (otherwise, not known which)
                                # odds are good this is what was cited.
                                alt_id = results.docs[0]['art_id']
                                log_everywhere_if(verbose, "info", f"Document ID {doc_id} not in Solr.  The correct ID seems to be {alt_id}. Using that instead!")
                                self.art_id_replaced = doc_id
                                self.art_id_exists = True
                                ret_val = self.art_id = alt_id
                                break # got it!

            except Exception as e:
                logger.warning(e)
        else:
            self.art_id_exists = False
                    
        if ret_val is None:
            msg = f"Document ID {self.art_id} not in Solr."
            if not resilient:
                log_everywhere_if(verbose, "warning", f"{msg}")
            else:
                log_everywhere_if(verbose, "warning", f"{msg} No alternative ID found.")
        elif verbose: # just info--else not needed in final
            if doc_id != ret_val:
                msg = f"Document ID {doc_id} not found. Resilience adjusted to {ret_val}."
                log_everywhere_if(verbose, "warning", f"{msg}")

        return ret_val

    ## this should be faster, using if smartsearchLib.is_value_in_field
    ## but isn't in my tests, and can be slightly slower
    ## keep for now to get it in code archive if we want to test more later.
    #def exists_with_resilience_slower(self, solrcon=None, resilient=False, verbose=False):
        #"""
        #Search Solr for the article ID, if not found, try some common variations:
          #- the ArticleID alt_standard
          #or
          #- the simple missing page suffix 'A'
          #or
          #- one of the volume variant letters (issue)
          #or
          #- Page 0 vs 1 to handle split book variation
        
        #Returns:
            #- the current art_id exists
            #- None if it doesn't
            #- OR a heuristically modified existant art_id if resilient=True and
              #a close match could be resolved
              
        #Side Effects:
            #- art_id changed if resilient id found
            #- art_id_replaced stores the replaced id
            
        #"""
        #starttime = time.time()
        #ret_val = None
        #if self.art_id_exists:
            #ret_val = self.art_id
        #elif resilient:
            #doc_id = self.art_id
            #if solrcon is None:
                #solrcon = EXTENDED_CORES.get("pepwebdocs")
            #try:
                #if smartsearchLib.is_value_in_field(doc_id, opasConfig.SEARCH_FIELD_LOCATOR):
                    #self.art_id_exists = True
                    #ret_val = doc_id
                #else: # try variations
                    ## TryAlternateID:
                    #alt_id = self.alt_standard
                    #if smartsearchLib.is_value_in_field(alt_id, opasConfig.SEARCH_FIELD_LOCATOR):
                        #self.art_id_replaced = doc_id
                        #self.art_id_exists = True
                        #self.art_id = alt_id
                        #ret_val = alt_id
                        #log_everywhere_if(verbose, "debug", f"Document ID {doc_id} not in Solr.  The correct ID seems to be {alt_id}. Using that instead!")

                    #if not ret_val and doc_id[-1].isnumeric():
                        ## missing page variant?
                        #alt_id = doc_id + "A"
                        #if smartsearchLib.is_value_in_field(alt_id, opasConfig.SEARCH_FIELD_LOCATOR):
                            #self.art_id_replaced = doc_id
                            #self.art_id_exists = True
                            #self.art_id = alt_id
                            #ret_val = alt_id
                            #log_everywhere_if(verbose, "debug", f"Document ID {doc_id} not in Solr.  The correct ID seems to be {alt_id}. Using that instead!")

                    #if not ret_val and doc_id[-1].isalpha():
                        ## try without page variant?
                        #alt_id = doc_id[:-1]
                        #if smartsearchLib.is_value_in_field(alt_id, opasConfig.SEARCH_FIELD_LOCATOR):
                            #self.art_id_replaced = doc_id
                            #self.art_id_exists = True
                            #self.art_id = alt_id
                            #ret_val = alt_id
                            #log_everywhere_if(verbose, "debug", f"Document ID {doc_id} not in Solr.  The correct ID seems to be {alt_id}. Using that instead!")
                    
                    #if not ret_val:
                        ## match volume variant?
                        #alt_id = self.alt_wild_standard
                        #if alt_id is not None:
                            #results = solrcon.search(q = f"art_id:{alt_id}")
                            #count = results.raw_response["response"]["numFound"]
                            #if count == 1:  # only accept alternative if there's only one match (otherwise, not known which)
                                ## odds are good this is what was cited.
                                #alt_id = results.docs[0]['art_id']
                                #log_everywhere_if(verbose, "debug", f"Document ID {doc_id} not in Solr.  The correct ID seems to be {alt_id}. Using that instead!")
                                #self.art_id_replaced = doc_id
                                #self.art_id_exists = True
                                #ret_val = self.art_id = alt_id
                            #elif count > 1:
                                #alt_id = results.docs[0]['art_id']
                                #log_everywhere_if(verbose, "debug", f"Found {count} matches for volume variant (issue). Using the first.")
                                #self.art_id_replaced = doc_id
                                #self.art_id_exists = True
                                #self.art_id = alt_id
                                #ret_val = self.art_id = alt_id

                    #if not ret_val:
                        ## match without volume variant?
                        #alt_id = self.alt_no_vol_suffix
                        #if self.alt_no_vol_suffix is not None:
                            #results = solrcon.search(q = f"art_id:{alt_id}")
                            #count = results.raw_response["response"]["numFound"]
                            #if count == 1:  # only accept alternative if there's only one match (otherwise, not known which)
                                ## odds are good this is what was cited.
                                #alt_id = results.docs[0]['art_id']
                                #log_everywhere_if(verbose, "debug", f"Document ID {doc_id} not in Solr.  The correct ID seems to be {alt_id}. Using that instead!")
                                #self.art_id_replaced = doc_id
                                #self.art_id_exists = True
                                #ret_val = self.art_id = alt_id

                    #if not ret_val:
                        ## page before or after?
                        #for n in (-1, 1):
                            #newloc = opasLocator.Locator(doc_id)
                            #if newloc.validate():
                                #try:
                                    #newloc.pgStart += n
                                #except Exception as e:
                                    #log_everywhere_if(verbose, "warning", f"Bad locator {loc_str}. Except: {e}")
                                #else:
                                    #revised_doc_id = newloc.articleID()
                            
                            #results = solrcon.search(q = f"art_id:{revised_doc_id}")
                            #count = results.raw_response["response"]["numFound"]
                            #if count == 1:  # only accept alternative if there's only one match (otherwise, not known which)
                                ## odds are good this is what was cited.
                                #alt_id = results.docs[0]['art_id']
                                #log_everywhere_if(verbose, "info", f"Document ID {doc_id} not in Solr.  The correct ID seems to be {alt_id}. Using that instead!")
                                #self.art_id_replaced = doc_id
                                #self.art_id_exists = True
                                #ret_val = self.art_id = alt_id
                                #break # got it!

            #except Exception as e:
                #logger.warning(e)
        #else:
            #self.art_id_exists = False
            
        #if ret_val is None:
            #msg = f"Document ID {self.art_id} not in Solr."
            #if not resilient:
                #log_everywhere_if(verbose, "warning", f"{msg}")
            #else:
                #log_everywhere_if(verbose, "warning", f"{msg} No alternative ID found.")
        #elif verbose: # just info--else not needed in final
            #endtime = time.time()
            #time_diff = endtime - starttime
            
            #if doc_id != ret_val:
                #msg = f"Document ID {doc_id} resilience fixed...returning {ret_val} {time_diff}s."
                #log_everywhere_if(verbose, "info", f"{msg}")
            
        #return ret_val
    

    
#------------------------------------------------------------------------------------------------------
    

classification_regex = re.compile("(?P<class>current|archive|future|free|special|offsite|preview)", re.IGNORECASE)
class ArticleInfo(BaseModel):
    """
    An entry from a documents metadata.
    
    Used to populate the MySQL table api_articles for relational type querying
       and the Solr core pepwebdocs for full-text searching (and the majority of
       client searches.

    >>> file = r"X:\\AWS_S3\\AWS PEP-Web-Live-Data\\_PEPArchive\Psyche\\066.2012\\PSYCHE.066.0268A(bKBD3).xml"
    >>> a = ArticleInfo(art_id="PSYCHE.066.0268A", fullfilename=file)
       


    >>> a = ArticleInfo(art_id="MPSA.043.0117A")
    >>> print (a.article_id_dict["art_id"])
    MPSA.043.0117A
    >>> file = r"X:\\AWS_S3\\AWS PEP-Web-Live-Data\\_PEPArchive\\PI\\003\\PI.003.0003A(bEXP_ARCH1).xml"
    >>> a = ArticleInfo(art_id="PI.003.0003A", fullfilename=file)

    """
    #************************************************************************************
    # pydantic model - object definitions for ArticleInfo
    #************************************************************************************
    #art_locator: Optional[opasLocator.Locator] = Field(opasLocator.Locator, title="PEP full article ID locator object")
    art_id: str = Field(None)             # Key!!!!
    article_id_dict: dict = Field({}, title="ArticleID model as dict")

    # other info, mostly optional
    art_id_from_filename: Optional[str]   # Should match key!!
    art_id_with_volume_letter: Optional[str]
    art_auth_citation: Optional[str]
    art_auth_mast: Optional[str] = Field(None, title="Author mast, for Solr")
    art_auth_mast_list: list = Field([], title="List of author names format for masts, for Solr")
    art_auth_mast_unlisted_str: Optional[str]
    # art_auth_mast_unlisted_list: list = Field([], title="List of author names")
    author_xml_list: list = Field([], title="List of authors, for Solr")
    author_xml: Optional[str]
    # authors_bibliographic: Optional[str]  
    # authors_bibliographic_list: list = Field([], title="List of authors")
    art_citeas_text: Optional[str]
    art_citeas_xml: Optional[str]
    art_doi: Optional[str]
    art_graphic_list: list = Field([], title="List of graphic references")
    art_is_maintoc: Optional[bool]
    art_isbn: Optional[str]
    art_issn: Optional[str]
    art_issue: Optional[str]
    art_issue_title: Optional[str]
    art_issue_title_str: Optional[str]
    art_issue_seqnbr: Optional[str]
    art_kwds: Optional[str]
    art_kwds_str: Optional[str]
    art_lang: Optional[str]
    art_locator_str: Optional[str]
    art_orig_rx: Optional[str]
    art_pgend: Optional[str]
    art_pgrg: Optional[str]
    art_pgstart: Optional[str]
    art_pgstart_prefix: Optional[str]
    pgstart_suffix: Optional[str]
    pgend_prefix: Optional[str]
    art_pgend: Optional[str]
    pgend_suffix: Optional[str]
    art_qual: Optional[str]
    art_subtitle: Optional[str]
    art_title: Optional[str]
    art_type: Optional[str]
    art_vol_int: Optional[int]
    art_vol_str: Optional[str]
    art_vol_suffix: Optional[str]
    art_vol_title: Optional[str]
    art_year_int: Optional[int]
    art_year_str: Optional[str]
    art_year_suffix: Optional[str]
    art_year2_str: Optional[str]
    art_year2_suffix: Optional[str]
    artinfo_meta_xml: Optional[str]
    artinfo_xml: Optional[str]
    author_list: list = Field([], title="List of author names")
    art_authors_mast_list: list = Field([], title="List of author names")
    art_authors_mast_list_strings: list = Field([], title="List of author names")
    art_author_id_list: list = Field([], title="List of author names")  
    art_author_ids_str: Optional[str]
    art_auth_citation: Optional[str]
    art_auth_citation_list: list = Field([], title="List of authors cited")
    bib_authors: Optional[str]
    bib_title: Optional[str]
    art_bib_rxlink_list: list = Field([], title="")
    bk_info_xml: Optional[str]
    bk_next_id: Optional[str]
    bk_publisher: Optional[str]
    bk_seriestoc: Optional[str]
    bk_subdoc: Optional[str]
    bk_title: Optional[str]
    src_embargo_in_years: Optional[str]
    embargoed: Optional[str] = Field("False")
    embargotype: Optional[str]
    file_classification: Optional[str]
    file_create_time: Optional[str]
    file_size: Optional[int]
    file_updated: Optional[bool]
    filedatetime: Optional[str]
    filename: Optional[str]
    filename_artidinfo: Optional[str]
    fullfilename: Optional[str]
    glossary_terms_dict: Optional[dict]
    glossary_terms_dict_str: Optional[str]
    last_update: Optional[str]
    main_toc_id: Optional[str]
    manuscript_date_str: Optional[str]
    metadata_dict: Optional[dict]
    preserve: Optional[str]
    processed_datetime: Optional[str]
    publisher_ms_id: Optional[str]
    src_code: Optional[str]
    src_code_active: Optional[str]
    src_is_book: Optional[bool]
    src_prodkey: Optional[str]
    src_title_abbr: Optional[str]
    src_title_full: Optional[str]
    src_type: Optional[str]
    start_sectname: Optional[str]
    start_sectlevel: Optional[str]
    verbose: Optional[bool]
    is_splitbook: Optional[bool]
    bib_journaltitle: Optional[str]
    issue_id_str: Optional[str]
    # stat fields for artstat
    art_abs_count: int = Field(0, title="")
    art_authors_count: int = Field(0, title="")
    # art_auth_count = int = Field(0, title="") # perhaps not needed
    art_citations_count: Optional[int]
    art_pgrx_count: int = Field(0, title="")
    art_figcount: int = Field(0, title="")
    art_headings_count: int = Field(0, title="")
    art_poems_count: int = Field(0, title="")
    art_notes_count: int = Field(0, title="")
    art_kwds_count: int = Field(0, title="")
    art_tblcount: int = Field(0, title="")
    art_terms_count: int = Field(0, title="")
    art_pgcount: int = Field(0, title="")
    art_ftns_count: int = Field(0, title="")
    art_quotes_count: int = Field(0, title="")
    art_dreams_count: int = Field(0, title="")
    art_dialogs_count: int = Field(0, title="")
    art_paras_count: int = Field(0, title="")
    art_words_count: int = Field(0, title="")
    art_chars_count: int = Field(0, title="")
    art_chars_meta_count: int = Field(0, title="")
    art_chars_no_spaces_meta_count: int = Field(0, title="") 
    art_chars_no_spaces_count: int = Field(0, title="")
    # glossary_terms_count: int = Field(0, title="Number of glossary terms found")
    ref_count: int = Field(0, title="Number of references")
    
    #class Config:
        #article_id_obj = Extra.allow # or 'allow' str
        
    def __init__(self, art_id, parsed_xml=None, logger=logger, filename_base="", fullfilename=None, verbose=None, **kwargs):
        super().__init__(**kwargs)

        # def __init__(self, parsed_xml, art_id, logger, filename_base, fullfilename=None, verbose=None):
        # let's just double check artid!
        self.art_id = art_id
        basic_art_info = ArticleID(art_id=art_id)
        if parsed_xml is None and fullfilename is not None:
            file_xml, final_fileinfo = fs.get_file_contents(fullfilename, localsecrets.XML_ORIGINALS_PATH)
            if file_xml is not None:
                try:
                    parsed_xml = etree.fromstring(opasxmllib.remove_encoding_string(file_xml), parser)
                except Exception as e:
                    if file_xml is None:
                        logger.error(f"Can't parse empty converted XML string")
                    else:
                        logger.error(f"Can't parse XML starting '{file_xml[0:64]}'")
                else:
                    if parsed_xml is None:
                        logger.error(f"Could not get article info from file {fullfilename}")
                        raise Exception("Error")               
        
        if not basic_art_info.is_ArticleID:
            if filename_base is not None:
                basic_art_info = ArticleID(art_id=filename_base) # use base filename for implied artinfo
                self.art_id_from_filename = basic_art_info.art_id
        
        if not basic_art_info.is_ArticleID: # separate from above since status can change
            file_contents_art_id = opasxmllib.xml_xpath_return_textsingleton(parsed_xml, "//artinfo/@id", None)
            basic_art_info = ArticleID(art_id=file_contents_art_id)
                
        if not opasgenlib.is_empty(parsed_xml):
            # critical to set these, used from basic_art_info below and it's not always part of the articleID
            # art_issue is a STRING, might be a range, or a name
            basic_art_info.art_issue = opasxmllib.xml_xpath_return_textsingleton(parsed_xml, '//artinfo/artiss/node()', default_return=None)
            try:
                basic_art_info.art_issue_int = int(basic_art_info.art_issue)
            except:
                basic_art_info.art_issue_int = None
        
        if basic_art_info.is_ArticleID and not opasgenlib.is_empty(parsed_xml):
            # should try not to call this this way, but for testing, it's useful.  
            basic_art_info.art_vol_str = opasxmllib.xml_xpath_return_textsingleton(parsed_xml, '//artinfo/artvol/node()', default_return=None)
            basic_art_info.src_code = parsed_xml.xpath("//artinfo/@j")[0]
            basic_art_info.src_code = basic_art_info.src_code.upper()  # 20191115 - To make sure this is always uppercase
            basic_art_info.art_vol_str = opasxmllib.xml_xpath_return_textsingleton(parsed_xml, '//artinfo/artvol/node()', default_return=None)
            
            # for compare/debug
            vol_actual = opasxmllib.xml_xpath_return_textsingleton(parsed_xml, '//artinfo/artvol/@actual', default_return=None)
            
            m = re.match("(\d+)([A-Z]*)", basic_art_info.art_vol_str)
            if m is None:
                logger.error(f"ArticleInfoError: Bad Vol # in element content: {basic_art_info.art_vol_str}")
                m = re.match("(\d+)([A-z\-\s]*)", vol_actual)
                if m is not None:
                    basic_art_info.art_vol_int = m.group(1)
                    logger.error(f"ArticleInfoError: Recovered Vol # from actual attr: {sbasic_art_info.art_vol_int}")
                else:
                    raise ValueError("ArticleInfoError: Severe Error in art_vol")
            else:
                basic_art_info.art_vol_int = m.group(1)
                if len(m.groups()) == 2:
                    basic_art_info.art_vol_suffix = m.group(2)

            # now convert to int
            try:
                basic_art_info.art_vol_int = int(basic_art_info.art_vol_int)
            except ValueError:
                logger.warning(f"Can't convert art_vol to int: {basic_art_info.art_vol_int} Error: {e}")
                basic_art_info.art_vol_suffix = basic_art_info.art_vol_int[-1]
                art_vol_ints = re.findall(r'\d+', basic_art_info.art_vol_str)
                # fixed search replace error on var name which doubled it up...may be cause of recent Dealloc errors? 
                if len(art_vol_ints) >= 1:
                    basic_art_info.art_vol_int = art_vol_ints[1]
                    basic_art_info.art_vol_int = int(basic_art_info.art_vol_int)
            except Exception as e:
                logger.warning(f"Can't convert art_vol to int: {basic_art_info.art_vol_int} Error: {e}")

        if basic_art_info.is_ArticleID:
            self.src_code = basic_art_info.src_code
            self.art_vol_int = basic_art_info.art_vol_int
            self.art_vol_str = basic_art_info.art_vol_str 
            self.art_vol_suffix = basic_art_info.art_vol_suffix
            if self.art_vol_suffix:
                self.art_id_with_volume_letter = f"{basic_art_info.src_code}.{basic_art_info.art_vol_str}"
                self.art_id_with_volume_letter += f".{basic_art_info.special_section_prefix}{basic_art_info.roman_prefix}{basic_art_info.art_pgstart}{basic_art_info.page_suffix}"
                
            self.src_code = basic_art_info.src_code
            if basic_art_info.art_issue_int is not None and basic_art_info.art_issue_int != 0:
                self.art_issue = str(basic_art_info.art_issue_int)
            elif basic_art_info.is_supplement:
                self.art_issue = "Supplement"
            else:
                self.art_issue = basic_art_info.art_issue

            self.art_pgstart = basic_art_info.art_pgstart
        else:
            raise ValueError(f"Fatal Error: {filename_base} is improperly named and does not have a valid article ID: {basic_art_info.dict()}")
        
        self.article_id_dict = basic_art_info.dict()
        self.bk_subdoc = None
        self.bk_seriestoc = None
        self.verbose = verbose
        self.src_code_active = 0
        self.src_is_book = False
        
        # Just init these.  Creator will set based on filename
        self.file_classification = None
        self.file_size = 0  
        self.filedatetime = ""
        self.filename = filename_base # filename without path
        self.fullfilename = fullfilename
        self.art_id_from_filename = parse_artid_from_filename(fullfilename)

        try: #  lookup source in db
            if self.src_code in ["ZBK", "IPL", "NLP"]:
                self.src_prodkey = pepsrccode = f"{self.src_code}%03d" % self.art_vol_int
                self.src_type = "book"
                self.src_is_book = True
            else:
                self.src_prodkey = pepsrccode = f"{self.src_code}"
                self.src_is_book = False

            self.src_title_abbr = sourceDB.sourceData[pepsrccode].get("sourcetitleabbr", None)
            self.src_title_full = sourceDB.sourceData[pepsrccode].get("sourcetitlefull", None)
            self.src_code_active = sourceDB.sourceData[pepsrccode].get("active", 0)
                
            # remove '*New*'  prefix if it's there
            try:
                if self.src_title_full is not None:
                    self.src_title_full = self.src_title_full.replace(opasConfig.JOURNALNEWFLAG, "")
                    src_title_full_safe = html.escape(self.src_title_full)
                else:
                    logger.warning(f"Source title full is None")
                    src_title_full_safe = ''
    
            except Exception as e:
                logger.error(f"ArticleInfoError: Source title escape error: {e}")
                src_title_full_safe = ''
                        
            self.src_embargo_in_years = sourceDB.sourceData[pepsrccode].get("wall", None)
            product_type = sourceDB.sourceData[pepsrccode].get("product_type", None)  # journal, book, video...
                
            if self.src_code in ["GW", "SE"]:
                self.src_type = "book"
            else:
                if type(product_type) == set:
                    try:
                        self.src_type = next(iter(product_type))
                    except Exception as e:
                        self.src_type = "exception"
                else:
                    self.src_type = product_type
                
        except KeyError as err:
            self.src_title_abbr = None
            self.src_title_full = None
            self.src_type = "book"
            self.src_embargo_in_years = None
            logger.warning("ArticleInfoError: Source %s not found in source info db.  Assumed to be an offsite book.  Or you can add to the api_productbase table in the RDS/MySQL DB", self.src_code)
        except Exception as err:
            logger.error("ArticleInfoError: Problem with this files source info. File skipped. (%s)", err)
            #processingErrorCount += 1
            return
        
        # now, the rest of the variables we can set from the data
        self.processed_datetime = datetime.utcfromtimestamp(time.time()).strftime(opasConfig.TIME_FORMAT_STR)

        if parsed_xml is not None:
            artinfo_xml = parsed_xml.xpath("//artinfo")[0] # grab full artinfo node, so it can be returned in XML easily.
            self.artinfo_xml = etree.tostring(artinfo_xml).decode("utf8")
            self.artinfo_meta_xml = parsed_xml.xpath("//artinfo/meta")
            self.embargoed = parsed_xml.xpath("//artinfo/@embargo")
            self.embargotype = opasxmllib.xml_xpath_return_textsingleton(parsed_xml, "//artinfo/@embargotype", default_return=None)
            if self.embargotype is not None:
                self.embargotype = self.embargotype.upper()
                if opasConfig.TEMP_IJPOPEN_VER_COMPAT_FIX:
                    if self.embargotype == "IJPOPEN_FULLY_REMOVED":
                        self.embargotype = "IJPOPEN_REMOVED" 

            self.art_issue_title = opasxmllib.xml_xpath_return_textsingleton(parsed_xml, '//artinfo/artissinfo/isstitle/node()', default_return=None)
            if self.art_issue_title is None:
                try:
                    self.art_issue_title = parsed_xml.xpath("//artinfo/@issuetitle")[0]
                except:
                    pass
            
            if 1: # Added for IJPOpen but could apply elsewhere
                self.metadata_dict = {}
                root = parsed_xml.getroottree()
                adldata_list = root.findall('meta/adldata')
                for adldata in adldata_list:
                    fieldname = adldata[0].text
                    fieldvalue = adldata[1].text
                    self.metadata_dict[fieldname] = fieldvalue 
            
                # Currently, 2022/12, these are only used by IJPOpen
                self.publisher_ms_id = self.metadata_dict.get("manuscript-id", "")
                self.manuscript_date_str = self.metadata_dict.get("submission-date", "")

            # special sequential numbering for issues used by journals like fa (we code it simply as artnbr in xml)
            self.art_issue_seqnbr = opasxmllib.xml_xpath_return_textsingleton(parsed_xml, '//artinfo/artnbr/node()', default_return=None)
            
            self.art_year_str = opasxmllib.xml_xpath_return_textsingleton(parsed_xml, '//artinfo/artyear/node()', default_return=None)
            m = re.match("(?P<yearint>[0-9]{4,4})(?P<yearsuffix>[a-zA-Z])?(\s*\-\s*)?((?P<year2int>[0-9]{4,4})(?P<year2suffix>[a-zA-Z])?)?", self.art_year_str)
            if m is not None:
                self.art_year_suffix = m.group("yearsuffix")
                self.art_year_str = m.group("yearint")
                self.art_year2_str = m.group("year2int")
                self.art_year2_suffix = m.group("year2suffix")
                self.art_year_int = int(m.group("yearint"))
            else:
                try:
                    art_year_for_int = re.sub("[^0-9]", "", self.art_year_str)
                    self.art_year_int = int(art_year_for_int)
                except ValueError as err:
                    logger.error("Error converting art_year to int: %s", self.art_year_str)
                    self.art_year_int = 0
    
            artInfoNode = parsed_xml.xpath('//artinfo')[0]
            self.art_type = opasxmllib.xml_get_element_attr(artInfoNode, "arttype", default_return=None)
            if self.art_type is not None and self.art_type.upper() == "TOC":
                self.art_is_maintoc = True
            else:
                self.art_is_maintoc = False
                
            self.art_vol_title = opasxmllib.xml_xpath_return_textsingleton(parsed_xml, '//artinfo/artvolinfo/voltitle/node()', default_return=None)
            if self.art_vol_title is None:
                # try attribute for value (lower priority than element above)
                self.art_vol_title = opasxmllib.xml_get_element_attr(artInfoNode, "voltitle", default_return=None)

            art_doi = self.art_doi = opasxmllib.xml_get_element_attr(artInfoNode, "doi", default_return=None) 
            self.art_issn = opasxmllib.xml_get_element_attr(artInfoNode, "ISSN", default_return=None) 
            self.art_isbn = opasxmllib.xml_get_element_attr(artInfoNode, "ISBN", default_return=None) 
            orig_rx = opasxmllib.xml_get_element_attr(artInfoNode, "origrx", default_return=None)
            if orig_rx is not None:
                orig_rx = opasLocator.Locator(orig_rx)
                if orig_rx.isValid():
                    self.art_orig_rx = orig_rx.articleID()
                
            self.start_sectlevel = opasxmllib.xml_get_element_attr(artInfoNode, "newseclevel", default_return=None)
            self.start_sectname = opasxmllib.xml_get_element_attr(artInfoNode, "newsecnm", default_return=None)
            if self.start_sectname is None:
                #  look in newer, tagged, data
                self.start_sectname = opasxmllib.xml_xpath_return_textsingleton(parsed_xml, '//artinfo/artsectinfo/secttitle/node()', default_return=None)
            
            if self.start_sectname is not None:
                self.start_sectname = opasgenlib.trimPunctAndSpaces(self.start_sectname)
            
            self.art_pgrg = opasxmllib.xml_get_subelement_textsingleton(artInfoNode, "artpgrg", default_return=None)  # note: getSingleSubnodeText(pepxml, "artpgrg")
            self.art_pgstart, self.art_pgend = opasgenlib.pgrg_splitter(self.art_pgrg)
    
            try:
                self.art_pgcount = int(parsed_xml.xpath("count(//pb)")) # 20200506
            except Exception as e:
                log_everywhere_if(gDbg2, level="warning", msg=f"article info error: {e}")
                self.art_pgcount = 0
                
            self.art_kwds = opasxmllib.xml_xpath_return_textsingleton(parsed_xml, "//artinfo/artkwds/node()", None)
            
            # art_pgrx_count
            try:
                self.art_pgrx_count = int(parsed_xml.xpath("count(//pgx)")) # 20220320
            except Exception as e:
                log_everywhere_if(gDbg2, level="warning", msg=f"article info error: {e}")
                self.art_pgrx_count = 0
                
            if 1: # counts
                # ************* new counts! 20210413 *******************************************
                try:
                    if self.art_kwds is not None:
                        self.art_kwds_count = self.art_kwds.count(",") + 1 # 20210413
                    else:
                        self.art_kwds_count = 0
                except Exception as e:
                    log_everywhere_if(gDbg2, level="warning", msg=f"article info error: {e}")
                    self.art_kwds_count = 0
        
                # art_abs_count
                try:
                    self.art_abs_count = int(parsed_xml.xpath("count(//abs)"))
                except Exception as e:
                    log_everywhere_if(gDbg2, level="warning", msg=f"article info error: {e}")
                    self.art_abs_count  = 0
        
                # art_ftns_count_count 
                try:
                    self.art_ftns_count = int(parsed_xml.xpath("count(//ftn)")) # 20210413
                except Exception as e:
                    log_everywhere_if(gDbg2, level="warning", msg=f"article info error: {e}")
                    self.art_ftns_count = 0
        
                # art_paras_count
                try:
                    self.art_paras_count = int(parsed_xml.xpath("count(//p)")) # 20210413
                except Exception as e:
                    log_everywhere_if(gDbg2, level="warning", msg=f"article info error: {e}")
                    self.art_paras_count = 0
        
                # art_headings_count
                try:
                    self.art_headings_count = int(parsed_xml.xpath("count(//*[self::h1 or self::h2 or self::h3 or self::h4 or self::h5 or self::h6])")) # 20210413
                except Exception as e:
                    log_everywhere_if(gDbg2, level="warning", msg=f"article info error: {e}")
                    self.art_headings_count = 0
        
                # art_terms_count
                try:
                    self.art_terms_count = int(parsed_xml.xpath('count(//impx[@type="TERM2"])')) # 20210413
                except Exception as e:
                    log_everywhere_if(gDbg2, level="warning", msg=f"article info error: {e}")
                    self.art_terms_count = 0
        
                # art_dreams_count
                try:
                    self.art_dreams_count = int(parsed_xml.xpath("count(//dream)")) # 20210413
                except Exception as e:
                    log_everywhere_if(gDbg2, level="warning", msg=f"article info error: {e}")
                    self.art_dreams_count = 0
        
                # art_dialogs_count
                try:
                    self.art_dialogs_count = int(parsed_xml.xpath("count(//dialog)")) # 20210413
                except Exception as e:
                    log_everywhere_if(gDbg2, level="warning", msg=f"article info error: {e}")
                    self.art_dialogs_count = 0
        
                # art_notes_count
                try:
                    self.art_notes_count = int(parsed_xml.xpath("count(//note)")) # 20210413
                except Exception as e:
                    log_everywhere_if(gDbg2, level="warning", msg=f"article info error: {e}")
                    self.art_notes_count = 0
        
                # art_poems_count
                try:
                    self.art_poems_count = int(parsed_xml.xpath("count(//poem)")) # 20210413
                except Exception as e:
                    log_everywhere_if(gDbg2, level="warning", msg=f"article info error: {e}")
                    self.art_poems_count = 0
                    
                # art_citations_count
                try:
                    self.art_citations_count = int(parsed_xml.xpath("count(//bx)")) # 20210413
                except Exception as e:
                    log_everywhere_if(gDbg2, level="warning", msg=f"article info error: {e}")
                    self.art_citations_count = 0
                
                # art_quotes_count
                try:
                    self.art_quotes_count = int(parsed_xml.xpath("count(//quote)")) # 20210413
                except Exception as e:
                    log_everywhere_if(gDbg2, level="warning", msg=f"article info error: {e}")
                    self.art_quotes_count = 0
        
                try:
                    self.art_tblcount = int(parsed_xml.xpath("count(//tbl)")) # 20200922
                except Exception as e:
                    log_everywhere_if(gDbg2, level="warning", msg=f"article info error: {e}")
                    self.art_tblcount = 0
        
                try:
                    self.art_figcount = int(parsed_xml.xpath("count(//figure)")) # 20200922
                except Exception as e:
                    log_everywhere_if(gDbg2, level="warning", msg=f"article info error: {e}")
                    self.art_figcount = 0
                    
                # art_chars_count
                try:
                    self.art_chars_count = int(parsed_xml.xpath("string-length(normalize-space(//node()))"))
                    self.art_chars_meta_count = int(parsed_xml.xpath("string-length(normalize-space(//meta))"))
                    self.art_chars_count -= self.art_chars_meta_count 
                except Exception as e:
                    log_everywhere_if(gDbg2, level="warning", msg=f"article info error: {e}")
                    self.art_chars_count  = 0
        
                try:
                    self.art_chars_no_spaces_count = int(parsed_xml.xpath("string-length(translate(normalize-space(//node()),' ',''))"))
                    self.art_chars_no_spaces_meta_count = int(parsed_xml.xpath("string-length(translate(normalize-space(//meta),' ',''))"))
                    self.art_chars_no_spaces_count -= self.art_chars_no_spaces_meta_count
                except Exception as e:
                    log_everywhere_if(gDbg2, level="warning", msg=f"article info error: {e}")
                    self.art_chars_no_spaces_count  = 0
        
                try:
                    self.art_words_count = self.art_chars_count - self.art_chars_no_spaces_count + 1
                except Exception as e:
                    log_everywhere_if(gDbg2, level="warning", msg=f"article info error: {e}")
                    self.art_words_count  = 0
        
                # ************* end of counts! 20210413 *******************************************
    
            self.art_graphic_list = parsed_xml.xpath('//graphic//@source')
            
            if self.art_pgstart is not None:
                self.art_pgstart_prefix, self.art_pgstart, self.pgstart_suffix = opasgenlib.pgnum_splitter(self.art_pgstart)
            else:
                self.art_pgstart_prefix, self.art_pgstart, self.pgstart_suffix = (None, None, None)
                
            if self.art_pgend is not None:
                self.pgend_prefix, self.art_pgend, self.pgend_suffix = opasgenlib.pgnum_splitter(self.art_pgend)
            else:
                self.pgend_prefix, self.art_pgend, self.pgend_suffix = (None, None, None)
    
            self.art_title = opasxmllib.xml_get_subelement_textsingleton(artInfoNode, "arttitle", skip_tags=["ftnx"])
            if self.art_title == "-": # weird title in ANIJP-CHI
                self.art_title = ""
    
            self.art_subtitle = opasxmllib.xml_get_subelement_textsingleton(artInfoNode, 'artsub')
            if self.art_subtitle == "":
                pass
            elif self.art_subtitle is None:
                self.art_subtitle = ""
            else:
                #self.artSubtitle = ''.join(etree.fromstring(self.artSubtitle).itertext())
                if self.art_title != "":
                    self.art_subtitle = ": " + self.art_subtitle
                    self.art_title = self.art_title + self.art_subtitle
                else:
                    self.art_title = self.art_subtitle
                    self.art_subtitle = ""
                    
            self.art_lang = parsed_xml.xpath('//pepkbd3/@lang')
            
            if self.art_lang == []:
                self.art_lang = [opasConfig.DEFAULT_DATA_LANGUAGE_ENCODING]
    
            try:
                self.art_lang = self.art_lang[0].lower()
            except:
                logger.warning(f"art_lang value error: {self.art_lang}")
                self.art_lang = opasConfig.DEFAULT_DATA_LANGUAGE_ENCODING
            
            self.author_xml_list = parsed_xml.xpath('//artinfo/artauth/aut')
            self.author_xml = opasxmllib.xml_xpath_return_xmlsingleton(parsed_xml, '//artinfo/artauth')
            authors_bibliographic, self.author_list, authors_bibliographic_list = opasxmllib.authors_citation_from_xmlstr(self.author_xml, listed="All") #listed=True)
            self.art_auth_citation = authors_bibliographic
            self.art_auth_citation_list = authors_bibliographic_list
            # ToDo: I think I should add an author ID to bib aut too.  But that will have
            #  to wait until later.
            # TODO: fix PEP2XML--in cases like AJRPP.004.0273A it put Anonymous in the authindexid.
            self.art_author_id_list = opasxmllib.xml_xpath_return_textlist(parsed_xml, '//artinfo/artauth/aut[@listed="true"]/@authindexid')
            self.art_authors_count = len(self.author_list)
            if self.art_author_id_list == []: # no authindexid
                logger.info("This document %s may be missing authindexids" % art_id)
                self.art_author_id_list = self.author_list
    
            self.art_author_ids_str = ", ".join(self.art_author_id_list)
            self.art_auth_mast, self.art_auth_mast_list = opasxmllib.author_mast_from_xmlstr(self.author_xml, listed=True)
            self.art_auth_mast_unlisted_str, art_auth_mast_unlisted_list = opasxmllib.author_mast_from_xmlstr(self.author_xml, listed=False)
            # self.art_auth_count = len(self.author_xml_list)
            # self.art_author_lastnames = opasxmllib.xml_xpath_return_textlist(parsed_xml, '//artinfo/artauth/aut[@listed="true"]/nlast')
            
            # self.art_all_authors = self.art_auth_mast + " (" + self.art_auth_mast_unlisted_str + ")"
    
            self.issue_id_str = f"<issue_id><src>{self.src_code}</src><yr>{self.art_year_str}</yr><vol>{self.art_vol_str}</vol><iss>{self.art_issue}</iss></issue_id>"
                
            try:
                if self.art_title is not None:
                    art_title_safe = html.escape(self.art_title)
                else:
                    logger.warning(f"Art title is None")
                    art_title_safe = ''
    
            except Exception as e:
                logger.error(f"ArticleInfoError: Art title escape error: {e}")
                art_title_safe = ''
    
            try:
                if self.art_pgrg is not None:
                    art_pgrg_safe = html.escape(self.art_pgrg)
                else:
                    logger.warning(f"Art title is None")
                    art_pgrg_safe = ''
    
            except Exception as e:
                logger.error(f"ArticleInfoError: Art PgRg escape error: {e}")
                art_pgrg_safe = ''

            try:
                if self.bk_title is not None:
                    src_title_full_safe = html.escape(self.bk_title)
                elif self.src_title_full is not None:
                    src_title_full_safe = html.escape(self.src_title_full)
                else:
                    logger.info(f"Source title is None")
                    src_title_full_safe = ''
            except Exception as e:
                logger.error(f"ArticleInfoError: Art bk_title escape error: {e}")
                art_pgrg_safe = ''
                
            # Usually we put the abbreviated title here, but that won't always work here.
            self.art_citeas_xml = u"""<p class="citeas"><span class="authors">%s</span> (<span class="year">%s</span>) <span class="title">%s</span>. <span class="sourcetitle">%s</span> <span class="vol">%s</span>:<span class="pgrg">%s</span></p>""" \
                %                   (self.art_auth_citation,
                                     self.art_year_str,
                                     art_title_safe,
                                     src_title_full_safe,
                                     self.art_vol_int,
                                     art_pgrg_safe
                                    )
            
            self.art_citeas_text = opasxmllib.xml_elem_or_str_to_text(self.art_citeas_xml)
            art_qual_node = parsed_xml.xpath("//artinfo/artqual")
            if art_qual_node != []:
                self.art_qual = opasxmllib.xml_get_element_attr(art_qual_node[0], "rx", default_return=None)
                try:
                    self.art_qual = opasLocator.Locator(self.art_qual).articleID()
                    # standardize for output
                    art_qual_node[0].attrib["rx"] = self.art_qual
                except Exception as e:
                    log_everywhere_if(True, "warning", f"Error standardizing art_qual: {e}")
            else:
                self.art_qual = parsed_xml.xpath("//artbkinfo/@extract")
                if self.art_qual == []:
                    self.art_qual = None
                else:
                    try:
                        self.art_qual = opasLocator.Locator(self.art_qual[0]).articleID()
                    except Exception as e:
                        log_everywhere_if(True, "warning", f"Error standardizing extract based art_qual: {e}")
    
            # will be None if not a book extract
            # self.art_qual = None
            if self.art_qual is not None:
                if isinstance(self.art_qual, list):
                    self.art_qual = str(self.art_qual[0])
                    
                if self.art_qual != self.art_id and self.src_is_book:
                    self.bk_subdoc = True
                else:
                    self.bk_subdoc = False
            else:
                self.bk_subdoc = False           
    
            refs = parsed_xml.xpath("/pepkbd3//be")
            self.bib_authors = []
            self.art_bib_rxlink_list = []
            self.bib_title = []
            self.bib_journaltitle = []
            
            for x in refs:
                try:
                    if x.attrib["rx"] is not None:
                        self.art_bib_rxlink_list.append(x.attrib["rx"])
                except:
                    pass
                journal = x.find("j")
                if journal is not None:
                    journal_lc = opasxmllib.xml_elem_or_str_to_text(journal).lower()
                    journal_lc = journal_lc.translate(str.maketrans('', '', string.punctuation))
                    self.bib_journaltitle.append(journal_lc)
    
                title = x.find("t")
                # bib article titles for faceting, get rid of punctuation variations
                if title is not None:
                    bib_title = opasxmllib.xml_elem_or_str_to_text(title)
                    bib_title = bib_title.lower()
                    bib_title = bib_title.translate(str.maketrans('', '', string.punctuation))
                    self.bib_title.append(opasxmllib.xml_elem_or_str_to_text(title))
    
                title = x.find("bst")
                # bib source titles for faceting, get rid of punctuation variations
                # cumulate these together with article title
                if title is not None:
                    bib_title = opasxmllib.xml_elem_or_str_to_text(title)
                    bib_title = bib_title.lower()
                    bib_title = bib_title.translate(str.maketrans('', '', string.punctuation))
                    self.bib_title.append(bib_title)
    
                auths = x.findall("a")
                for y in auths:
                    if opasxmllib.xml_elem_or_str_to_text(x) is not None:
                        self.bib_authors.append(opasxmllib.xml_elem_or_str_to_text(y))
            
            self.ref_count = len(refs )
            # clear it, we aren't saving it.
            refs  = None
            
            self.bk_info_xml = opasxmllib.xml_xpath_return_xmlsingleton(parsed_xml, "/pepkbd3//artbkinfo") # all book info in instance
            # break it down a bit for the database
            self.main_toc_id = opasxmllib.xml_xpath_return_textsingleton(parsed_xml, "/pepkbd3//artbkinfo/@extract", None)
            if self.main_toc_id is not None:
                self.main_toc_id = str(self.main_toc_id)
                #self.main_toc_id = str(opasLocator.Locator(self.main_toc_id))
                
            self.bk_title = opasxmllib.xml_xpath_return_textsingleton(parsed_xml, "/pepkbd3//bktitle", None)
            self.bk_publisher = opasxmllib.xml_xpath_return_textsingleton(parsed_xml, "/pepkbd3//bkpubandloc", None)
            self.bk_seriestoc = opasxmllib.xml_xpath_return_textsingleton(parsed_xml, "/pepkbd3//artbkinfo/@seriestoc", None)
            self.bk_next_id = opasxmllib.xml_xpath_return_textsingleton(parsed_xml, "//artbkinfo/@next", None)
            #if self.bk_next_id is not None:
                #self.bk_next_id = opasLocator.Locator(self.bk_next_id)
            
            # self.bk_pubyear = opasxmllib.xml_xpath_return_textsingleton(pepxml, "/pepkbd3//artbkinfo/bkpubyear", default_return=self.art_year_str)
            # hard code special cases SE/GW if they are not covered by the instances
            if self.bk_seriestoc is None:
                if self.src_code == "SE":
                    self.bk_seriestoc = "SE.000.0000A"
                if self.src_code == "GW":
                    self.bk_seriestoc = "GW.000.0000A"               
    
            # if article xml has glossary_terms, set glossary_terms fields
            glossary_terms_dict_addon = parsed_xml.xpath("//unit[@type='glossary_term_dict']")
            if glossary_terms_dict_addon:
                glossary_terms_dict_addon = glossary_terms_dict_addon[0]
                if glossary_terms_dict_addon is not None:
                    self.glossary_terms_dict_str = etree.tostring(glossary_terms_dict_addon).decode("utf8")
                    self.glossary_terms_dict = parse_glossary_terms_dict(self.glossary_terms_dict_str)
                    # self.glossary_terms_count = len(self.glossary_terms_dict)
            else:
                self.glossary_terms_dict_str = None       
                self.glossary_terms_dict = {}
                # self.glossary_terms_count = 0
    
            # check art_id's against the standard, old system of locators.
            try:
                art_locator = opasLocator.Locator(self.art_id)                                                  
                if self.art_id != art_locator.articleID():
                    logger.warning(f"art_id: {self.art_id} is not the same as the computed locator: {art_locator} ")
                    
                # Take advantage of Locator object for conversion data required.
                self.is_splitbook = art_locator.thisIsSplitBook
            except Exception as e:
                logger.error(f"Problem converting {self.art_id} to locator")
            
            # end parsed_xml section
            # #################################################################################

            
        if self.art_issue is None:
            if self.src_code in ["IJPOPEN"] and self.art_issue_title is not None:
                issue_num = IJPOPENISSUES.get(self.art_issue_title)
                self.art_issue = issue_num
                
        if self.verbose and self.art_vol_title is not None:
            print (f"\t...Volume title: {self.art_vol_title}")
    
        if self.verbose and self.art_issue_title is not None:
            print (f"\t...Issue title: {self.art_issue_title}")
    
    # Defining file_classification in the constructor introduces too much overhead for search
    def set_file_classification(self):
        try:
            match = classification_regex.search(str(self.fullfilename))
            self.file_classification = match.group("class").lower()
        except Exception as e:
            logger.warning("Could not determine file classification for %s (%s)" % (self.fullfilename, e))

if __name__ == "__main__":
    import sys
    sys.path.append('./config') 

    print (40*"*", "ArticleID module test", 40*"*")
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
