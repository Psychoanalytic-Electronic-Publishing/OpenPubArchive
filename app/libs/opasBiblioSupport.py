#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=C0321,C0103,C0301,E1101,C0303,E1004,C0330,R0915,R0914,W0703,C0326
# Disable many annoying pylint messages, warning me about variable naming for example.
# yes, in my Solr code I'm caught between two worlds of snake_case and camelCase.

""" 
OPAS - opasBiblioSupport

Support for information contained in references in bibliographies.

Module Assumptions:

   BibEntry class is general to accommodate be, maybe binc (TBD)

   During a compile to EXP_ARCH files from KBD files:
      - we need to check if the DB has an overriding locator (rx, rxcf) 
      - we may NOT want to check if rx exists, because it may simply not have been loaded yet. (TBD)
      - we need to save to DB
   
   During a "load" from EXP_ARCH compiled files:
      - we want to do the minimum for speed.
      - we need to check if the DB has an overriding locator (rx, rxcf)
      - we need to check to ensure all locator's (rx) exist (rxcf TBD)

"""
__author__      = "Neil R. Shapiro"
__copyright__   = "Copyright 2019-2023, Psychoanalytic Electronic Publishing"
__license__     = "Apache 2.0"

import sys
sys.path.append('../libs')
sys.path.append('../config')
sys.path.append('../libs/configLib')

#import os
from datetime import datetime
#import string
import re
#import statistics
#from typing import List, Generic, TypeVar, Optional
import models
# import time

import lxml
from lxml import etree
import roman
# import jellyfish

import logging
logger = logging.getLogger(__name__)
from loggingDebugStream import log_everywhere_if    # log as usual, but if first arg is true, also put to stdout for watching what's happening

gDbg2 = True
gDbg3 = True

parser = lxml.etree.XMLParser(encoding='utf-8', recover=True, resolve_entities=False)

from pydantic.main import Field # for later: BaseModel, 

#import localsecrets
import opasConfig
import opasGenSupportLib as opasgenlib
import opasXMLHelper as opasxmllib
import opasCentralDBLib
ocd = opasCentralDBLib.opasCentralDB()

import opasPySolrLib

# import opasLocator
# import opasArticleIDSupport
# import modelsOpasCentralPydantic
from opasLocator import Locator
from opasLocalID import LocalID

# new for processing code
import PEPJournalData
jrnlData = PEPJournalData.PEPJournalData()
import PEPBookInfo
known_books = PEPBookInfo.PEPBookInfo()
SPECIAL_BOOK_NAME_PATTERN = "((Ges|Gs)\.?\s+Schr\.?)|(Collected Papers)"
book_titles_mismarked = re.compile(SPECIAL_BOOK_NAME_PATTERN, flags=re.I)
SPECIAL_BOOK_RECOGNITION_PATTERN = "Wien" # Wien=Vienna
BOOK_PUBLISHERS = "hogarth|basic books|harper|other press|analytic press|dover publications|france\:|london\:|norton|press|random house|new york\:|UK\:|routledge|paris|munich|university press|karnac|henry holt"
SOLR_RESTRICTED_PUNCT = '\-\:\"\'\,/\[\]\(\)'

# Heuristic Settings for Biblio matching
SAME_REF_WTD_LIKELY = 0.45
SAME_REF_WTD_VERY_LIKELY = 0.55
SAME_REF_WTD_VERY_VERY_LIKELY = 0.65
SAME_AUTH_LIKELY = 0.50
SAME_AUTH_VERY_LIKELY = 0.75    # used to ensure right match for rx, otherwise rxcf
SAME_TITLE_LIKELY = 0.65        
SAME_TITLE_VERY_LIKELY = 0.80    # used to ensure right match for rx, otherwise rxcf
SAME_YEAR_VERY_LIKELY = 0.75
SAME_FULLCITE_VERY_LIKELY = 0.50  # Reference text vs citeastext from RDS database

#------------------------------------------------------------------------------------------------------------
#  Support functions
#------------------------------------------------------------------------------------------------------------

def copy_model(model):
    """
    Copy a pydantic model
    """
    return type(model)(**model.dict())

def copy_model_fields(model, sub_model):
    for field_name, field in model.__dict__.items():
        setattr(sub_model, field_name, field)
    return sub_model

#------------------------------------------------------------------------------------------------------
def get_reference_correction_articles(ocd, article_id, verbose=None):
    """
    Return the reference corrections for an article.  Use a mySQL like type wildcard for more than one
      article.
    
    Returns a list of biblioxml models. 
    """
    ret_val = None
        
    select = f"""SELECT distinct art_id from opasloader_refcorrections
                 WHERE art_id LIKE '{article_id}'
                 and reintegrated = 0;
                 """

    try:
        results = ocd.get_select_as_list_of_dicts(select)
    except Exception as e:
        # maybe the table isn't here on this machine.  So just return None.  The record is not Found
        #  but log it as a warning
        logger.info(f"Load from opasloader_refcorrections failed. May not be present. Error: ({e})")
    
    if results:
        ret_val = results
    
    # return a single model (record) or None if not in the table
    return ret_val  # return True for success

#------------------------------------------------------------------------------------------------------
def update_reference_correction_status(ocd,
                                       art_id,
                                       verbose=False):
    """
   
    """
    ret_val = False
    caller_name = "reference_correction record"
    
    sqlcpy = f"""
                UPDATE opasloader_refcorrections
                SET reintegrated = 1
                WHERE art_id = %s
              """
        
    query_params = (art_id, )
    
    try:
        # commit automatically handled by do_action_query
        res = ocd.do_action_query(querytxt=sqlcpy, queryparams=query_params)
    except Exception as e:
        errStr = f"{caller_name}: update error {e}"
        logger.error(errStr)
        if opasConfig.LOCAL_TRACE: print (errStr)
    else:
        ret_val = True
    
    return ret_val

#------------------------------------------------------------------------------------------------------
def get_reference_correction_list(ocd, article_id, verbose=None):
    """
    Return the reference corrections for an article.  Use a mySQL like type wildcard for more than one
      article.
    
    Returns a list of biblioxml models. 
    """
    ret_val = None
    # if reintegrated, never use these again    
    select = f"""SELECT * from opasloader_refcorrections
                 WHERE art_id LIKE '{article_id}'
                 and reintegrated = 0; 
              """

    try:
        results = ocd.get_select_as_list_of_models(select, model=models.Biblioxml)
    except Exception as e:
        # maybe the table isn't here on this machine.  So just return None.  The record is not Found
        #  but log it as a warning
        logger.info(f"Load from opasloader_refcorrections failed. May not be present. Error: ({e})")
    
    if results:
        ret_val = results
    
    # return a single model (record) or None if not in the table
    return ret_val  # return True for success

#------------------------------------------------------------------------------------------------------
def get_reference_correction(ocd, article_id, ref_local_id=None, verbose=None):
    """
    Temporary:
       Return a single reference from the opasloader_refcorrections table in opascentral
    
    TBD: This is an old table we used instead of editing all the old XML files to add in
         markup on non-PEP references.  These references should at some point
         be merged with the xml files and deprecating the table.
    
    >>> import opasCentralDBLib
    >>> ocd = opasCentralDBLib.opasCentralDB()  
    >>> corrected_bibentry = get_reference_correction(ocd, 'CPS.031.0617A', 'B0021')
    >>> corrected_bibentry.ref_xml[:120]
    '<be id = "B021"><a><l>Freud</l>, A.</a> <y>1936</y> <bst>The ego and the mechanisms of defense</bst> New York: <bp>Inter'
    """
    
    # The local_ids in the the correction table and the xml are 4 digits, rather than 3, which is what the biblioxml table has.
    local_id = LocalID(ref_local_id)
    ref_local_id = local_id.localIDStr
    
    ret_val = None
        
    select = f"""SELECT * from opasloader_refcorrections
                 WHERE art_id = '{article_id}'
                 and ref_local_id = '{ref_local_id}'
                 and reintegrated = 0;
                 """

    try:
        results = ocd.get_select_as_list_of_models(select, model=models.Biblioxml)
    except Exception as e:
        # maybe the table isn't here on this machine.  So just return None.  The record is not Found
        #  but log it as a warning
        logger.info(f"Load from opasloader_refcorrections failed. May not be present. Error: ({e})")
    
    if results:
        ret_val = results[0]
    
    # return a single model (record) or None if not in the table
    return ret_val  # return True for success

#------------------------------------------------------------------------------------------------------
def check_for_known_books(ref_text):
    ret_val = known_books.getPEPBookCodeStr(ref_text)
    if ret_val[0] is not None:
        ret_val[2] = "pattern"

    return ret_val

#------------------------------------------------------------------------------------------------------
def check_for_vol_suffix(ocd, loc_str, verbose=False):
    ret_val = None
    newloc = Locator(loc_str)
    if newloc.validate():
        if newloc.jrnlVol[-1].isnumeric():
            newloc.jrnlVol.volSuffix = "%"
            new_loc_str = newloc.articleID()
            new_locs = ocd.article_exists(new_loc_str)
            if len(new_locs) == 1:
                ret_val = new_locs[0][0]
                log_everywhere_if(verbose, level="debug", msg=f"\t\tCorrecting Bib ID with vol variant: {ret_val}.")

    return ret_val    

#------------------------------------------------------------------------------------------------------
def check_for_page_start(ocd, loc_str, page_offset=-1, verbose=False):
    ret_val = None
    newloc = Locator(loc_str)
    if newloc.validate():
        try:
            newloc.pgStart += page_offset
        except Exception as e:
            log_everywhere_if(verbose, "info", f"Bad locator {loc_str}")
        else:
            if newloc.isValid():
                new_loc_str = newloc.articleID()
                new_locs = ocd.article_exists(new_loc_str)
                if len(new_locs) == 1:
                    ret_val = new_locs[0][0]
                    log_everywhere_if(verbose, level="debug", msg=f"\t\tCorrecting Bib ID with page-1: {ret_val}.")
                #else:
                    #log_everywhere_if(verbose, "warning", f"Bad locator in reference {loc_str}->{new_loc_str}(article doesn't exist)")
    return ret_val    

#------------------------------------------------------------------------------------------------------
def strip_extra_spaces(textstr:str):
    if textstr is not None:
        ret_val = textstr.strip()
        ret_val = re.sub(" +", " ", ret_val)
    else:
        ret_val = None

    return ret_val

#------------------------------------------------------------------------------------------------------
# BiblioMatch Class
#  (eventually switch to pydantic model)
#------------------------------------------------------------------------------------------------------
class BiblioMatch(object):
    ref_rx: str = Field(None)
    ref_rx_confidence: float = Field(0, title="Confidence level of assigned rx. Manual QA = 1, code parsed = .99, Heuristic = .1 to .97")
    ref_rxcf: str = Field(None, title="List of possible article ID codes for this reference")
    ref_rxcf_confidence: float = Field(0, title="Max confidence level of rxcf codes")
    ref_xml: str = Field(None)
    ref_exists: bool = Field(False, title="Indicates this ref link has been verified")
    ref_link_source: str = Field(None, title="Source of rx link, 'xml', 'database', or 'pattern'")
    link_updated: bool = Field(False, title="Indicates whether the data was corrected during load. If so, may need to update DB.  See link_source for method")
    record_updated: bool = Field(False, title="Indicates whether the data was corrected during load. If so, may need to update DB.")
    last_update: datetime = Field(None)
    
    def __init__(self, bib_entry=None, verbose=False):
        if bib_entry:
            #self.__dict__ = bib_entry.__dict__.copy()    # just a shallow copy
            copy_model_fields(bib_entry, self)

    def __str__(self):
        #return f"{self.ref_rx}, {self.ref_rx_confidence}, {self.link_updated}, '{self.ref_rxcf}', {self.ref_rxcf_confidence}"
        return str(self.__dict__)
    
    def __repr__(self):
        # return f"{self.ref_rx}, {self.ref_rx_confidence}, {self.link_updated}, '{self.ref_rxcf}', {self.ref_rxcf_confidence}"
        return str(self.__dict__)
        #return self.ref_rx, self.ref_rx_confidence, self.link_updated, self.ref_rxcf, self.ref_rxcf_confidence
        

#------------------------------------------------------------------------------------------------------------
#  BiblioEntry Class
#------------------------------------------------------------------------------------------------------------
class BiblioEntry(models.Biblioxml):
    """
    An entry from a documents bibliography.
    
    Used to populate the MySQL table BIBLIO_TABLE (e.g., api_biblioxml) for statistical gathering
       and the Solr core pepwebrefs for searching in special cases.
       
    >>> import opasCentralDBLib
    >>> ocd = opasCentralDBLib.opasCentralDB()  

    >>> ref = '<be id="B036"><a><l>Freud</l>, S.</a> &amp; <a><l>Ferenczi</l>, S.</a> (<y>1993</y>). <bst>Correspondence</bst> (Vol. <v>1</v>, 1908-1914). Cambridge, MA: <bp>Harvard University Press</bp>.</be>'   
    >>> parsed_ref = etree.fromstring(ref, parser=parser)
    >>> be_journal = BiblioEntry(art_id="AJP.072.0016A", ref_or_parsed_ref=parsed_ref)
    >>> result = be_journal.identify_heuristic(verbose=False)
    >>> be_journal.ref_rx

    >>> ref = '<be id="B070"><a><l>Money-Kyrle</l>, R.</a> (<y>1968</y>). <t>Cognitive development.</t> <j>The International Journal of Psycho-Analysis</j>, <v>49</v>, <pp>691-698</pp>.</be>'
    >>> parsed_ref = etree.fromstring(ref, parser=parser)
    >>> be_journal = BiblioEntry(art_id="ANIJP-TR.007.0157A", ref_or_parsed_ref=parsed_ref)
    >>> # reset them if in database or pattern per above
    >>> be_journal.ref_rx = None
    >>> be_journal.ref_rx_confidence = 0
    >>> result = be_journal.identify_nonheuristic()
    >>> be_journal.ref_rx
    'IJP.049.0691A'

    >>> be_journal = BiblioEntry(art_id="CPS.031.0617A", ref_or_parsed_ref='<be lang="en" id="B024">Freud, S. 1905 On psychotherapy Standard Edition 7</be>')
    >>> be_journal.ref_rx

    >>> be_journal = BiblioEntry(art_id="CPS.031.0617A", ref_or_parsed_ref='<be lang="en" id="B054">Klein, M. 1946 Notes on some schizoid mechanisms In:The writings of Melanie Klein Ed. R.E. Money-Kyrle et al. London: Hogarth Press, 1975</be>')
    >>> be_journal.ref_rx
    
    >>> be_journal.identify_heuristic(verbose=False)
    {'ref_rx': 'IPL.104.0001A', 'ref_rx_confidence': 0.88, 'link_updated': True, 'link_source': 'heuristic', 'ref_rxcf': 'IJP.027.0099A:0.87, PAQ.018.0122A:0.46, IJP.088.0387A:0.46', 'ref_rxcf_confidence': 0.87}
    
    >>> check_for_page_start(ocd, "AIM.014.0194A", page_offset=-1)
    'AIM.014.0193A'
    
    >>> check_for_page_start(ocd, "AIM.014.0192A", page_offset=1)
    'AIM.014.0193A'
    
    #Bad reference
    >>> be_journal = BiblioEntry(art_id="JICAP.019.0358A", ref_or_parsed_ref='<be id="B0036" reftype="journal" class="mixed-citation"><a><l>Ramires</l>, V. R. R.</a>, <a><l>Godinho</l>, L. R.</a>, &amp; <a> <l>Goodman</l>, G.</a> (<y>2017</y>). <t class="article-title">The therapeutic process of a child diagnosed with disruptive mood dysregulation disorder</t>. <j>Psychoanalytic Psychology</j>, <v> 34</v>(<bs>4</bs>), <pp>pap0000134</pp>. <webx type="url" url="https://doi.org/10.1037/pap0000134">https://doi.org/10.1037/pap0000134</webx></be>')
    >>> be_journal.ref_rx
    'PPSY.034.0134A'
        
    """
    
    def __init__(self, art_id, art_year=None, ref_or_parsed_ref=None, db_bib_entry=None, verbose=False, **kwargs):
        super().__init__(**kwargs)
        self.ref_link_source = None
        self.link_updated = False
        self.record_updated = False
        self.ref_in_pep = None
        self.ref_pgstart = ""
        self.ref_pgrg = ""
        self.ref_exists = False
        self.ref_title = ""
        self.ref_sourcecode = ""
        self.ref_sourcetype = ""
        self.ref_is_book = False
        self.art_id = art_id
        self.record_from_db = False

        # allow either string xml ref or parsed ref
        if ref_or_parsed_ref is None and not db_bib_entry:
            raise ValueError("You must provide a ref or parsed ref")
        elif db_bib_entry:
            #self.__dict__ = db_bib_entry.__dict__.copy()    # just a shallow copy
            #self = copy_model(db_bib_entry)
            copy_model_fields(db_bib_entry, self)
            ref_or_parsed_ref = self.ref_xml
            self.record_from_db = True

        if isinstance(ref_or_parsed_ref, str):
            parsed_ref = etree.fromstring(ref_or_parsed_ref, parser=parser)
            ref_entry_xml = ref_or_parsed_ref
            self.ref_xml = ref_entry_xml
        elif isinstance(ref_or_parsed_ref, object):
            parsed_ref = ref_or_parsed_ref
            ref_entry_xml = etree.tostring(parsed_ref, with_tail=False)
            ref_entry_xml = ref_entry_xml.decode("utf8") # convert from bytes
            self.ref_xml = ref_entry_xml
        else:
            raise TypeError("arg must be str or etree")
           
        self.ref_local_id = opasxmllib.xml_get_element_attr(parsed_ref, "id")

        # see if the xml for this has been replaced.  
        # NOT NEEDED. The updated records have been integrated into the original files as of 2023-02-16
        #ref_corrected_entry = get_reference_correction(ocd, art_id, self.ref_local_id)
        #if ref_corrected_entry:
            #print (f"\t...Ref correction loaded to replace {self.ref_text}.")
            #self.record_updated = True
            #self.ref_text = ref_corrected_entry.ref_text
            #self.ref_xml = ref_corrected_entry.ref_xml
            #self.ref_xml_corrected = True
            #parsed_ref = etree.fromstring(ref_corrected_entry.ref_xml, parser=parser)
        #else:
            #self.ref_xml_corrected = False
        
        # take the parsed ref from whence it came! (db, instance, or ref_corrected)
        self.parsed_ref = parsed_ref
        self.ref_xml = re.sub(" +", " ", self.ref_xml)
        if art_year is not None:
            try:
                self.art_year = int(art_year)
            except Exception as e:
                self.art_year = 0

        ref_text = opasxmllib.xml_elem_or_str_to_text(self.parsed_ref)
        ref_text = strip_extra_spaces(ref_text)
        self.ref_text = ref_text
        
        if not db_bib_entry:
            self.last_update = datetime.today()
            self.ref_rx = opasxmllib.xml_get_element_attr(self.parsed_ref, "rx", default_return=None)
            if self.ref_rx:
                default_rx_confidence = str(opasConfig.RX_CONFIDENCE_KEYED_VERY_LIKELY)
                self.ref_link_source = opasConfig.RX_LINK_SOURCE_DOCUMENT
            else:
                default_rx_confidence = "0"
            self.ref_rx_confidence = opasxmllib.xml_get_element_attr(self.parsed_ref, "rxconf", default_return=default_rx_confidence)
            self.ref_rx_confidence = opasgenlib.str_to_float(self.ref_rx_confidence, default=0)
            
            self.ref_rxcf = opasxmllib.xml_get_element_attr(self.parsed_ref, "rxcf", default_return=None) # related rx
            self.ref_rxcf_confidence = opasxmllib.xml_get_element_attr(self.parsed_ref, "rxcfconf", default_return="0") # related rx
            self.ref_rxcf_confidence = opasgenlib.str_to_float(self.ref_rxcf_confidence, default=0)

            self.record_from_db = False
        else:
            self.last_update = db_bib_entry.last_update 
            self.ref_rxp = None
            self.ref_rxp_confidence = 0
            self.ref_rx = db_bib_entry.ref_rx
            self.ref_rx_confidence = db_bib_entry.ref_rx_confidence
            self.ref_rxcf = db_bib_entry.ref_rxcf
            self.ref_rxcf_confidence = db_bib_entry.ref_rxcf_confidence          
            
        self.ref_rxp = opasxmllib.xml_get_element_attr(self.parsed_ref, "rxp", default_return=None)
        self.ref_rxp_confidence = opasxmllib.xml_get_element_attr(self.parsed_ref, "rxps", default_return=None)
        if self.ref_rxp and not self.ref_rx:
            if ocd.article_exists(self.ref_rxp):
                self.ref_rx = self.ref_rxp
                if not self.ref_rxp_confidence:
                    self.ref_rx_confidence = opasConfig.RX_CONFIDENCE_PROBABLE
                else:
                    self.ref_rx_confidence = round(opasgenlib.str_to_float(self.ref_rxp_confidence, default=opasConfig.RX_CONFIDENCE_PROBABLE) / 100, 2)
                self.ref_link_source = opasConfig.RX_LINK_SOURCE_DOCUMENT_RXP
                self.ref_rxp = None
                self.ref_rxp_confidence = None
                self.link_updated = True
                self.ref_in_pep = True

        if not self.ref_pgrg:
            self.ref_pgrg = opasxmllib.xml_get_subelement_textsingleton(self.parsed_ref, "pp")
            self.ref_pgrg = opasgenlib.first_item_grabber(self.ref_pgrg, re_separator_ptn=";|,", def_return=self.ref_pgrg)
            self.ref_pgrg = opasgenlib.trimLeadingNonDigits(self.ref_pgrg)
            self.ref_pgrg = self.ref_pgrg[:23]
        
        self.ref_volume = opasxmllib.xml_get_subelement_textsingleton(self.parsed_ref, "v")
        self.ref_volume = self.ref_volume[:23]
        if self.ref_volume:
            self.ref_volume_isroman = opasgenlib.is_roman_str(self.ref_volume.upper())
            if self.ref_volume_isroman:
                try:
                    self.ref_volume_int = roman.fromRoman(self.ref_volume.upper()) # use roman lib which generates needed exception
                except Exception as e:
                    self.ref_volume_isroman = False
                    try:
                        self.ref_volume_int = int(self.ref_volume)
                    except Exception as e:
                        log_everywhere_if(True, "warning", msg=f"BibEntry vol {self.ref_volume} is neither roman nor int {e}")
                        self.volume_int = 0
            else:
                if self.ref_volume.isnumeric():
                    self.ref_volume_int = int(self.ref_volume)
                else:
                    self.ref_volume_int = 0
        else:
            self.ref_volume_int = 0
            self.ref_volume_isroman = False

        self.ref_publisher = opasxmllib.xml_get_subelement_textsingleton(self.parsed_ref, "bp")
        self.ref_publisher = self.ref_publisher[:254]
        journal_title = opasxmllib.xml_get_subelement_textsingleton(self.parsed_ref, "j")
        book_title = opasxmllib.xml_get_subelement_textsingleton(self.parsed_ref, "bst")
        self.ref_sourcetitle = None
        # worth the check here for books since not all books are tagged.
        book_markup = self.parsed_ref.xpath("//bst|bp|bsy|bpd")
        journal = self.parsed_ref.xpath("//j")

        # special book handling
        if book_titles_mismarked.match(journal_title):
            book_title = journal_title
            journal_title = None
            # set below since now we have book_title and not journal title
            #self.ref_is_book = True
            #self.ref_sourcetype = "book"

        if (book_markup or book_title or self.ref_publisher) and not journal_title:
            self.ref_is_book = True
            self.ref_sourcetype = "book"
            self.ref_sourcetitle = book_title  # book title
        elif journal_title or journal:
            self.ref_sourcetype = "journal"
            self.ref_is_book = False
            self.ref_sourcetitle = journal_title
        else:
            if re.search(BOOK_PUBLISHERS, ref_text, flags=re.I):
                self.ref_sourcetype = "book"
                self.ref_is_book = True
            else:
                self.ref_sourcetype = "unknown"
                self.ref_is_book = False

            if journal_title and book_title:
                self.ref_sourcetitle = f"{journal_title} / {book_title}"
            else:
                self.ref_sourcetitle = f"{journal_title}{book_title}"

        # see if we have info to link SE/GW etc., these are in a sense like journals
        if not self.ref_sourcecode:
            if PEPJournalData.PEPJournalData.rgxSEPat2.search(self.ref_text):
                self.ref_in_pep = True
                self.ref_sourcecode = "SE"
                self.ref_is_book = True
                self.record_updated = True
                if verbose: print (f"\t...Recognized SE. Record will be updated.")
                
            elif PEPJournalData.PEPJournalData.rgxGWPat2.search(self.ref_text):
                if not re.search("G\.\s?W\.\s?</a>", self.ref_text):
                    self.ref_in_pep = True
                    self.ref_sourcecode = "GW"
                    self.ref_is_book = True
                    self.record_updated = True
                    if verbose: print (f"\t...Recognized GW. Record will be updated.")
        elif self.ref_sourcecode in ("GW", "SE", "IPL", "NLP", "ZBK"):
                self.ref_in_pep = True
                self.ref_is_book = True
            
        if self.ref_is_book:
            year_of_publication = opasxmllib.xml_get_subelement_textsingleton(self.parsed_ref, "bpd")
            if not year_of_publication:
                year = opasxmllib.xml_get_subelement_textsingleton(self.parsed_ref, "y")
                year_of_publication = opasgenlib.remove_all_punct(year) # punct_set=[',', '.', ':', ';', '(', ')', '\t', r'/', '"', "'", "[", "]"])
                #self.record_updated = True
            if not self.ref_sourcetitle and book_title:
                ## sometimes has markup
                self.ref_sourcetitle = book_title  # book title (bst)
                if verbose: print (f"\t...Found book title '{book_title}'. Record will be updated.")
                self.record_updated = True
                
        if year_of_publication != "":
            # make sure it's not a range or list of some sort.  Grab first year
            year_of_publication = opasgenlib.year_grabber(year_of_publication)
        else:
            # try to match
            try:
                m = re.search(r"\(([A-z]*\s*,?\s*)?([12][0-9]{3,3}[abc]?)\)", self.ref_xml)
                if m is not None:
                    year_of_publication = m.group(2)
            except Exception as e:
                logger.warning("no match %s/%s/%s" % (year_of_publication, self.parsed_ref, e))
            
        if year_of_publication != "" and year_of_publication is not None:
            year_of_publication = re.sub("[^0-9]", "", year_of_publication)

        self.ref_year = year_of_publication
        if self.ref_year != "" and self.ref_year is not None:
            try:
                self.ref_year_int = int(self.ref_year[0:4])
            except ValueError as e:
                logger.error("Error converting year_of_publication to int: %s / %s.  (%s)" % (self.ref_year, self.ref_xml, e))
            except Exception as e:
                logger.error("Error trying to find untagged bib year in %s (%s)" % (self.ref_xml, e))
                
        else:
            self.ref_year_int = 0
            
        ref_title = opasxmllib.xml_get_subelement_textsingleton(self.parsed_ref, "t")
        if ref_title:
            ref_title = strip_extra_spaces(ref_title)
            ref_title = ref_title[:1023]
            self.ref_title = ref_title
        elif self.ref_sourcetitle and self.ref_is_book:
            ref_title = self.ref_sourcetitle
            self.ref_title = ref_title 
        
        author_name_list = [etree.tostring(x, with_tail=False).decode("utf8") for x in self.parsed_ref.findall("a") if x is not None]
        self.ref_authors_xml = '; '.join(author_name_list)
        self.ref_authors_xml = self.ref_authors_xml[:2040]
        author_list = [opasxmllib.xml_elem_or_str_to_text(x) for x in self.parsed_ref.findall("a") if opasxmllib.xml_elem_or_str_to_text(x) is not None]  # final if x gets rid of any None entries which can rarely occur.
        self.ref_authors = '; '.join(author_list)
        self.ref_authors = self.ref_authors[:2040]
        self.ref_doi = self.parsed_ref.findtext("webx[@type='doi']")

        try:
            if self.ref_rx is None:
                self.identify_nonheuristic(verbose=verbose)
        except:
            log_everywhere_if(verbose, "debug", f"Biblio Entry ID issues {art_id, self.ref_local_id}")
        else:
            if gDbg2:
                log_everywhere_if(self.ref_rx, "debug", f"\t\t...BibEntry linked to {self.ref_rx} loaded")

    #------------------------------------------------------------------------------------------------------------
    def update_bib_entry(self, bib_match, verbose=False):
        if bib_match:
            #self.__dict__ = bib_match.__dict__.copy()    # just a shallow copy
            #self = copy_model(bib_match)
            copy_model_fields(bib_match, self)            

    #------------------------------------------------------------------------------------------------------------
    def identify_nonheuristic(self, pretty_print=False, verbose=False):
        """
          (Heuristic Search is now a separate process to optimize speed.)
          
          returns ref_rx, ref_rxconfidence, link_updated
          
          >>> import opasCentralDBLib
          >>> ocd = opasCentralDBLib.opasCentralDB()  

          >>> ref = '<be id="B0006" reftype="journal" class="mixed-citation"><a class="western"><l>Beebe</l>, B.</a>, &amp; <a class="western"> <l>Lachmann</l>, F.</a> (<y>2002</y>). <t class="article-title">Organizing principles of interaction from infant research and the lifespan prediction of attachment: Application to adult treatment</t>. <j>Journal of Infant, Child, and Adolescent Psychotherapy</j>, <v> 2</v>(<bs>4</bs>), <pp>61 - 89</pp>&#8211;. doi:<webx type="doi">10.1080/15289168.2002.10486420</webx></be>'
          >>> parsed_ref = etree.fromstring(ref, parser=parser)
          >>> be_journal = BiblioEntry(art_id="FA.013A.0120A", ref_or_parsed_ref=parsed_ref)
          >>> be_journal.ref_rx, be_journal.ref_rx_confidence, be_journal.link_source
          ('JICAP.002D.0061A', 0.91, 'variation')

          >>> # reset them if in database or pattern per above
          >>> be_journal.ref_rx = None
          >>> be_journal.ref_rx_confidence = 0
          >>> result = be_journal.identify_nonheuristic()
          >>> be_journal.ref_rx
          'JICAP.002D.0061A'
          
          >>> ref = '<be id="B070"><a><l>Money-Kyrle</l>, R.</a> (<y>1968</y>). <t>Cognitive development.</t> <j>The International Journal of Psycho-Analysis</j>, <v>49</v>, <pp>691-698</pp>.</be>'
          >>> parsed_ref = etree.fromstring(ref, parser=parser)
          >>> be_journal = BiblioEntry(art_id="ANIJP-TR.007.0157A", ref_or_parsed_ref=parsed_ref)
          >>> be_journal.ref_rx, be_journal.ref_rx_confidence, be_journal.link_source
          ('IJP.049.0691A', 0.91, 'pattern')

          >>> ref = '<be id="B070"><a><l>Money-Kyrle</l>, R.</a> (<y>1968</y>). <t>Cognitive development.</t> <j>The International Journal of Psycho-Analysis</j>, <v>49</v>, <pp>691-698</pp>.</be>'
          >>> parsed_ref = etree.fromstring(ref, parser=parser)
          >>> be_journal = BiblioEntry(art_id="ANIJP-TR.007.0157A", ref_or_parsed_ref=parsed_ref)
          >>> # reset them if in database or pattern per above
          >>> be_journal.ref_rx = None
          >>> be_journal.ref_rx_confidence = 0
          >>> result = be_journal.identify_nonheuristic()
          >>> be_journal.ref_rx
          'IJP.049.0691A'
      
          >>> be_journal = BiblioEntry(art_id="FA.013A.0120A", ref_or_parsed_ref='<be id="B009"><a><l>Sternberg</l>, J</a> &amp; <a><l>Scott</l>, A</a> (<y>2009</y>) <t>Editorial.</t> <j>British Journal of Psychotherapy</j>, <v>25</v> (<bs>2</bs>): <pp>143-5</pp>.</be>')
          >>> be_journal.ref_rx, be_journal.ref_rx_confidence, be_journal.link_source
          ('BJP.025.0143A', 0.91, 'pattern')

          >>> # reset them if in database or pattern per above
          >>> be_journal.ref_rx = None
          >>> be_journal.ref_rx_confidence = 0
          >>> result = be_journal.identify_nonheuristic()
          >>> be_journal.ref_rx
          'BJP.025.0143A'

          >>> ref = '<be label="19" id="B019"><a><l>Freud</l></a>, <bst>Beyond the Pleasure Principle</bst> (London, <y>1950</y>), pp. <pp>17</pp> ff.</be>'
          >>> be_journal = BiblioEntry(art_id="FA.013A.0120A", ref_or_parsed_ref=ref)
          >>> be_journal.ref_rx, be_journal.ref_rx_confidence, be_journal.link_source
          (None, 0, None)
          >>> # reset them if in database or pattern per above
          >>> be_journal.ref_rx = None
          >>> be_journal.ref_rx_confidence = 0
          >>> result = be_journal.identify_nonheuristic()
          >>> be_journal.ref_rx

        """
    
        #pep_ref = False
        ref_id = self.ref_local_id
        ret_val = None, None
        link_updated = False
        
        if self.ref_rx is None:
            # still no known rx
            if self.ref_is_book:
                loc_str, match_val, whatever = known_books.getPEPBookCodeStr(self.ref_text)
                if loc_str is not None:
                    self.ref_rx = loc_str 
                    self.ref_rx_confidence = opasConfig.RX_CONFIDENCE_PROBABLE
                    self.ref_link_source = opasConfig.RX_LINK_SOURCE_PATTERN
                    msg = f"\t\t\t...Matched Book {match_val}."
                    log_everywhere_if(verbose, level="debug", msg=msg)
            
            if self.ref_sourcecode and not link_updated:
                if not opasgenlib.is_empty(self.ref_pgrg):
                    try:
                        self.ref_pgstart, bib_pgend = self.ref_pgrg.split("-")
                    except Exception as e:
                        self.ref_pgstart = self.ref_pgrg
                else:
                    if self.ref_is_book:
                        self.ref_pgstart = 0
                    else:
                        self.ref_pgstart = bib_pgend = None
                
                if self.ref_pgstart or self.ref_is_book:
                    locator = Locator(strLocator=None,
                                      jrnlCode=self.ref_sourcecode, 
                                      jrnlVolSuffix="", 
                                      jrnlVol=self.ref_volume, 
                                      jrnlIss=None, 
                                      pgVar="A", 
                                      pgStart=self.ref_pgstart, 
                                      jrnlYear=self.ref_year, 
                                      localID=ref_id, 
                                      keepContext=1, 
                                      forceRoman=False, 
                                      notFatal=True, 
                                      noStartingPageException=True, 
                                      #filename=artInfo.filename
                                      )
                    
                    if locator.valid:
                        #pep_ref = True
                        loc_str = str(locator)
                        if ocd.article_exists(loc_str):
                            msg = f"\t\t...Locator verified {loc_str}"
                            log_everywhere_if(verbose, level="debug", msg=msg)
                            self.ref_rx = loc_str
                            self.ref_exists = True
                            self.ref_rx_confidence = opasConfig.RX_CONFIDENCE_PROBABLE
                            link_updated = True
                            self.ref_link_source = opasConfig.RX_LINK_SOURCE_PATTERN
                        else:
                            # see if there's a vol variant
                            self.ref_exists = False
                            loc_str = locator.articleID()
                            newloc = locator.check_for_offset_page_start(verbose=False) # log it here, not there
                            if newloc is not None:
                                msg = f"\t\t...Page start fix: chgd {loc_str} to {newloc} in ref"
                                log_everywhere_if(verbose, level="debug", msg=msg)
                                self.ref_rx = str(newloc)
                                self.ref_rx_confidence = opasConfig.RX_CONFIDENCE_PROBABLE
                                link_updated = True
                                self.ref_link_source = opasConfig.RX_LINK_SOURCE_VARIATION
                            else:
                                newloc = locator.check_for_missing_vol_suffix(verbose=False) # log it here, not there
                                if newloc is not None:
                                    msg = f"\t\t...VolSuffix missing: chgd {loc_str} to {newloc} in ref"
                                    log_everywhere_if(verbose, level="debug", msg=msg)
                                    self.ref_rx = str(newloc)
                                    self.ref_rx_confidence = opasConfig.RX_CONFIDENCE_PROBABLE
                                    link_updated = True
                                    self.ref_link_source = opasConfig.RX_LINK_SOURCE_VARIATION
                else:
                    locator = None

                if self.ref_in_pep and not self.ref_exists:
                    if locator.valid == 0:
                        msg = f"\t\tBib ID {ref_id} loc not valid {locator.articleID()} (components: {self.ref_sourcecode}/{self.ref_volume}/{self.ref_pgstart}) {opasgenlib.text_slice(self.ref_text, start_chr_count=25, end_chr_count=50)}"
                        log_everywhere_if(verbose, level="debug", msg=msg[:opasConfig.MAX_LOGMSG_LEN])
                    else:
                        msg = f"\t\tBib ID {ref_id} loc valid {locator.articleID()} but doesn't exist (components: {self.ref_sourcecode}/{self.ref_volume}/{self.ref_pgstart}) {opasgenlib.text_slice(self.ref_text, start_chr_count=25, end_chr_count=50)}"
                        log_everywhere_if(verbose, level="debug", msg=msg[:opasConfig.MAX_LOGMSG_LEN])
        else:
            self.ref_link_source = opasConfig.RX_LINK_SOURCE_RX
            
        self.link_updated = link_updated
        #ret_val = self.ref_rx, self.ref_rx_confidence, self.link_updated
        ret_val = BiblioMatch()
        ret_val.ref_exists = self.ref_exists
        ret_val.ref_rx = self.ref_rx
        ret_val.ref_rx_confidence = self.ref_rx_confidence
        ret_val.link_updated = self.link_updated
        ret_val.ref_link_source = self.ref_link_source
        ret_val.ref_rxcf = self.ref_rxcf
        ret_val.ref_rxcf_confidence = self.ref_rxcf_confidence
        
        return ret_val

    #------------------------------------------------------------------------------------------------------------
    def identify_heuristic(self,
                           minrx_similarity_title=SAME_TITLE_LIKELY,
                           minrxcf_wtd_similarity=SAME_REF_WTD_LIKELY,
                           minrx_similarity_author=SAME_AUTH_LIKELY,
                           verbose=False):
        """
        >>> ref='<be id="B072"><a><l>Freud</l>, Sigmund.</a> (<y>1917</y>). <t>&#8216;Mourning and Melancholia.&#8217;</t> <bst>Collected Papers</bst>. Vol. <v>IV</v>. London: <bp>Hogarth Press</bp>.</be>'
        >>> be_journal = BiblioEntry(art_id="PAQ.084.0589A", ref_or_parsed_ref=ref)
        >>> result = be_journal.identify_heuristic(verbose=False)
        >>> be_journal.ref_rx
        'SE.014.0237A'
        >>> be_journal.ref_rxcf
        'IJPSP.008.0363A:0.6, IJPOPEN.002.0090A:0.53, ZBK.038.0095A:0.49, ZBK.140.0078A:0.49, PAQ.074.0083A:0.47'

        >>> ref = '<be label="19" id="B019"><a><l>Freud</l></a>, <bst>Beyond the Pleasure Principle</bst> (London, <y>1950</y>), pp. <pp>17</pp> ff.</be>'       
        >>> be_journal = BiblioEntry(art_id="FA.013A.0120A", ref_or_parsed_ref=ref)
        >>> result = be_journal.identify_heuristic()
        >>> be_journal.ref_rx
        'SE.018.0001A'
        
        """

        title_distance = "~1"
        min_words = 3
        query = "art_id:*"
        #time1 = time.time()
        #if self.ref_sourcetitle:
            #source_title = self.ref_sourcetitle
            #query += f" AND art_sourcetitlefull:({self.ref_sourcetitle})"
            
        title = self.ref_title or self.ref_sourcetitle

        if title:
            query = f"art_title:\"{title}\"{title_distance} OR art_sourcetitlefull:\"{title}\"{title_distance}"
            words = len(title.split(" "))
            skip = False
        else:
            words = ""
            if verbose: 
                print(f"\tNo title or source title ({self.ref_xml})")
            skip = True
        
        if words <= min_words:
            skip = True
        if not skip:
            title_list = []
            result, return_status = opasPySolrLib.search_text(query=query,
                                                              limit=opasConfig.HEURISTIC_SEARCH_MAX_COUNT,
                                                              similar_count=opasConfig.HEURISTIC_SEARCH_MAX_COUNT, 
                                                              offset=0,
                                                              full_text_requested=False,
                                                              req_url=opasConfig.CACHEURL # so it doesn't log missing session id
                                                              ) 
            if return_status[0] == 200:
                if result.documentList.responseInfo.fullCount > 100 and words <= min_words:
                    if verbose: print (f"\tToo many hits ({result.documentList.responseInfo.fullCount} and too few words in title {words}")
                else:
                    result_count = result.documentList.responseInfo.count
                    if result_count > 0 and result_count <= opasConfig.HEURISTIC_SEARCH_MAX_COUNT:
                        for item in result.documentList.responseSet[0:opasConfig.MAX_CF_LIST]:
                            locator = Locator(item.documentID)
                            weighted_score, considered = \
                                self.get_weighted_confidence(item_score=item.score,            # solr_score
                                                             item_title=item.title,
                                                             item_sourcetitle=item.sourceTitle,
                                                             item_reftext=item.documentRef,
                                                             item_authors=item.authorCitation,
                                                             item_isbook=locator.isBook()
                                                             )
                            considered["rx"] = item.documentID
                            considered["source_title"] =  title
                            if weighted_score >= minrxcf_wtd_similarity:
                                title_list.append(considered)
                        
                        if title_list:
                            title_list = sorted(title_list, key=lambda d: d["score"], reverse=True)
                            rx_confidence = title_list[0]["score"]
                            if rx_confidence >= minrx_similarity_title and considered["sim_auth"] >= minrx_similarity_author and words > 1:
                                self.ref_rx = title_list[0]["rx"]
                                self.ref_rx_confidence = round(rx_confidence,2) # opasConfig.RX_CONFIDENCE_PROBABLE
                                self.link_updated = True
                                self.ref_link_source = opasConfig.RX_LINK_SOURCE_TITLE_HEURISTIC
                                
                                if 0: # on second thought, leave it in considered, so a reviewer can decide to manually 
                                      # remove it from rx in the table, if not a good match, 
                                      # and there will still be a link to it in rxcf
                                    if considered["sim_auth"] >= SAME_AUTH_VERY_LIKELY:
                                        # if the authors are likely the same, remove rx link from title list
                                        #   otherwise, leave it.  That way, the rx can be manually deleted,
                                        #   but this likely related reference stays in the rxcf list.
                                        #   Minor downside: the link will stay in both if it's not deleted.
                                        title_list = title_list[1:]
                                if verbose:
                                    print (f"\t***Found: {self.ref_rx}:{self.ref_rx_confidence}")
                                    #if gDbg2: time.sleep(2) # Pause
                                
                            if title_list: # take the rest as rxcf
                                self.ref_rxcf = [f'{item["rx"]}:{item["score"]:.2f}' for item in title_list]
                                self.ref_rxcf = self.ref_rxcf[:opasConfig.HEURISTIC_SEARCH_LIST_MAX_LEN]
                                self.ref_rxcf = ", ".join(self.ref_rxcf)
                                self.ref_rxcf_confidence = round(max([item["score"] for item in title_list]), 2)
                                self.link_updated = True
                                self.ref_link_source = opasConfig.RX_LINK_SOURCE_HEURISTIC
                                if verbose:
                                    print (f"\t***Related: {self.ref_rxcf}")
                                    if gDbg2:
                                        for n in title_list:
                                            print (f"\t\tRelated: rxcf: {n['rx']} - {opasgenlib.text_slice(n['ref_text_match'])}")
                                        #time.sleep(1) # Pause
        
        ret_val = BiblioMatch()
        ret_val.record_updated = self.record_updated
        ret_val.ref_rx = self.ref_rx
        ret_val.ref_rx_confidence = self.ref_rx_confidence
        ret_val.link_updated = self.link_updated
        ret_val.ref_link_source = self.ref_link_source
        ret_val.ref_rxcf = self.ref_rxcf
        ret_val.ref_rxcf_confidence = self.ref_rxcf_confidence
        #if verbose:
            #time2 = time.time()
            #heuristic_time = time2 - time1
            #print (f"\t...Heuristics time: {heuristic_time:.4f}")
        
        return ret_val

    #------------------------------------------------------------------------------------------------------------
    def lookup_title_in_db(self, ocd, pub_type_match_required=True, verbose=False):
        """
        Try to match the title in the Database
        >>> import opasCentralDBLib
        >>> ocd = opasCentralDBLib.opasCentralDB()  

        >>> be_journal = BiblioEntry(art_id="AIM.013.0319A", ref_or_parsed_ref='<be label="33" id="B033"><a>S. <l>Freud</l></a>, <bst>New Introductory Lectures on Psychoanalysis</bst>, <bp>Hogarth Press</bp>, London, <bpd>1933</bpd>, p. <pp>106</pp>.</be>')
        >>> be_journal.ref_rx
        
        >>> result = be_journal.lookup_title_in_db(ocd)
        >>> be_journal.ref_rx
        'SE.022.0001A'
        
        >>> # wrong page number supplied below, so no RX at first.  Then we look up title
        >>> be_journal = BiblioEntry(art_id="ANIJP-TR.007.0157A", ref_or_parsed_ref='<be id="B008"><a><l>Freud</l>, S.</a>, (<y>1917</y>), <t>“Mourning and melancholia”,</t> <bst>SE</bst> <v>14</v>, <pp>242-58</pp>.</be>', verbose=False)
        >>> be_journal.ref_rx   

        >>> result = be_journal.lookup_title_in_db(ocd)
        >>> result.ref_rx
        'SE.014.0237A'

        """
        ret_val = None
        if not opasgenlib.is_empty(self.ref_title):
            ref_title = opasgenlib.remove_all_punct(self.ref_title)
        else:
            ref_title = opasgenlib.remove_all_punct(self.ref_sourcetitle)

        # try to record and avoid conditions of false positive matches on tile
        exception_condition = len(ref_title) < 15 and self.ref_is_book
            
        if not exception_condition:
            select = f"""SELECT art_id,
                                art_year,
                                art_auth_citation,
                                art_citeas_text,
                                bk_info_xml,
                                art_title
                         from api_articles
                         where art_title sounds like '{ref_title}'
                         """
            results = ocd.get_select_as_list(select)
            if results:
                for result in results:
                    ret_val = result[0]
                    art_author_citation = result[2]
                    art_citation = result[3]
                    ref_year = result[1]
                    if result[4]:
                        article_is_book = True
                    else:
                        article_is_book = False
                    #if self.ref_is_book and article_is_book or pub_type_match_required == False:
                        #pub_type_match = True
                    #else:
                        #pub_type_match = False
                    
                    art_title = result[5]
                    art_title = opasgenlib.remove_all_punct(art_title)

                    title_confidence = opasgenlib.similarityText(art_title, ref_title)
                    if not ref_title:
                        # ignore this one
                        if verbose: print (f"\t...No title found, ignoring this one from now on.")
                        self.ref_rx_confidence = opasConfig.RX_CONFIDENCE_NEVERMORE
                        self.link_updated = True                        
                    else:
                        weighted_score, considered = \
                            self.get_weighted_confidence(item_title=art_title,
                                                         item_reftext=art_citation,
                                                         item_authors=art_author_citation,
                                                         item_isbook=article_is_book
                                                         )
                        weighted_score = round(weighted_score, 2)
                        if title_confidence >= SAME_TITLE_VERY_LIKELY:
                            if weighted_score > SAME_REF_WTD_VERY_VERY_LIKELY:
                                self.ref_rx = ret_val
                                if ref_year == self.ref_year_int:
                                    self.ref_rx_confidence = weighted_score
                                    self.link_updated = True
                                    self.ref_link_source = opasConfig.RX_LINK_SOURCE_TITLE_AND_YEAR                            
                                    break
                                else:
                                    self.ref_rx_confidence = weighted_score
                                    self.link_updated = True
                                    self.ref_link_source = opasConfig.RX_LINK_SOURCE_TITLE
                                    break
                            else:
                                if not self.ref_rxcf and weighted_score > SAME_REF_WTD_LIKELY:
                                # use rxcf
                                    self.ref_rxcf = f"{ret_val}:{weighted_score}"
                                    self.ref_rxcf_confidence = weighted_score
                                    self.ref_link_source = opasConfig.RX_LINK_SOURCE_TITLE_WEIGHTED                            
                                    self.link_updated = True
            else:
                ret_val = None
        
        #ret_val = self.ref_rx, self.ref_rx_confidence, self.link_updated
        ret_val = BiblioMatch()
        ret_val.ref_rx = self.ref_rx
        ret_val.ref_rx_confidence = self.ref_rx_confidence
        ret_val.link_updated = self.link_updated
        ret_val.ref_link_source = self.ref_link_source
        ret_val.ref_rxcf = self.ref_rxcf
        ret_val.ref_rxcf_confidence = self.ref_rxcf_confidence
        ret_val.ref_xml = self.ref_xml
        
        return ret_val  

    #------------------------------------------------------------------------------------------------------------
    def lookup_more_exact_artid_in_db(self, ocd):
        """
        >>> import opasCentralDBLib
        >>> ocd = opasCentralDBLib.opasCentralDB()  
        
        >>> be_journal = BiblioEntry(art_id="AIM.014.0193A", ref_or_parsed_ref='<be label="7" id="B007"><a>S. <l>Freud</l></a>, <t>&#8216;The Theme of the Three Caskets&#8217;,</t> <bst>Standard Edition</bst> <v>12</v>.</be>')
        >>> be_journal.ref_rx
        'SE.012.0000A'
        >>> result = be_journal.lookup_more_exact_artid_in_db(ocd)
        >>> result
        {'ref_rx': 'SE.012.0289A', 'ref_rx_confidence': 0.91, 'link_updated': True, 'link_source': 'title', 'ref_rxcf': None, 'ref_rxcf_confidence': 0, 'ref_xml': '<be label="7" id="B007"><a>S. <l>Freud</l></a>, <t>&#8216;The Theme of the Three Caskets&#8217;,</t> <bst>Standard Edition</bst> <v>12</v>.</be>'}
        
        >>> result = be_journal.compare_to_database(ocd)
        >>> be_journal.ref_rx
        'SE.012.0289A'
    
        >>> be_journal = BiblioEntry(art_id="PAQ.084.0589A", ref_or_parsed_ref='<be id="B034"><a><l>Freud</l>, S.</a> (<y>1937</y>). <t>Analysis terminable and interminable.</t> <bst>S. E.</bst>, <v>23</v>.</be>')
        >>> be_journal.ref_rx
        'SE.023.0000A'
        
        >>> result = be_journal.lookup_more_exact_artid_in_db(ocd)
        >>> result
        {'ref_rx': 'SE.023.0209A', 'ref_rx_confidence': 0.99, 'link_updated': True, 'link_source': 'title', 'ref_rxcf': None, 'ref_rxcf_confidence': 0, 'ref_xml': '<be id="B034"><a><l>Freud</l>, S.</a> (<y>1937</y>). <t>Analysis terminable and interminable.</t> <bst>S. E.</bst>, <v>23</v>.</be>'}
        
        """
        # prior_rx = self.ref_rx
        ret_val = self.ref_rx
        if self.ref_rx:
            if isinstance(self.ref_rx, Locator):
                part1 = self.ref_rx.jrnlCode
                part2 = self.ref_rx.jrnlVol
            else:
                ref_id_parts = self.ref_rx.split('.')
                part1 = ref_id_parts[0]
                part2 = ref_id_parts[1]
        
            art_id_prefix_match = f"{part1}.{part2}.%"
            like_clause = f"AND art_id LIKE '{art_id_prefix_match}'"
        else:
            like_clause = ""
        
        if not opasgenlib.is_empty(self.ref_title):
            ref_title = self.ref_title
        else:
            ref_title = self.ref_sourcetitle
        try:
            ref_title = opasgenlib.remove_all_punct(ref_title, additional_chars="\t/,[]‘’1234567890")
        except Exception as e:
            ref_title = ref_title               
        
        if ref_title is not None:
            select = f"""SELECT art_id,
                                art_year,
                                art_title,
                                art_auth_citation
                         FROM api_articles
                         WHERE
                              art_title sounds like '{ref_title}'
                              {like_clause};
                      """

            results = ocd.get_select_as_list(select)
            if results and self.ref_rx != results[0][0]:
                ret_val = results[0][0]
                ref_year = results[0][1]
                art_title = results[0][2]
                self.ref_rx = ret_val
                art_auth_citaton = results[0][3]
                author_similarity = opasgenlib.similarityText(art_auth_citaton, self.ref_authors)
                title_similarity = opasgenlib.similarityText(art_title, ref_title)
                self.link_updated = True
                #self.record_updated = True
                self.ref_link_source = opasConfig.RX_LINK_SOURCE_TITLE
                if ref_year == self.ref_year_int \
                  and author_similarity >= SAME_AUTH_VERY_LIKELY \
                  and title_similarity >= SAME_TITLE_VERY_LIKELY:
                    self.ref_rx_confidence = opasConfig.RX_CONFIDENCE_AUTO_VERY_LIKELY
                else:
                    self.ref_rx_confidence = opasConfig.RX_CONFIDENCE_PROBABLE
        
        ret_val = self.ref_rx, self.ref_rx_confidence, self.link_updated
        # this is just used for shorter returns in doctests...perhaps get rid of this?
        #ret_val = BiblioMatch()
        #copy_model_fields(ret_val, self)
        #ret_val.ref_rx = self.ref_rx
        #ret_val.ref_rx_confidence = self.ref_rx_confidence
        #ret_val.link_updated = self.link_updated
        #ret_val.record_updated = self.record_updated
        #ret_val.ref_link_source = self.ref_link_source
        #ret_val.ref_rxcf = self.ref_rxcf
        #ret_val.ref_rxcf_confidence = self.ref_rxcf_confidence
        #ret_val.ref_xml = self.ref_xml
        return ret_val  
                
    #------------------------------------------------------------------------------------------------------------
    def compare_to_database(self, ocd, verbose=False):
        """
        Compare the rx for this with the Database table opasConfig.BIBLIO_TABLE (e.g., api_biblioxml2)
          stored ref_rx and ref_rx_confidence
        
        => Update the object links if database is a higher confidence level
        => Return False if it's not updated in either place
        
        >>> import opasCentralDBLib
        >>> ocd = opasCentralDBLib.opasCentralDB()  

        >>> ref = '<be id="B018"><a><l>Ogden</l>, TH.</a>, (<y>2004a</y>), <t>“An introduction to the reading of Bion”,</t> <j>Int J Psychoanal</j>, <v>85</v>: <pp>285-300</pp>.</be>'
        >>> parsed_ref = etree.fromstring(ref, parser=parser)
        >>> be_journal = BiblioEntry(art_id="ANIJP-TR.007.0157A", ref_or_parsed_ref=parsed_ref)
        >>> be_journal.ref_rx
        'IJP.085.0285A'

        >>> result = be_journal.compare_to_database(ocd)
        >>> be_journal.ref_rx
        'IJP.085.0285A'
    
        >>> be_journal = BiblioEntry(art_id="IJPOPEN.003.0061A", ref_or_parsed_ref='<be label="34)" id="B034"><a><l>Joseph</l>, B.</a> (<y>1982</y>). <t>Addiction to Near-Death.</t> In: <bst>Psychic Equilibrium and Psychic Change</bst>. Hove:<bp>Routledge</bp>, <bpd>1989</bpd>.</be>')
        >>> be_journal.ref_rx
        'NLP.009.0001A'

        >>> result = be_journal.compare_to_database(ocd)
        >>> be_journal.ref_rx
        'NLP.009.0001A'


        """
        # ret_val = self.ref_rx, self.ref_rx_confidence, self.link_updated
        ret_val = False
        db_bibref = ocd.get_references_from_biblioxml_table(self.art_id, self.ref_local_id)
        
        if db_bibref:
            bib_refdb_model = db_bibref[0]   
            self.record_from_db = True
            if bib_refdb_model.ref_rx:
                if bib_refdb_model.ref_rx_confidence > self.ref_rx_confidence:
                    # make sure it's clean
                    loc_str = Locator(bib_refdb_model.ref_rx).articleID()
                    if ocd.article_exists(loc_str):
                        self.ref_exists = True
                        self.ref_in_pep = True
                        self.ref_rx = loc_str
                        self.ref_rx_confidence = bib_refdb_model.ref_rx_confidence
                        self.link_updated = True
                        self.ref_link_source = opasConfig.RX_LINK_SOURCE_DB
                        ret_val = True
                        #ret_val = self.ref_rx, self.ref_rx_confidence, self.link_updated
                else:
                    loc_str = Locator(self.ref_rx).articleID()
                    if ocd.article_exists(loc_str):
                        self.ref_exists = True
                        self.ref_in_pep = True
                        self.link_updated = True
                        self.ref_link_source = opasConfig.RX_LINK_SOURCE_DOCUMENT
                        if self.ref_rx_confidence == 0:
                            self.ref_rx_confidence = opasConfig.RX_CONFIDENCE_KEYED_VERY_LIKELY
                        ret_val = True
                        

            if bib_refdb_model.ref_rxcf:
                if self.ref_rxcf:
                    if bib_refdb_model.ref_rxcf_confidence > self.ref_rxcf_confidence:
                        # make sure it's clean
                        self.ref_rxcf = bib_refdb_model.ref_rxcf
                        self.ref_rxcf_confidence = round(bib_refdb_model.ref_rxcf_confidence, 2)
                        self.link_updated = True
                        self.ref_link_source = opasConfig.RX_LINK_SOURCE_DB
                        ret_val = True
                        #ret_val = self.ref_rx, self.ref_rx_confidence, self.link_updated
                else:
                    self.ref_rxcf = bib_refdb_model.ref_rxcf
                    self.ref_rxcf_confidence = round(bib_refdb_model.ref_rxcf_confidence, 2)
                    self.link_updated = True
                    self.ref_link_source = opasConfig.RX_LINK_SOURCE_DB
                    ret_val = True
        else:
            self.record_from_db = False


        return ret_val

    #------------------------------------------------------------------------------------------------------------
    def get_weighted_confidence(self,
                                item_score=None,  # solr_score
                                item_title=None,
                                item_sourcetitle=None,
                                item_reftext=None,
                                item_authors=None,
                                item_isbook=None, 
                                verbose=False):

        total_weights = 0
        weight_title = 8
        weight_score = 5
        weight_sourcetitle = 3
        weight_text = 3
        weight_author = 4
        weight_typematch = 7
        similarity_score_ref_title_wt = 0
        similarity_score_ref_sourcetitle_wt = 0
        similarity_score_ref_text_wt = 0
        similarity_score_ref_authors_wt = 0
        similarity_score_type_match_wt = 0
        similarity_score_ref_title = 0
        similarity_score_ref_sourcetitle = 0
        similarity_score_ref_text = 0
        similarity_score_ref_authors = 0
        similarity_score_type_match = 0

        if item_score:
            similarity_score_solr = item_score / 10
            similarity_score_solrF = similarity_score_solr * weight_score
            total_weights += weight_score
        else:
            similarity_score_solr = 0
            similarity_score_solrF = 0
            
        ref_title = opasgenlib.remove_all_punct(self.ref_title, additional_chars=SOLR_RESTRICTED_PUNCT)
        item_title = opasgenlib.remove_all_punct(item_title, additional_chars=SOLR_RESTRICTED_PUNCT)
        
        if ref_title:
            similarity_score_ref_title = opasgenlib.similarityText(ref_title, item_title)
            similarity_score_ref_title_wt = similarity_score_ref_title * weight_title
            total_weights += weight_title
            
            
        if item_sourcetitle:
            similarity_score_ref_sourcetitle = opasgenlib.similarityText(self.ref_sourcetitle, item_sourcetitle)
            similarity_score_ref_sourcetitle_wt = similarity_score_ref_sourcetitle * weight_sourcetitle
            total_weights += weight_sourcetitle
            
        if item_reftext:
            similarity_score_ref_text = opasgenlib.similarityText(self.ref_text, item_reftext)
            similarity_score_ref_text_wt = similarity_score_ref_text * weight_text
            total_weights += weight_text

        if item_authors:
            similarity_score_ref_authors = opasgenlib.similarityText(self.ref_authors, item_authors)
            similarity_score_ref_authors_wt = similarity_score_ref_authors * weight_author
            total_weights += weight_author
            
        similarity_score_type_match = item_isbook and self.ref_is_book
        similarity_score_type_match_wt = similarity_score_type_match * weight_typematch
        total_weights += weight_typematch
        
        ret_val = (similarity_score_ref_title_wt + \
                   similarity_score_solrF + \
                   similarity_score_ref_sourcetitle_wt + \
                   similarity_score_ref_text_wt + \
                   similarity_score_ref_authors_wt + \
                   similarity_score_type_match_wt) / total_weights

        score = min(.99, round(ret_val, 2))        
        considered = { "score": score,
                       "solrscore": similarity_score_solr,
                       "sim_auth": similarity_score_ref_authors,
                       "sim_ref": similarity_score_ref_text,
                       "sim_reftitle": similarity_score_ref_title,
                       "sim_srctitle": similarity_score_ref_sourcetitle,
                       "art_title": item_title,
                       "ref_text": self.ref_text, 
                       "ref_text_match": item_reftext
                     }
        
        #if gDbg2 and verbose: print (f"\tOverall Similarity score: {score}")
        #if gDbg2 and verbose: print (f"\tTypeMatch: {type_match}")
        #if gDbg2 and verbose: print (f"\tMatch {score} Considered: {considered}")
        
        return ret_val, considered
    #------------------------------------------------------------------------------------------------------------
    def get_ref_correction(self, ocd, verbose=False):
        """
        Compare the rx for this with the Database table api_biblioxml2 stored ref_rx and ref_rx_confidence
        Update the object links if database is a higher confidence level
        
        Return the final ref_rx or None if it's not available in either place
        
        >>> import opasCentralDBLib
        >>> ocd = opasCentralDBLib.opasCentralDB()  
    
        >>> ref = '<be id="B018"><a><l>Ogden</l>, TH.</a>, (<y>2004a</y>), <t>“An introduction to the reading of Bion”,</t> <j>Int J Psychoanal</j>, <v>85</v>: <pp>285-300</pp>.</be>'
        >>> parsed_ref = etree.fromstring(ref, parser=parser)
        >>> be_journal = BiblioEntry(art_id="ANIJP-TR.007.0157A", ref_or_parsed_ref=parsed_ref)
        >>> be_journal.ref_rx
        'IJP.085.0285A'
        >>> result = be_journal.get_ref_correction(ocd)
        >>> be_journal.ref_rx
        'IJP.085.0285A'
    
    
        """
        ret_val = self
        if self.ref_rx is not None or self.ref_rxcf is not None:
            ret_val = get_reference_correction(ocd, self.art_id, self.ref_local_id)
        
        return ret_val

#------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    sys.path.append('./config') 

    print (40*"*", "opasBiblioSupport Tests", 40*"*")
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
