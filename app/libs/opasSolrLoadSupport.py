#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=C0321,C0103,C0301,E1101,C0303,E1004,C0330,R0915,R0914,W0703,C0326
# Disable many annoying pylint messages, warning me about variable naming for example.
# yes, in my Solr code I'm caught between two worlds of snake_case and camelCase.

""" 
OPAS - opasSolrLoadSupport  

"""
__author__      = "Neil R. Shapiro"
__copyright__   = "Copyright 2019-2021, Psychoanalytic Electronic Publishing"
__license__     = "Apache 2.0"
__version__     = "2021.1213.1" 

import sys
sys.path.append('../libs')
sys.path.append('../config')
sys.path.append('../libs/configLib')

import os
# from datetime import datetime
# import time
import string
import re
import urllib.request, urllib.parse, urllib.error
import random
from contextlib import closing

import lxml
from lxml import etree
parser = lxml.etree.XMLParser(encoding='utf-8', recover=True, resolve_entities=False)
# import html
# import mysql.connector
# import json
# import roman

import localsecrets
import opasConfig
# from configLib.opasIJPConfig import IJPOPENISSUES
import opasCentralDBLib

import opasGenSupportLib as opasgenlib
import opasXMLHelper as opasxmllib
import opasArticleIDSupport
import loaderConfig
# import opasLocator
# import opasArticleIDSupport
import logging
logger = logging.getLogger(__name__)
from loggingDebugStream import log_everywhere_if    # log as usual, but if first arg is true, also put to stdout for watching what's happening

# import modelsOpasCentralPydantic

import PEPGlossaryRecognitionEngine
glossEngine = PEPGlossaryRecognitionEngine.GlossaryRecognitionEngine(gather=False)

def read_stopwords(): 
    with open(localsecrets.HIGHLIGHT_STOP_WORDS_FILE) as f:
        stopWordList = f.read().splitlines()
    
    stopPattern = "<[ib]>[A-Z]</[ib]>"
    for n in stopWordList:
        stopPattern += f"|<[ib]>{n}</[ib]>"

    ret_val = re.compile(stopPattern, re.IGNORECASE)
    return ret_val

# Module Globals
rc_stopword_match = read_stopwords() # returns compile re for matching stopwords 

#------------------------------------------------------------------------------------------------------------
#  Support functions
#------------------------------------------------------------------------------------------------------------
def find_article_id(solrcon, parsed_id:opasArticleIDSupport.ArticleID, verbose=False) -> str:
    """
    Search Solr for the article ID, if not found, try some common variations:
      - the ArticleID alt_standard
      or
      - the simple missing page suffix 'A'
      or
      - one of the volume variant letters (issue)
      or
    """
    ret_val = None
    doc_id = parsed_id.art_id
    try:
        results = solrcon.search(q = f"art_id:{doc_id}")
        if results.raw_response["response"]["numFound"] > 0:
            ret_val = doc_id
        else: # try variations
            # TryAlternateID:
            alt_id = parsed_id.alt_standard
            results = solrcon.search(q = f"art_id:{alt_id}")
            if results.raw_response["response"]["numFound"] > 0:
                logger.debug(f"Document ID {doc_id} not in Solr.  The correct ID seems to be {alt_id}. Using that instead!")
                ret_val = alt_id
            elif doc_id[-1].isnumeric():
                # missing page variant?
                alt_id = doc_id + "A"
                results = solrcon.search(q = f"art_id:{alt_id}")
                if results.raw_response["response"]["numFound"] == 1:  # only accept alternative if there's only one match (otherwise, not known which)
                    # odds are good this is what was cited.
                    logger.debug(f"Document ID {doc_id} not in Solr.  The correct ID seems to be {alt_id}. Using that instead!")
                    ret_val = alt_id
            elif doc_id[-1].isalpha():
                # try without page variant?
                alt_id = doc_id[:-1]
                results = solrcon.search(q = f"art_id:{alt_id}")
                if results.raw_response["response"]["numFound"] == 1:  # only accept alternative if there's only one match (otherwise, not known which)
                    # odds are good this is what was cited.
                    logger.debug(f"Document ID {doc_id} not in Solr.  The correct ID seems to be {alt_id}. Using that instead!")
                    ret_val = alt_id
            else:
                # match volume variant?
                alt_id = parsed_id.alt_wild_standard
                results = solrcon.search(q = f"art_id:{alt_id}")
                count = results.raw_response["response"]["numFound"]
                if count == 1:  # only accept alternative if there's only one match (otherwise, not known which)
                    # odds are good this is what was cited.
                    log_everywhere_if(verbose, "info", f"Document ID {doc_id} not in Solr.  The correct ID seems to be {results.docs[0]['art_id']}. Using that instead!")
                    ret_val = results.docs[0]["art_id"]
                elif count > 1:
                    log_everywhere_if(verbose, "warning", f"Found {count} matches for volume variant (issue). Using the first.")
                    ret_val = results.docs[0]["art_id"]
                else:
                    log_everywhere_if(verbose, "warning", f"Document ID {doc_id} not in Solr.  No alternative ID found.")
    except Exception as e:
        logger.warning(e)

    return results, ret_val
    
def non_empty_string(strval): 
    try:
        return strval if strval != "" else None
    except Exception as e:
        return None  # note if type is not string, it returns None

def strip_tags(value, compiled_tag_pattern):
    """
    Strip tags matching the compiled_tag_pattern.
    
    """
    ret_val = value
    m = compiled_tag_pattern.match(value)
    if m:
        ret_val = m.group("word")
        if ret_val is None:
            ret_val = "pagebreak"
        ret_val = ret_val.translate(str.maketrans('','', '!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~'))

    return ret_val

def remove_values_from_terms_highlighted_list(the_list, remove_stop_words=True, start_tag_pattern = "<(i|b|bi|bui|fi|impx[^>]*?)>", end_tag_pattern="</(i|b|bi|bui|impx|fi)>"):
    """
    Using the list of stop words read in at initialization, remove these from the words used for highlighted lists.
    
    >> remove_values_from_terms_highlighted_list(["<i>not</i>","<i>is</i>","<i>and</i>", "<i>she</i>", "<i>The Interpretation of Dreams</i>", "<i>will</i>", "<i>I</i>", "<i>be</i>" ])
    ['The Interpretation of Dreams']
    """
    stripPattern = f".*<pb>.*|({start_tag_pattern}[\s\n\t]*)+(?P<word>[^<]+?)[\s\n]*({end_tag_pattern})+"
    cStripPattern = re.compile(stripPattern, re.IGNORECASE)
    # passing the compiled pattern saves from recompiling for every value in function
    if remove_stop_words:
        return [strip_tags(value, compiled_tag_pattern = cStripPattern) for value in the_list if not rc_stopword_match.match(value)]
    else:
        return [strip_tags(value, compiled_tag_pattern = cStripPattern) for value in the_list]

def strip_extra_spaces(textstr:str):
    if textstr is not None:
        ret_val = textstr.strip()
        ret_val = re.sub(" +", " ", ret_val)
    else:
        ret_val = None

    return ret_val

#------------------------------------------------------------------------------------------------------
def get_file_dates_solr(solrcore, art_id=None, filename=None):
    """
    Fetch the art_id, file_name, file_last_modified, timestamp
      from Solr either from the file name or art_id
    """
    ret_val = {}
    max_rows = 1000000

    if art_id is not None:
        getFileInfoSOLR = f"art_level:1 && art_id:{art_id}"
    elif filename is not None:
        basename = os.path.basename(filename)

        # these legal file name chars are special chars to Solr, so escape them!
        b_escaped = basename.translate(str.maketrans({"(":  r"\(", 
                                                      ")":  r"\)", 
                                                      "-":  r"\-", 
                                                      ":":  r"\:", 
                                                      }))    

        getFileInfoSOLR = f'art_level:1 && file_name:"{b_escaped}"'
    else:
        raise Exception("Must supply filename or art_id")

    try:
        results = solrcore.search(getFileInfoSOLR, fl="art_id, file_name, file_last_modified, timestamp", rows=max_rows)
    except Exception as e:
        msg = f"FileDatesError: Solr Query: {e}"
        logger.error(msg)
        # let me know whatever the logging is!
        if opasConfig.LOCAL_TRACE: print (msg)
    else:
        if results.hits > 0:
            ret_val = results.docs
        else:
            ret_val = {}

    return ret_val
#------------------------------------------------------------------------------------------------------
def process_article_for_glossary_core(pepxml, artInfo, solr_gloss, fileXMLContents, verbose=None):
    """
    Process the special PEP Glossary documents.  These are linked to terms in the document
       as popups.
    """
    ret_val = False
    glossary_groups = pepxml.xpath("/pepkbd3//dictentrygrp")  
    group_count = len(glossary_groups)
    msg = f"\t...Processing XML for Glossary Core. File has {group_count} groups."
    log_everywhere_if(verbose, "info", msg)

    # processedFilesCount += 1

    all_dict_entries = []
    for glossary_group in glossary_groups:
        glossary_group_xml = etree.tostring(glossary_group, with_tail=False)
        glossary_group_id = opasxmllib.xml_get_element_attr(glossary_group, "id")
        glossary_group_term = opasxmllib.xml_get_subelement_textsingleton(glossary_group, "term")
        glossary_group_also = opasxmllib.xml_get_subelement_xmlsingleton(glossary_group, "dictalso")
        if glossary_group_also == "":
            glossary_group_also = None
        logger.info("Processing Term: %s" % glossary_group_term)
        # count_terms += 1
        dict_entries = glossary_group.xpath("dictentry")  
        group_term_count = len(dict_entries)
        counter = 0
        for dict_entry in dict_entries:
            counter += 1
            this_dict_entry = {}
            dict_entry_id = glossary_group_id + ".{:03d}".format(counter)
            dict_entry_term = opasxmllib.xml_get_subelement_textsingleton(dict_entry, "term")
            if dict_entry_term == "":
                dict_entry_term = glossary_group_term
            dict_entry_term_type = dict_entry.xpath("term/@type")  
            if dict_entry_term_type != []:
                dict_entry_term_type = dict_entry_term_type[0]
            else:
                dict_entry_term_type = "term"
            
            dict_entry_src = opasxmllib.xml_get_subelement_textsingleton(dict_entry, "src")
            dict_entry_also = opasxmllib.xml_get_subelement_xmlsingleton(dict_entry, "dictalso")
            if dict_entry_also == "":
                dict_entry_also = None
            dict_entry_def = opasxmllib.xml_get_subelement_xmlsingleton(dict_entry, "def")
            dict_entry_def_rest = opasxmllib.xml_get_subelement_xmlsingleton(dict_entry, "defrest")
            this_dict_entry = {
                "term_id"             : dict_entry_id,
                "group_id"            : glossary_group_id,
                "art_id"              : artInfo.art_id,
                "term"                : dict_entry_term,
                "term_type"           : dict_entry_term_type,
                "term_source"         : dict_entry_src,
                "term_also"           : dict_entry_also,
                "term_def_xml"        : dict_entry_def,
                "term_def_rest_xml"   : dict_entry_def_rest,
                "group_name"          : glossary_group_term,
                "group_also"          : glossary_group_also,
                "group_term_count"    : group_term_count,
                "text"                : str(glossary_group_xml, "utf8"),
                "file_name"           : artInfo.filename,
                "timestamp"           : artInfo.processed_datetime,
                "file_last_modified"  : artInfo.filedatetime
            }
            all_dict_entries.append(this_dict_entry)

    # We collected all the dictentries for the group.  Now lets save the whole shebang
    try:
        response_update = solr_gloss.add(all_dict_entries)  # lets hold off on the , _commit=True)

        if not re.search('"status":0', response_update):
            logger.info(response_update)
        ret_val = True

    except Exception as err:
        logger.error("GlossaryError: Solr exception %s", err)

    return ret_val    
#------------------------------------------------------------------------------------------------------
def process_article_for_doc_core(pepxml, artInfo, solrcon, file_xml_contents, include_paras=False, verbose=None):
    """
    Extract and load data for the full-text core.  Whereas in the Refs core each
      Solr document is a reference, here each Solr document is a PEP Article.

      This core contains bib entries too, but not subfields.

      TODO: Originally, this core supported each bibliography record in its own
            json formatted structure, saved in a single field.  However, when
            the code was switched from PySolr to Solrpy this had to be removed,
            since Solrpy prohibits this for some reason.  Need to raise this
            as a case on the issues board for Solrpy.

    """
    ret_val = False
    msg = f"\t...Loading XML to Docs Core."
    log_everywhere_if(verbose, "info", msg)

    art_lang = pepxml.xpath('//@lang')
    if art_lang == []:
        art_lang = ['en']
    
    body_xml = opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//body", default_return=None)
    appxs_xml = opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//appxs", default_return=None)
    # added 2023-03-28 per issue 
    if opasConfig.DELETE_EXTRANEOUS_TAGS:
        if opasxmllib.xml_get_elements(pepxml, "/*/meta", default_return=None):
            try:
                # delete fields we don't want in full-text search
                file_xml_contents = opasxmllib.xml_remove_tags_from_xmlstr(file_xml_contents, ["meta"])
            except Exception as e:
                log_everywhere_if(verbose, "debug", f"Can't delete specified extraneous tags from full-text {e}")
            else:
                # repair XML front matter
                log_everywhere_if(verbose, "info", "\t...Deleted specified extraneous tags from full-text")
                file_xml_contents + loaderConfig.DEFAULT_XML_DECLARATION + "\n" + opasConfig.PEP_KBD_DOCTYPE + "\n" + file_xml_contents       

    # see if this is an offsite article
    if artInfo.file_classification == opasConfig.DOCUMENT_ACCESS_OFFSITE:
        # certain fields should not be stored in returnable areas.  So full-text searchable special field for that.
        offsite_contents = True
        offsite_ref =  """<p>This article or book is part of our Offline collection. It is searchable, for your convenience,
 but the full text is not part of our PEP-Archive collection. Here is a link where you may get access: <webx url="https://www.doi.org/%s">https://www.doi.org/%s</webx>.</p>
 """ % (urllib.parse.quote(artInfo.art_doi), urllib.parse.quote(artInfo.art_doi))
        summaries_xml = f"""<abs>
                            {offsite_ref}
                            </abs>
                         """
        excerpt = excerpt_xml = abstracts_xml = summaries_xml
    else:
        offsite_contents = False
        summaries_xml = opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//summaries", default_return=None)
        abstracts_xml = opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//abs", default_return=None)
        # multiple data fields, not needed, search children instead, which allows search by para
        excerpt = None
        excerpt_xml = None
        if artInfo.art_type == opasConfig.ARTINFO_ARTTYPE_TOC_INSTANCE: # "TOC"
            # put the whole thing in the abstract!  Requires some extra processing though
            #heading = opasxmllib.get_running_head( source_title=artInfo.src_title_abbr,
                                                   #pub_year=artInfo.art_year,
                                                   #vol=artInfo.art_vol,
                                                   #issue=artInfo.art_issue,
                                                   #pgrg=artInfo.art_pgrg,
                                                   #ret_format="HTML"
                                                   #)
            #pepxml.remove(pepxml.find('artinfo'))
            #pepxml.remove(pepxml.find('meta'))
            excerpt_xml = pepxml
            excerpt = opasxmllib.xml_str_to_html(excerpt_xml, transformer_name=opasConfig.TRANSFORMER_XMLTOHTML_EXCERPT)
            # excerpt = re.sub("\[\[RunningHead\]\]", f"{heading}", excerpt, count=1)
            
        else:
            # copy abstract or summary to excerpt, if not there, then generate it.
            # this is so that an app can rely on excerpt to have the abstract or excerpt (or summary)
            # TODO: later consider we could just put the excerpt in abstract instead, and make abstract always HTML.
            #       but for now, I like to be able to distinguish an original abstract from a generated one.
            if abstracts_xml is not None:
                excerpt_xml = abstracts_xml[0]
            elif summaries_xml is not None:
                excerpt_xml = summaries_xml[0]
            else:
                excerpt_xml = opasxmllib.get_first_page_excerpt_from_doc_root(pepxml)

            excerpt = opasxmllib.xml_str_to_html(excerpt_xml, document_id=artInfo.art_id)
                
    excerpt_xml = opasxmllib.xml_elem_or_str_to_xmlstring(excerpt_xml, None)
    
    # include_paras is now False by default...only include for special source codes.
    # this of course affects the ability for the server to search by "true" paragraphs.  To enable this feature
    # you must load with command line option includeparas=True
    if include_paras == True or artInfo.src_code in loaderConfig.SRC_CODES_TO_INCLUDE_PARAS:
        children = doc_children() # new instance, reset child counter suffix
        children.add_children(stringlist=opasxmllib.xml_xpath_return_xmlstringlist_withinheritance(pepxml, "//body//p|//body//p2", attr_to_find="lang"),
                              parent_id=artInfo.art_id,
                              parent_tag="p_body",
                              default_lang=art_lang[0])
        children.add_children(stringlist=opasxmllib.xml_xpath_return_xmlstringlist_withinheritance(pepxml, "//h1|//h2|//h3|//h4|//h5|//h6", attr_to_find="lang"),
                              parent_id=artInfo.art_id,
                              parent_tag="p_heading",
                              default_lang=art_lang[0])
        children.add_children(stringlist=opasxmllib.xml_xpath_return_xmlstringlist_withinheritance(pepxml, "//quote//p|//quote//p2", attr_to_find="lang"),
                              parent_id=artInfo.art_id,
                              parent_tag="p_quote",
                              default_lang=art_lang[0])
        children.add_children(stringlist=opasxmllib.xml_xpath_return_xmlstringlist_withinheritance(pepxml, "//dream//p|//dream//p2", attr_to_find="lang"),
                              parent_id=artInfo.art_id,
                              parent_tag="p_dream",
                              default_lang=art_lang[0])
        children.add_children(stringlist=opasxmllib.xml_xpath_return_xmlstringlist_withinheritance(pepxml, "//poem//p|//poem//p2", attr_to_find="lang"),
                              parent_id=artInfo.art_id,
                              parent_tag="p_poem",
                              default_lang=art_lang[0])
        children.add_children(stringlist=opasxmllib.xml_xpath_return_xmlstringlist_withinheritance(pepxml, "//note//p|//note//p2", attr_to_find="lang"),
                              parent_id=artInfo.art_id,
                              parent_tag="p_note",
                              default_lang=art_lang[0])
        children.add_children(stringlist=opasxmllib.xml_xpath_return_xmlstringlist_withinheritance(pepxml, "//dialog//p|//dialog//p2", attr_to_find="lang"),
                              parent_id=artInfo.art_id,
                              parent_tag="p_dialog",
                              default_lang=art_lang[0])
        children.add_children(stringlist=opasxmllib.xml_xpath_return_xmlstringlist_withinheritance(pepxml, "//panel//p|//panel//p2", attr_to_find="lang"),
                              parent_id=artInfo.art_id,
                              parent_tag="p_panel",
                              default_lang=art_lang)
        children.add_children(stringlist=opasxmllib.xml_xpath_return_xmlstringlist_withinheritance(pepxml, "//caption//p", attr_to_find="lang"),
                              parent_id=artInfo.art_id,
                              parent_tag="p_caption",
                              default_lang=art_lang[0])
        children.add_children(stringlist=opasxmllib.xml_xpath_return_xmlstringlist_withinheritance(pepxml, "//bib//be|//binc", attr_to_find="lang"),
                              parent_id=artInfo.art_id,
                              parent_tag="p_bib",
                              default_lang=art_lang[0])
        children.add_children(stringlist=opasxmllib.xml_xpath_return_xmlstringlist_withinheritance(pepxml, "//appxs//p|//appxs//p2", attr_to_find="lang"),
                              parent_id=artInfo.art_id,
                              parent_tag="p_appxs",
                              default_lang=art_lang[0])
        # summaries and abstracts
        children.add_children(stringlist=opasxmllib.xml_xpath_return_xmlstringlist_withinheritance(pepxml, "//summaries//p|//summaries//p2|//abs//p|//abs//p2", attr_to_find="lang"),
                              parent_id=artInfo.art_id,
                              parent_tag="p_summaries",
                              default_lang=art_lang[0])

        child_list = children.child_list
        # indented status
        msg = f"\t\t-->Adding children, tags/counts: {children.tag_counts}"
        logger.info(msg)
        if opasConfig.LOCAL_TRACE:
            print (msg)
    else:
        child_list = None
        
    art_kwds_str = opasgenlib.string_to_list(artInfo.art_kwds)
    terms_highlighted = opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//body/*/b|//body/*/i|//body/*/bi|//body/*/bui")
                        #opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//body/*/i") 
    terms_highlighted = remove_values_from_terms_highlighted_list(terms_highlighted)
    # get counts dynamically, or from file
    if artInfo.glossary_terms_dict is None:
        matched_word_count, artInfo.glossary_terms_dict = glossEngine.getGlossaryLists(pepxml, art_id=artInfo.art_id, verbose=verbose)

    glossary_terms_list = list(artInfo.glossary_terms_dict.keys())
    # 
    # After much testing, and contemplating which is best: **Always use terms rather than groups.**
    # 
    glossary_group_terms_list = glossary_terms_list
    #glossary_group_terms = pepxml.xpath("//body/*/impx/@grpname")
    #glossary_group_terms_list = PEPGlossaryRecognitionEngine.split_glossary_group_terms(glossary_group_terms)

    freuds_italics = opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//body/*/fi", default_return=None)
    if freuds_italics is not None:
        freuds_italics = remove_values_from_terms_highlighted_list(freuds_italics)

    if artInfo.art_title is not None:
        title_str = artInfo.art_title.translate(str.maketrans('', '', string.punctuation)) # remove all punct for sorting
    else:
        title_str = None

    vol_title = non_empty_string(artInfo.art_vol_title)
    if vol_title is not None:
        vol_title_str = vol_title.translate(str.maketrans('', '', string.punctuation))
    else:
        vol_title_str = None
        
    bk_title_xml = opasxmllib.xml_xpath_return_xmlsingleton(pepxml, "//artbkinfo/bktitle", default_return = None)
    if bk_title_xml is not None:
        bk_title = opasxmllib.xml_string_to_text(bk_title_xml)
        bk_title_str = bk_title.translate(str.maketrans('', '', string.punctuation)), # remove all punct for sorting
    else:
        bk_title_str = None

    if artInfo.src_title_full is not None:
        art_sourcetitlefull_str = artInfo.src_title_full.translate(str.maketrans('', '', string.punctuation)) # remove all punct for sorting,
    else:
        art_sourcetitlefull_str = None
        
    bk_title_series_xml = opasxmllib.xml_xpath_return_xmlsingleton(pepxml, "//bktitle", default_return = None)
    if bk_title_series_xml is not None:
        bk_title_series = opasxmllib.xml_string_to_text(bk_title_series_xml)
        bk_title_series_str = bk_title_series.translate(str.maketrans('', '', string.punctuation))
    else:
        bk_title_series_str = None
        
    if artInfo.art_issue_title is not None:
        art_issue_title_str = artInfo.art_issue_title.translate(str.maketrans('', '', string.punctuation))
    else:
        art_issue_title_str = None
    
    new_rec = {
                "id": artInfo.art_id,                                         # important =  note this is unique id for every reference
                "art_id" : artInfo.art_id,                                    # important
                "art_embargo" : artInfo.embargoed,                            # limit display if true (e.g., IJPOpen removed articles)
                "art_embargotype" : artInfo.embargotype,
                "title" : artInfo.art_title,                                  # important
                "title_str" : title_str, # remove all punct, this is only used for sorting
                "art_title_xml" : opasxmllib.xml_xpath_return_xmlsingleton(pepxml, "//arttitle", default_return = None),
                "art_title_str" : title_str, # remove all punct, this is only used for sorting
                "art_sourcecode" : artInfo.src_code,                 # important
                "art_sourcecode_active": artInfo.src_code_active,
                "art_sourcetitleabbr" : artInfo.src_title_abbr,
                "art_sourcetitlefull" : artInfo.src_title_full,
                "art_sourcetitlefull_str" : art_sourcetitlefull_str, # remove all punct for sorting,
                "art_sourcetype" : artInfo.src_type,
                "art_product_key" : artInfo.src_prodkey,
                # abstract_xml and summaries_xml should not be searched, but useful for display without extracting
                "abstract_xml" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//abs", default_return = None),
                "summaries_xml" : summaries_xml,
                "art_excerpt" : excerpt,
                "art_excerpt_xml" : excerpt_xml,
                "art_offsite" : offsite_contents, #  true if it's offsite
                "author_bio_xml" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//nbio", default_return = None),
                "author_aff_xml" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//autaff", default_return = None),
                "bk_title_xml" : bk_title_xml,
                "bk_title_str" : bk_title_str, # remove all punct for sorting
                "bk_subdoc" : artInfo.bk_subdoc,
                "art_info_xml" : artInfo.artinfo_xml,
                "bk_alsoknownas_xml" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//artbkinfo/bkalsoknownas", default_return = None),
                "bk_editors_xml" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//bkeditors", default_return = None),
                "bk_seriestitle_xml" : bk_title_series_xml,
                "bk_seriestitle_str" : bk_title_series_str, # remove all punct for sorting,,,
                "bk_series_toc_id" : artInfo.bk_seriestoc,
                "bk_main_toc_id" : artInfo.main_toc_id,
                "bk_next_id" : artInfo.bk_next_id,
                "caption_text_xml" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml,"//caption", default_return = None),
                "caption_title_xml" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//ctitle", default_return = None),
                "headings_xml" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//h1|//h2|//h3|//h4|//h5|//h6", default_return = None), # reinstated 2020-08-14
                "meta_xml" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//meta", default_return = None),
                # very important field for displaying the whole document or extracting parts
                "text_xml" : file_xml_contents,
                "timestamp" : artInfo.processed_datetime,                     # important
                "file_last_modified" : artInfo.filedatetime,
                "file_classification" : non_empty_string(artInfo.file_classification),
                "file_size" : artInfo.file_size,
                "file_name" : artInfo.filename,
                "art_subtitle_xml" : opasxmllib.xml_xpath_return_xmlsingleton(pepxml, "//artsub", default_return = None),
                "art_citeas_xml" : artInfo.art_citeas_xml,
                #"art_cited_all" : cited_counts.countAll,
                #"art_cited_5" : cited_counts.count5,
                #"art_cited_10" : cited_counts.count10,
                #"art_cited_20" : cited_counts.count20,
                "body_xml" : body_xml[0],
                "appxs_xml": appxs_xml, # list
                "authors" :  artInfo.author_list, # artInfo.art_all_authors,
                "art_authors" : artInfo.author_list,
                "art_authors_count" : artInfo.art_authors_count,
                "art_authors_mast" : non_empty_string(artInfo.art_auth_mast),
                "art_authors_mast_list" : non_empty_string(artInfo.art_auth_mast_list),
                "art_authors_mast_list_strings" : non_empty_string(artInfo.art_auth_mast_list),
                # next two fields may be temp, but I want to compare mast to ids
                "art_authors_ids" : artInfo.art_author_id_list,
                "art_authors_ids_str" : non_empty_string(artInfo.art_author_ids_str),
                # end insertion ################################################
                "art_authors_citation" : non_empty_string(artInfo.art_auth_citation),
                "art_authors_citation_list" : non_empty_string(artInfo.art_auth_citation_list),
                "art_authors_unlisted" : non_empty_string(artInfo.art_auth_mast_unlisted_str),
                "art_authors_xml" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//aut", default_return = None),
                "art_year" : non_empty_string(artInfo.art_year_str),
                "art_year_int" : artInfo.art_year_int,
                "art_vol" : artInfo.art_vol_int,
                "art_vol_suffix" : non_empty_string(artInfo.art_vol_suffix),
                "art_vol_title" : vol_title,
                "art_vol_title_str" : vol_title_str, # remove all punct for sorting
                "art_pgrg" : non_empty_string(artInfo.art_pgrg),
                "art_pgcount" : artInfo.art_pgcount,
                "art_tblcount" : artInfo.art_tblcount,
                "art_figcount" : artInfo.art_figcount,
                "art_kwds_count" : artInfo.art_kwds_count,
                "art_abs_count" : artInfo.art_abs_count,
                "art_ftns_count" : artInfo.art_ftns_count,
                "art_paras_count" : artInfo.art_paras_count,
                "art_headings_count" : artInfo.art_headings_count,
                "art_terms_count" : artInfo.art_terms_count,
                "art_dreams_count" : artInfo.art_dreams_count,
                "art_dialogs_count" : artInfo.art_dialogs_count,
                "art_notes_count" : artInfo.art_notes_count,
                "art_poems_count" : artInfo.art_poems_count,
                "art_citations_count" : artInfo.art_citations_count,
                "art_quotes_count" : artInfo.art_quotes_count,
                "art_chars_count" : artInfo.art_chars_count,
                "art_chars_no_spaces_count" : artInfo.art_chars_no_spaces_count,
                "art_words_count" : artInfo.art_words_count,
                "art_graphic_list" : artInfo.art_graphic_list,
                "reference_count" : artInfo.ref_count,
                "art_iss" : artInfo.art_issue,
                "art_iss_title" : artInfo.art_issue_title,
                "art_iss_title_str" : art_issue_title_str, # remove all punct for sorting
                "art_iss_seqnbr" : artInfo.art_issue_seqnbr, # sequential issue numbering 1-n from start by some journals
                "art_doi" : artInfo.art_doi,
                "art_lang" : artInfo.art_lang,
                "art_issn" : artInfo.art_issn,
                "art_isbn" : artInfo.art_isbn,
                "art_origrx" : artInfo.art_orig_rx,
                "art_qual" : artInfo.art_qual,
                "art_kwds" : artInfo.art_kwds, # pure search field, but maybe not as good as str
                "art_kwds_str" : art_kwds_str, # list, multivalue field for faceting
                "glossary_terms": glossary_terms_list,
                "glossary_group_terms": glossary_group_terms_list,
                "freuds_italics": freuds_italics,
                "art_type" : artInfo.art_type,
                "art_newsecnm" : artInfo.start_sectname,
                "art_newseclevel" : artInfo.start_sectlevel,
                "terms_xml" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//impx[@type='TERM2']", default_return=None),
                "terms_highlighted" : terms_highlighted,
                "dialogs_spkr" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//dialog/spkr/node()", default_return=None),
                "panels_spkr" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//panel/spkr", default_return=None),
                "poems_src" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//poem/src/node()", default_return=None), # multi
                "dialogs_xml" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//dialog", default_return=None), # multi
                "dreams_xml" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//dream", default_return=None), # multi
                "notes_xml" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//note", default_return=None),
                "panels_xml" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//panel", default_return=None),
                "poems_xml" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//poem", default_return=None), # multi
                "quotes_xml" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//quote", default_return=None), # multi
                "references_xml" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//be|binc", default_return=None), # multi
                "tables_xml" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//tbl", default_return=None), # multi
                #"bk_pubyear" : artInfo.bk_pubyear, # added but not needed, since it can comes from productdabase, but leave commented in just in case
                "bib_authors" : artInfo.bib_authors,
                "bib_title" : artInfo.bib_title,
                "bib_journaltitle" : artInfo.bib_journaltitle,
                "bib_rx" : artInfo.art_bib_rxlink_list,
                "art_level" : 1,
                "meta_marked_corrections" : opasxmllib.xml_xpath_return_xmlstringlist(pepxml, "//cgrp[contains(@type,'era')]", default_return=None), # multi,
                #"art_para" : parasxml, 
                "_doc" : child_list  # children.child_list
              }

    #experimental paras save
    # parasxml_update(parasxml, solrcon, artInfo)
    # format for pysolr (rather than solrpy, supports nesting)
    try:
        solrcon.add([new_rec], commit=False)
    except Exception as err:
        #processingErrorCount += 1
        errStr = "SolrDocsError: Art:%s: Err:%s" % (artInfo.art_id, err)
        logger.error(errStr)
        if opasConfig.LOCAL_TRACE: print (errStr)
        # ret_val = False
    else:
        ret_val = True # ok!
        
    return ret_val

class doc_children(object):
    """
    Create an list of child strings to be used as the Solr nested document.
    The parent_tag allows different groups of subelements to be added and separately searchable.
    
    """
    def __init__(self):
        self.count = 0
        self.child_list = []
        self.tag_counts = {}
        
    def add_children(self, stringlist, parent_id, parent_tag=None, level=2, default_lang=None):
        """
        params:
         - stringlist is typically going to be the return of an xpath expression on an xml instance
         - parent_id is the typically going to be the Solr ID of the parent, and this is suffixed
                     to produce a similar but unique id for the child
         - parent_tag for indicating where this was located in the main instance, e.g., references, dreams, etc.
         - level for creating children at different levels (even if in the same object)
        """
        for n in stringlist:
            self.count += 1
            try:
                self.tag_counts[parent_tag] += 1
            except: # initialize
                self.tag_counts[parent_tag] = 1
            
            # special attr handling.  Later look and see if this is slowing us down...
            currelem = etree.fromstring(n, parser=parser)
            lang = currelem.attrib.get("lang", default_lang)
            para_lgrid = currelem.attrib.get("lgrid", None)
            para_lgrx = currelem.attrib.get("lgrx", None)
            if para_lgrx is not None:
                para_lgrx = [item.strip() for item in para_lgrx.split(',')]
                
            self.child_list.append({"id": parent_id + f".{self.count}",
                                    "para_art_id": parent_id,
                                    "art_level": level,
                                    "parent_tag": parent_tag,
                                    "lang": lang,
                                    "para": n,
                                    "para_lgrid" : para_lgrid,
                                    "para_lgrx" : para_lgrx
                                  })
        return self.count

#------------------------------------------------------------------------------------------------------
def process_info_for_author_core(pepxml, artInfo, solrAuthor, verbose=None):
    """
    Get author data and write a record for each author in each document.  Hence an author
       of multiple articles will be listed multiple times, once for each article.  But
       this core will let us research by individual author, including facets.
       
    """
    #------------------------------------------------------------------------------------------------------
    # update author data
    #<!-- ID = PEP articleID + authorID -->
    
    ret_val = False
    msg = f"\t...Loading XML to Author Core."
    log_everywhere_if(verbose, "info", msg)
    
    try:
        # Save author info in database
        authorPos = 0
        for author in artInfo.author_xml_list:
            authorID = author.attrib.get('authindexid', None)
            if authorID is None:
                authorID = opasxmllib.authors_citation_from_xmlstr(author)
                try:
                    authorID = authorID[0]
                except:
                    authorID = "GenID" + "%05d" % random.randint(1, 5000)
            authorListed = author.attrib.get('listed', "true")
            if authorListed.lower() == "true":
                authorPos += 1
            authorRole = author.attrib.get('role', None)
            authorRoleOther = author.attrib.get('other', None)
            authorXML = opasxmllib.xml_elem_or_str_to_xmlstring(author)
            authorDocid = artInfo.art_id + "." + ''.join(autid for autid in authorID if autid.isalnum())
            authorBio = opasxmllib.xml_xpath_return_textsingleton(author, "nbio")
            try:
                authorAffID = author.attrib['affid']
            except KeyError as e:
                authorAffil = None  # see if the add still takes!
            else:
                authAffIDs = authorAffID.split(" ")
                if not opasConfig.MERGE_AFFIDS: # True XML way to do it, but note that authorAffil will be concatenated affil models for the authors affIDs
                    authorAffil = ""
                    for authorAffID in authAffIDs:
                        affil = pepxml.xpath('//artinfo/artauth/autaff[@affid="%s"]' % authorAffID)
                        authorAffil += etree.tostring(affil[0]).decode("utf-8")
                else: # merge this authors institutions into a single Affil for this author
                    authAffIDs = authorAffID.split(" ")
                    authorAffil = f'<autaff affid="{authAffIDs[0]}">'
                    for authorAffID in authAffIDs:
                        affil = pepxml.xpath('//artinfo/artauth/autaff[@affid="%s"]/instit' % authorAffID)
                        authorAffil += etree.tostring(affil[0]).decode("utf-8")
                    authorAffil += "</autaff>"                    

            adoc = []
            adoc.append({
                "id": authorDocid,         # important =  note this is unique id for every author + artid
                "art_id": artInfo.art_id,
                "title": artInfo.art_title,
                "authors": artInfo.art_author_id_list,
                "art_author_id": authorID,
                "art_author_listed": authorListed,
                "art_author_pos_int": authorPos,
                "art_author_role": authorRole,
                "art_author_role_other": authorRoleOther,
                "art_author_bio": authorBio,
                "art_author_affil_xml": authorAffil,
                "art_year_int": artInfo.art_year_int,
                "art_sourcetype": artInfo.src_type,
                "art_sourcetitlefull": artInfo.src_title_full,
                "art_citeas_xml": artInfo.art_citeas_xml,
                "art_author_xml": authorXML,
                "file_last_modified": artInfo.filedatetime,
                "file_classification": artInfo.file_classification,
                "file_name": artInfo.filename,
                "timestamp": artInfo.processed_datetime  # When batch was entered into core
            })
               
            try:  
                response_update = solrAuthor.add(adoc)
                
                if not re.search('"status":0', response_update):
                    msg = "AuthorCoreError: Save error for %s: %s (%s)" % (artInfo.art_id, err, response_update)
                    logger.error(msg)
                    if opasConfig.LOCAL_TRACE: print (msg)
            except Exception as err:
                #processingErrorCount += 1
                errStr = "AuthorCoreError: Exception for %s: %s" % (artInfo.art_id, err)
                if opasConfig.LOCAL_TRACE: print (errStr)
                logger.error(errStr)
            else:
                ret_val = True # ok!
                

    except Exception as err:
        #processingErrorCount += 1
        errStr = "AuthorCoreError: Exception for %s: %s" % (artInfo.art_id, err)
        if opasConfig.LOCAL_TRACE: print (errStr)
        logger.error(errStr)

    return ret_val
#------------------------------------------------------------------------------------------------------
#def add_reference_to_biblioxml_table(ocd, artInfo, bib_entry, verbose=None):
    #"""
    #Adds the bibliography data from a single document to the biblioxml table in mysql database opascentral.
    
    #This database table is used as the basis for the cited_crosstab views, which show most cited articles
      #by period.  It replaces fullbiblioxml which was being imported from the non-OPAS document database
      #pepa1db, which is generated during document conversion from KBD3 to EXP_ARCH1.  That was being used
      #as an easy bridge to start up OPAS.
      
    #Note: This data is in addition to the Solr pepwebrefs (biblio) core which is added elsewhere.  The SQL table is
          #primarily used for the cross-tabs, since the Solr core is more easily joined with
          #other Solr cores in queries.  (TODO: Could later experiment with bridging Solr/SQL.)
          
    #Note: More info than needed for crosstabs is captured to this table, but that's as a bridge
          #to potential future uses.
          
          #TODO: Finish redefining crosstab queries to use this base table.
      
    #"""
    #ret_val = False
    
    ##if bib_entry.rx is None or bib_entry.rxcf is None:
        ### Read from the current table
        ## old_bib_entry = ocd.get_references_from_biblioxml_table(article_id=bib_entry.art_id, ref_local_id=bib_entry.ref_id)
        ##if bib_entry.rx is None:
            ##bib_entry.rx = old_bib_entry.rx
            ##bib_entry.bib_rx_confidence = old_bib_entry.bib_rx_confidence 

        ##if bib_entry.rxcf is None:
            ##bib_entry.rx = old_bib_entry.rxcf
            ##bib_entry.bib_rxcf_confidence = old_bib_entry.bib_rxcf_confidence 
        
    #insert_if_not_exists = r"""REPLACE
                               #INTO api_biblioxml2 (
                                    #art_id,
                                    #bib_local_id,
                                    #art_year,
                                    #bib_rx,
                                    #bib_rx_confidence,
                                    #bib_sourcecode, 
                                    #bib_rxcf, 
                                    #bib_rxcf_confidence,
                                    #bib_authors, 
                                    #bib_authors_xml, 
                                    #bib_articletitle, 
                                    #bib_sourcetype, 
                                    #bib_sourcetitle, 
                                    #bib_pgrg, 
                                    #bib_year, 
                                    #bib_year_int, 
                                    #bib_volume, 
                                    #bib_publisher,
                                    #doi,
                                    #full_ref_xml,
                                    #full_ref_text
                                    #)
                                #values (%(art_id)s,
                                        #%(ref_local_id)s,
                                        #%(year_int)s,
                                        #%(rx)s,
                                        #%(rx_confidence)s,
                                        #%(rx_sourcecode)s,
                                        #%(rxcf)s,
                                        #%(rxcf_confidence)s,
                                        #%(author_list_str)s,
                                        #%(authors_xml)s,
                                        #%(ref_title)s,
                                        #%(ref_source_type)s,
                                        #%(source_title)s,
                                        #%(pgrg)s,
                                        #%(year_of_publication)s,
                                        #%(year_of_publication_int)s,
                                        #%(volume)s,
                                        #%(publishers)s,
                                        #%(ref_doi)s,
                                        #%(ref_entry_xml)s,
                                        #%(ref_text)s
                                        #);
                            #"""
    #query_param_dict = bib_entry.__dict__
    ## need to remove lists, even if they are not used.
    #del query_param_dict["author_list"]
    #del query_param_dict["author_name_list"]
    #del query_param_dict["ref"]

    #res = ""
    #try:
        #res = ocd.do_action_query(querytxt=insert_if_not_exists, queryparams=query_param_dict)
    #except Exception as e:
        #errStr = f"AddToBiblioDBError: insert (returned {res}) error {e}"
        #logger.error(errStr)
        #if opasConfig.LOCAL_TRACE: print (errStr)
        
    #else:
        #ret_val = True
        
    #return ret_val  # return True for success
#------------------------------------------------------------------------------------------------------
def add_article_to_api_articles_table(ocd, artInfo, verbose=None):
    """
    Adds the article data from a single document to the api_articles table in mysql database opascentral.
    
    This database table is used as the basis for linking articles.
     
    """
    ret_val = False
    msg = f"\t...Saving metadata to Articles DB."
    log_everywhere_if(verbose, "info", msg)
    
    # reduce object
  
    insert_if_not_exists = r"""REPLACE
                               INTO api_articles (
                                    art_id,
                                    art_doi,
                                    art_type,
                                    art_lang,
                                    art_kwds,
                                    art_auth_mast,
                                    art_auth_citation,
                                    art_title,
                                    src_title_abbr,
                                    src_code,
                                    art_year,
                                    art_vol,
                                    art_vol_str,
                                    art_vol_suffix,
                                    art_issue,
                                    art_pgrg,
                                    art_pgstart,
                                    art_pgend,
                                    main_toc_id,
                                    start_sectname,
                                    bk_info_xml,
                                    bk_title,
                                    bk_publisher,
                                    art_citeas_xml,
                                    art_citeas_text,
                                    ref_count,
                                    fullfilename,
                                    manuscript_date_str,
                                    filename,
                                    filedatetime
                                    )
                                values (
                                        %(art_id)s,
                                        %(art_doi)s,
                                        %(art_type)s,
                                        %(art_lang)s,
                                        %(art_kwds)s,
                                        %(art_auth_mast)s,
                                        %(art_auth_citation)s,
                                        %(art_title)s,
                                        %(src_title_abbr)s,
                                        %(src_code)s,
                                        %(art_year)s,
                                        %(art_vol_int)s,
                                        %(art_vol_str)s,
                                        %(art_vol_suffix)s,
                                        %(art_issue)s,
                                        %(art_pgrg)s,
                                        %(art_pgstart)s,
                                        %(art_pgend)s,
                                        %(main_toc_id)s,
                                        %(start_sectname)s,
                                        %(bk_info_xml)s,
                                        %(bk_title)s,
                                        %(bk_publisher)s,
                                        %(art_citeas_xml)s,
                                        %(art_citeas_text)s,
                                        %(ref_count)s,
                                        %(fullfilename)s,
                                        %(manuscript_date_str)s,
                                        %(filename)s,
                                        %(filedatetime)s
                                        );
                            """

    query_params = {
        "art_id": artInfo.art_id,
        "art_doi": artInfo.art_doi,
        "art_type": artInfo.art_type,
        "art_lang":  artInfo.art_lang,
        "art_kwds":  artInfo.art_kwds,
        "art_auth_mast":  artInfo.art_auth_mast,
        "art_auth_citation": artInfo.art_auth_citation, 
        "art_title":  artInfo.art_title,
        "src_title_abbr":  artInfo.src_title_abbr,  
        "src_code":  artInfo.src_code,  
        "art_year":  artInfo.art_year_str,
        "art_vol_int":  artInfo.art_vol_int,
        "art_vol_str":  artInfo.art_vol_str,
        "art_vol_suffix":  artInfo.art_vol_suffix,
        "art_issue":  artInfo.art_issue,
        "art_pgrg":  artInfo.art_pgrg,
        "art_pgstart":  artInfo.art_pgstart,
        "art_pgend":  artInfo.art_pgend,
        "main_toc_id":  artInfo.main_toc_id,
        "start_sectname":  artInfo.start_sectname,
        "bk_info_xml":  artInfo.bk_info_xml,
        "bk_title":  artInfo.bk_title,
        "bk_publisher":  artInfo.bk_publisher,
        "art_citeas_xml":  artInfo.art_citeas_xml,
        "art_citeas_text":  artInfo.art_citeas_text,
        "ref_count":  artInfo.ref_count,
        "fullfilename" : artInfo.fullfilename,
        "filename":  artInfo.filename,
        "manuscript_date_str" : artInfo.manuscript_date_str,
        "filedatetime": artInfo.filedatetime
    }

    # string entries above must match an attr of the art_info instance.
    #query_param_dict = art_info.__dict__.copy()
    # the element objects in the author_xml_list cause an error in the action query 
    # even though that dict entry is not used.  So removed in a copy.
    #query_param_dict["author_xml_list"] = None
        
    try:
        # commit automatically handled by do_action_query
        res = ocd.do_action_query(querytxt=insert_if_not_exists, queryparams=query_params)
    except Exception as e:
        errStr = f"AddToArticlesDBError: insert error {e}"
        logger.error(errStr)
        if opasConfig.LOCAL_TRACE: print (errStr)
    else:
        ret_val = True
        
    return ret_val  # return True for success

#------------------------------------------------------------------------------------------------------
def add_to_tracker_table(ocd, art_id, verbose=None):
    """
    Adds the article data from a single document to the tracker table in mysql database opascentral.
    
    If the article is added successfully, that means it's a new article...never seen before.
    Return True so it can be logged.
    Else return False, and it will not be logged as database update.

    >>> import opasCentralDBLib
    >>> ocd =  opasCentralDBLib.opasCentralDB()
    >>> add_to_tracker_table(ocd, "AI.001.0001A") # already in table so should return false
    False
    """
    ret_val = False
    # caller_name = "add_to_tracker_table"
    insert_if_not_exists = r"""INSERT
                               INTO article_tracker (art_id)
                               values (
                                        %(art_id)s
                                      );
                            """

    # string entries above must match an attr of the art_info instance.
    query_param_dict = {}
    # the element objects in the author_xml_list cause an error in the action query 
    # even though that dict entry is not used.  So removed in a copy.
    query_param_dict["art_id"] = art_id
        
    try:
        res = ocd.do_dataupdate_query(querytxt=insert_if_not_exists, queryparams=query_param_dict)
    except Exception as e:
        pass # normal
    else:
        ret_val = True
        
    return ret_val  # return True for success

#--------------------------------------------------------------------------------
def check_if_start_of_section(ocd, art_id, fname=None):
    """
    2023-02-08 This may not be needed anymore, if we remove "start of section" flagging from the
      server, which was a workaround.
    
    """
    ret_val = False
    sql = f"select * from vw_opasloader_article_firstsectnames where art_id='{art_id}'"
    try:
        ocd.open_connection(caller_name=fname)
        if ocd.db is not None:
            with closing(ocd.db.cursor(buffered=True, dictionary=True)) as curs:
                try:
                    curs.execute(sql)
                    warnings = curs.fetchwarnings()
                    if warnings:
                        for warning in warnings:
                            logger.warning(warning)
    
                    if curs.rowcount >= 1:
                        try:
                            data_for_debug = curs.fetchall()
                            logger.info(f"Article info: {data_for_debug}")
                            ret_val = True
                        except Exception as e:
                            print (f"Error: {e}")
                except Exception as e:
                    print (f"Error: {e}")
        else:
            logger.error("Can't load table.  ocd.db is None.")
            
        ocd.close_connection (caller_name=fname)
        
    
    except Exception as e:
        logger.error(e)
    else:
        logger.info("Article is the start of a new section.")
        
    return ret_val

#--------------------------------------------------------------------------------
def garbage_collect_stat(ocd):
    """
    Clean out any records that aren't in articles.
    """
    ret_val = None
    # procname = "garbage_collect_stat"
    sqlActionQry = "delete from artstat where artstat.articleID not in (select art_id from api_articles)"
    try:
        # commit automatically handled by do_action_query
        ret_val = ocd.do_action_query(sqlActionQry, queryparams=None)
    except Exception as e:
        logger.error(e)
        ret_val = False
    else:
        print ("Cleaned up artstat: removed article statistics for any article ids not in article table.")
        
    return ret_val

#--------------------------------------------------------------------------------
def add_to_artstat_table(ocd, artInfo, verbose=None):
    """
    update the corresponding database record
    """
    ret_val = False
    msg = f"\t...Saving statistics to artStat table."
    log_everywhere_if(verbose, "info", msg)
    
    if artInfo is None:
        print ("Error!")
    else:
        selInsert = r"""REPLACE INTO artstat
                            (
                                    articleID,
                                    Citations,
                                    `References`,
                                    Reflinks,
                                    PgxJumps,
                                    Figures,
                                    Poems,
                                    Tables,
                                    Pages,
                                    Footnotes,
                                    Quotes,
                                    Dreams,
                                    Dialogs,
                                    Paragraphs,
                                    Words,
                                    Chars,
                                    NonspaceChars,
                                    modTime,
                                    createTime,
                                    pubyear,
                                    glossaryDict
                            )
                        values
                           (
                                    %(art_id)s,
                                    %(art_citations_count)s,
                                    %(ref_count)s, 
                                    %(bib_rx)s,   
                                    %(art_pgrx_count)s,  
                                    %(figcount)s,
                                    %(art_poems_count)s,
                                    %(art_tblcount)s,
                                    %(art_pgcount)s,
                                    %(art_ftns_count)s,
                                    %(art_quotes_count)s,
                                    %(art_dreams_count)s,
                                    %(art_dialogs_count)s,
                                    %(art_paras_count)s,
                                    %(art_words_count)s,
                                    %(art_chars_count)s,
                                    %(art_chars_no_spaces_count)s,
                                    %(modtime)s,
                                    %(createtime)s,
                                    %(pubyear)s,
                                    %(glossarydict)s
                            )
                            """

    query_params = {
                       "art_id":artInfo.art_id,
                       "art_citations_count":artInfo.art_citations_count, 
                       "ref_count":artInfo.ref_count,                   # number of references
                       "bib_rx": len(artInfo.art_bib_rxlink_list),      # number of linked references
                       "art_pgrx_count":artInfo.art_pgrx_count,         # pgxjumps
                       "figcount":artInfo.art_figcount,
                       "art_poems_count":artInfo.art_poems_count,
                       "art_tblcount":artInfo.art_tblcount,
                       "art_pgcount":artInfo.art_pgcount,
                       "art_ftns_count":artInfo.art_ftns_count,
                       "art_quotes_count":artInfo.art_quotes_count,
                       "art_dreams_count":artInfo.art_dreams_count,
                       "art_dialogs_count":artInfo.art_dialogs_count,
                       "art_paras_count":artInfo.art_paras_count,
                       "art_words_count":artInfo.art_words_count,
                       "art_chars_count":artInfo.art_chars_count,
                       "art_chars_no_spaces_count":artInfo.art_chars_no_spaces_count,
                       "modtime":opasCentralDBLib.date_to_db_date(artInfo.filedatetime),
                       "createtime":opasCentralDBLib.date_to_db_date(artInfo.file_create_time),
                       "pubyear":artInfo.art_year_str,
                       "glossarydict": artInfo.glossary_terms_dict_str
                    }
    
    # string entries above must match an attr of the art_info instance.
    #query_param_dict = art_info.__dict__.copy()
    # the element objects in the author_xml_list cause an error in the action query 
    # even though that dict entry is not used.  So removed in a copy.
    #query_param_dict["author_xml_list"] = None
        
    try:
        # commit automatically handled by do_action_query
        res = ocd.do_action_query(querytxt=selInsert, queryparams=query_params)
    except Exception as e:
        errStr = f"DBError: insert error {e}"
        logger.error(errStr)
        if opasConfig.LOCAL_TRACE: print (errStr)
    else:
        ret_val = True

    return ret_val  # return True for success


if __name__ == "__main__":
    sys.path.append('./config') 

    print (40*"*", "opasLoadSupportLib Tests", 40*"*")
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
