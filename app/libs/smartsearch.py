import re
import sys
from datetime import datetime
from optparse import OptionParser
import logging
import opasGenSupportLib as opasgenlib

import opasConfig
import smartsearchLib

logger = logging.getLogger(__name__)

from opasSchemaInfoLib import SchemaInfo
docschemainfo = SchemaInfo()
import schemaMap
from opasArticleIDSupport import ArticleID

def smart_search(smart_search_text):
    """
    Function to take an input string and parse out information to do a search against the DOCS core schema.
    
    Some simple syntax is implemented to help identify certain functionality.
    
        schema_field:terms  = Solr field from schema, search terms against it.  (Will be presented
            to solr as field:(terms).  Multiple terms are permitted, but currently, only one field
            specification is permitted.  Field names are converted to lower case automatically.
            
            art_doi:10.1111/j.1745-8315.2012.00606.x
            art_authors_text:[tuckett and fonagy]            
           
        doi = Just enter a DOI
           10.1111/j.1745-8315.2012.00606.x 
        
        AuthorName Year = One or more names (initial capital), followed by a year
            Tuckett and Fonagy 1972
            Tuckett and Fonagy (1972)
            
    >>> ret_dict = smart_search("009:1015")
    >>> del ret_dict['smart_search']
    >>> ret_dict
    {'vol': '9', 'pgrg': '1015', 'search_type': 'vol and page range'}
    
    >>> ret_dict = smart_search("authors:Tuckett, D.")
    >>> del ret_dict['smart_search']
    >>> ret_dict
    {'schema_field': 'authors', 'schema_value': 'Tuckett, D.', 'search_type': 'document field'}
    
    >>> ret_dict = smart_search("the_author:Tuckett, D.")
    >>> del ret_dict['smart_search']
    >>> ret_dict
    {'schema_field': 'authors', 'schema_value': 'Tuckett, D.', 'search_type': 'document field'}
    
    >>> ret_dict = smart_search("the cat in the hat was that")
    >>> del ret_dict['smart_search']
    >>> ret_dict
    {'search_type': 'paragraph search', 'schema_value': '"the cat in the hat was that"~25'}
    
    >>> ret_dict = smart_search("solr::art_authors_text:[tuckett and fonagy]")
    >>> del ret_dict['smart_search']
    >>> ret_dict
    {'search_type': 'ADVANCED', 'schema_field': 'solr', 'schema_value': 'art_authors_text:[tuckett and fonagy]'}
    
    >>> ret_dict = smart_search("Freud, S. ( 1938), Some elementary lessons in psycho-analysis, Standard Edition. 23:279-286. pp. London: Hogarth Press, 1964.")
    >>> del ret_dict['smart_search']
    >>> ret_dict
    {'author_list': ['Freud, S.'], 'yr': '1938', 'vol': '23', 'pgrg': '279-286', 'search_type': 'authors year vol pgrg'}

    >>> ret_dict = smart_search("009:1015")
    >>> del ret_dict['smart_search']
    >>> ret_dict
    {'vol': '9', 'pgrg': '1015', 'search_type': 'vol and page range'}
    
    >>> ret_dict = smart_search("Tuckett")
    >>> del ret_dict['smart_search']
    >>> ret_dict
    {'search_type': 'author citation', 'schema_field': 'art_authors_citation', 'schema_value': 'Tuckett'}
       
    >>> ret_dict = smart_search("Kohut, H. & Wolf, E. S. (1978)")
    >>> del ret_dict['smart_search']
    >>> ret_dict
    {'author_list': ['Kohut, H. & Wolf, E. S.'], 'yr': '1978', 'search_type': 'authors and year'}
    
    >>> ret_dict = smart_search("Tuckett 1982")
    >>> del ret_dict['smart_search']
    >>> ret_dict
    {'author_list': ['Tuckett'], 'yr': '1982', 'search_type': 'authors and year'}
    
    >>> ret_dict = smart_search("'the cat in the hat was that'")
    >>> del ret_dict['smart_search']
    >>> ret_dict
    {'search_type': 'literal', 'schema_value': "'the cat in the hat was that'"}
    
    >>> tst = []
    >>> tst.append("Emerson, R. W. (1836), An essay on nature. In: The Selected Writings of Ralph Waldo Emerson, ed. W. H. Gilman. New York: New American Library, 1965, pp. 186-187.")
    >>> tst.append("Rapaport, D. and Gill, M. M. ( 1959). The Points of View and Assumptions of Metapsychology. Int. J. Psycho-Anal.40:153-162")
    >>> tst.append("Freud, S. ( 1938), Some elementary lessons in psycho-analysis, Standard Edition. 23:279-286. pp. London: Hogarth Press, 1964.")
    >>> tst.append("Waelder, R. ( 1962). Psychoanalysis, Scientific Method, and Philosophy. J. Amer. Psychoanal. Assn.10:617-637")

    >>> ret = []
    >>> for n in tst: ret.append(smart_search(n))
    >>> for n in ret: del n['smart_search'];print(n)
    {'author_list': ['Emerson, R. W.'], 'yr': '1836', 'search_type': 'pattern authors and year'}
    {'author_list': ['Rapaport, D. and Gill, M. M.'], 'yr': '1959', 'vol': '40', 'pgrg': '153-162', 'search_type': 'pattern authors year vol pgrg'}
    {'author_list': ['Freud, S.'], 'yr': '1938', 'vol': '23', 'pgrg': '279-286', 'search_type': 'pattern authors year vol pgrg'}
    {'author_list': ['Waelder, R.'], 'yr': '1962', 'vol': '10', 'pgrg': '617-637', 'search_type': 'pattern authors year vol pgrg'}
    """
    # recognize Smart Search inputs
    ret_val = {}

    for_words_only_remove_punct = '!"#$%&()+,-./;:<=>@[\\]^_`{|}~' # punctuation to be removed from words only searches - wild cards accepted

    # get rid of leading spaces and zeros
    smart_search_text = smart_search_text.lstrip(" 0")
    # get rid of trailing spaces and zeros
    smart_search_text = smart_search_text.rstrip(" ")
    # get rid of smart quotes
    smart_search_text = re.sub("“|”", "'", smart_search_text)
    # See if it has a field and then text with colon
    m = re.match(smartsearchLib.rx_quoted_str_with_colon_and_coloned_field_prefix, smart_search_text)
    if m:
        # split in 2
        fieldname = m.group("field")
        quotedstr = m.group("qstr")
        quotedstr = quotedstr.replace(":", "")
        # remove the colons in the quote
        smart_search_text = fieldname + quotedstr
    
    if "TO" not in smart_search_text:
        m = re.search("\[.*to.*\]", smart_search_text, re.IGNORECASE)
        if m is None: # no Solr ranges
            remove_characters = ("[", "]")
            for character in remove_characters:
                smart_search_text = smart_search_text.replace(character, "")            
    
    if re.match(r"^([\"\'])(.*)\1$", smart_search_text):
        # literal string
        ret_val[opasConfig.KEY_SEARCH_SMARTSEARCH] = f"Matched articles for {opasConfig.SEARCH_TYPE_LITERAL}: ({smart_search_text})"
        ret_val[opasConfig.KEY_SEARCH_TYPE] = opasConfig.SEARCH_TYPE_LITERAL
        ret_val[opasConfig.KEY_SEARCH_VALUE] = smart_search_text
        
    smart_article_id = ArticleID(art_id=smart_search_text) # now from opasArticleIDSupport 2022-06-05
    if smart_article_id.is_ArticleID:
        # locator (articleID)
        doc_id = smart_article_id.exists_with_resilience(resilient=True, verbose=True)        
        if doc_id:
            ret_val = {opasConfig.SEARCH_FIELD_LOCATOR: doc_id}
        #if smartsearchLib.is_value_in_field(smart_article_id.standardized, opasConfig.SEARCH_FIELD_LOCATOR):
            #ret_val = {opasConfig.SEARCH_FIELD_LOCATOR: smart_article_id.standardized}
        #elif smartsearchLib.is_value_in_field(smart_article_id.alt_standard, opasConfig.SEARCH_FIELD_LOCATOR):
            #ret_val = {opasConfig.SEARCH_FIELD_LOCATOR: smart_article_id.alt_standard}           
        
    if ret_val == {}: # (opasConfig.SEARCH_TYPE_ADVANCED, "ADVANCED")
        # this is not much different than search_type_fielded, except the Solr query will be cleaner and perhaps more flexible.
        #  but I'll leave fielded in for now, because for a simple field search the syntax is a little better.
        m = re.match(smartsearchLib.advanced_syntax, smart_search_text)
        if m is not None:
            ret_val[opasConfig.KEY_SEARCH_SMARTSEARCH] = m.group(3)
            ret_val[opasConfig.KEY_SEARCH_TYPE] = opasConfig.ADVANCED
            ret_val[opasConfig.KEY_SEARCH_FIELD] = "solr"
            ret_val[opasConfig.KEY_SEARCH_VALUE] = f"{ret_val[opasConfig.KEY_SEARCH_SMARTSEARCH]}"
            ret_val[opasConfig.KEY_SEARCH_SMARTSEARCH] = f"Matched articles for advanced search: {ret_val[opasConfig.KEY_SEARCH_SMARTSEARCH]} "

    if ret_val == {}: # (opasConfig.list_of_rxs, "CF List")    #e.g., JAA.028.0740A:0.66, PSAR.086.0967A:0.66, IJP.080.0189A:0.65, PAQ.068.0313A:0.65, PPSY.016.0481A:0.61
        m = re.match(smartsearchLib.list_of_rxs, smart_search_text)
        if m is not None:
            list_of_rxs = m.group("schema_value")
            label = m.group("custom_label")
            if list_of_rxs:
                # get rid of confidences
                list_of_rxs = re.sub(r"\:(0\.[0-9]{1,2}|1\.0)", "", list_of_rxs)
                or_bar_rxs = re.sub("\,\s*", " || ", list_of_rxs)
                ret_val[opasConfig.KEY_SEARCH_SMARTSEARCH] = f"art_id:({or_bar_rxs})"
                ret_val[opasConfig.KEY_SEARCH_TYPE] = opasConfig.ADVANCED
                ret_val[opasConfig.KEY_SEARCH_FIELD] = "solr"
                ret_val[opasConfig.KEY_SEARCH_VALUE] = f"art_id:({or_bar_rxs})"
                if label is not None:
                    label = label.replace("//", "")
                else:
                    label = "Matching Articles"

                ret_val[opasConfig.KEY_SEARCH_SMARTSEARCH] = f"{label}"
    
    if ret_val == {}:
        # Smartpatterns:   pattern: descriptive pattern label     
        patterns1 = {
                     smartsearchLib.rx_doi : opasConfig.SEARCH_TYPE_DOI,
                     smartsearchLib.rx_solr_field: opasConfig.SEARCH_TYPE_FIELDED, 
                     #smartsearchLib.rx_syntax: opasConfig.SEARCH_TYPE_ADVANCED,
                     smartsearchLib.rx_author_list_year_vol_pgrg : "authors year vol pgrg",
                     smartsearchLib.rx_author_list_and_year : "authors and year",
                     smartsearchLib.rx_cit_vol_pgrg : "citation vol/pg",
                     smartsearchLib.rx_year_pgrg : "a page range",
                     smartsearchLib.rx_vol_pgrg : "vol and page range",
                     smartsearchLib.rx_vol_wildcard : "vol and page wildcard",
                     smartsearchLib.rx_year_vol_pgrg : "yr vol page range"
                    }

        for rx_str, label in patterns1.items():
            m = re.match(rx_str, smart_search_text)
            if m is not None:
                ret_val = {**ret_val, **m.groupdict()}
                if label == opasConfig.SEARCH_TYPE_FIELDED:
                    # found a field, maybe. Check it.  Keep going if not a field name
                    if ret_val["schema_field"] not in docschemainfo.doc_fields:
                        # try to translate it
                        alt_name = schemaMap.user2solrfieldname(ret_val["schema_field"])
                        if alt_name not in docschemainfo.doc_fields:
                            # not a proper schema field, so reset ret_val
                            ret_val = {}
                            continue
                        else:
                            ret_val["schema_field"] = alt_name
                            if ret_val["schema_field"] in ["art_sourcecode", ]:
                                try:
                                    ret_val["schema_value"] = ret_val["schema_value"].upper()
                                except Exception as e:
                                    pass
                                
                            
                ret_val[opasConfig.KEY_SEARCH_SMARTSEARCH] = f"Matched {label}: {smart_search_text}"
                ret_val[opasConfig.KEY_SEARCH_TYPE] = f"{label}"
                break
                
        if ret_val == {}:
            # nothing found yet.
            # is it in quotes (phrase?)
            search_words = smart_search_text.split(" ")
            word_count = len(search_words)
            search_words = [re.sub('\"|\\\:', "", n) for n in search_words]
            cleaned_search_str = " ".join(search_words)
            cleaned_search_str = smartsearchLib.cleanup_solr_query(cleaned_search_str)
            percent_upper, upper_count = smartsearchLib.percentage_uppercase_words(cleaned_search_str)
            # print (f"Search string eval: Percent upper: {percent_upper}, count: {upper_count}, common: {smartsearchLib.has_common_words(cleaned_search_str)}")

            pattern_boolean_test = "&&|\|\||\s(AND|OR|NOT)\s"
            has_bool = re.search(pattern_boolean_test, cleaned_search_str) # case sensitive
            # has_bool_insensitive = re.search(pattern_boolean_test, words, flags=re.I) # case sensitive
            
            if smartsearchLib.is_value_in_field(cleaned_search_str, core="docs", field=opasConfig.SEARCH_FIELD_AUTHOR_CITATION, match_type="proximate") \
               and percent_upper >= 50 and not smartsearchLib.has_common_words(cleaned_search_str):
                # see if it's a list of names
                ret_val[opasConfig.KEY_SEARCH_TYPE] = opasConfig.SEARCH_TYPE_AUTHOR_CITATION
                ret_val[opasConfig.KEY_SEARCH_FIELD] = opasConfig.SEARCH_FIELD_AUTHOR_CITATION 
                ret_val[opasConfig.KEY_SEARCH_VALUE] = f"{cleaned_search_str}"
                ret_val[opasConfig.KEY_SEARCH_SMARTSEARCH] = f"Matched articles by authors: ({cleaned_search_str})"

            if ret_val == {}:
                if ":" not in cleaned_search_str:
                    if smartsearchLib.is_value_in_field(cleaned_search_str, core="docs", field=opasConfig.SEARCH_FIELD_AUTHOR_CITATION, match_type="boolean") != 0:
                        # boolean name search
                        if percent_upper >= 50 and has_bool and not smartsearchLib.has_common_words(cleaned_search_str): 
                            # see if it's a list of names
                            ret_val[opasConfig.KEY_SEARCH_TYPE] = opasConfig.SEARCH_TYPE_AUTHOR_CITATION
                            ret_val[opasConfig.KEY_SEARCH_FIELD] = opasConfig.SEARCH_FIELD_AUTHOR_CITATION 
                            ret_val[opasConfig.KEY_SEARCH_VALUE] = f"{cleaned_search_str}"
                            ret_val[opasConfig.KEY_SEARCH_SMARTSEARCH] = f"Matched by authors: (boolean query: {cleaned_search_str})"

            if ret_val == {}:
                if 0 != smartsearchLib.is_value_in_field(cleaned_search_str, core="docs", field=opasConfig.SEARCH_FIELD_TITLE, match_type="ordered") == 1: # unique match only
                    if word_count > 4:
                        ret_val["title"] = re.sub(f'[{for_words_only_remove_punct}]', '', cleaned_search_str)
                        ret_val[opasConfig.KEY_SEARCH_TYPE] = opasConfig.SEARCH_TYPE_TITLE
                        ret_val[opasConfig.KEY_SEARCH_SMARTSEARCH] = f"Matched words in titles: {cleaned_search_str}"
    
            if ret_val == {}:
                # unique match only
                if 1 == smartsearchLib.is_value_in_field(cleaned_search_str, core="docs", field=opasConfig.SEARCH_FIELD_TITLE, match_type="proximate"): 
                    if word_count > 4:
                        ret_val["title"] = re.sub(f'[{for_words_only_remove_punct}]', '', cleaned_search_str)
                        ret_val[opasConfig.KEY_SEARCH_TYPE] = opasConfig.SEARCH_TYPE_TITLE
                        ret_val[opasConfig.KEY_SEARCH_SMARTSEARCH] = f"Matched words in titles: {cleaned_search_str}"

            if ret_val == {}:
                if smartsearchLib.all_words_start_upper_case(smart_search_text) and "*" not in smart_search_text:
                    # try to build a list of names, and check them individually
                    new_q = ""
                    names = smartsearchLib.get_list_of_name_ids(smart_search_text)
                    for name in names:
                        try:
                            res = smartsearchLib.is_value_in_field(name, core="docs", field=opasConfig.SEARCH_FIELD_AUTHORS, match_type="adjacent")
                            if res:
                                # ok, this is a list of names
                                if new_q != "":
                                    new_q += f" && '{name}'"
                                else: # first name
                                    new_q = f'{name}'
                            else: # name not found, they all have to be
                                new_q = ""
                                break 
                        except Exception as e:
                            logger.warning(f"Value error for {name}. {e}")
                    
                    if new_q != "":
                        ret_val[opasConfig.KEY_SEARCH_TYPE] = opasConfig.SEARCH_TYPE_AUTHORS
                        ret_val[opasConfig.KEY_SEARCH_FIELD] = opasConfig.SEARCH_FIELD_AUTHORS 
                        ret_val[opasConfig.KEY_SEARCH_VALUE] = f"{new_q}"
                        ret_val[opasConfig.KEY_SEARCH_SMARTSEARCH] = f"Matched author names"
                    else:
                        #  join the names
                        name_conjunction = " && ".join(names)
                        if smartsearchLib.is_value_in_field(name_conjunction, core="docs", field=opasConfig.SEARCH_FIELD_AUTHOR_CITATION, match_type="adjacent"):
                            ret_val[opasConfig.KEY_SEARCH_TYPE] = opasConfig.SEARCH_TYPE_AUTHOR_CITATION
                            ret_val[opasConfig.KEY_SEARCH_FIELD] = opasConfig.SEARCH_FIELD_AUTHOR_CITATION
                            ret_val[opasConfig.KEY_SEARCH_VALUE] = f"{name_conjunction}"
                            ret_val[opasConfig.KEY_SEARCH_SMARTSEARCH] = f"Matched articles for authors: {name_conjunction} "

            if ret_val == {}:
                if 1 != smartsearchLib.is_value_in_field(cleaned_search_str, core="docs", field=opasConfig.SEARCH_FIELD_TEXT, match_type="proximate"):
                    orig_smart_search_text = smart_search_text
                    if not opasgenlib.in_quotes(smart_search_text):
                        if not opasgenlib.is_boolean(smart_search_text):
                            if not opasgenlib.in_brackets(smart_search_text) and word_count > 1:
                                # phrase search, remove punctuation which if on the first word can be mistaken for field identifiers (fix 2022-02-05)
                                punct = '!"#$%&()+,./:;<=>@[\\]^_`{|}~' # string.punctuation - wild cards accepted (2022-04-15 Removed ' from string) (2922-11-07 removed - from string)
                                phrase_search = re.sub(f'[{punct}]', '', smart_search_text)
                                smart_search_text = f'"{phrase_search}"~25'         
                                smart_search_text = opasgenlib.add_smart_quote_search(smart_search_text)
                                ret_val[opasConfig.KEY_SEARCH_TYPE] = opasConfig.SEARCH_TYPE_PARAGRAPH
                                ret_val[opasConfig.KEY_SEARCH_SMARTSEARCH] = f"Matched paragraphs with terms: ({orig_smart_search_text})"
                                ret_val[opasConfig.KEY_SEARCH_VALUE] = f"{smart_search_text}"
                            else:
                                ret_val["wordsearch"] = re.sub(":", "\:", smart_search_text)
                                ret_val["wordsearch"] = opasgenlib.add_smart_quote_search(ret_val["wordsearch"]) # temp, until we load smartquotes only
                                ret_val[opasConfig.KEY_SEARCH_TYPE] = opasConfig.SEARCH_TYPE_WORDSEARCH
                                ret_val[opasConfig.KEY_SEARCH_SMARTSEARCH] = f"Matched articles for words: ({orig_smart_search_text})"
                        else:
                            smart_search_text = re.sub ("\snot\s", " NOT ", smart_search_text)
                            smart_search_text = opasgenlib.add_smart_quote_search(smart_search_text)
                            ret_val[opasConfig.KEY_SEARCH_TYPE] = opasConfig.SEARCH_TYPE_BOOLEAN
                            ret_val[opasConfig.KEY_SEARCH_SMARTSEARCH] = f"Matched articles for boolean string: ({orig_smart_search_text})"
                            ret_val[opasConfig.KEY_SEARCH_VALUE] = f"{orig_smart_search_text}"
                            ret_val[opasConfig.KEY_SEARCH_VALUE] = re.sub("\s+(AND)\s+", " && ", ret_val[opasConfig.KEY_SEARCH_VALUE], flags=re.IGNORECASE)
                            ret_val[opasConfig.KEY_SEARCH_VALUE] = re.sub("\s+(OR)\s+", " || ", ret_val[opasConfig.KEY_SEARCH_VALUE], flags=re.IGNORECASE)
                            ret_val[opasConfig.KEY_SEARCH_VALUE] = re.sub("\s+(NOT)\s+", " NOT ", ret_val[opasConfig.KEY_SEARCH_VALUE], flags=re.IGNORECASE)
                    else:
                        # literal with extra at the end, could be proximate with distance, maybe other solr searches?
                        ret_val[opasConfig.KEY_SEARCH_SMARTSEARCH] = f"Matched articles for: ({orig_smart_search_text})"
                        ret_val[opasConfig.KEY_SEARCH_TYPE] = opasConfig.SEARCH_TYPE_LITERAL
                        ret_val[opasConfig.KEY_SEARCH_VALUE] = f"{orig_smart_search_text}"
                    
   
    #  cleanup 
    if ret_val.get("art_id") is not None:
        ret_val["art_id"] = ret_val["art_id"].upper()
        ret_val[opasConfig.KEY_SEARCH_TYPE] = opasConfig.SEARCH_TYPE_ID
        ret_val[opasConfig.KEY_SEARCH_FIELD] = opasConfig.SEARCH_FIELD_LOCATOR
        ret_val[opasConfig.KEY_SEARCH_SMARTSEARCH] = f"Matched articles for article ID {ret_val['art_id'].upper()}:"
    elif ret_val.get("doi") is not None:
        pass # we're done

    if ret_val == {}:
        # word search
        ret_val["wordsearch"] = re.sub(f'[{for_words_only_remove_punct}]', '', smart_search_text)
        ret_val[opasConfig.KEY_SEARCH_TYPE] = opasConfig.SEARCH_TYPE_WORDSEARCH
        ret_val[opasConfig.KEY_SEARCH_SMARTSEARCH] = f"Matched articles with words: {smart_search_text}"

    ret_val = smartsearchLib.dict_clean_none_terms(ret_val)
    
    # parse author list
    author_list = ret_val.get("author_list")
    if author_list is not None:
        working_author_list = re.sub(" and ", ", ", author_list, re.I)
        try:
            alist = opasgenlib.get_author_list_not_comma_separated(working_author_list)
            ret_val["author_list"] = alist
        except Exception as e:
            logger.warning(f"Can't parse and replace author list {e}")
    
        if alist == []:
            # try and?
            try:
                alist = opasgenlib.get_author_list_and_separated(author_list)
                ret_val["author_list"] = alist
            except Exception as e:
                logger.warning(f"Can't parse and replace author list {e}")
            
        ret_val["author_list"] = smartsearchLib.get_list_of_name_ids(author_list)
    return ret_val

if __name__ == "__main__":
    global options  # so the information can be used in support functions
    options = None
    programNameShort = "TestSmartSearch"  
    logFilename = programNameShort + "_" + datetime.today().strftime('%Y-%m-%d') + ".log"

    parser = OptionParser(usage="%prog [options] - PEP Solr Reference Text Data Loader", version="%prog ver. 0.1.14")
    parser.add_option("--test", dest="testmode", action="store_true", default=False,
                      help="Run Doctests")

    (options, args) = parser.parse_args()

    if options.testmode:
        import doctest
        doctest.testmod()
        print ("Fini. Tests complete.")
        sys.exit()

    while "a" is not None:
        ssrch = input ("Enter search:")
        result = smart_search(ssrch)



