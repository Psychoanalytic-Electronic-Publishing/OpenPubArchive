# -*- coding: utf8 -*-
"""
 Part of the larger PEPReferenceParser from the Python 2 Scilab code to process XML

"""
import re
import sys
# import time
#from UserDict import UserDict
import logging
logger = logging.getLogger(__name__)
# from loggingDebugStream import log_everywhere_if    # log as usual, but if first arg is true, also put to stdout for watching what's happening
from typing import Optional # List, Generic, TypeVar, 
from datetime import datetime

from pydantic.main import Field # removed Field, causing an error on AWS

import opasConfig
import opasGenSupportLib as opasgenlib
from opasGenSupportLib import trimPunctAndSpaces, atoiYear
import PEPJournalData
gJrnlData = PEPJournalData.PEPJournalData()
# import opasDocuments
from opasDocuments import PageNumber #, PageRange
from opasLocator import Locator
import PEPBookInfo
bookInfo = PEPBookInfo.PEPBookInfo()

gDbg1 = 0	# details
gDbg2 = 1	# High level

#--------------------------------------------------------------------------------
class StrReferenceParser(object):
    """

    >>> ref = "Freud, S. 1905 On psychotherapy Standard Edition 7"
    >>> str_parser = StrReferenceParser()
    >>> str_parser.parse_str(ref)

    >> ref = 'Jones, D. (2009), Addiction and pathological accommodation: An intersubjective look at impediments to the utilization of Alcoholics Anonymous. Internat. J. Psychoanal. Self Psychol., 4:212–233.'
    >> str_parser = StrReferenceParser()
    >> str_parser.parse_str(ref)


    """
    art_id: str = Field(None, title="Article ID (locator) of the instance containing the reference")
    bib_local_id: str = Field(None, title="ID of the be or binc element in the instance")
    art_year: Optional[int]
    bib_ref_in_pep: bool = Field(False)
    bib_rx: str = Field(None)
    bib_rx_confidence: float = Field(0)
    bib_rxcf: str = Field(None)
    bib_rxcf_confidence: float = Field(0)
    bib_sourcetype: str = Field(None, title="journal, book, unknown") # could add video
    # bib_articletitle: str = Field(None)
    bib_authors: str = Field(None)
    # bib_authors_xml: str = Field(None)
    bib_title: Optional[str] = Field(default=None)
    full_ref_text: str = Field(None)
    bib_sourcetype: str = Field(None)
    bib_sourcetitle: str = Field(None)
    # full_ref_xml: str = Field(None)
    bib_pgrg: str = Field(None)
    doi: Optional[str]
    bib_year: str = Field(None)
    bib_year_int: Optional[int]
    bib_volume: str = Field(None)
    bib_volume_int: Optional[int]
    bib_volume_isroman: bool = Field(False, title="True if bib_volume is roman")
    bib_publisher: str = Field(None)
    last_update: datetime = Field(None)


    #--------------------------------------------------------------------------------
    def __init__(self):
        """
        -----------------------------------------------------------
        Load the translation dictionary for parsing references
        -----------------------------------------------------------
        """

        self.art_id = None
        self.bib_local_id = None
        self.art_year = 0
        self.bib_ref_in_pep = False
        self.bib_rx = None
        self.bib_rx_confidence = 0
        self.bib_rxcf = None
        self.bib_rxcf_confidence = None
        self.bib_sourcetype = None
        self.bib_authors = None
        self.bib_title = None
        self.full_ref_text = None
        self.bib_sourcecode = None
        self.bib_sourcetitle = None
        self.bib_pgrg = None
        self.doi = None
        self.bib_year = None
        self.bib_year_int = 0
        self.bib_volume = None
        self.bib_volume_int = 0
        self.bib_volume_isroman = None
        self.bib_publisher = None
        self.last_update = None
        self.biblioDB = None
        self.rgxJrnlPubLocations = None

        # pattern matchers are at the class level...so only need to initialize once.
        try:
            if self.reDictInitd==1:
                return
        except:
            self.reDictInitd = 0

        #Reusable Patterns (should not have group names)
        # spatIntValue = "\d+"
        # spatValueRange = spatIntValue + "\s*\-\s*" + spatIntValue
        spatCommaSep = "\s*,\s*"
        spatAndSep = "\s+and|&|;\s+"
        # spatCommaSepOrSpace = "\s*,?\s*"
        spatName = "[^><\",;.]+?"
        #spatVol = "[1-9][0-9]{0,2}[A-z]?" # up to 3 digit vol
        spatVol = "[1-9][0-9]{0,2}" # up to 3 digit vol
        # Note: spatLabeledVol def chgd 20071029
        spatLabeledVol = "((?P<ntype>(vol|no))\.?\s*(?P<volno>" + spatVol + "))"
        spatAuthorListSep = "(,\s*?|\s+?and\s+?|\s+?&\s+?)"

        patVol = "(?P<vol>"+spatVol+")"
        patPgRg = "(?P<pgrg>(?P<pgstart>[1-9][0-9]*)(\-(?P<pgend>[1-9][0-9]*))?)"

        patEBKPgRg = "pp\.\s*" + patPgRg
        patYear = """\(?\s*
                        (?P<year>(16|17|18|19|20)[0-9]{2,2})
                       |(in\s+prep\.?(aration)?)
                       |(in\s+press)
                   \s*\)?"""
        patEditors = "(?P<eds>\(Eds?\.\))"
        patEndOfAuthors = "([\']|\s+-\s+|" + patYear + "|" + patEditors + "|\s(The|Reply|Some)\s"")"
        patAuthors = """(?P<authordate>
                           (?P<authors>.+?)\s*""" + patEndOfAuthors + ")"
        # Journal Matching Patterns, in order
        spatPeriod = """(  (monograph\s+of
                           |annual
                           |quart\S+
                           |bull\S+)
                       \s+)?"""
        spatNationality = """(( am(\Sr\S+|\.)
                               |brit\S+
                               |can\S+
                               |french
                               |scan\S+
                               |swed\S+
                               |austral\S+
                               |germ\S+
                               |indian
                               |southw\S+
                               |meninger
                               |int(\.|ernational)
                             )
                             \s+)?"""

        spatJClass = "((arch\S+|journ\S+|annals|j\.)\s+)"
        spatOfThe = """((of\s+(the\s+)?)|(de\s+(la\s+)?))?"""
        spatNationalityModifier = """((Soci\S+\s+des)\s+)?"""
        #spatNationality = "((American|British|Canadian|French|Scandinavian|Swedish|Australian|German|Indian|Southwest(ern)?|Int(\.|ernational))\s+)?"
        spatFielldAdjModifier = "((acad\S+\s+of)\s+)?"
        spatFieldAdj = """((mathematical and statistical
                           |medical
                           |gen\S+
                           |cognitive
                           |clinical
                           |developmental
                           |genetic
                           |social
                           |ment\S+
                           |crim\S+
                           |child
                           |behav\S+(\s+and)?
                           |(studies\son))\s+)?"""
        spatFieldClass = """(   (Psycho\S+
                               |Soc\S+
                               |Medicine
                               |Psychia\S+
                               |Nervous\s+and\s+Mental\s+Disease
                               |Anthro-?pology
                               |History
                               |Sci(\.|ence)
                               |Folk-?lore
                               |Develop(\.|ment)
                               |Alcohol
                               |Brain\s+Science
                               |Clini\S+
                               |de\s+Paris)
                               ,?\s+
                           )
                         """
        spatFieldSuffix = """(
                               (Treatment\s+and\s+Evaluation
                                |Society
                                |Psych\S+
                                |Bull\S+
                                |Rev\S+
                                |Quart\S+
                                |Assn\S+
                                )
                                ,?\s+
                            )?
                         """
        spatJournal = '((The\s+)?' + spatPeriod + spatNationalityModifier + spatNationality + spatJClass + spatOfThe + spatNationalityModifier + spatNationality + spatFielldAdjModifier + spatFieldAdj + spatFieldClass + spatFieldSuffix + ")"
        patJournal = '(?P<journal>(' + spatJournal + '))'
        patBook = """(
                               Press
                               |New\s+York(\:|\,)
                               |NY:
                               |MA:
                               |PA:
                               |Philadelphia(\:|\,)
                               |Boston(\:|\,)
                               |Chicago(\:|\,)
                               |England(\:|\,)
                               |NJ(\:|\,)
                               |CA(\:|\,)
                               |VT(\:|\,)
                               |S\.\s*E\.?
                               |Standard\s+Edition
                               |London(\:|\,)
                               |Paris(\:|\,)
                               |Geneva(\:|\,)
                               |Paper\s+Presented\b
                               |Cleveland(\:|\,)
                               |DC(\:|\,)
                               |Florence(\:|\,)
                               |Oxford(\:|\,)
                               |(\:)\s+McGraw-Hill
                               |(\:)\s+Penguin
                               |In\s+.*\beds?\.\b
                               |In\s+Preparation
                     )
                 """

        patPublisherArea = """\(?(
                                       Harmondsworth
                                       | Middlesex
                                       | London:
                                       | New\sYork
                                       | Bloomington
                                       | Cambridge
                                       | Philadelphia
                                       | Chichester
                                       | Boston
                                       | N\.?Y\.?:
                                       | M\.?A\.?:
                                       | P\.?A\.?:
                                       | N\.?J\.?:
                                       | C\.?A\.?:
                                       | V\.?T\.?:
                                       | Chicago
                                       | England
                                       | San\sFrancisco
                                       | Paris
                                       | Geneva
                                       | Cleveland
                                       | Oxford
                                       | Melbourne
                                       | Detroit
                                       | Edited By
                                       | Journal
                                 )\)?
                              """

        patPubLocations = """(?P<plc>
                                       London
                                       |New\s+York
                                       |NY
                                       |MA
                                       |PA
                                       |Philadelphia
                                       |Boston
                                       |Chicago
                                       |England
                                       |NJ
                                       |CA
                                       |VT
                                       |Paris
                                       |Geneva
                                       |Cleveland
                                       |DC
                                       |Oxford
                                       )(\:|\,)"""

        self.rgxJrnlPubLocations = re.compile(patPubLocations, re.VERBOSE | re.IGNORECASE)

        # Specific Args (should have group names)

        # new patterns
        patFirstName = "(?P<firstname>"+spatName+")"
        patLastName = "(?P<lastname>"+spatName+")"
        patInitial = "(?P<initial>[A-Z]{1,1}?\.)"
        patIssue = "(\s*\((no\.\s)?(?P<issue>[1-9])\))?"
        patVolSuppl = "(\s*?\(?(?P<suffix>supp(\.|lement))\)?)?"
        patVolPgRg = patVol + patIssue + patVolSuppl + '(\:\s*|\s+)' + patPgRg
        patAuthorNameFirstInitials =  spatAuthorListSep + "?" + "(?P<author>" + patLastName + spatCommaSep + "(" + patInitial + "\s*){1,4})"
        patAuthorNameFirstName =  spatAuthorListSep + "?" + "(?P<author>" + patLastName + spatCommaSep + "(" + patFirstName + "\s*){1,2})"
        patAuthorNameSplit = patAuthorNameFirstInitials + spatAuthorListSep + "?"
        #patEachAuthor = '((?P<last>[^\,"]+?)[ ]*\,[ ]*(?P<fi>[^\,"]+?))'
        #print patAuthorName
        patInBookSource = "(?P<citedbooktitle>In(\:?\s+)(?P<editedbooktitle>.*?)\.).*" + patPublisherArea
        self.gRegcInBookSource = re.compile(patInBookSource, re.VERBOSE)  # Must be case sensitive to keep from getting false hits on 'in' without a colon (might be better without the optional colon!)
        self.gRegcPgRg = re.compile(patPgRg, re.VERBOSE)
        self.gRegcLabeledVol = re.compile(spatLabeledVol, re.IGNORECASE | re.VERBOSE)
        self.gRegcVolPages = re.compile(patVolPgRg, re.IGNORECASE | re.VERBOSE)
        self.gRegcYear = re.compile(patYear, re.IGNORECASE | re.VERBOSE)
        self.gRegcJournal = re.compile(patJournal, re.IGNORECASE | re.VERBOSE)
        self.gRegcAuthors = re.compile(patAuthors, re.IGNORECASE | re.VERBOSE)
        self.gRegcIsBook = re.compile(patBook, re.IGNORECASE | re.VERBOSE)
        self.gRegcppPgRg = re.compile(patEBKPgRg, re.IGNORECASE | re.VERBOSE)
        self.gRegcVolSup = re.compile(patVol + patIssue + patVolSuppl, re.IGNORECASE | re.VERBOSE)

        # used to break down author name list
        self.gRegcAuthorInitials = re.compile(patAuthorNameFirstInitials, re.IGNORECASE | re.VERBOSE)
        self.gRegcAuthorFirstName = re.compile(patAuthorNameFirstName, re.IGNORECASE | re.VERBOSE)
        self.gRegcAndSep = re.compile(spatAndSep, re.IGNORECASE | re.VERBOSE)
        self.gRegcAuthorListSplit = re.compile(patAuthorNameSplit, re.IGNORECASE | re.VERBOSE)
        self.reDictInitd = 1

    #--------------------------------------------------------------------------------
    def parse_str(self, ref_text, art_id=None, bib_local_id=None, probable_source="", report=0, reset_data=1):
        """
        ------------------------------------------------------------------
        Parse an untagged reference entry and build a component dictionary

        The ProbableSource parameter can be used to independently specify
        	the journal title, if it is known from another source such
        	as the formatted (italicized text)

        Returns the key, if one is able to be computer
        ------------------------------------------------------------------

        """

        ret_val = None
        bauthors = ""
        byear = ""
        bvol = ""
        bpgrg = None
        bissue = None
        bpgstart = 0
        bpgend = 0
        bjournal = ""

        self.art_id = art_id
        self.bib_local_id = bib_local_id

        self.full_ref_text = ref_text
        m = self.gRegcVolPages.search(ref_text)
        if m is not None:
            bvol = m.group("vol")
            bpgrg = m.group("pgrg")
            bpgstart = m.group("pgstart")
            bpgend = m.group("pgend")
            bissue = m.group("issue")
            if gDbg1:
                print("Ref - Vol: %s PgRg: %s PgSt: %s PgEnd: %s" % (bvol, bpgrg, bpgstart, bpgend))
            if opasgenlib.is_empty(bpgend):
                bpgend = bpgstart
        else:
            # just note this, don't raise error, because this could be a book
            if gDbg1:
                print("Ref - No Vol Info: %s" % (ref_text))

        if bissue is not None:
            bissue = trimPunctAndSpaces(bissue)

        # volume might not be part of a vol:pgrg group, so look again if not found
        if bvol == "":
            #print "Trying/Getting Book Vol"
            bvol = self.__get_book_vol(ref_text)

        # get PEP Journal Info
        (jrnlCode, pepCode, fullJournalName) = gJrnlData.getPEPJournalCode(ref_text)
        if jrnlCode is not None and jrnlCode not in gJrnlData.notInPEPList:
            #"PEP Journal!!!"
            self.bib_sourcecode = jrnlCode
            self.bib_sourcetitleabbr = pepCode
            self.bib_sourcetitle = fullJournalName    # journal name as cited
            self.ref_source_type = "journal"
            self.bib_ref_in_pep = True                        # Journal is in PEP!
        else:
            seException = get_se_volexceptions(ref_text)
            if seException is not None:
                # then it's the poor way of saying SE, and we have a vol.
                #print "Special SE Notation Exception Recognized"
                self.bib_sourcecode = "SE"
                bvol = self.bib_volume = seException
                self.bib_sourcetitleabbr = "Standard Ed."
                self.bib_sourcetitle = "SE"
                self.ref_source_type = "journal"
                self.bib_ref_in_pep = True # Journal is in PEP!
            else:
                if not opasgenlib.is_empty(probable_source):
                    bjournal = trimPunctAndSpaces(probable_source)

                    #bjournal = opasgenlib.doEscapes(bjournal)
                else:
                    # see if we recognize the non-PEP journal in this using patterns
                    m = self.gRegcJournal.search(ref_text)
                    if m is not None: #matched.
                        bjournal = m.group("journal")
                        #bjournal = bjournal.strip()
                        if self.bib_sourcetitle is None:
                            self.bib_sourcetitle = trimPunctAndSpaces(bjournal)
                    else:
                        #print self.gRegcJournal.pattern
                        if gDbg1: print("Ref - No Journal Info: %s" % (ref_text))

        # Get Year, (1965)
        m = self.gRegcYear.search(ref_text)
        if m is not None:
            byear = m.group("year")
            noYear = 0
            if byear is not None:
                byear = trimPunctAndSpaces(byear)
        else:
            if gDbg1: raise "No Year: %s" % (ref_text)
            logger.debug("Reference has no year information: %s" % (ref_text))
            noYear = 1


        m = self.gRegcAuthors.search(ref_text)
        if m is not None:
            #bauthors = opasgenlib.doEscapes(string.strip(m.group("authors")))
            bauthors = m.group("authors")
            bauthors = bauthors.strip()
            if not opasgenlib.is_empty(bauthors):
                if bauthors[0] in ("'", '"'):
                    bauthors = re.sub('(\"\').*?(\1)', "", bauthors)

                if bauthors != "":
                    self.bib_authors = bauthors
                    self.bib_author_list = self.__parse_authors(bauthors)
                else:
                    logger.debug(f"Ref - No Authors (quoted text removed!): {ref_text}")
            else:
                logger.debug("Ref - No Authors: %s" % (ref_text))
        else:
            err = "Ref - No Author Info: %s" % (ref_text)
            if noYear == 0: #can't find authors without year
                logger.debug(err)
            else:
                logger.debug(err)

        self.bib_year = atoiYear(byear)
        self.bib_pgrg = trimPunctAndSpaces(bpgrg)

        # look for special page range
        m = self.gRegcppPgRg.search(ref_text)
        if m is not None:
            bpgrg = m.group("pgrg")
            bpgstart = m.group("pgstart")
            bpgend = m.group("pgend")
            if opasgenlib.is_empty(bpgend):
                bpgend = bpgstart

        self.bib_pgstart = PageNumber(bpgstart)
        self.bib_pgend = PageNumber(bpgend)
        if opasgenlib.isRoman(bvol):
            bvol = opasgenlib.convRomanToArabic(bvol)  # Returns number whether arabic or roman
        self.bib_volume = bvol


        if self.bib_year == 0:
            self.bib_year = None

        if self.bib_year is None:
            if self.bib_volume is not None:
                gJrnlData.getYear(self.bib_sourcecode, self.bib_volume)
            else:
                if self.bib_sourcecode == "SE":
                    # the number found is probably the volume, so grab it and look for the year
                    if gDbg1: print("Ref - Trying to get the SE Volume in ", ref_text)
                    patSE = "(?P<se>((<i>)?\&SE\;|SE[\.,]|(S\.\s*E\.)|(Std\.|((Stand(\.|ard)))\s+Ed(\.|ition|it\.))(</i>)?))"
                    patSEVol = patSE + ",?\s+" + "(?P<vol>[1-2][0-9])(\s|,)"
                    m = re.search(patSEVol, ref_text, re.IGNORECASE)
                    if m is not None:
                        vol = m.group("vol")
                        if not opasgenlib.is_empty(vol):
                            self.bib_volume = repr(opasgenlib.convertStringToArabic(vol))
                            if gDbg1: print ("Found SE Volume!")

                logger.debug("Ref - No year or volume information: %s" % (ref_text))

            # see if we found it!  If not, try something else.
            if self.bib_volume is None:
                # try a special vol search for labeled volume)
                roman = "(?P<romannum>m*(cm|dc{0,3}|cd|c{0,3})(xc|lx{0,3}|xl|x{0,3})(ix|vi{0,3}|iv|i{0,3}))"
                m = re.search("vol[\.:]?\s*(?P<vol>[0-9]{1-4}|" + roman + ")", ref_text, re.IGNORECASE)
                if m is not None:
                    # new vol
                    vol = m.group("vol")
                    if not opasgenlib.is_empty(vol):
                        self.bib_volume = repr(opasgenlib.convertStringToArabic(vol))
                        self.bib_volume_isroman = True

        self.bib_probablysource = probable_source

        # Now figure out title!
        if 1: 
            if self.bib_year is not None:
                str1 = "\(?\s*" + str(self.bib_year) + "[a-z]?\s*\)?"
            elif self.bib_authors is not None:
                str1 = opasgenlib.remove_all_punct(self.bib_authors, additional_chars="\t/,[]‘’1234567890")
                self.bib_authors = str1
            else:
                str1 = None

            # MUST use local var jrnlName because we wan't the original name found!!!!
            #if opasgenlib.is_empty(self.bib_sourcetitle):
                #str2 = """\(?(
                                                            #Harmondsworth
                                                            #| Middlesex
                                                            #| London:
                                                            #| New\sYork
                                                            #| Bloomington
                                                            #| Cambridge
                                                            #| Philadelphia
                                                            #| Chichester
                                                            #| Boston
                                                            #| NY:
                                                            #| MA:
                                                            #| PA:
                                                            #| Chicago
                                                            #| England
                                                            #| NJ:
                                                            #| CA:
                                                            #| VT:
                                                            #| Paris
                                                            #| Geneva
                                                            #| Cleveland
                                                            #| Oxford
                                                            #| Melbourne
                                                            #| Detroit
                                                            #| Edited By
                                                            #| Journal
                                                    #)\)?
                                            #"""
            #else:
                #str2 = "(" + self.bib_sourcetitle + ")"

            if str1 is not None:
                # use date/authors and journal name; find title in between
                srcTSeries = self.bib_sourcetitle
                # 'Jones, D. (2009), Addiction and pathological accommodation: An intersubjective look at impediments to the utilization of Alcoholics Anonymous. Internat. J. Psychoanal. Self Psychol., 4:212–233.'

                if srcTSeries is not None:
                    try:
                        rgP = fr"(?P<front>.*?\))(?P<title>.*?)(?P<journal>{re.escape(srcTSeries)})?"
                    except:
                        try:
                            rgP = f"\s*\.?\s*\'?(?P<title>[A-Z][^\.]+?)[\.\?]?(?P<journal>{re.escape(srcTSeries)})?"
                        except:
                            try:
                                rgP = f"\s*\.?\s*'?(?P<title>[A-Z][^\.]+?)[\.\?]?(?P<journal>{srcTSeries})?"
                            except Exception as e:
                                print("Fail in setting srctitleseries", e)
                else:
                    rgP = "\s*\.?\s*'?(?P<title>[A-Z][^\.]+?)\."

                m = re.search(rgP, ref_text, flags=re.IGNORECASE | re.VERBOSE)
                if m is not None:
                    self.bib_title = trimPunctAndSpaces(m.group("title"))
                    #print "PICKED UP TITLE!!!!", self.bib_title
                else: # see if there was a year, but it was in the wrong place
                    str1 = self.bib_authors
                    rgP = "\s*\.?\s*'?(?P<title>[A-Z][^\.]+?)\."
                    m = re.compile(opasgenlib.do_re_escapes(str1) + rgP, re.VERBOSE).search(ref_text)
                    if m is not None:
                        self.bib_title = trimPunctAndSpaces(m.group("title"))
            else:
                # what do we use!
                logger.debug("PEPReferenceParserStr - Not enough info in ref to search for title: '%s'." % ref_text.rstrip())

        bookCode, sRatio, refObj = bookInfo.getPEPBookCodeStr(ref_text)
        if bookCode is not None:
            #print "BookCode: ", bookCode
            ret_val = bookCode
            # should we fill the ref structure with standard data?  Or leave it as is?  Based on sRatio?
            #bookRefDict = self.bookInfo.getPEPBookInfo(bookCode)

        else:
            if self.bib_ref_in_pep:
                try:
                    ret_val = Locator(jrnlCode=self.bib_sourcecode,
                                      #jrnlVolSuffix=self.bib_vol_suffix,
                                      jrnlVol=self.bib_volume,
                                      jrnlYear=self.bib_year,
                                      #pgVar=self.bib_pgvar,
                                      pgStart=self.bib_pgstart,
                                      noStartingPageException=False).articleID()
                except Exception as e:
                    print("Exception: ", e)
                except:
                    print(f"Ref - Can't form locator for str_parse")

        self.bib_rx = ret_val
        self.bib_rx_confidence = opasConfig.RX_CONFIDENCE_PARSED_PROBABLE
        return ret_val

    #--------------------------------------------------------------------------------
    def __parse_authors(self, bAuthors):
        """
        ------------------------------------------------------------------
        Parse an untagged author field from a biblio entry
         and build an author dictionary
        ------------------------------------------------------------------
        """
        retVal = []

        authList1 = self.gRegcAndSep.split(bAuthors)
        for auth in authList1:
            #authList = self.gRegcAuthorListSplit.split(auth1)
            #print "AuthList Split 2:", authList
            #for auth in authList:
            if auth is not None:
                m = self.gRegcAuthorInitials.match(auth)
                if m is not None:
                    aName = m.group("author")
                    aName = aName.strip()
                    retVal.append(aName)
                else:
                    retVal.append(auth)

        #print retVal
        return retVal

    #--------------------------------------------------------------------------------
    def __get_book_vol(self, strText):
        """
        Some volume numbers are marked, rather than being part of a volume,
        page range combination.  This finds them!
        """
        m = self.gRegcLabeledVol.search(strText)
        if m !=None:
            jrnlVol = m.group("volno")
        else:
            jrnlVol = None
        #print "Returning jrnlVol: ", jrnlVol
        return jrnlVol

    #--------------------------------------------------------------------------------
    def __find_book_pub_location(self, refText):
        """
        Find the publication location, which is not, unfortunately, ever tagged!
        """
        retVal = None
        if not opasgenlib.is_empty(self.bib_publisher):
            # split text at publisher
            refList = refText.split(self.bib_publisher)
            m = self.rgxJrnlPubLocations.search(refList[0])
            if m is not None:
                retVal = m.group("plc")
            else:
                # try matching on . pattern
                m = re.match("(\.)\s+(?P<plc>.*?)\:", refList[0])
                if m is not None:
                    retVal = m.group("plc")
                else:
                    print("Ref - Cant find publication location in '%s'" % refText)

        return retVal

    #--------------------------------------------------------------------------------
    def __load_journal_set_from_db(self, forceLoad=0):
        """
        Load a set of journal names from the DB, to help to identify
        	when a journal name is used.
        Only loads if not already loaded, or if force load is selected.
        """

        if self.journalSet is None or forceLoad == 1:
            if gDbg1: print("Loading journal match set from DB")
            # load journals
            dbc = self.biblioDB.db.cursor()
            dbc.execute("""SELECT jrnlname, jrnlcode
                            FROM stdjournalnames
                            WHERE jrnlcode IS NOT NULL
                            AND jrnlcode <> ''
                            ORDER BY length(jrnlname) desc""")
            self.journalSet = dbc.fetchall()
            self.journalSetCompiled = []
            regcJrnlNamesAll = ""
            for (jrnlName, jrnlCode) in self.journalSet:
                if len(jrnlName)>0:
                    namergxPrep = opasgenlib.do_re_escapes(jrnlName)
                    regcJrnlName = re.compile(namergxPrep, re.IGNORECASE)
                    self.journalSetCompiled.append((jrnlName, jrnlCode, regcJrnlName))
                    regcJrnlNamesAll += "" + namergxPrep + "|"
            # take off extra "or"
            if len(regcJrnlNamesAll)>1: regcJrnlNamesAll = regcJrnlNamesAll[:-1]
            # compile
            self.regcJrnlCompleteSet = re.compile(regcJrnlNamesAll, re.IGNORECASE)
            #print self.regcJrnlCompleteSet.pattern

        return

    #--------------------------------------------------------------------------------
    def __search_str_for_journal_name(self, bStrReference):
        """
        Search the string encoded reference for a journal name, using the database
        	of journal names.
        Return the journal name
        """
        retVal = None
        self.__load_journal_set_from_db()
        m = self.regcJrnlCompleteSet.search(bStrReference)
        if m is not None:
            retVal = m.group()
            #print "Match!!!", retVal

        return retVal

    #--------------------------------------------------------------------------------
    def __get_non_pep_journal_code_from_db(self, bJournalName=None, bStrReference=None):
        """
        Lookup a journal name in the biblio database, and return a tuple, with
        	(
        		jrnlCode - Standard journal code (not PEP jrnlCode) though
        		jrnlName - Standard name for citations
        	)

        """

        retVal = (None, None)
        found = 0

        if self.biblioDB is not None:
            self.__load_journal_set_from_db()
            if bJournalName is None:
                # find the journal name
                if bStrReference!=None:
                    bJournalName = self.__search_str_for_journal_name(bStrReference)
                else:
                    raise "Ref - Either jrnlName or the reference must be provided"

            if not opasgenlib.is_empty(bJournalName):
                # try for exact match
                for (jrnlName, jrnlCode, regcJrnlName) in self.journalSetCompiled:
                    if bJournalName == jrnlName:
                        retVal = (jrnlCode, jrnlName)
                        found = 1
                        break

                #exact match didn't work; try search (separated from above for speed!)
                if found == 0:
                    for (jrnlName, jrnlCode, regcJrnlName) in self.journalSetCompiled:
                        if regcJrnlName.search(bJournalName):
                            # journal found
                            retVal = (jrnlCode, jrnlName)
                            found = 1
                            break

        return retVal


    #--------------------------------------------------------------------------------
    def get_vol_and_pages(self, refText):
        """
        Find the publication location, which is not, unfortunately, ever tagged!
        """
        bvol = bpgrg = bpgstart = bpgend = bissue = None
        m = self.gRegcVolPages.search(refText)
        if m is not None:
            bvol = m.group("vol")
            bvol = bvol.strip()
            bpgrg = m.group("pgrg")
            bpgrg = bpgrg.strip()
            bpgstart = m.group("pgstart")
            bpgend = m.group("pgend")
            bissue = m.group("issue")
            if opasgenlib.is_empty(bpgend):
                bpgend = bpgstart
        else:
            # just note this, don't raise error, because this could be a book
            if gDbg1:
                print("Ref - No Vol Info: %s" % (refText))

        return (bvol, bpgrg, bpgstart, bpgend, bissue)


#--------------------------------------------------------------------------------
def get_se_volexceptions(strText):
    """
    Return Volume number if this is a shorthand SE with a volume number

    >>> get_se_volexceptions('Freud, S. (1905). Fragment of an analysis of a case of hysteria. SE 7.')
    '7'
    >>> get_se_volexceptions('Freud, S. 1910 The future prospects of psychoanalytic therapy SE 11 London: Hogarth Press, 1953')
    '11'

    """
    rgPat = r"\bSE\s+(?P<vol>1?[0-8])\b"
    m = re.search(rgPat, strText)
    if m is not None:
        # this is probably SE, and we know volume
        retVal = m.group("vol")
    else:
        retVal = None

    return retVal

#------------------------------------------------------------------------------------------------------------
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
