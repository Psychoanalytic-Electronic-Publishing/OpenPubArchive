# -*- coding: UTF-8 -*-
"""
The basis of links and the ID system in PEP is the "Locator" managed by the Locator
 class in this module.

This module adapted from a much older module used in PEPXML to compile PEP instances since 200x!

  ** Slowly being adopted to opas **

"""
__author__      = "Neil R. Shapiro"
__license__     = "Apache 2.0"
__version__     = "2022.0609" 
__status__      = "Development"

programNameShort = "opasLocator" # Library to build and decompile locators (articleIDs/document ids)

import sys, os.path
import logging
logger = logging.getLogger(programNameShort)
from loggingDebugStream import log_everywhere_if

global gDbg1
gDbg1 = False

import copy
import re

from opasConfig import BOOK_CODES_ALL, gSplitBooks, REFBOOK, REFBOOKSERIES, REFBOOKSERIESARTICLE, REFBOOKARTICLE, REFJOURNALARTICLE
# from pydantic import BaseModel, Field, ValidationError, validator, Extra

import PEPJournalData
import opasXMLSplitBookSupport

import opasGenSupportLib as opasgenlib
import opasDocuments
import opasLocalID
import opasDocuments
import opasCentralDBLib
ocd = opasCentralDBLib.opasCentralDB()

# set to 1 for doctests only

# id locator - but watch for special values, i.e., APA.44S.00xi0, roman page numbers-allow but only lower numbers < 20

def pageStartException(jrnlCode, fullFilename, noExceptionPageStart=None):
    retVal = noExceptionPageStart
    if jrnlCode in ["IJPOPEN"] and fullFilename is not None:
        patMatch = re.match(".*(IJPOPEN)\..*\.(?P<pgStart>.*)A", fullFilename, re.IGNORECASE)
        if patMatch is not None:
            retVal = patMatch.group("pgStart")
            # Special Rule: pgStart derived from filename
            retVal = opasDocuments.PageNumber(retVal, forceRoman=False)
    return retVal

#----------------------------------------------------------------------------------------
# CLASS DEF: locator
#----------------------------------------------------------------------------------------
class Locator:
    """
    The basis of links and the ID system in PEP is the "Locator".

    The Locator is an ID consisting of a  SourceCode "." VolumeNumber "." PageNumber

         SourceCode is the PEP assigned Journal or Book Code.  These are given in
         VolumeNumber and PageNumber are themselves classes to manage these complex entities.
         (e.g., Volumes are three digit numbers, but they can have suffixes.  Pages can have
         prefixes and suffixes.)

    An Example of a Locator in string form is:

    APA.033S.0032A

    or

    APA.021.R0015A

    >>> Locator("ZBK.001.0012").articleID()==Locator("ZBK.001.00120").articleID()
    True
    >>> a = Locator("RPP-CS.013.0025A")
    >>> a.check_for_missing_vol_suffix()
    'RPP-CS.013B.0025A'
    >>> a = Locator("PI.012.0569A")
    >>> a.check_for_offset_page_start(1)
    PI.012.0570A
    >>> a = Locator("PI.012.0571A")
    >>> a.check_for_offset_page_start(-1)
    PI.012.0570A
    >>> Locator("IJP.095.E0001A").articleID()
    'IJP.095.E0001A'
    >>> Locator("IJP.095.ER0001A").articleID()
    'IJP.095.ER0001A'
    >>> Locator("IJP.095.NP0001A").articleID()
    'IJP.095.NP0001A'
    >>> Locator("IJP.095.NPR0001A").articleID()
    'IJP.095.NPR0001A'
    >>> Locator("PI.1992.0012").articleID()==Locator("PI.1992.00120").articleID()
    True
    >>> Locator("PI.1992.0012").articleID()==Locator("PI.012.00120").articleID()
    False
    >>> Locator("ZBK.1.0012").articleID()==Locator("ZBK.001.00120").articleID()
    True
    >>> Locator("ZBK.01.00120").articleID()==Locator("ZBK.001.00120").articleID()
    True
    >>> Locator("IPL.1.0012").articleID()==Locator("IPL.001.00120").articleID()
    True
    >>> Locator("ZBK.001.0012").localID("B002")=="ZBK.001.0012A.B002"
    True
    >>> Locator("ZBK.001.0012").localID("B0002")
    'ZBK.001.0012A.B002'
    >>> Locator("[Field RefID:IJP.79.00010]").articleID()
    'IJP.079.0001A'
    >>> Locator("IJP.013.0001A").articleID(a1v4Format=1)
    'IJP.013.00010'
    >>> Locator("IJP.013.0001B").articleID(a1v4Format=1)
    'IJP.013.00011'
    >>> Locator("IJP.013.0001C").articleID(a1v4Format=1)
    'IJP.013.00012'
    >>> Locator("IJP.013.0001C").articleID(a1v4Format=0)
    'IJP.013.0001C'
    >>> Locator(jrnlCode="IJP", jrnlVolSuffix=None, jrnlVol="72", pgVar="A", pgStart="43")
    IJP.072.0043A
    >>> print (Locator("ANIJP-IT.2006.0000").isBook())
    False
    >>> print (Locator("SE.001.0000").isBook())
    True
    >>> Locator("zbk.052.0001A").localID("P0607")
    'ZBK.052.0001A.P0607'
    >>> "ZBK.052.0001A.H00607" == Locator("zbk.052.0001A").localID("H0607")
    True
    >>> Locator("zbk.052.0001A").localID("H0607F")
    'ZBK.052.0001A.H00607F'
    >>> Locator(jrnlCode="IJP", jrnlVolSuffix=None, jrnlVol="72", pgVar="A", pgStart="43")
    IJP.072.0043A
    >>> Locator(jrnlCode="IJP", jrnlVolSuffix=None, jrnlVol="72", pgVar="A", pgStart="43")
    IJP.072.0043A
    """

    # for later conversion to pydantic
    
    #pgVar:  str = Field(None, title="")
    #valid: bool = Field(False, title="")
    #locatorType: int = Field(0, title="")				# 0 is a article ID, 1 is idx id
    #validError: str = Field("", title="")
    #jrnlCode:  str = Field(None, title="")
    #jrnlVol:  str = Field(None, title="")
    #jrnlYear:  str = Field(None, title="")
    #jrnlIss:  str = Field(None, title="")
    #idxNamePrefix:  str = Field(None, title="")
    #pgStart:  str = Field(None, title="")
    #pgEnd:  str = Field(None, title="")
    #pgVar:  str = Field("", title="")
    #pgSuffix:  str = Field("", title="")
    #prePrefix:  str = Field("", title="")
    #prefix:  str = Field("", title="")
    #suffix:  str = Field("", title="")
    #filename: str = Field("", title="")
    #art_info: str = Field("", title="")
    #noStartingPageException: bool = Field(False, title="")
    #thisIsSplitBook: bool = Field(False, title="")
    #isMainTOC: bool = Field(False, title="")
    #notFatal: bool = Field(False, title="")
    #orig_locator_list: list = Field(None)   # in case the input val was not string but a list of strings    

    # limits
    MINJRNLABBRLEN = 2
    MAXJRNLABBRLEN = 12	  #A1v16

    ptnLocatorPrefix = """
                          (?P<locator>
                            (?P<idxname>TOJ\.)?
                            (?P<articleID>"""

    ptnLocatorJrnlAbbr = """(?P<jrnl>[A-Z\-]{%s,%s})""" % (MINJRNLABBRLEN, MAXJRNLABBRLEN)
    # Added BK to volsuffix for books as of A1v4
    # As of A1v6, suffix goes to T, because of FA!
    ptnLocatorVolSuffix = """(\.
                                     (
                                         ((?P<vol>\d{1,4})(?P<volsuffix>[A-Z])?)
                                       | (?P<year>\d{4,4})
                                     )
                               )"""
    ptnLocatorBase = (ptnLocatorPrefix
                      +  ptnLocatorJrnlAbbr
                      +  ptnLocatorVolSuffix
                      +  """
                             \.(?P<preprefix>(NP|E|S))?(?P<romanpre>[R])?(?P<pgst>[0-9IXV]{4,4})(?P<var>([A-Z]|\d))?
                            )
                            (\.(?P<localID>[^\"\]]+))?
                          )
                      """)

    ptnPreLoc =  """
                     (?P<pre>
                         (\[)?([\"\'])?
                         (?P<queryID>(group\s+?|field\s+[^:]+?\:)\s*)?
                         ([\"\'])?
                     )
                  """

    ptnPostLoc =  """
                     (?P<post>
                         ([\"\'])?
                         ([\"\'])?(\])?
                     )
                   """

    rgxTOJLocator = re.compile(	"(" + ptnPreLoc +
                                       """
                                     (?P<idxname>TOJ\.)"""
                                       + ptnLocatorJrnlAbbr
                                       + "(" + ptnLocatorVolSuffix + ")?"
                                       + """$"""
                                       + ptnPostLoc
                                       + ")", re.VERBOSE | re.IGNORECASE
                                       )

    rgxLocator = re.compile("("
                            + ptnPreLoc
                            + ptnLocatorBase + ptnPostLoc +
                            ")", re.VERBOSE | re.IGNORECASE)

    #--------------------------------------------------------------------------------
    def __init__(self, strLocator=None, jrnlCode=None, jrnlVolSuffix="", jrnlVol=None, jrnlIss=None, pgVar="A",
                 pgStart=None, jrnlYear=None, localID=None, keepContext=1, forceRoman=False, notFatal=True,
                 noStartingPageException=True, filename=None, art_info=None, **kwargs):
        """
        Initialize the locator.  Make sure that other args, except keepcontext, are not specified if
            strLocator is.
        """       
        # private storage of local id
        
        self.__reset()
        self.filename = filename
        self.orig_locator_list = [] # in case original was a list
        
        self.art_info = art_info # provide full information about the xml of the article we're dealing with

        if jrnlVolSuffix is None:
            jrnlVolSuffix = ""

        # cases where you don't want a starting page exception
        #  If the link specified a local ID, chances are they know the right instance
        if localID is not None:
            self.noStartingPageException = True
        else:
            self.noStartingPageException = noStartingPageException

        self.thisIsSplitBook = None
        self.isMainTOC = False				# set to true if this is a mainTOC for a split book
                                            # This info is kept in the locator, since it's needed to handle locator
                                            # exceptions (going to the mainTOC rather than page 1)
        self.notFatal = notFatal			# should locator errors be "fatal" to the process?
        self.__keepContext__ = keepContext	# if this is 1, the prefix and suffix around the locator are stored
                                            # during the decompile.
                                            # this is usually not what is wanted, but just in case!
        if strLocator is not None:
            if (None, "", None, None, "A", None, None, None) != (jrnlCode, jrnlVolSuffix, jrnlVol, jrnlIss, pgVar, pgStart, jrnlYear,  localID):
                raise Exception("Locator Init: Cannot specify arguments in addition to strLocator")
            if isinstance(strLocator, str):  # supports string and unicode Was type(strLocator) == type(""):
                self.decompile(strLocator)
            elif isinstance(strLocator, list):
                self.orig_locator_list = strLocator
                strLocator = self.orig_locator_list[0]
                self.decompile(str(strLocator))
            elif isinstance(strLocator, Locator):
                self.decompile(str(strLocator))
            else:
                try:
                    self.decompile(strLocator)
                except:
                    errMsg = "CODING ERROR: Bad conversion type %s for Locator: %s" % (type(strLocator), str(strLocator))
                    raise Exception(errMsg)
        else:
            if jrnlCode is not None:
                self.jrnlCode = jrnlCode.upper()
            else:
                self.jrnlCode = jrnlCode

            self.jrnlVol = opasDocuments.VolumeNumber(volNum=jrnlVol, volSuffix=jrnlVolSuffix)
            self.jrnlYear = jrnlYear
            self.jrnlIss = jrnlIss
            self.pgVar = pgVar
            # check if there needs to be a pgStart Exception, e.g., IJPOPEN
            pgStart = pageStartException(jrnlCode, fullFilename=self.filename,
                                         noExceptionPageStart=pgStart)

            if isinstance(pgStart, opasDocuments.PageNumber):
                self.pgStart = pgStart
            else:
                if pgStart is None:
                    self.pgStart = opasDocuments.PageNumber(0, forceRoman=False)
                else:
                    self.pgStart = opasDocuments.PageNumber(pgStart, forceRoman=forceRoman)

            self.pgEnd = copy.copy(self.pgStart)
            logger.debug("Locator Parameters: ", jrnlCode, jrnlVolSuffix, jrnlVol, jrnlIss, self.pgStart, pgVar, jrnlYear)
            self.__standardize()
            self.validate()

    #--------------------------------------------------------------------------------
    def __reset(self):
        """
        Clear all the object instance attributes
        """
        self.valid = False				    # flag if locator is currently incomplete
        self.locatorType = 0				# 0 is a article ID, 1 is idx id
        self.validError = ""
        self.jrnlCode = None
        self.jrnlVol = None			# Always String
        self.jrnlYear = None
        self.jrnlIss = None
        #self.jrnlVolSuffix = ""
        self.pgVar = "A"
        self.idxNamePrefix = ""
        self.pgStart = None
        self.pgEnd = None
        self.pgSuffix = None
        self.prePrefix = ""
        self.prefix = ""
        self.suffix = ""
        self.__localIDRefTemp = None
        self.__localIDRef = None
        self.__localIDType = ""		# One char.  B = Biblio, P = Page, R = Page (roman)

    #----------------------------------------------------------------------------------------
    def __cmp__(self, other):
        """
        Compare two locators.  Return 0 if equal,
           1 if the first is greater, -1 if the second is greater.

        """

        if other is None:
            retVal = 1
            return retVal
        elif isinstance(other, str):  # supports string and unicode Was type(other) == type(""):
            #print ("Locator Compare - strings")
            cmpTo = str(self)
        elif isinstance(other, Locator):
            #print ("Locator Compare - Objects")
            cmpTo = str(self)
            other = str(other)
        else:
            raise Exception("Locator Compare - Mismatched Objects")

        if cmpTo == other:
            retVal = 0
        elif cmpTo > other:
            retVal = 1
        else:
            retVal = -1

        return retVal

    #--------------------------------------------------------------------------------
    def __repr__(self):
        """
        Return a displayable/printable version of a locator.
        """

        retVal = None
        if self.locatorType == 0:
            # no need for P0000 local ID!
            if self.__localIDRef is not None and self.__localIDRef != "P0000":
                retVal = self.prefix + self.idxNamePrefix + self.articleID() + "." + str(self.__localIDRef) + self.suffix
            else:
                if self.articleID() is not None:
                    try:
                        retVal = self.prefix + self.idxNamePrefix + self.articleID() + self.suffix
                    except Exception as e:
                        print (f"Error displaying article ID ({e})")
                        
        # if its an index type (TOJ)
        else:
            raise Exception("TOJ Type locator request.")

        return str(retVal)

    #--------------------------------------------------------------------------------
    def __getslice__(self, start, end):
        """
        Get part of a locator
        """
        start = max(start, 0)
        end = max(end, 0)
        tmpLoc = self.articleID()
        retVal = tmpLoc[start:end]
        return retVal

    #--------------------------------------------------------------------------------
    def __getitem__(self, index):
        """
        Get a component of the locator, either Journal name, Volume, Page (start), or Vol Suffix
        """
        if not isinstance(index, int):
            mbr = index[0].upper()
            if mbr == "J":	# journalcod
                retVal = self.jrnlCode
            elif mbr == "V":
                retVal = self.jrnlVol
            elif mbr == "P":
                retVal = self.pgStart
            elif mbr == "S":
                retVal = self.jrnlVol.volSuffix
        else:
            tmpLoc = self.articleID()
            retVal = tmpLoc[index]

        return str(retVal)

    #--------------------------------------------------------------------------------
    def getLocalID(self):
        """
        Get the localID component only
        """
        if self.__localIDRef is not None:
            retVal = str(self.__localIDRef)
        else:
            retVal = None

        return retVal

    #--------------------------------------------------------------------------------
    def hasLocalID(self):
        """
        Return true if this locator has a localID component set.
        """
        if self.__localIDRef is not None:
            retVal = True
        else:
            retVal = False

        return retVal

    #--------------------------------------------------------------------------------
    def __len__(self):
        """
        Return the length of the locator
        """
        return len(str(self))

    #--------------------------------------------------------------------------------
    def __standardize(self, oldStyleVar=0):
        """
        Make sure the attributes of this locator are all in standard form
        """
        global gDbg1
        
        # standardize number formats and make sure we fill in any field
        # that is not known but can be deduced
        if self.pgVar is None:
            self.pgVar = "A"
        else:
            if self.pgVar in ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]:
                # this is a numeric page variant, change to a letter.
                if gDbg1: print ("Old Variant: ", self.pgVar)
                val = int(self.pgVar)
                self.pgVar = chr(ord("A") + val)
                if gDbg1: print ("New Variant: ", self.pgVar)

            elif isinstance(self.pgVar, int):	# numeric
                raise Exception("Numeric variant.(%d)..add code" % self.pgVar)

        if oldStyleVar==1:
            self.pgVar = str(ord(self.pgVar) - ord("A"))
            logger.info(f"Old Style Variant Requested: {self.pgVar}")

        self.__exceptions() # Adjustments for exceptions

    #--------------------------------------------------------------------------------
    def isValid(self):
        """
        Return true if locator is well formed. It may not exist though.
        """
        return self.valid

    #--------------------------------------------------------------------------------
    def baseCode(self, jrnlCode=None, jrnlVol=None):
        """
        Return jrnlcode + volume number.  In effect, this is a better bookCode
        than a regular locator, because an entire book, which might be multiple
        articles, is the code + the vol/id, but not the page number, which
        may vary.

        >>> aLoc = Locator("IJP.052.0001")
        >>> aLoc.baseCode()
        'IJP052'

        """
        if jrnlCode is None:
            jrnlCode = self.jrnlCode

        if jrnlVol is None:
            jrnlVol = self.jrnlVol
        else:
            jrnlVol = opasDocuments.VolumeNumber(jrnlVol)

        # return a base code, either of the specified locator, or the forced parameter version
        return "%s%s" % (jrnlCode, jrnlVol.volID())

    #--------------------------------------------------------------------------------
    def isVariant(self):
        """
        	Is this locator a variant (other than the default variant, "A")
        """
        if self.pgVar != "A" and not opasgenlib.is_empty(self.pgVar):
            #print ("VARIANT/TYPE: ", self.pgVar, type(self.pgVar))
            retVal = True
        else:
            retVal = False

        return retVal

    #--------------------------------------------------------------------------------
    def validate(self):
        """
        Validate the locator, marking the error flag if
        incomplete, as well as returning status

        Mod Note: this used to standardize as well, but as of 9/2003, this was removed
        			for the new locator module design
        """
        self.valid = False
        try:
            if opasgenlib.is_empty(self.jrnlCode) \
              or self.jrnlVol is None or self.jrnlVol == 0 \
              or (opasgenlib.is_empty(self.pgStart) \
              and opasgenlib.is_empty(self.idxNamePrefix)):
                if self.jrnlVol is not None:
                    errStr = "Incomplete/Invalid ID: %s/v%s/p%s" % (self.jrnlCode, self.jrnlVol.volID(), self.pgStart)
                else:
                    errStr = "Incomplete/Invalid ID: %s/v%s/p%s" % (self.jrnlCode, self.jrnlVol, self.pgStart)
                
                self.validError = errStr
                logger.debug(errStr)
                self.valid = False
            else:
                
                #if gDbg1: print ("Locator is valid.")
                self.valid = True
        except Exception as e:
            logger.error(f"Error validating Locator. {e}")

        return self.valid

    #------------------------------------------------------------------------------------------------------
    def check_for_missing_vol_suffix(self, verbose=False):
        ret_val = None
        if self.validate():
            if self.jrnlVol[-1].isnumeric():
                new_loc = copy.copy(self)
                new_loc.jrnlVol.volSuffix = "%"
                new_locs = ocd.get_article_records(new_loc)
                if len(new_locs) == 1:
                    ret_val = new_locs[0].get("art_id", None)
                    log_everywhere_if(verbose, level="debug", msg=f"\t\tCorrecting Art ID with vol variant: {ret_val}.")
                else:
                    log_everywhere_if(verbose, level="warning", msg=f"\t\tTried to correct Art ID, looking up vol variant failed. Must be EXACTLY one possibility, and there were {len(new_locs)}.")
    
        return ret_val    
    
    #------------------------------------------------------------------------------------------------------
    def check_for_offset_page_start(self, page_offset=-1, verbose=False):
        ret_val = None
        if self.validate():
            new_loc = copy.copy(self)
            try:
                new_loc.pgStart += page_offset
            except Exception as e:
                log_everywhere_if(verbose, "info", f"Bad locator {loc_str}")
            else:
                if new_loc.isValid():
                    if ocd.article_exists(new_loc):
                        ret_val = new_loc
                        log_everywhere_if(verbose, level="debug", msg=f"\t\tCorrecting Bib ID with page-1: {ret_val}.")
                    #else:
                        #log_everywhere_if(verbose, "warning", f"Bad locator in reference {loc_str}->{new_loc_str}(article doesn't exist)")
        return ret_val    

    #--------------------------------------------------------------------------------
    def forceArticleID(self, forceJrnlVol=None, forceJrnlVolSuffix=None, offsetPgStart=None, forceJrnlCode=None, forceYear=None, forcePgVar=None, forcePgStart=None, noStartingPageException=None):
        """
        if forceJrnlVolSuffix, offsetPageStart, forceJrnlCode the current locator's values are changed and
        	and article id based on the forced information is returned.

        	>>> Locator("IJP.013.0001C").forceArticleID(forceJrnlVolSuffix="A", forcePgVar="B")
        	'IJP.013A.0001B'
        	>>> Locator("IJP.013A.0001B").forceArticleID(forceJrnlVolSuffix="")
        	'IJP.013.0001B'

        """
        if noStartingPageException is not None:
            self.noStartingPageException = noStartingPageException

        if self.validate():
            self.origLocator = self.articleID()
            #validate = 0

            if opasgenlib.is_empty(forceJrnlVol) or forceJrnlVol == 0:
                forceJrnlVol=self.jrnlVol

            if forceJrnlVolSuffix==None:
                forceJrnlVolSuffix=self.jrnlVol.volSuffix

            if forceJrnlCode==None:
                forceJrnlCode=self.jrnlCode

            if forcePgVar==None:
                forcePgVar=self.pgVar

            if forcePgStart==None:
                if self.pgStart==None:
                    pgStart = opasDocuments.PageNumber(1)
                else:
                    pgStart = self.pgStart
            else:
                pgStart = forcePgStart

            # This was added Aug 2003 to allow checks of the database for slightly askew page numberinbg
            if offsetPgStart is not None:
                pgStart = self.pgStart + offsetPgStart

            retLoc = Locator(jrnlCode=forceJrnlCode,
                             jrnlVolSuffix=forceJrnlVolSuffix,
                             jrnlVol=forceJrnlVol,
                             pgVar=forcePgVar,
                             pgStart=pgStart,
                             jrnlYear=forceYear,
                             noStartingPageException=noStartingPageException)

            logger.debug(f"TEMP LOC: {retLoc}, {forceJrnlVolSuffix}")
            retVal=retLoc.articleID()
            logger.debug("TEMP ARTICLEID:", retVal)
        else:
            errMsg = "Could not force locator; original locator %s is not valid" % self
            logger.error(f"Severe - Locator - {errMsg} ")
            retVal = ""

        return str(retVal)

    #--------------------------------------------------------------------------------
    def isBook(self):
        """
        Returns true if the locator is from a book
        """
        if self.jrnlCode in BOOK_CODES_ALL:
            return True
        else:
            return False

    #--------------------------------------------------------------------------------
    def isSplitBook(self, jrnlCode=None, jrnlVol=None):
        """
        Returns true if the jrnlCode and Vol are from a split book

        >>> testLoc = Locator()
        >>> testLoc.isSplitBook("SE", "4")
        True

        """
        #print ("isSplitBook: ", self.thisIsSplitBook)
        if self.thisIsSplitBook==None:
            splitBookVal = self.__checkSplitBook(jrnlCode=jrnlCode, jrnlVol=jrnlVol)
            # print ("SplitBookVal: ", splitBookVal)
            if splitBookVal is not None:
                # this is a split book, but the first instance may be EITHER 0000 or 0001
                self.thisIsSplitBook = True
            else:
                # not a split book
                self.thisIsSplitBook = False

        return self.thisIsSplitBook

    #--------------------------------------------------------------------------------
    def __checkSplitBook(self, jrnlCode=None, jrnlVol=None):
        """
        See if the jrnlcode/vol combination is a split book.
        If
        	it's a split book with a 0000 mainTOC, it returns 0,
        	for other split books, it returns 1.
        	For nonsplit books, it returns None.
        """
        if jrnlCode==None:
            if self.jrnlCode is not None:
                jrnlCode=self.jrnlCode.upper()
        else:
            jrnlCode=jrnlCode.upper()

        if jrnlCode is None:
            print("No journal code!")
            return None

        if jrnlVol is None:
            if self.jrnlVol is not None:
                jrnlVol="%s" % self.jrnlVol.volID()
            else:
                log_everywhere_if(True, "error", "No volume number")
        else:
            jrnlVol="%s" % opasDocuments.VolumeNumber(jrnlVol).volID()

        if opasgenlib.is_empty(jrnlCode):
            errMsg =  "Bad JrnlCode '%s'" % (jrnlCode)
            logger.error(f"Severe - Locator - {errMsg} ")


        retVal = gSplitBooks.get("%s%s" % (jrnlCode, jrnlVol), None)
        logger.debug("Check Split Book: %s%s (retval: %s)" % (jrnlCode, jrnlVol, retVal))

        return retVal

    #--------------------------------------------------------------------------------
    def __exceptions(self):
        """
        Adjust locator data for exceptions;
        """
        retVal = False
        if self.noStartingPageException != True:
            if self.pgStart == 1 or self.pgStart == 0:
                if self.isSplitBook():
                    # this is an exception
                    retVal = opasDocuments.PageNumber(0)

        #print ("EXCEPTION CHECK: ", self.jrnlCode, self.pgStart)
        if self.jrnlCode == "SE":
            #import PEPJournalData
            logger.debug("Check Exception: SE data Vol %s, Pg: %s" % (self.jrnlVol, self.pgStart))
            pg, vol = PEPJournalData.processPage(self.pgStart, self.jrnlVol)
            if vol != self.jrnlVol:
                logger.info("Exception: SE Vol/PgStartdata adjusted. Vol %s/%s, Pg: %s/%s" % (self.jrnlVol, vol, self.pgStart, pg))
                self.pgStart = opasDocuments.PageNumber(pg)
                self.jrnlVol = opasDocuments.VolumeNumber(vol)
                retVal = True

        logger.debug("Locator Page number Adjusted to 0 for special case!", self.noStartingPageException)

        return retVal

    #--------------------------------------------------------------------------------
    def articleID(self, a1v4Format=0, errReturn=None, noStartingPageException=None, notFatal=False):
        """
        Return a locator string, from the current locator
        """

        # Make sure information is in standard format
        self.__standardize(oldStyleVar=a1v4Format)
        if noStartingPageException is not None:
            self.noStartingPageException = noStartingPageException

        holdFatal = self.notFatal
        self.notFatal = notFatal
        self.pgStart = pageStartException(self.jrnlCode, fullFilename=self.filename, noExceptionPageStart=self.pgStart)

        if self.validate():
            retVal = ""
            theArticleIDTuple = (	self.jrnlCode,
                                         self.jrnlVol.volID(), # formats volume and suffix
                                         self.prePrefix,
                                         self.pgStart.pgPrefix,
                                         int(self.pgStart),
                                         self.pgVar)

            if self.locatorType==0:
                # now format standard article ID dynamically each time
                try:
                    patStr = "%s.%s.%s%s%04d%s"
                    retVal = patStr % theArticleIDTuple

                except Exception as e:
                    # 9/5/2003 : removed raise from here; just log them, since this routine
                    # is used for reference locators as well as primary locators.
                    logger.error(f"""jrnlCode: '%s'
                                     jrnlVol: '%s'
                                     prePrefix: '%s'
                                     romanPrefix: '%s'
                                     pgStart: '%s'
                                     pgVar: '%s' {e}""" % theArticleIDTuple)
            else:
                # this could be a Jump to TOJ
                #retVal = "%s.%s.%02d%s" % (	self.idxNamePrefix,
                #					  		self.jrnlCode,
                #		  			  		int(self.jrnlVol),
                #		  			  		self.jrnlVol.volSuffix)
                if errReturn is None:
                    raise Exception("Bad locator type for article ID")
                else:
                    retVal = errReturn
        else: # Cannot make locator
            if errReturn is not None:
                retVal = errReturn
            else:
                #errMsg = "Cannot form locator.  %s" % self.validError
                #logger.error(f"Severe - Locator - {errMsg} ")
                #retVal = errReturn
                retVal = None

            self.notFatal = holdFatal

        return retVal

    #--------------------------------------------------------------------------------
    def decompile(self, theStr):
        """
        Decompile an existing locator from a string (parse the various components, and
        	set the current locator data to those values.)

            >>> loc = Locator()
            >>> loc.decompile("SPR.037.0079A(bT2F).xml")
            1
            >>> loc.articleID()
            'SPR.037.0079A'
            >>> loc.decompile('X:\\_PEPA1\\_PEPa1v\\_PEPCurrent\\SPR\\037.2014\\SPR.037.0079A(bT2F).xml')
            1
            >>> loc.decompile("SPR.037.0079A(bT2F).xml")
            1


        """
        self.__reset()		# Make sure to clear values!
        retVal = 1

        if theStr == "":
            raise Exception("Locator string is Empty")

        theStr = os.path.basename(theStr)

        lm = self.rgxLocator.match(theStr)
        if lm is None:
            errMsg = "Illformed Locator (decompile): '%s'" % theStr
            logger.error(f"Severe - Locator - {errMsg} ")
            self.validError = errMsg
            # break it up mannually
            jrnl, vol, pg = theStr.split(".")
            self.jrnlCode = jrnl.upper()
            if len(vol) > 3 and not vol[-1].isalpha():
                self.jrnlYear = vol
            else:
                if vol[-1].isalpha():
                    self.jrnlVol = opasDocuments.VolumeNumber(vol, volSuffix=opasgenlib.default(vol[-1], ""))
                else:
                    self.jrnlVol = opasDocuments.VolumeNumber(vol, volSuffix="")
            
        else:
            jrnl = lm.group("jrnl")
            if jrnl is not None:
                self.jrnlCode = jrnl.upper()
            self.jrnlVol = opasDocuments.VolumeNumber(lm.group("vol"), volSuffix=opasgenlib.default(lm.group("volsuffix"), ""))
            if self.jrnlVol is None:
                self.jrnlYear = lm.group("year")

            # here we need the roman prefix, because it's part of the ascii coding of locators
            romanPrefix = opasgenlib.default(lm.group("romanpre"), "")
            if romanPrefix=="R":
                roman=True
            else:
                roman=False
            self.pgStart = opasDocuments.PageNumber(opasgenlib.default(lm.group("pgst"), ""), forceRoman=roman)
            self.pgVar = opasgenlib.default(lm.group("var"), "0")

            # these are "optional" fields
            self.idxNamePrefix = opasgenlib.default(lm.group("idxname"), "")

            localIDSuffix = lm.group("localID")
            if localIDSuffix is not None:
                self.__localIDRef = opasLocalID.LocalID(localIDSuffix)
            else:
                self.__localIDRef = None

            if self.__keepContext__ != 0:
                # new prePrefix required because of odd page numbering
                self.prePrefix = opasgenlib.default(lm.group("preprefix"), "")
                self.prefix = opasgenlib.default(lm.group("pre"), "")
                self.suffix = opasgenlib.default(lm.group("post"), "")

        self.__standardize()
        # validate if it's not a TOJ locator
        if self.locatorType != 1:
            if not self.isSplitBook():
                self.validate()
            else:
                pass

        return retVal

    #--------------------------------------------------------------------------------
    def localIDType(self):
        """
        Return a single character string which represents the current type of the localID

        For example,
        	 "P"  means the LocalID is a page ID
        	 "B"  means the LocalID is a biblio ID
        etc.

        """

        if self.__localIDRef is not None:
            return self.__localIDRef.localIDType
        else:
            return None

    #--------------------------------------------------------------------------------
    def localID(self, localIDRef=None, saveLocalID=1, noArticleID=False):
        """
        Return a string that is a standard locator and/or local id (current article ID + localID)

        	If localIDRef is a full locator with article id, discard
        		the article id in localIDRef and replace it with the
        		current locator's article ID.

        	If no localIDRef is specified, the local ID of the
        		instance is returned.

        	If localIDRef is specified, this is integrated into
        		the locator, and the new full localID (current
        		article ID + localID) is returned

        The localIDRef is not saved in the instance, unless saveLocalID is set to 1

        If parameter noArticleID is specified, only the localID component
        	of the full localID is returned.

        Also: Standardizes the local ID component (no spaces are
        		allowed, so they are replaced with "_"

        """
        retVal = ""
        retValSuffix = ""
        split_book_data = opasXMLSplitBookSupport.SplitBookData(database_connection=ocd)

        if localIDRef is not None:
            # parameter Specified.
            localIDRefTemp = opasLocalID.LocalID(localIDRef)
            localIDRef = localIDRefTemp
            if saveLocalID == 1:
                self.__localIDRef = localIDRefTemp

            retValSuffix = str(localIDRefTemp)

        else: # get any stored localIDRef
            if self.__localIDRef is not None:
                localIDRef = retValSuffix = str(self.__localIDRef)

        if opasgenlib.is_empty(retValSuffix):
            # no local ID, none stored
            retVal = None
        else:
            if self.articleID(notFatal=True) is not None and noArticleID == False:
                # if we are not returning the article ID, separator is ""
                if not opasgenlib.is_empty(retValSuffix):
                    retValSep = "."
                else:
                    retValSep = ""

                # default return, unless split is called for
                retVal = self.articleID(notFatal=True) + retValSep + str(retValSuffix)

                # nested checks are to do as little as possible unless absolutely necessary
                # the worst case scenario is having to look it up in the DB and that's time consuming.
                if self.__localIDRef.localIDType[0] == "P":
                    if self.isSplitBook():
                        # lookup where this page is found!
                        newLoc = Locator(self.forceArticleID(forcePgStart=self.__localIDRef.localIDVal), art_info=self.art_info)
                        splitArticleID = split_book_data.get_splitbook_page_instance(book_code=newLoc.jrnlCode,
                                                                                     vol=str(newLoc.jrnlVol),
                                                                                     page_id=retValSuffix[1:])
                        if splitArticleID is not None:
                            retVal = Locator(splitArticleID).articleID() + retValSep + str(retValSuffix)
                        else:
                            #see if the page # is an exception!
                            newLoc = Locator(self.forceArticleID(forcePgStart=self.__localIDRef.localIDVal))
                            splitArticleID = split_book_data.get_splitbook_page_instance(book_code=newLoc.jrnlCode, vol=newLoc.jrnlVol, page_id=retValSuffix[1:])
                            logger.warning("The page lookup for %s.%s didn't find any instances" % (newLoc.baseCode(), retValSuffix))
                    #else:
                    #	print "Not split book"
            else:
                retVal = retValSuffix
                if opasgenlib.is_empty(retVal):
                    raise Exception("LocalID Only option requested, but no LocalID Available")

        return retVal

    #--------------------------------------------------------------------------------
    def sourceType(self):
        """
        Determine what type of source/reference this is in PEP.

        Return the string constant.  For example:

        	REFBOOK 			 = 	"RefBook"
        	REFBOOKSERIES 		 = 	"RefBookSeries"
        	REFJOURNALARTICLE 	 =  "RefJrnlArticle"		# (in journal)
        	REFBOOKARTICLE		 = 	"RefBookArticle"		# (article in book)
        	REFBOOKSERIESARTICLE = 	"RefBookSeriesArticle"	# (article in series book)
        	REFABSTRACT 		 = 	"RefAbstract"
        	REFSECTION 		 	 = 	"RefSection"			# Format of "section citation"


        >>> print (Locator("ZBK.001.0012").sourceType())
        RefBook
        >>> print (Locator("PI.1992.0012").sourceType())
        RefJrnlArticle

        """
        retVal = None
        if self.isBook():
            if not self.isSplitBook() or self.isMainTOC:
                if self.jrnlCode in ['IPL', 'NLP', 'SE']:
                    retVal = REFBOOKSERIES
                else:
                    retVal = REFBOOK
            else:
                if self.jrnlCode in ['IPL', 'NLP', 'SE']:
                    retVal = REFBOOKSERIESARTICLE
                else:
                    retVal = REFBOOKARTICLE
        else:
            retVal = REFJOURNALARTICLE

        return retVal

    #--------------------------------------------------------------------------------
    def splitArticleID(self, includeLocalID=1):
        """
        Return the components of the current article or local ID, in a list
        (similar to the way "split" works.)
        """
        if includeLocalID==1:
            local_id_str = str(self.localID())
            ret_val = local_id_str.split(".")
        else:
            ret_val = str(self.articleID()).split(".")
        
        return ret_val

    #--------------------------------------------------------------------------------
    def articleIDPrefix(self):
        """
        Return the jrnlcode and vol number in a locator format.
        """
        # note, while we could just rebuild this from components, this allows
        # the locator format to be build only in the articleID routine, making
        # for easier maintenance, if not a small performance penalty.
        listItems = self.articleID().split(".")
        retVal = listItems[0] + "." + listItems[1]
        return retVal

#--------------------------------------------------------------------------------
def baseOfBaseCode(baseCode):
    """
    Return the base (jrnlCode) of the baseCode

    >>> print (baseOfBaseCode("IJP.001"))
    IJP
    >>> print (baseOfBaseCode("IJP001"))
    IJP
    >>> print (baseOfBaseCode("JOAP221"))
    JOAP
    >>> print (baseOfBaseCode("ANIJP-IT.2006"))
    ANIJP-IT
    >>> print (baseOfBaseCode("anijp-it.2006"))
    ANIJP-IT

    """
    retVal = re.split("\.|[0-9]", baseCode.upper())
    return retVal[0]

#--------------------------------------------------------------------------------
def isLocator(idString):
    """
    Return True if the string is a valid locator (either a locator instance,
    or a string representation of it)
    """

    retVal = False
    if isinstance(idString, Locator): # is it a locator?
        retVal = True
    else:	# see if it's a string version of locator!
        if idString is not None:
            if not isinstance(idString, str):  # supports string and unicode Was if type(idString) != type(""):
                tStr = str(idString)
            else:
                tStr = idString

            m = Locator.rgxLocator.match(tStr)
            if m==None:
                retVal = False
            else:
                retVal = True

    return retVal


#--------------------------------------------------------------------------------
#--------------------------------------------------------------------------------
#--------------------------------------------------------------------------------
if __name__ == "__main__":
    """
    Run Locator test routines
    """

    import doctest
    aLoc = Locator()
    doctest.testmod()
    print("Tests Done")
    sys.exit()
