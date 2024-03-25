# -*- coding: UTF-8 -*-
"""
This is the original module used to provide journal data for the PEPXML processing engine used to build from KBD3 and import files.

This module adapted from a much older module used in PEPXML to compile PEP instances since 200x!

  ** Slowly being adopted to opas **
  
  Should probably be integrated into opasProductLib - some routines are perhaps done better there from the database rather than code (newer module), and from the database

"""
#
# 20071005 - Added BAFC
#
#
# To add a new journal:
#    1) Add dictionary of issues jrnl = {year:vol, year2:[vol2, vol3]}
#      if gPEPBuild == "A1v15":  # working "new journal
#
#    2) Add to all = {"AIM":aim,} to map the dictionary to the jrnlcode string for the build version if g
#
#    3) Add the journal abbreviation to the jrnlAbbr dictionary
#         jrnlAbbr = {
#            "AIM"       : "Am. Imago",
#         }
#    4) Add the full journal name
#       # Journal names used for TOJ indexes
#
#       jrnlFull = {
#            "AIM"       : "American Imago",
#
#     5) Add a regex to recognize the journal in references to  jrnlPEPPatterns
#
#       jrnlPEPPatterns = {
#            "AIM"          :       "\&AIM\;|" + patOptThe + patAmerican + patReqdSpace + patImago,
#
#     6) Include the new pattern in the list of patterns to be searched:
#
#        if gPEPBuild == "A1v15":
#           rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("DR" ), re.VERBOSE | re.IGNORECASE), "DR"   ))
#           rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("CJP" ), re.VERBOSE | re.IGNORECASE), "CJP"   ))
#
#     7) Add the journal to the ISSN table in the database
#
#     (While it might be good to put this information in the database instead, so no new build was needed to add a journal,
#      that would mean we'd need a database update sent to Aptara each time we added a journal.)

"""
Class Module to encapsulate journal metadata information used to validate PEP journal data.
"""
import sys
sys.path.append('../libs')
sys.path.append('../config')
sys.path.append('../libs/configLib')

import logging
logger = logging.getLogger(__name__)

import re
# import codecs
import opasGenSupportLib as opasgenlib
import opasDocuments

gDbg1 = 0  # details
gDbg2 = 1  # High level
import opasCentralDBLib
ocd = opasCentralDBLib.opasCentralDB()

#============================================================================================
class PEPJournalData:
    """
    Journal check and identification info.

    >>>
    """

#--------------------------------------------------------------------------------
def processPage(page, pvol=None):
    """
        setup and format the page info in a page object.

        >>> processPage("21")
        ('P0021', None)
        >>> processPage("300-321")
        ('P0300', None)
    """
    if not opasgenlib.is_empty(page):
        page = opasDocuments.PageRange(page).pgStart
        if pvol==15:
            if page>241:
                if gDbg1: print("Adjusting Page, PVol: ", page, pvol)
                pvol=16
        elif pvol==4:
            if page>=628:
                if gDbg1: print("Adjusting Page, PVol: ", page, pvol)
                pvol=5
        elif pvol==5:
            if page<628 and page > 0:
                if gDbg1: print("Adjusting Page, PVol: ", page, pvol)
                pvol=4
        page = page.format(keyword=page.LOCALID)
        #print "XML PageStart (vol: %s): %s " % (pvol, page)
    return page, pvol

#==================================================================================================
# Main Standalone (Test) Routines
#==================================================================================================
if __name__ == "__main__":

    import sys
    import doctest

    doctest.testmod()
    sys.exit()






