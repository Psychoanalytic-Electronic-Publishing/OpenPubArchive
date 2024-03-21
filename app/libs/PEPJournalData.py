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

global gJrnlData

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

    # Journal names used for TOJ indexes
    jrnlFull = {
        # new journals
        # prior journals
        "ADPSA"     : "Almanach der Psychoanalyse",
        "AIM"       : "American Imago",
        "AJP"       : "American Journal of Psychoanalysis",                                 # new
        "AJRPP"     : "Attachment: New Directions in Relational Psychoanalysis and Psychotherapy",
        "AOP"       : "Annual of Psychoanalysis",
        "ANRP"      : "Italian Psychoanalytic Annual",
        "ANIJP-CHI" : "Chinese Annual of Psychoanalysis",
        "ANIJP-DE"  : "Int. Psychoanalyse",
        "ANIJP-EL"  : "Ετήσια ελληνική έκδοση",                                            # new
        "ANIJP-IT"  : "Annata Psicoanalitica Internazionale",
        "ANIJP-FR"  : "Annee Psychanalytique Internationale",
        "ANIJP-TR"  : "Turkish Annual of Psychanal. Int.",                                  # new (just a temporary title to hold this XXX)
        "APA"       : "Journal of the American Psychoanalytic Association",
        "APM"       : "Journal Revista de Psicoanálisis",                                   # A1v14 (2014)
        "APS"       : "Journal of Applied Psychoanalytic Studies",                          # v2017 (related to IJAPS, precursor)
        "BAFC"      : "Bulletin of the Anna Freud Centre",
        "BAP"       : "Bulletin of the American Psychoanalytic Association",
        "BIP"       : "Bulletin of the International Psycho-Analytical Association",
        "BJP"       : "British Journal of Psychotherapy",
        "CFP"       : "Couple and Family Psychoanalysis",                                    # new
        "BPSIVS"    : "Boston Psychoanalytic Society and Institute",                         # new 6/25/2014
        "CJP"       : "Canadian Journal of Psychoanalysis",
        "CPS"       : "Contemporary Psychoanalysis",
        "DR"        : "DIVISION/Review: A Quarterly Psychoanalytic Forum",
        "FA"        : "Free Associations",                                                  # re-new
        "FD"        : "Fort Da",
        "GAP"       : "Gender and Psychoanalysis",
        "GW"        : "Gesammelte Werke",
        "IFP"       : "International Forum of Psychoanalysis",

        "IJFP"      : "The International Journal of Forensic Psychotherapy",
        "JPT"       : "Journal of Psychological Therapies",
        "OEDA"      : "Oedipus Annual - Greek Annual Psychoanalytic Review",
        "PPC"       : "Psychoanalysis and Psychotherapy in China",

        "IJP"       : "International Journal of Psycho-Analysis" ,
        "IJPOPEN"   : "International Journal of Psycho-Analysis Open" ,
        "IJAPS"     : "International Journal of Applied Psychoanalytic Studies",
        "IJP-ES"    : "International Journal of Psycho-Analysis en Espanol",
        "IJPSP"     : "International Journal of Psychoanalytic Self Psychology",
        "IJPSPPSC"  : "Psychoanalysis, Self, and Context",
        "IMAGO"     : "Imago",
        "IPL"       : "International Psycho-Analytical Library",
        "IRP"       : "International Review of Psycho-Analysis",
        "IZPA"      : "Internationale Zeitschrift für Psychoanalyse".encode("utf8"),
        "JAA"       : "Journal of the American Academy of Psychoanalysis and Dynamic Psychiatry",
        "JBP"       : "Jahrbuch Der Psychoanalyse",
        "JCP"       : "Journal of Clinical Psychoanalysis",
        "JCPTX"     : "Journal of Child Psychotherapy",
        "JEP"       : "Journal of European Psychoanalysis",                                 # Not yet
        "JICAP"     : "Journal of Infant, Child & Adolescent Psychotherapy",
        "JOAP"      : "Journal of Analytical Psychology",
        "JPPF"      : "Jahrbuch für psychoanalytische und psychopathologische Forschung",
        "KAPA"      : "Journal of Korean Association of Psychoanalysis",                    # A1v15
        "LU-AM"     : "Luzifer-Amor: Zeitschrift zur Geschichte der Psychoanalyse",         # A1v2019
        "MPSA"      : "Modern Psychoanalysis",
        "NP"        : "Neuropsychoanalysis",
        "NLPX"      : "New Library of Psycho-Analysis",
        "NLP"       : "New Library of Psychoanalysis",
        "NYPSIVS"   : "New York Psychoanalytic Society & Institute",
        "OAJPSI"    : "Offsite American Journal of Psychiatry",
        "OFFSITE"   : "Offsite Articles",
        "OPUS"      : "Organizational and Social Dynamics",                                 # new
        "PAH"       : "Psychoanalysis and History",
        "PAQ"       : "Psychoanalytic Quarterly",
        "PB"        : "Psychoanalytic Books",
        "PCAS"      : "Psychoanalysis Culture and Society",
        "PCS"       : "Psychoanalysis and Contemporary Science",
        "PCT"       : "Psychoanalysis and Contemporary Thought",
        "PD"        : "Psychoanalytic Dialogues",
        "PDPSY"     : "Psychodynamic Psychiatry",
        "PI"        : "Psychoanalytic Inquiry",
        "PPERSP"    : "Psychoanalytic Perspectives",
        "PSAR"      : "Psychoanalytic Review",
        "PSC"       : "Psychoanalytic Study of the Child",
        "PSP"       : "Progress in Self Psychology",
        "PSU"       : "Psicoterapia e Scienze Umane",
        "PSW"       : "Psychoanalytic Social Work",
        "PPSY"      : "Psychoanalytic Psychology",
        "PPTX"      : "Psychoanalytic Psychotherapy",
        "PSABEW"    : "Psychoanalytische Bewegung",
        "PSYCHE"    : "Psyche",
        "PY"        : "Psikanaliz Yazıları",
        "RBP"       : "Revue Belge de Psychanalyse",
        "REVAPA"    : "Revista de psicoanálisis",
        "RFP"       : "Revue française de psychanalyse",
        "RIP"       : "Rivista Italiana di Psicoanalisi",
        "RPP-CS"    : "Revue psychoanalytická psychoterapie",
        "RRP"       : "Revue Roumaine de Psychoanalyse",
        "RPSA"      : "Rivista di Psicoanalisi",
        "SE"        : "Standard Edition",
        "SEX"       : "The Standard Edition of the Complete Psychological Works of Sigmund Freud",
        "SGS"       : "Studies in Gender and Sexuality",
        "SPR"       : "Scandinavian Psychoanalytic Review",
        "WMK"       : "Writings of Melanie Klein (book series)",
        "TVPA"      : "Tijdschrift voor Psychoanalyse",
        "ZBK"       : "Classic Books",
        "ZBPA"      : "Zentralblatt für Psychoanalyse",
        "ZPSAP"     : 'Zeitschrift f\xfcr psychoanalytische P\xe4dagogik'.encode("utf-8"),
        # video streams
        "AFCVS"     : "Anna Freud Center Video Collection",
        "PCVS"      : "Philoctetes Center Video Collection",
        "PEPVS"     : "PEP Videostream",
        "PEPGRANTVS" : "PEP Video Grants",
        "PEPTOPAUTHVS" : "PEP/UCL Top Authors Project",
        "UCLVS"     : "University College of London Video Collection",
        "IPSAVS"    : "Institute of Psychoanalysis Video Collection",
        "IJPVS"     : "International Journal of Psychoanalysis Video Collection",
        "SFCPVS"    : "San Francisco Center for Psychoanalysis Video Collection",
        "SPIVS"     : "Società Psicoanalitica Italiana  Video Collection"
    }

    #--------------------------------------------------------------------------------
    def getJournalFull(self, sourceCode):
        """
        Return the official PEP source title for this code.
        """
        retVal = self.jrnlFull.get(sourceCode)
        return retVal

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






