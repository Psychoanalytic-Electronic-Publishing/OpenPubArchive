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

    #as checked in http://journalseek.net/cgi-bin/journalseek/
    # Note though they had:
    #           Int Rev Psycho Anal
    #           J Am Acad Psychoanal Dyn Psychiatr
    #               (Note also I found we had the wrong ISSN for this journal!)
    #               Info at: http://www.guilford.com/cgi-bin/cartscript.cgi?page=pr/jnap.htm&dir=periodicals/per_psych&cart_id=
    #           No abbrev listed for Neuro-Psychoanalysis
    #           No abbrev listed for Psychoanalytic Dialogues
    #           PSC had wrong ISSN!
    #           Had Psychoanal. Stud. Child, but that's stupid to substitute the y for a .
    #
    # 2009-12-03 Removed hyphen from Neuropsychoanalysis per request in Sept
    jrnlAbbr = {
        # new journals
        # prior journals
        "ADPSA"     : "Almanach d. PsA.",
        "AIM"       : "Am. Imago",
        "AJP"       : "Am. J. Psychoanal.",
        "AJRPP"     : "Att: New Dir. Relat. Psychoanal. Psychother.",                  # found instances and not abbreviated, at least in AJRPP self citations.
        "ANRP"      : "Ital. Psychoanal. Annu.",
        "ANIJP-CHI" : "Chinese Ann. Psychoanal.",
        "ANIJP-DE"  : "Int. Psychoanalyse",
        "ANIJP-EL"  : "Ετήσια ελληνική έκδοση",                                           # new
        "ANIJP-FR"  : "L'Annee Psychanal. Int.",
        "ANIJP-IT"  : "L'Annata Psicoanal. Int.",
        "ANIJP-TR"  : "Ulusl. Psikanaliz Yıll.",                                           # from Nilüfer Erdem email 2013-09-23
        "AOP"       : "Annu. Psychoanal.",
        "APA"       : "J. Amer. Psychoanal. Assn.",
        "APM"       : "Rev. Psicoanál. Asoc. Psico. Madrid",                              # A1v14 (2014) from journal form
        "APS"       : "J. Appl. Psychoanal. Stud.",
        "BAP"       : "Bul. Amer. Psychoanal. Assn.",
        "BAFC"      : "Bul. Anna Freud Centre",
        "BIP"       : "Bul. Int. Psychoanal. Assn.",
        "BJP"       : "Brit. J. Psychother.",
        "BPSIVS"    : "Bos. Psychoanal. Soc. Inst.",                                       # new 6/25/2014
        "CFP"       : "Cpl. Fam. Psychoanal.",                                             # not seen, made up from other uses of the terms
        "CJP"       : "Can. J. Psychoanal.",
        "CPS"       : "Contemp. Psychoanal.",
        "DR"        : "DIVISION/Rev.",
        "FA"        : "Free Associations",
        "FD"        : "Fort Da",
        "GAP"       : "Gender and Psychoanal.",
        "GW"        : "Gesammelte Werke",
        "IFP"       : "Int. Forum Psychoanal.",
        "IJP"       : "Int. J. Psychoanal." ,
        "IJPOPEN"   : "Int. J. Psychoanal. Open" ,
        "IJAPS"     : "Int. J. Appl. Psychoanal. Stud.",
        "IJP-ES"    : "Int. J. Psychoanal. Es.",
        "IJPSP"     : "Int. J. Psychoanal. Self Psychol.",
        "IJPSPPSC"  : "Psychonal. Self Cxt.",
        "IMAGO"     : "Imago",
        "IPL"       : "Int. Psycho-Anal. Lib. ",
        "IRP"       : "Int. Rev. Psycho-Anal.",
        "IZPA"      : "Int. Z. Psychoanal.",
        "JAA"       : "J. Am. Acad. Psychoanal. Dyn. Psychiatr.",
        "JBP"       : "Jahrb. Psychoanal.",
        "JCP"       : "J. Clin. Psychoanal.",
        "JCPTX"     : "J. Child Psychother.",
        "JEP"       : "J. Eur. Psychoanal.",
        "JICAP"     : "J. Infant Child Adolesc. Psychother.",
        "JOAP"      : "J. Anal. Psychol.",
        "JPPF"      : "Jahrb. Psychoanalyt. Psych. Forsch.",

        "IJFP"      : "Int. J. Forens. Psychoanal.",
        "JPT"       : "J. Psych. Ther.",
        "OEDA"      : "Oed. Ann.",
        "PPC"       : "Psychoanal. Psychother. China",

        "KAPA"      : "J. Korean Assoc. Psychoanal.",
        "LU-AM"     : "Luzifer-Amor",
        "MPSA"      : "Mod. Psychoanal.",
        "NP"        : "Neurpsychoanalysis",
        "NLPX"      : "New Lib. of Psycho-Anal.",
        "NLP"       : "New Library of Psychoanalysis",
        "NYPSIVS"   : "N.Y. Psychoanal. Soc. Inst.",
        "OAJPSI"    : "Offsite Amer. J. Psi",
        "OFFSITE"   : "Offsite Articles",
        "OPUS"      : "Organ. Soc. Dyn.",
        "PAQ"       : "Psychoanal. Q.",
        "PAH"       : "Psychoanal. Hist.",
        "PB"        : "Psa. Books",
        "PCT"       : "Psychoanal. Contemp Thought",
        "PCAS"      : "Psychoanal. Cult. Soc.",
        "PCS"       : "Psychoanal. Contemp Sci",
        "PD"        : "Psychoanal. Dial.",
        "PDPSY"     : "Psychodyn. Psi.",
        "PI"        : "Psychoanal. Inq.",
        "PPERSP"    : "Psychoanal. Persp.",
        "PPSY"      : "Psychoanal. Psychol.",
        "PPTX"      : "Psychoanal. Psychother.",
        "PSABEW"    : "Psychoanal. Bew.",
        "PSAR"      : "Psychoanal. Rev.",
        "PSC"       : "Psychoanal. Study Child",
        "PSP"       : "Progr. Self Psychol.",
        "PSYCHE"    : "Psyche",
        "PSU"       : "Psicoter. Sci. Um.",
        "PSW"       : "Psychoanal. Soc. Work",
        "PY"        : "Psk. Yaz.",
        "RBP"       : "Rev. Belg. Psychanal.",
        "REVAPA"    : "Rev. Psicoanal.",
        "RFP"       : "Rev. Fr. Psychanal.",
        "RIP"       : "Rivista Italian Psicoanal.",
        "RPP-CS"    : "Rev. psychoanal. psychoter.",
        "RPSA"      : "Rivista Psicoanal.",
        "RRP"       : "Rom. J. Psychoanal.",
        "SE"        : "Standard Edition",
        "SGS"       : "Stud. Gend. Sex.",
        "SPR"       : "Scand. Psychoanal. Rev.",
        "TVPA"      : "Tijdschr. Psychoanal.",
        "ZBPA"      : "Zbl. Psyca",
        "ZBK"       : "Classic Books",
        "ZPSAP"     : "Z. Psychoanalyt Pädagogik",
        # videostreams
        "AFCVS"     : "A. Freud Ctr. Vid. Coll.",
        "PCVS"      : "Ph. Ctr. Vid. Coll.",
        "PEPVS"     : "PEP Vid. Coll.",
        "PEPGRANTVS" : "PEP Grant Proj. Vid. Coll.",
        "PEPTOPAUTHVS" : "PEP/UCL Top Aut. Proj. Vid Coll.",
        "UCLVS"     : "U. Coll. London Vid. Coll.",
        "IPSAVS"    : "Inst. Psychoanal. Vid. Coll.",
        "IJPVS"     : "Int. J. Psychoanal. Vid. Coll..",
        "SFCPVS"    : "San Franc. Cntr for Psa. Vid. Coll.",
        "SPIVS"     : "Soc. Psi It. Vid. Coll."
    }

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


    # Journal codes which are returned from Patterns which are false positives, these are not processed as PEP Journals
    notInPEPList = ["IPP", "XPS", "FAP"] #, "MPSP"]
    nonEnglishJournals = ["APM", "JBP", "RBP", "RPSA", "RIP", "PSYCHE", "GW", "LU-AM", "ANIJP-CHI", "ANIJP-FR", "ANIJP-DE", "ANIJP-IT", "ANIJP-TR", "ANIJP-RU", "ANIJP-ES", "ANIJP-PT", "IJP-ES", "REVAPA"]


    # PEP journal patterns to use
    patOptOf = "(of\s+)?"
    patOptThe = "(The\s+)?"
    patOptIn = "(in\s*?)?"
    #patOptAnd = "(and\s+)?"
    patOptAnd = "((\&(amp;)?|and)\s*?)?"
    patReqdSpace = "\s+?"
    patOptSpace = "\s*?"
    patOptAndThe = "((\&\s+|and\s+)?the\s+)?"
    patAttachment = r"Attach(\b|\.|ment)"
    patBooks = "Books"
    patOptColon = "(\s?\:\s?)?"
    patDirections = "Directions"
    patIn = "in"
    patRelational = r"Relat(\b|\.|ional)"
    patOptOfThe = "(of\s+(the\s+)?)?"
    patOptDash = "(\s?\-\s?)?"              # optional dash with optional spaces (if the dash is there)
    patAcademy = "Acad(\.|emy)?"
    patAdolescent = r"Adolesc(\b|\.|ent)"
    patAmerican = r"(Am(er)?(\b|\.|ican))"
    patAnalytical = "(([Aa]nal)(ytic|ytical|yt)?(\.)?)"
    patAnnual = r"(Ann(\b|\.|u\.|ual))"
    patApplied = "Applied"
    patAssociation =  "(Ass(oc|n)?\.?|Association)"
    patAssociations =  "(Ass(oc|n)?s\.?|Associations)"
    patBulletin = r"Bul(l)?(n)?(\.|etin)"
    patBritish = r"Brit(.|ish)"
    patChinese = r"Chin(.|ese)"
    patChina = r"China"
    patChild = r"(Ch((ild)?(\b|\.)))"
    patClinical = r"C(\.|lin(\b|\.|ical)?)?"
    patContemporary = "\s*Contemp(\.?|orary)"
    patContext = "\s*Cont(\.|ext)"
    patCouple = r"((Coup(le)?|Cpl)(\b|\.))"
    patCulture = r"C(\.|ult(\b|\.|ure)?)?"
    patDe = r"(de)?"
    patDialogues = r"((Dial|Dialogues|Dialog)(\.|\b))"
    patDynamic = r"(Dyn(amics?)?(\.|\b)?)"
    patFamily = "Fam(\.|ily)"
    patForum = "F(\.|orum)"
    patForensic = "F(\.|orensic|orens\.?)"
    patGender = "Gend(\.|er)?"
    patHistory = "His(\.|t\.?|tory)"
    patImago = r"Im(\b|\.|ago\.?)"
    patInternational = r"(I\.|Int(\b|l\.?|\.|ern(at)?(\.?|ional\.?)))" # Added I. 2012-03-15 variation seen in anijp-de
    patInquiry = "(Inq\.|Inquiry)"
    patInfant = "Infant"
    patJournal = r"(J(\b|\.|our\.?(nal)?))"
    patJahrBuch = r"Jahr\.?(b\.?(uch)?)?"
    patJournalOf = patJournal + "(\s+of\s*?)?"
    patJournalForThe = r"((J(\b|\.|our\.?(nal)?))(\s+for\s+the)?)?"
    patJPCS = "(\s+JPCS:\s*)?"
    patModern = r"Mod(\b|\.|ern)"
    patNew = "New"
    patOrganizat = r"Org(an|aniz|anization(al)?|anis|anisation(al)?)(\.|\b)"
    patOpen = r"(Open)"
    patOedipus = "Oed(\b|\.|ipus)" # 2022
    patProgress = r"Prog(r|ress)(\.|\b)"
    patPsychiatry =  "(Psychiatry)"
    patPsychoanalysis = "(Psa\.?|Psycho(\-?)(an\.?|anal\.?|analys(is)?))" # made ending required, otherwise it trips on similar names
    patPsychoanalytic = "(Psa.?|Psycho(\-?)anal(\.|ytic)?)" # use psa for this too, many references to psa rev.
    patPsychoanalytical = "Psycho(\-?)anal(\.|ytic|ytical)?"
    patPsychodynamic = r"Psychodyn(\b|\.|am\.|amic)"
    patPsychology = r"Psych(\b|ol|oi|ology)\.?"
    patPsychological = r"Psych(\b|ol|ological)\.?" # 2022
    patPerspectives = r"Pers(\b|p|pect|pectives?)\.?"
    patPsychotherapy = r"Psychother(\b|\.|apy)"
    patQuarterly = r"(Q(((\b|uart?)\.?)|erly))"
    patReview = r"(Rev(\b|\.|iew)|R(\b|\.))"
    patRevue = r"(Rev(\b|\.|ue)|R(\b|\.))"
    patRomanian = "Rom(anian|\.)"
    patRevista = r"Rev(\.|ista)"
    patRivista = r"Riv(\.|ista)"
    patScience = r"(Sci(\b|\.|ence))"
    patSocial = r"(Soc(\b|\.|ial))"
    patSociety = r"(Soc(\b|\.|iety))"
    patSelf = "Self\s+"
    patSelfPsychology = "self(\-?\s*?)" + patPsychology
    patSexuality = r"Sex(ual(ity)?)?(\b|\.)"
    patStudy = r"St(ud(y|ies)?)?(\b|\.)"
    patTherapies = "Ther(\.|ap\.?|apies)" # 2022
    patWork = "Work"


    # international
    patDer = "(der\s+)?"  # need the space here, since if der is missing, there won't be a space following, just the preceeding
    patEn = r"(en)"
    patEspanol = "(Espa[ñn]ol)"
    patFrancaise = "((fran[çc](aise)?(\.)?)|(fr(\.)))"
    patFur = "(für|fur|f\.?)"
    patGesch = "Geschichte"
    patInternationale = r"(I\.|Int(\b|l\.?|\.|ernat(\.?|ionale?\.?)))"
    patItalian = r"It(al(ian)?)?\.?"
    patCanadian = "Can(ad(ian|\.)|\.)?"
    patScandinavian = "(Scand(\.|in|inavian|anavian))"
    patEuropean = "(E(\.|ur(\.|opean)?)?)"
    patPsicoanalisi = "(Psa|Psicoanal(\.|isi))"
    patPsicoanalisis = "(Psa|psi|psicoan[áa]l(\.|isis))"
    patZentrablatt = "(Zentralblatt?|Zbl\.?|Zentrablatt)"
    patPadagogik = r"(Pädagogik|Päd|Pad)"
    patPsychoanalytika = "(Psa\.?|Psycho(\-?)(an\.?|anal\.?|analyt\.?(ika)?))" # czech - rpp-cs
    patPsychoanalytische = "(Psa\.?|Psycho(\-?)(an\.?|anal\.?|analyt\.?(ische)?))" # czech - rpp-cs
    patPsychoanalyse = "(Psa\.?|Psyca\.?|Psycho(\-?)(an\.?|anal\.?)|Psychoanalyse)"
    patPsychanalyse = "(Psa\.?|Psyca\.?|Psych(\-?)(an\.?|anal\.?)|Psychanalyse)"
    patPsychoterapie = r"psychoter(\b|\.|apie)" # czech rpp-cs
    patTVPATif = "Tijdschr(\.|ift)"
    patTVPAVoor = "voor"
    patZeit = "Zeitschrift\s+"
    patZeitschrift = r"(Z\.|Zeit(schrift|\.?)|Ztschr\.?|Ztscht\.?|Zeitschr\.?|Zs\.?|Zschr\.?)"
    patZur = "zur"


    jrnlPEPPatterns = {
        "AIM"          :       "\&AIM\;|" + patOptThe + patAmerican + patReqdSpace + patImago,
        "AJP"          :       "\&AJP\;|" + patAmerican + patReqdSpace + patJournal + patReqdSpace + patOptOf + patPsychoanalysis,
        "AJRPP"        :       "\&AJRPP\;|" + patAttachment + patOptColon + patNew + patReqdSpace + patDirections + patReqdSpace + patOptIn + patReqdSpace + patPsychotherapy + patReqdSpace + patOptAnd + patReqdSpace + patRelational + patReqdSpace + patPsychoanalysis + "(" + patReqdSpace + patJournal + ")?",
        "ANIJP-DE"     :       "\&ANIJPDE\;|" + patInternational + "Ausgewählte\s+Beiträge\s+aus\s+dem\s*\"?" + patInternational + patJournal + patOptOf + patPsychoanalysis + "\"?",
        "ANIJP-EL"     :       "\&ANIJPEL\;", # placeholder for full data XXX
        "ANIJP-CHI"    :       "\&ANIJPCHI\;|" + patChinese + patReqdSpace + patAnnual + patReqdSpace + patOptOf + patPsychoanalysis,
        "ANIJP-FR"     :       "\&ANIJPFR\;|(L\')?Ann(\xe9|e)e\s+Psychanalytique\s+Int(ern)?(.|ationale)",
        "ANIJP-IT"     :       "\&ANIJPIT\;|(L\')?Ann(ata)\s+Psicoan(alitica)\s+Int(ern)?(.|azionale)",
        "ANIJP-TR"     :       "\&ANIJPTR\;|Uluslararası\s+Psikanaliz\s+Yıllı(ğ|g)ı",
        "ANRP"         :       "\&ANRP\;|" + patItalian + patReqdSpace + patPsychoanalytic + patReqdSpace + patAnnual,
        "AOP"          :       "\&AOP\;|(The\s+?)?" + patAnnual + patReqdSpace + patOptOf + patPsychoanalysis,
        "APA"          :       "\&APA\;|JAPA|(" + patJournal + patReqdSpace + patOptOf + patOptThe + ")?" + patAmerican + patReqdSpace + patPsychoanalytic + patReqdSpace + patAssociation,
        "APM"          :       "\&APM\;|" + patRevista + patReqdSpace + patDe + patReqdSpace + "Psi(coan(a|á)lisis)?",
        "APS"          :       "\&APS\;|" + patJournalOf + patReqdSpace + patApplied + patReqdSpace + patPsychoanalytic + patReqdSpace + patStudy,
        "BAFC"         :       "\&BAFC\;|" + patBulletin + patReqdSpace + patOptOfThe + "(A(nna|\.)\s+?Freud\s+?(Cen(tre|\.|ter))|(Hamp(\.|stead)\s+Clin(\.|ic)))",
        "BAP"          :       "\&BAP\;|" + patBulletin + patReqdSpace + patOptOfThe + patAmerican + patReqdSpace + patPsychoanalytic + patReqdSpace + patAssociation,
        "BIP"          :       "\&BIP\;|" + patBulletin + patReqdSpace + patOptOfThe + patInternational + patReqdSpace + patPsychoanalytical + patReqdSpace + patAssociation,
        "BJP"          :       "\&BJP\;|" + patBritish + patReqdSpace + patJournal + patReqdSpace + patOptOf + patPsychotherapy,
        "CFP"          :       "\&CFP\;|" + patCouple + patReqdSpace + patOptAnd + patFamily + patReqdSpace + patPsychoanalysis,
        "CJP"          :       "\&CJP\;|" + patCanadian + patReqdSpace + patJournalOf + patReqdSpace + patPsychoanalysis,
        "CPS"          :       "\&CPS\;|" + patContemporary + patReqdSpace + patPsychoanalysis,
        "FA"           :       "\&FA\;|Free" + patReqdSpace + patAssociations,
        "DR"           :       "Div(\.|ision)?" + patReqdSpace + "Rev(.?|iew)?",
        "FD"           :       "\&FD\;|Fort\s+?Da\.?",
        "GAP"          :       "\&GAP\;|" + patGender + patReqdSpace + patOptAnd + patPsychoanalysis,
        "GW"           :       r"(?P<jrnlname>\&GW\;|GW|(G\.W\.)|Ges(\.|ammelte)\s+W(\.|erke))",
        "IFP"          :       "\&IFP\;|" + patInternational + patReqdSpace + patForum + patReqdSpace + patOptOf + patPsychoanalysis,
        # added version without spaces to IJP since it seems to be a mode of citing in ANIJP-DE
        "IJP"          :       "\&IJP\;|IJP|I\.J\.\s*(P\.(A\.)?|(Psycho\-?anal(.|ysis)?))|" + patOptThe + patInternational + patReqdSpace + patJournal + patReqdSpace + patOptOf + patPsychoanalysis + "(?!(\s+?Self))",
        "IJAPS"        :       "\&IJAPS\;|" + patInternational + patReqdSpace + patJournalOf + patReqdSpace + patApplied + patReqdSpace + patPsychoanalytic + patReqdSpace + patStudy,
        "IJPOPEN"      :       "\&IJP\;|" + patOptThe + patInternational + patReqdSpace + patJournal + patReqdSpace + patOptOf + patPsychoanalysis + patOpen,
        "IJPES"        :       "\&IJP\;|IJP|I\.J\.\s*(P\.(A\.)?|(Psycho\-?anal(.|ysis)?))|" + patOptThe + patInternational + patReqdSpace + patJournal + patReqdSpace + patOptOf + patPsychoanalysis + patEn + patEspanol,
        "IJPSP"        :       "\&IJPSP\;|" + patInternational + patReqdSpace + patJournalOf + patReqdSpace + patPsychoanalytic + patReqdSpace + patSelfPsychology,
        "IJPSPPSC"     :       "\&IJPSPPSC\;|" + patPsychoanalysis + patReqdSpace + patSelf + patReqdSpace + patOptAnd + patReqdSpace + patContext,
        "IMAGO"        :       "\&IMAGO\;|" + patImago,
        "IRP"          :       "\&IRP\;|" + patInternational + patReqdSpace + patReview + patReqdSpace + patOptOf + patPsychoanalysis,

        "IZPA"         :       "&IZPA\;|I\.?Z\.?f\.?[ ]*Ps\.?|" + "(" + patInternationale + patReqdSpace + ")?" + patZeitschrift + patReqdSpace + patFur + patReqdSpace + patPsychoanalyse + "$",

        "JAA"          :       "\&JAA\;|" + patJournalOf + patReqdSpace + patOptThe + patAmerican + patReqdSpace + patAcademy + patReqdSpace + patOptOf + patPsychoanalysis + "(" + patReqdSpace + patOptAnd + patDynamic + patReqdSpace + patPsychiatry + ")?",
        "JCP"          :       "\&JCP\;|" + patJournalOf + patReqdSpace + patClinical + patReqdSpace + patPsychoanalysis,
        "JCPTX"        :       "\&JCPTX\;|" + patJournal + patReqdSpace + patOptOf + patChild + patReqdSpace + patPsychotherapy,
        "JBP"          :       "\&JBP\;|" + patJahrBuch + patReqdSpace + patDer + patPsychoanalyse,
        "JEP"          :       "\&JEP\;|" + patJournalOf + patReqdSpace + patEuropean + patReqdSpace + patPsychoanalysis,
        "JICAP"        :       "\&JICAP\;|" + patJournal + patReqdSpace + patOptOf + patInfant + "(\s*?,?\s+?)" + patChild + ",?" + patOptSpace + patOptAnd + patAdolescent + patReqdSpace + patPsychotherapy,
        "JOAP"         :       "\&JOAP\;|J\.?A\.?P\.?|" + patJournal + patReqdSpace + patOptOf + patAnalytical + patReqdSpace + patPsychology,
        #Luzifer-Amor: Zeitschrift zur Geschichte der Psychoanalyse
        #Luzifer-Amor: Zeitschrift zur Geschichte der Psychoanalyse
        "LU-AM"        :       "\&LUAM\;|Luzifer[\-\s]Amor" + "(" + "\s*[\-\:\.]?\s*" + patZeitschrift + patReqdSpace + patZur + patReqdSpace + patGesch + patReqdSpace + patDer + patPsychoanalyse + ")?",
        "MPSA"         :       "\&MPSA\;|" + patModern + patReqdSpace + patPsychoanalysis,
        "NP"           :       "\&J?NP\;|(" + patJournalOf + ")?Neuro(\-?)\s*"  + patPsychoanalysis,
        "OPUS"         :       "\&OPUS\;|" + patOrganizat + patReqdSpace + patOptAnd + patSocial + patReqdSpace + patDynamic,
        "PAH"          :       "\&PAH\;|" + patPsychoanalysis + patReqdSpace + patOptAnd + patHistory,
        "PAQ"          :       "\&PAQ\;|This\s+Quarterly|" + patPsychoanalytic + patReqdSpace + patQuarterly + "(?!(\s*\,?\s+Inc\.|\s+Press))",
        "PB"           :       "\&PB\;|" + patPsychoanalytic + patReqdSpace + patBooks,
        "PCS"          :       "\&PCS\;|" + patPsychoanalysis + patReqdSpace + patOptAnd + patContemporary + patReqdSpace + patScience,

        #  new for 2022
        "PCAS"         :       "\&PCAS\;|" + patJournalForThe + patOptSpace + patPsychoanalysis + patOptSpace + patOptOf + patOptSpace + patCulture + patOptSpace + patOptAnd + patReqdSpace + patSociety,
        "IJFP"         :       "\&IJFP\;|" + patInternational + patReqdSpace + patJournalOf + patReqdSpace + patForensic + patReqdSpace + patPsychotherapy,
        "JPT"          :       "\&JPT\;|" + patJournalOf + patReqdSpace + patPsychological + patReqdSpace + patTherapies,
        "OAJPSI"       :       "\&OAJPSI;|" + patAmerican + patJournalOf + patPsychiatry,
        "OEDA"         :       "\&OEDA\;|" + patOedipus + patReqdSpace + patAnnual,
        "PPC"          :       "\&PPC\;|" + patPsychoanalysis + patReqdSpace + patOptAnd + patReqdSpace + patPsychotherapy + patReqdSpace + patIn + patReqdSpace + patChina,

        "PCT"          :       "\&PCT\;|" + patPsychoanalysis + patReqdSpace + patOptAnd + patContemporary + patReqdSpace + "(Thought)",
        "PD"           :       "\&PD\;|" + patPsychoanalytic + patReqdSpace + patDialogues,
        "PDPSY"        :       "\&PDPSY\;|" + patPsychodynamic + patReqdSpace + patPsychiatry,
        "PI"           :       "\&PI\;|" + patPsychoanalytic + patReqdSpace + patInquiry,
        "PPERSP"       :       "\&PPERSP\;|" + patPsychoanalytic + patReqdSpace + patPerspectives,
        "PPSY"         :       "\&PPSY\;|" + patPsychoanalytic + patReqdSpace + patPsychology,
        "PPTX"         :       "\&PPTX\;|" + patPsychoanalytic + patReqdSpace + patPsychotherapy,
        "PSAR"         :       "\&PSAR\;|" + patOptThe + "(" + patPsychoanalysis + patOptAndThe + ")?" + patOptSpace + patPsychoanalytic + patReqdSpace + patReview,
        "PSC"          :       "\&PSC\;|" + patOptThe + patPsychoanalytic + patReqdSpace + patStudy + patReqdSpace + patOptOf + patOptThe + "Child",
        "PSP"          :       "\&PSP\;|" + patProgress + patReqdSpace + patOptIn + patSelfPsychology,
        "PSYCHE"       :       "\&PSYCHE\;|Psyche" + "(" + patOptDash + "Z\.?" + patReqdSpace + patPsychoanalyse + ")?",
        "PSU"          :       "\&PSU\;|" + "Psicoterapia" + patReqdSpace + "e" + "Scienze" + patReqdSpace + "Umane",
        "PSW"          :       "\&PSW\;|" + patPsychoanalytic + patReqdSpace + patSocial + patReqdSpace + patWork,
        "PY"           :       "Psikanaliz|Psk."+ patReqdSpace + "Yazıları|Yaz.",
        "RBP"          :       "\&RBP\;|R(ev(ue)?)?(\.\s*|\s+)B(elg(e?))?(\.)?\s+(de\s+)?Psych(\.|an(\.|al\.|alyse))",
        "REVAPA"       :       r"\&REVAPA\;|Riv(\.|ista)\s+(de\s+)?" + patPsicoanalisis,
        "RFP"          :       r"\&RFP\;|" + "(Rev(\\b|\\.|ue)?|R(\\b|\\.?))\\s+?((fran(\\xe7|c)(aise)?(\\.)?|fr(\\.?))\\s+?)" + "(de\\s+)?" + "(Psa\.?|Psyca\.?|Psych(\-?)(an\.?|anal\.?|analyse))",
        "RPP-CS"       :       "\&RPP-CS\;|" + patRevue + patReqdSpace + patPsychoanalytika + patReqdSpace + patPsychoterapie,
        "RRP"          :       "\&RRP\;|RRP|(Rev(\.|ue)?\s+Roum(\.|aine)\s+de\s+Psychanal(\.|yse))|" + patRomanian + patReqdSpace + patJournal + patReqdSpace + patPsychoanalysis,
        "RIP"          :       "\&RIP\;|Riv(ista|\.)\s+It(aliana)\s+(di\s+)?" + patPsicoanalisi,
        "RPSA"         :       r"\&RPSA\;|Riv(\.|ista)\s+(di\s+)?" + patPsicoanalisi,
        "SE"           :       r"(?P<jrnlname>\&SE\;|(S\.E\.)|.*(The)?\s*(Std\.|((Stand(\.|ard)))\s+Ed(\.|ition|it\.))(\s*(of\s+)?(the\s+)?complete\s+psychological\s+works\s+(of\s+)?(Sigmund\s+)?Freud)?,?|((the\s+)?complete\s+psychological\s+works\s+of\s+Sigmund\s+Freud))",
        "SGS"          :       "\&SGS\;|" + patStudy + patReqdSpace + patOptIn + patGender + patReqdSpace + patOptAnd + patSexuality,
        "SPR"          :       "\&SPR\;|" + patScandinavian + patReqdSpace + patPsychoanalytic + patReqdSpace + patReview,
        "TVPA"         :       "\&TVPA\;|" + patTVPATif + patReqdSpace + patTVPAVoor + patReqdSpace + patPsychoanalyse,
        "ZBPA"         :       "\&ZBPA\;|" + patZentrablatt + patReqdSpace + "(" + patFur + patReqdSpace + ")?" + patPsychoanalyse,
        "ZPSAP"        :       "\&ZPSAP\;|" + patZeitschrift + patReqdSpace + "(" + patFur + patReqdSpace + ")?" + patPsychoanalytische + patReqdSpace + patPadagogik,

        # Bad Journal Patterns - Catch and don't link
        "FAP"          :       "\&FA\;|Free\s+?Ass(\.|oc\.|ociations?)\sPress",
        "IPP"          :       "\&IPP\;|Int(\.|ernational)\s+J(\.|ournal)\s+(of\s+)?Psycho(\-?)anal(\.|ysis)\s+?(\&\s+?|and\s+?)?Psycho(\-?)ther(\.|apy)",
        "XPS"          :       "\&XPS\;|Psycho(\.|somatic)\s+Med(\.|icine)\s+(and)?\s+Contemp(\.|orary)\s+" + patPsychoanalysis,

    }

    #print jrnlPEPPatterns.get("ANRP"   )
    #rgxSEPat = re.compile(jrnlPEPPatterns.get("SE"), re.VERBOSE | re.IGNORECASE)
    rgxSEPat = re.compile(jrnlPEPPatterns.get("SE"), re.VERBOSE | re.IGNORECASE)
    SEPat2 = r"\bSE\b|S\.\s?E\.|Standard Ed(ition|.)" # Keep sep so not so many false positives.  Use only on XML areas
    GWPat2 = r"\bGW\b|G\.\s?W\." # Keep sep so not so many false positives.  Use only on XML areas
    
    SEVolPrefix = "("+jrnlPEPPatterns.get("SE")+"|"+SEPat2+")"
    SEVolNumPre = """,?\s*(<v>)?(vol\.?)?\s*"""
    SEVolNumPreVolReq = """,?\s*(<v>)?(vol\.?)\s+"""
    romanNumber = r"""((\s|:)
        I{1,3}V?
        |VI{0,3}
        |IX
        |XI{0,3}V?
        |XVI{0,3}
        |XI?X
        |XXI{0,3}V?
        )\b
        """
    SEVolNum = r"""
        \b(?P<bvol>(4(/|\-|,)5
        |1[0-9]
        |2[0-4]
        |[1-9]
        |%s
        )\b
        )""" % romanNumber

    SEVolColonPage = """:\s*(?P<bpgs>[1-9][0-9]{0,2})"""

    SEPgs = r"""(?P<extra>(\D*?London:?\s+Hogarth\s+Press)?\D*?\s*,?\s*(p{0,2}\.?,?\s*))?:?\s*(?P<bpgs>[1-9][0-9]{0,2}){0,1}\b"""

    rgxSEPat2 = re.compile(SEPat2)
    rgxGWPat2 = re.compile(GWPat2)

    rgxSEFalsePositives = re.compile("Jelliffe", re.VERBOSE | re.IGNORECASE)

    #rgxSEVol =     re.compile(rgxVolPrefix+rgxVol, re.VERBOSE | re.IGNORECASE)
    rgxSEVol =  re.compile(SEVolPrefix+SEVolNumPre+SEVolNum+SEPgs, re.VERBOSE | re.IGNORECASE)
    rgxSEVol2 = re.compile(SEVolNumPreVolReq+SEVolNum+SEPgs, re.VERBOSE | re.IGNORECASE)
    rgxSEVol3 = re.compile("(<i>)?\s*"+SEVolNum+"\s*(</i>)?", re.VERBOSE | re.IGNORECASE)
    rgxSEVolPageOnly = re.compile(SEVolNum+SEVolColonPage, re.VERBOSE | re.IGNORECASE)

    #rgxSEVol =     re.compile("("+jrnlPEPPatterns.get("SE")+"|"+"SE[\.,])\s*:?\s*(?P<bvol>(4/5|1[0-9]|2[0-4]|[1-9]))(\s*,?\s*(?P<bpgs>[0-9]+)?)", re.VERBOSE)
    rgxJrnlPEPPatterns = []
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("RFP" ), re.VERBOSE | re.IGNORECASE), "RFP"))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("LU-AM"   ), re.VERBOSE | re.IGNORECASE), "LU-AM" ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("IPP" ), re.VERBOSE | re.IGNORECASE), "IPP"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("XPS" ), re.VERBOSE | re.IGNORECASE), "XPS"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("FAP" ), re.VERBOSE | re.IGNORECASE), "FAP"   ))

    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("AIM" ), re.VERBOSE | re.IGNORECASE), "AIM"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("AJP"     ), re.VERBOSE | re.IGNORECASE), "AJP"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("AJRPP"), re.VERBOSE | re.IGNORECASE), "AJRPP"))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("ANIJP-IT"), re.VERBOSE | re.IGNORECASE), "ANIJP-IT"  ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("ANIJP-FR"), re.VERBOSE | re.IGNORECASE), "ANIJP-FR"  ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("ANIJP-EL"), re.VERBOSE | re.IGNORECASE), "ANIJP-EL"))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("ANIJP-TR"), re.VERBOSE | re.IGNORECASE), "ANIJP-TR"))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("ANRP"    ), re.VERBOSE | re.IGNORECASE), "ANRP"  ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("AOP" ), re.VERBOSE | re.IGNORECASE), "AOP"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("APM"), re.VERBOSE | re.IGNORECASE), "APM"))
    # BAP needs to be before APA (when patterns are used)
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("BAP" ), re.VERBOSE | re.IGNORECASE), "BAP"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("APA" ), re.VERBOSE | re.IGNORECASE), "APA"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("BAFC"), re.VERBOSE | re.IGNORECASE), "BAFC"  ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("BIP" ), re.VERBOSE | re.IGNORECASE), "BIP"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("BJP"), re.VERBOSE | re.IGNORECASE), "BJP"))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("CFP"),  re.VERBOSE | re.IGNORECASE), "CFP"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("CJP" ), re.VERBOSE | re.IGNORECASE), "CJP"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("DR" ), re.VERBOSE | re.IGNORECASE), "DR"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("FA"  ), re.VERBOSE | re.IGNORECASE), "FA"    ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("FD"  ), re.VERBOSE | re.IGNORECASE), "FD"    ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("CPS" ), re.VERBOSE | re.IGNORECASE), "CPS"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("GAP" ), re.VERBOSE | re.IGNORECASE), "GAP"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("GW"  ), re.VERBOSE | re.IGNORECASE), "GW"    ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("IFP" ), re.VERBOSE | re.IGNORECASE), "IFP"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("IJAPS"), re.VERBOSE | re.IGNORECASE), "IJAPS"    ))
    # IJAPS should be ahead of APS
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("APS"), re.VERBOSE | re.IGNORECASE), "APS"    ))

    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("IJPSP"), re.VERBOSE | re.IGNORECASE), "IJPSP"    ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("IJPSPPSC"), re.VERBOSE | re.IGNORECASE), "IJPSPPSC"    ))
    # IJPSP should be ahead of IJP
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("IJP" ), re.VERBOSE | re.IGNORECASE), "IJP"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("IJPOPEN" ), re.VERBOSE | re.IGNORECASE), "IJPOPEN"))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("IMAGO" ), re.VERBOSE | re.IGNORECASE), "IMAGO"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("IRP" ), re.VERBOSE | re.IGNORECASE), "IRP"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("JAA" ), re.VERBOSE | re.IGNORECASE), "JAA"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("JBP" ), re.VERBOSE | re.IGNORECASE), "JBP"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("JCP" ), re.VERBOSE | re.IGNORECASE), "JCP"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("JCPTX"   ), re.VERBOSE | re.IGNORECASE), "JCPTX" ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("JICAP"   ), re.VERBOSE | re.IGNORECASE), "JICAP" ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("JOAP"), re.VERBOSE | re.IGNORECASE), "JOAP"  ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("MPSA"    ), re.VERBOSE | re.IGNORECASE), "MPSA"  ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("NP"  ), re.VERBOSE | re.IGNORECASE), "NP"    ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("OAJPSI"), re.VERBOSE | re.IGNORECASE), "OAJPSI"))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("OPUS"), re.VERBOSE | re.IGNORECASE), "OPUS"))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("PAH" ), re.VERBOSE | re.IGNORECASE), "PAH"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("PAQ" ), re.VERBOSE | re.IGNORECASE), "PAQ"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("PB" ), re.VERBOSE | re.IGNORECASE), "PB"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("PD"  ), re.VERBOSE | re.IGNORECASE), "PD"    ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("PDPSY"), re.VERBOSE | re.IGNORECASE), "PDPSY" ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("PI"  ), re.VERBOSE | re.IGNORECASE), "PI"    ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("PPSY"  ), re.VERBOSE | re.IGNORECASE), "PPSY"))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("PCS" ), re.VERBOSE | re.IGNORECASE), "PCS"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("PCAS" ), re.VERBOSE | re.IGNORECASE), "PCAS" ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("PCT" ), re.VERBOSE | re.IGNORECASE), "PCT"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("PSC" ), re.VERBOSE | re.IGNORECASE), "PSC"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("PPERSP" ), re.VERBOSE | re.IGNORECASE), "PPERSP"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("PSP" ), re.VERBOSE | re.IGNORECASE), "PSP"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("PSU" ), re.VERBOSE | re.IGNORECASE), "PSU"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("PSW" ), re.VERBOSE | re.IGNORECASE), "PSW"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("PY" ), re.VERBOSE | re.IGNORECASE), "PY"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("RBP" ), re.VERBOSE | re.IGNORECASE), "RBP"))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("RIP" ), re.VERBOSE | re.IGNORECASE), "RIP"))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("RPSA" ), re.VERBOSE | re.IGNORECASE), "RPSA"))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("RRP"), re.VERBOSE | re.IGNORECASE), "RRP"))
    rgxJrnlPEPPatterns.append((rgxSEPat,                                                                "SE"    ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("SGS"  ), re.VERBOSE | re.IGNORECASE), "SGS"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("SPR" ), re.VERBOSE | re.IGNORECASE), "SPR"   ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("PSAR" ), re.VERBOSE | re.IGNORECASE), "PSAR"  ))  # must be after spr?
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("PPTX" ), re.VERBOSE | re.IGNORECASE), "PPTX"  ))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("PSYCHE"), re.VERBOSE), "PSYCHE"))                  # removed ignore case for this. 2014-01-15
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("ZBPA"), re.VERBOSE | re.IGNORECASE), "ZBPA"))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("ZPSAP"), re.VERBOSE | re.IGNORECASE), "ZPSAP"))
    rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("IZPA"), re.VERBOSE | re.IGNORECASE), "IZPA"))


    #rgxJrnlPEPPatterns.append((re.compile(jrnlPEPPatterns.get("JEP" ), re.VERBOSE | re.IGNORECASE), "JEP"   ))

     #--------------------------------------------------------------------------------
    def getPEPJournalCode(self, strText, exactText = False):
        """
        Given a full journal name, as it might be "cited", returns a tuple with
            (
                PEPCode,
                PEPAbbr,
                Journal Name as Cited
            )

        NOTE: refEntry must be a ReferenceMetadata instance

        If PEP Journal, returns:
            (PEPCode, PEPAbbr, FullJournalName)
        If this is not a PEP journal, returns:
            (None, None, None)

        Test Cases:
            >>> jrnlData = PEPJournalData()
            >>> jrnlData.getPEPJournalCode(u'Standard Edition')
            'SE'
            >>> jrnlData.getPEPJournalCode(u'Psychoanalytic Books')[0]
            'PB'
            >>> jrnlData.getPEPJournalCode(u'Psa. Books')[0]
            'PB'
            >>> jrnlData.getPEPJournalCode(u'Revue francaise de psychanalyse')[0]
            'RFP'
            >>> jrnlData.getPEPJournalCode(u'Rev. fr. psa.')[0]
            'RFP'
            >>> jrnlData.getPEPJournalCode('Revue fran\xe7aise de psychanalysee')[0]
            'RFP'
            >>> jrnlData.getPEPJournalCode('Ztschr. f. Psa. P\xe4d.')[0]
            'ZPSAP'
            >>> jrnlData.getPEPJournalCode('Ztschr. f. Psa. P\xc3\xa4d.')[0]
            'ZPSAP'
            >>> jrnlData.getPEPJournalCode("Zschr. psychoanal. Pädagogik.")[0]
            'ZPSAP'
            >>> jrnlData.getPEPJournalCode("Z. Psychoanal. Pädagogik")[0]
            'ZPSAP'
            >>> jrnlData.getPEPJournalCode("Zs. für psa. Pädagogik")[0]
            'ZPSAP'
            >>> jrnlData.getPEPJournalCode("Ztschr. Psa. Päd.")[0]
            'ZPSAP'
            >>> jrnlData.getPEPJournalCode("Int. Zeitschr. f. PsA.")[0]
            'IZPA'
            >>> jrnlData.getPEPJournalCode("Internat. J. Psa.")[0]
            'IJP'
            >>> jrnlData.getPEPJournalCode("Intern. J. Psa.")[0]
            'IJP'
            >>> jrnlData.getPEPJournalCode("Zentralblatt Psychoanal.")[0]
            'ZBPA'
            >>> jrnlData.getPEPJournalCode("Zbl. Psyca.")[0]
            'ZBPA'
            >>> jrnlData.getPEPJournalCode("Zentralblatt Psychoanal.")[0]
            'ZBPA'
            >>> jrnlData.getPEPJournalCode("I.J. Psycho-Anal")[0]
            'IJP'
            >>> jrnlData.getPEPJournalCode("I.J. Psycho-Anal.")[0]
            'IJP'
            >>> jrnlData.getPEPJournalCode("I.J.P.")[0]
            'IJP'
            >>> jrnlData.getPEPJournalCode("I.J.P.A.")[0]
            'IJP'
            >>> jrnlData.getPEPJournalCode("IJP")[0]
            'IJP'
            >>> jrnlData.getPEPJournalCode("Can J Psychoanal")[0]
            'CJP'
            >>> jrnlData.getPEPJournalCode("Stud Gend Sex")[0]
            'SGS'
            >>> jrnlData.getPEPJournalCode("Stud. Gend. Sex.")[0]
            'SGS'
            >>> jrnlData.getPEPJournalCode("Psa. Study of the Child")[0]
            'PSC'
            >>> jrnlData.getPEPJournalCode("International Journal of Psychoanalysis")[0]
            'IJP'
            >>> jrnlData.getPEPJournalCode("This Quarterly")[0]
            'PAQ'
            >>> jrnlData.getPEPJournalCode("International. Journal of Psycho-Analysis")[0]
            'IJP'
            >>> jrnlData.getPEPJournalCode("Int. J. of Psa")[0]
            'IJP'
            >>> jrnlData.getPEPJournalCode("Int. J. Psa")[0]
            'IJP'
            >>> jrnlData.getPEPJournalCode("Journal of the American Psychoanalytic Association")[0]
            'APA'
            >>> jrnlData.getPEPJournalCode("American Imago")[0]
            'AIM'
            >>> jrnlData.getPEPJournalCode("Am Imago")[0]
            'AIM'
            >>> jrnlData.getPEPJournalCode("Am Imago.")[0]
            'AIM'
            >>> jrnlData.getPEPJournalCode("Amer Imago")[0]
            'AIM'
            >>> jrnlData.getPEPJournalCode("Amer. Imago")[0]
            'AIM'
            >>> jrnlData.getPEPJournalCode("Amer. Im.")[0]
            'AIM'
            >>> jrnlData.getPEPJournalCode("Amer. Im")[0]
            'AIM'
            >>> jrnlData.getPEPJournalCode("American Journal of Psychoanalysis")[0]
            'AJP'
            >>> jrnlData.getPEPJournalCode("Amer J Psychoanal")[0]
            'AJP'
            >>> jrnlData.getPEPJournalCode("Am J Psychoanal")[0]
            'AJP'
            >>> jrnlData.getPEPJournalCode("Am Jour Psychoanal.")[0]
            'AJP'
            >>> jrnlData.getPEPJournalCode("Annual  of  Psychoanal")[0]
            'AOP'
            >>> jrnlData.getPEPJournalCode("Annual  of  Psa")[0]
            'AOP'
            >>> jrnlData.getPEPJournalCode("Ann Psychoanal")[0]
            'AOP'
            >>> jrnlData.getPEPJournalCode("J of the Am Psychoanalytic Association")[0]
            'APA'
            >>> jrnlData.getPEPJournalCode("J Am Psychoanalytic Association")[0]
            'APA'
            >>> jrnlData.getPEPJournalCode("Bull. of the Amer Psychoanalytic Association")[0]
            'BAP'
            >>> jrnlData.getPEPJournalCode("Bull. of Intl. Psycho-analytic Assn.")[0]
            'BIP'
            >>> jrnlData.getPEPJournalCode("Bull. of the intl. Psycho-analytic Assn.")[0]
            'BIP'
            >>> jrnlData.getPEPJournalCode("Bull. of Intl. Psycho-analytic Assn.")[0]
            'BIP'
            >>> jrnlData.getPEPJournalCode("Free Associations")[0]
            'FA'
            >>> jrnlData.getPEPJournalCode("Free Assns")[0]
            'FA'
            >>> jrnlData.getPEPJournalCode("Int. J of Psychoanalytic Self Psychology")[0]
            'IJPSP'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("AIM"))[0]
            'AIM'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("AJP"))[0]
            'AJP'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("AOP"))[0]
            'AOP'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("ANRP"))[0]
            'ANRP'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("APA"))[0]
            'APA'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("BAFC"))[0]
            'BAFC'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("BAP"))[0]
            'BAP'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("BIP"))[0]
            'BIP'
            >>> jrnlData.getPEPJournalCode("Bul. Int. Psychoanal. Assn.")[0]
            'BIP'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("CJP"))[0]
            'CJP'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("CPS"))[0]
            'CPS'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("FA"))[0]
            'FA'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("FD"))[0]
            'FD'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("GAP"))[0]
            'GAP'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("IFP"))[0]
            'IFP'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("IJP"))[0]
            'IJP'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("IJPSP"))[0]
            'IJPSP'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("IRP"))[0]
            'IRP'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("JAA"))[0]
            'JAA'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("JICAP"))[0]
            'JICAP'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("JCP"))[0]
            'JCP'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("JOAP"))[0]
            'JOAP'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("IJPSP"))[0]
            'IJPSP'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("JCPTX"))[0]
            'JCPTX'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("MPSA"))[0]
            'MPSA'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("PAH"))[0]
            'PAH'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("PAQ"))[0]
            'PAQ'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("PCT"))[0]
            'PCT'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("PCS"))[0]
            'PCS'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("PI"))[0]
            'PI'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("PD"))[0]
            'PD'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("PSAR"))[0]
            'PSAR'
            >>> jrnlData.getPEPJournalCode("Psychoanalytic Review")[0]
            'PSAR'
            >>> jrnlData.getPEPJournalCode("The Psychoanalytic Review")[0]
            'PSAR'
            >>> jrnlData.getPEPJournalCode("The Psa Review")[0]
            'PSAR'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("PSC"))[0]
            'PSC'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("PSP"))[0]
            'PSP'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("PPTX"))[0]
            'PPTX'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("PPSY"))[0]
            'PPSY'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("RBP"))[0]
            'RBP'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("RIP"))[0]
            'RIP'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("RPSA"))[0]
            'RPSA'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("NP"))[0]
            'NP'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("SGS"))[0]
            'SGS'
            >>> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("SPR"))[0]
            'SPR'
            >>> jrnlData.getPEPJournalCode("Journal of the American Academy of Psychoanalysis")[0]
            'JAA'
            >>> jrnlData.getPEPJournalCode("J Am Psychoanal Assoc")[0]
            'APA'
            >>> jrnlData.getPEPJournalCode("Psychoanal Rev")[0]
            'PSAR'
            >>> jrnlData.getPEPJournalCode("Int J Psychoanal")[0]
            'IJP'
            >>> jrnlData.getPEPJournalCode("J Anal Psychol")[0]
            'JOAP'
            >>> jrnlData.getPEPJournalCode("Psychoanal Q")[0]
            'PAQ'
            >>> jrnlData.getPEPJournalCode("J Am Acad Psychoanal Dyn Psychiatry")[0]
            'JAA'
            >>> jrnlData.getPEPJournalCode("Am J Psychoanal ")[0]
            'AJP'

            >> jrnlData.getPEPJournalCode(jrnlData.getJournalFull("JEP"))[0]
            'JEP'
            >> jrnlData.getJournalFull("JEP")
            >> jrnlData.jrnlPEPPatterns["JEP"]



            >>> jrnlData.getPEPJournalCode("Amer. Imago")[0]
            'AIM'
            >>> jrnlData.getPEPJournalCode("Ital. Psychoanal. Annu.")[0]
            'ANRP'
            >>> jrnlData.getPEPJournalCode("Am. J. Psychoanal.")[0]
            'AJP'
            >>> jrnlData.getPEPJournalCode("Annu. Psychoanal.")[0]
            'AOP'
            >>> jrnlData.getPEPJournalCode("J. Amer. Psychoanal. Assn.")[0]
            'APA'
            >>> jrnlData.getPEPJournalCode("Bul. Anna Freud Centre")[0]
            'BAFC'
            >>> jrnlData.getPEPJournalCode("Bul. Amer. Psychoanal. Assn.")[0]
            'BAP'
            >>> jrnlData.getPEPJournalCode("Bul. Int. Psychoanal. Assn.")[0]
            'BIP'
            >>> jrnlData.getPEPJournalCode("Can. J. Psychoanal.")[0]
            'CJP'
            >>> jrnlData.getPEPJournalCode("Contemp. Psychoanal.")[0]
            'CPS'
            >>> jrnlData.getPEPJournalCode("Free Associations")[0]
            'FA'
            >>> jrnlData.getPEPJournalCode("Fort  Da")[0]
            'FD'
            >>> jrnlData.getPEPJournalCode("Gender and Psychoanal.")[0]
            'GAP'
            >>> jrnlData.getPEPJournalCode("Int. Forum Psychoanal.")[0]
            'IFP'
            >>> jrnlData.getPEPJournalCode("Int. J. Psycho-Anal.")[0]
            'IJP'
            >>> jrnlData.getPEPJournalCode("Int. J. Psychoanal. Self Psychol.")[0]
            'IJPSP'
            >>> jrnlData.getPEPJournalCode("Int. Rev. Psycho-Anal.")[0]
            'IRP'
            >>> jrnlData.getPEPJournalCode("J. Am. Acad. Psychoanal. Dyn. Psychiatr.")[0]
            'JAA'
            >>> jrnlData.getPEPJournalCode("J. Clin. Psychoanal.")[0]
            'JCP'
            >>> jrnlData.getPEPJournalCode("J. Child Psychother.")[0]
            'JCPTX'
            >>> jrnlData.getPEPJournalCode("Neuro-Psychoanalysis")[0]
            'NP'
            >>> jrnlData.getPEPJournalCode("J. Infant Child Adolesc. Psychother.")[0]
            'JICAP'
            >>> jrnlData.getPEPJournalCode("J. Anal. Psychol.")[0]
            'JOAP'
            >>> jrnlData.getPEPJournalCode("Mod. Psychoanal.")[0]
            'MPSA'
            >>> jrnlData.getPEPJournalCode("Neuro-Psychoanalysis")[0]
            'NP'
            >>> jrnlData.getPEPJournalCode("Psychoanal. Hist.")[0]
            'PAH'
            >>> jrnlData.getPEPJournalCode("Psychoanal. Q.")[0]
            'PAQ'
            >>> jrnlData.getPEPJournalCode("Psychoanal. Contemp. Sci.")[0]
            'PCS'
            >>> jrnlData.getPEPJournalCode("Psychoanal. Contemp. Thought")[0]
            'PCT'
            >>> jrnlData.getPEPJournalCode("Psychoanal. Dial.")[0]
            'PD'
            >>> jrnlData.getPEPJournalCode("Psychoanal. Inq.")[0]
            'PI'
            >>> jrnlData.getPEPJournalCode("Psychoanal. Psychol.")[0]
            'PPSY'
            >>> jrnlData.getPEPJournalCode("Psychoanal. Psychother.")[0]
            'PPTX'
            >>> jrnlData.getPEPJournalCode("Psychoanal. Rev.")[0]
            'PSAR'
            >>> jrnlData.getPEPJournalCode("Psychoanal. St. Child")[0]
            'PSC'
            >>> jrnlData.getPEPJournalCode("Progr. Self Psychol.")[0]
            'PSP'
            >>> jrnlData.getPEPJournalCode("Rev. Belg. Psychanal.")[0]
            'RBP'
            >>> jrnlData.getPEPJournalCode("Psychoanalytic Social Work")[0]
            'PSW'
            >>> jrnlData.getPEPJournalCode("Scand. Psychoanal. Rev.")[0]
            'SPR'

        """
        # use this to lookup journal codes from a journal name

        ret_val = (None, None, None)
        found = False

        if strText != "":
            try:
                if not isinstance(strText, str):
                    strText = strText.encode("utf8")   # 2018-01-19 Make sure it's UTF8 (if error, then it is already, so pass)
            except Exception as e:
                logger.warning("Encoding error: %s" % e)

            for (rgxJournalPtrn, code) in self.rgxJrnlPEPPatterns:
                #print "Pattern: ", rgxJournalPtrn.pattern, code
                if exactText:
                    m = rgxJournalPtrn.match(strText)
                else:
                    m = rgxJournalPtrn.search(strText)

                if m is not None:
                    found = True
                    jrnlName = self.jrnlAbbr.get(code)
                    ret_val = (code, self.jrnlAbbr.get(code, None), jrnlName)
                    if gDbg1:
                        if exactText:
                            print("getPEPJournalCode found exactmatch (code, abbr, full name):", ret_val)
                        else:
                            print("getPEPJournalCode found (code, abbr, full name):", ret_val)
                    break

        if gDbg1:
            if not found:
                print("PEP Journal Not found for: ", strText)
        return ret_val

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






