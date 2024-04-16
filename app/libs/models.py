#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=C0321,C0103,C0301,E1101,C0303,E1004,C0330,R0915,R0914,W0703,C0326

__author__      = "Neil R. Shapiro"
__copyright__   = "Copyright 2019-2021, Psychoanalytic Electronic Publishing"
__license__     = "Apache 2.0"
__version__     = "2021.0213.1" # per changes pushed earlier today to project
__status__      = "Development"

# Breaking change in Pydantic with 1.8... changed Schema to Field, and no longer offering the equivalency
#  Interesting though--seems to be breaking loading AWS but not here, and both appear to be using 1.6.1 as per
#  requirements.txt
# 
#  Breaking Change, remove old deprecation aliases from v1, #2415 by @samuelcolvin:
#      remove notes on migrating to v1 in docs
#      remove Schema which was replaced by Field
#      remove Config.case_insensitive which was replaced by Config.case_sensitive (default False)
#      remove Config.allow_population_by_alias which was replaced by Config.allow_population_by_field_name
#      remove model.fields which was replaced by model.__fields__
#      remove model.to_string() which was replaced by str(model)
#      remove model.__values__ which was replaced by model.__dict__

import sys
sys.path.append('../libs')

# import re
# import os.path

#import time
import datetime
from datetime import datetime
from typing import List, TypeVar, Optional # Generic
import opasConfig
# from pysolr import Results

from enum import Enum
#from lxml import etree

class ExtendedEnum(Enum):
    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))
    
from pydantic.main import BaseModel, Field

#-------------------------------------------------------
#
# Detail level Field structures
#
#-------------------------------------------------------
OpasDB = TypeVar('OpasDB')   
  
#-------------------------------------------------------
# Enums
#-------------------------------------------------------
class ListTypeEnum(Enum):
    volumelist = "volumelist"
    documentList = "documentlist"
    advancedSearchList = "advancedsearchlist"
    authorPubList = "authorpublist"
    authorIndex = "authorindex"
    imageURLList = "imageurllist"
    licenseInfo = "licenseinfo"
    sourceInfoList = "sourceinfolist"
    whatsNewList = "newlist"
    mostCitedList = "mostcited"
    mostViewedList = "mostviewed"
    searchAnalysisList = "srclist"
    TermIndex = "termindex"
    reportList = "reportlist"
    
class ReportTypeEnum(str, Enum):
    def __str__(self):
        return '{0}'.format(self.value)
    
    sessionLog = "Session-Log"
    userSearches = "User-Searches"
    documentViews = "Document-View-Stat"
    documentViewLog = "Document-View-Log"
    characterCounts = "Character-Count-Report"
    characterCountsDetails = "Character-Count-Details-Report"
    characterCountsBookDetails = "Character-Count-Details-Book-Report"
    productTable = "Product-Table"
    
class TermTypeIDEnum(str, ExtendedEnum):
    termid = "ID"
    termname = "Name"
    termgroup = "Group"
    
class QueryParserTypeEnum(Enum):
    edismax = "edismax"
    dismax = "dismax"
    standard = "lucene"
    lucene = "lucene"
    
class SearchModeEnum(Enum):
    searchMode = "Searching"
    documentFetchMode = "DocumentFetch"
    moreLikeTheseMode = "MoreLikeThese"
    queryAnalysisMode = "QueryAnalysis"
   
class TimePeriod(Enum):
    five = '5'
    ten = '10'
    twenty = '20'
    alltime = 'all'

#--------------------------------------------------------------------
# ArticleIfno, pydantic model defined in opasArticleIDSupport
#--------------------------------------------------------------------
#class ArticleInfo(BaseModel):
    #"""
    #An entry from a documents metadata.
    #See opasArticleIDSupport for model and supporting functions
    #"""    
#--------------------------------------------------------------------
# Biblio records, moved from modelsOpasCentralPydantic
#--------------------------------------------------------------------
class BiblioxmlGeneric(BaseModel):
    row: dict = Field({}, title="Fully flexible content row from Database")

class Biblioxml(BaseModel):
    art_id: str = Field(None, title="Article ID (locator) of the instance containing the reference")
    ref_local_id: str = Field(None, title="ID of the be or binc element in the instance")
    art_year: Optional[int]
    ref_rx: str = Field(None)
    ref_rx_confidence: float = Field(0, title="Confidence level of assigned rx. Manual QA = 1, code parsed = .99, Heuristic = .1 to .97")
    ref_rxcf: str = Field(None, title="List of possible article ID codes for this reference")
    ref_rxcf_confidence: float = Field(0, title="Max confidence level of rxcf codes")
    ref_sourcecode: str = Field(None)
    last_update: datetime = Field(None)
    ref_authors: str = Field(None)
    ref_authors_xml: str = Field(None)
    ref_link_source: str = Field(None, title="Source of rx link, 'xml', 'database', or 'pattern'")
    # ref_articletitle: str = Field(None)
    art_title: str = Field(None)
    ref_rxp: str = Field(None, title="Previous system method to correct rx value without overwriting.  Will now use to figure correct rx")
    ref_rxp_confidence: float = Field(0, title="rxp confidence value")
    ref_text: str = Field(None)
    ref_sourcetype: str = Field(None, title="journal, book, unknown") # could add video
    ref_is_book: bool = Field(False)
    ref_sourcetitle: str = Field(None)
    ref_authors_xml: str = Field(None)
    ref_xml: str = Field(None)
    # ref_xml_corrected: bool = Field(False) No longer needed 2023-02-16 (references have been integrated into KBD3 XML)
    ref_pgrg: str = Field(None)
    ref_pgstart: str = Field("")
    ref_doi: Optional[str]
    ref_title: str = Field(None)
    ref_year: str = Field(None)
    ref_year_int: Optional[int]
    ref_volume: str = Field(None)
    ref_volume_int: Optional[int]
    ref_volume_isroman: bool = Field(False, title="True if ref_volume is roman")
    ref_publisher: str = Field(None)
    ref_in_pep: bool = Field(False, title="Indicates this is a PEP reference")
    ref_exists: bool = Field(False, title="Indicates this ref link has been verified")
    link_updated: bool = Field(False, title="Indicates whether the data was corrected during load. If so, may need to update DB.  See link_source for method")
    record_from_db: bool = Field(False, title="This entry is from the biblioDB")
    record_updated: bool = Field(False)
    skip_incremental_scans: Optional[bool] # Field(False, title="Use to skip for speed during normal scans. Can reset it (in bulk) for a full rescan--in that way it's different than NEVERMORE")
    skip_reason: str = Field(None)
    parsed_ref: Optional[object]

#-------------------------------------------------------
# Error Return classes (Can also return "No Error", 200 httpcode)
#-------------------------------------------------------
class ErrorReturn(BaseModel):
    httpcode: int = Field(200, title="HTTP code")
    session_id: str = Field(None, title="Session ID")
    user_authenticated: bool = Field(None, title="User authenticated")
    error: str = Field(None, title="Error class or title")
    error_description: str = Field(None, title="Error description")
    subsystem_error_description: str = Field(None, title="Error description from subsystem, e.g., from Authorization System")
    subsystem_error_model: dict = Field(None, title="Subsystem Error details")

#-------------------------------------------------------
# Status return structure, standard part of most models
#-------------------------------------------------------
class ResponseInfo(BaseModel):
    count: int = Field(0, title="The number of returned items in the accompanying ResponseSet list.")
    limit: int = Field(0, title="The limit set by the API client for the ResponseSet list.")
    offset: int = Field(None, title="The offset the ResponseSet list begins as requested by the client to index into the full set of results, i.e., for paging through the full set.")
    page: int = Field(None, title="If the request for a document was for a specific page number, the page number is listed here.  Offset will then reflect the relative page number from the start of the document.")
    fullCount: int = Field(None, title="The number of items that could be returned without a limit set.")
    fullCountComplete: bool = Field(None, title="How many matches 'theoretically' matched, though they cannot be returned for some reason, such as a search engine limitation on returned data.")
    facetCounts: dict = Field(None, title="A dictionary of requested facet information (counts of results mapped for specified fields")
    totalMatchCount: int = Field(None, title="The number of items in the complete match set that can be retrieved and paged through.")  # used in PEPEasy paging controls
    listLabel: str = Field(None, title="Descriptive title of data return for SourceInfoList, e.g., Book List, Journal List, Video List. Should be used elsewhere too.")
    listType: ListTypeEnum = Field(None, title="ListTypeEnum based identifier of the return structure, e.g., 'documentList'.")
    supplementalInfo: dict = Field(None, title="Additional info supplied based on the endpoint")
    scopeQuery: list = Field(None, title="The query strings applied: [query_string, filter_string]")
    description: str = Field(None, title="A semantic description explaining the action/response")
    request: str = Field(None, title="The URL request (endpoint) that resulted in this response.")
    core: str = Field(None, title="The Solr Core classname used (e.g., docs, authors).")
    solrParams: dict = Field(None, title="A dictionary based set of the parameters passed to the Solr search engine for this request.")
    errors: ErrorReturn = Field(None, title="Any Error information")
    dataSource: str = Field(None, title="Version (Date Update) of the database")
    authenticated: bool = Field(None, title="If request was processed as authenticated")
    timeStamp: str = Field(None, title="Server timestamp of return data.")   

#-------------------------------------------------------
# Simple API Status class
#-------------------------------------------------------
class APIStatusItem(BaseModel):
    opas_version: str = Field(None, title="Version of OPAS")
    timeStamp: str = Field(None, title="Current time")   

#-------------------------------------------------------
# General Data Encapsulation classes
#-------------------------------------------------------
class AccessLimitations(BaseModel):
    accessType: str = Field(None, title="Class of access")
    accessChecked: bool = Field(False, title="Access was checked. If false, accessLimited value is just defaulted to False")
    accessLimited: bool = Field(True, title="True if the data can not be provided for this user")
    accessLimitedCode: int = Field(None, title="If an error code is returned from the server, pass it back here for ease of client processing")
    accessLimitedClassifiedAsCurrentContent: bool = Field(False, title="True if the data is considered Current Content (embargoed). Note True does not mean it's limited for this user(See accessLimited for that).")
    accessLimitedReason: str = Field(None, title="Explanation of limited access status")
    accessLimitedDebugMsg: str = Field(None, title="Developer level info for tracing access issues")
    accessLimitedAuthResponse: dict = Field(None, title="Response from authorization subsystem")
    accessLimitedDescription: str = Field(None, title="Description of why access is limited")
    accessLimitedPubLink: str = Field(None, title="Link to publisher") 
    doi: str = Field(None, title="Document Object Identifier, without base URL")
    
#-------------------------------------------------------
# Data Return classes
#-------------------------------------------------------
class AlertListItem(BaseModel):
    alertName: str
    alertSubscribeStatus: bool
    alertSubscribeDate: str
    action: str = Field(None, title="")

class AlertListStruct(BaseModel):
    responseInfo: ResponseInfo 
    responseSet: List[AlertListItem] = []

class AlertList(BaseModel):
    alertList: AlertListStruct

#-------------------------------------------------------
class authorInfo(BaseModel):
    first: str = Field(None, title="First name")
    middle: str = Field(None, title="Middle name")
    last: str = Field(None, title="Last name")
    title: str = Field(None, title="Prename title (Mr. Mrs. Dr.") 
    affil: str = Field(None, title="Affiliation")
    
class authorList(BaseModel):
    authorList: List[authorInfo] = []   

#-------------------------------------------------------

class AuthorPubListItem(BaseModel):
    authorID: str = Field(None, title="Author ID as indexed by the system.")
    documentID: str = Field(None, title="Doc ID for this publication by the author.")
    documentRef: str = Field(None, title="The bibliographic form presentation of the information about the document, as in the 'citeas' area or reference sections (text-only).")
    documentRefHTML: str = Field(None, title="Same as documentRef but in HTML.")
    year: str = Field(None, title="Year of publication of this list item.")
    documentURL: str = Field(None, title="API Endpoint URL (minus base) to access this document.")
    score: float = None

class AuthorPubListStruct(BaseModel):
    responseInfo: ResponseInfo 
    responseSet: List[AuthorPubListItem] = []

class AuthorPubList(BaseModel):
    authorPubList: AuthorPubListStruct

#-------------------------------------------------------

class AuthorIndexItem(BaseModel):
    authorID: str = Field(None, title="Author ID as indexed by the system.")
    publicationsURL: str = Field(None, title="Endpoint URL for this API to retrieve a list of this authors publications.")
    publicationsCount: int = Field(None, title="The number of publications in this database by this author.")
    
class AuthorIndexStruct(BaseModel):
    responseInfo: ResponseInfo
    responseSet: List[AuthorIndexItem] = []

class AuthorIndex(BaseModel):
    authorIndex: AuthorIndexStruct

#-------------------------------------------------------
class ClientConfigs(BaseModel):
    config_id: int = 0
    client_id: int = 0
    config_name: str = None
    config_settings: str = None
    session_id: str
    last_update: datetime = None
    
#-------------------------------------------------------
class ClientConfigItem(BaseModel):
    """
    Dictionary to hold client configuration settings as set by the client administrator
    """
    api_client_id: int = Field(0, title="Identifies the client APP, e.g., 2 for the PEP-Web client; this is used to look up the client apps unique API_KEY in the database when needed")
    session_id: str = Field(None, title="A generated session Identifier number the client passes in the header to identify the session")
    configName: str = Field(None, title="Unique name (within client ID) to save and retrieve configuration")
    configSettings: dict = Field({}, title="Dictionary with all configuration settings")

class ClientConfigList(BaseModel):
    configList: List[ClientConfigItem]

#-------------------------------------------------------
class MoreLikeThisItem(BaseModel):
    documentID: str = Field(None, title="Document ID/Locator", description="The multiple-section document ID, e.g., CPS.007B.0021A.B0012 in a biblio, or CPS.007B.0021A as a document ID.")
    

#-------------------------------------------------------
class DocumentListItem(BaseModel):
    coreName: str = Field(None, title="Core", description="Core from which the item was retrieved")
    documentID: str = Field(None, title="Document ID/Locator", description="The multiple-section document ID, e.g., CPS.007B.0021A.B0012 in a biblio, or CPS.007B.0021A as a document ID.")
    docType:  str = Field(None, title="Document Type (Classification)", description="e.g., ART(article), ABS(abstract), ANN(announcement), COM(commentary), ERR(errata), PRO(profile), (REP)report, or (REV)review")
    documentRef: str = Field(None, title="Document Ref (bibliographic)", description="The bibliographic form presentation of the information about the document, as in the 'citeas' area or reference sections (text-only).")
    documentRefHTML: str = Field(None, title="Same as documentRef but in HTML.")
    documentRefXML: str = Field(None, title="Same as documentRef but in XML.")  # currently same as documentRefHTML but for semantic clarity listed
    documentMetaXML: str = Field(None, title="Metadata content in XML, , e.g., element meta")
    documentInfoXML: str = Field(None, title="The document meta information in XML, e.g., element artinfo")
    title: str = Field(None, title="Document Title")
    authorMast: str = Field(None, title="Author Names", description="The author names as displayed below the title in an article.")
    #2020-10-19, new convenience listing of author info (requires Field change to parse during load)
    authorList: list = Field(None, title="List of individual author data parsed from documentInfoXML", description="List of individual author data parsed from documentInfoXML")
    authorCitation: str = Field(None, title="Author Names Citation style", description="The author names as they would appear in a reference.")
    origrx: str = Field(None, title="Original Document (documentID)", description="Document idref (documentID) linking this to an original document, e.g, this is a translation of...")
    relatedrx: str = Field(None, title="Closely Related Documents (documentID)", description="Document idref (documentID) associating all closely related documents to this one, e.g., this is a commentary on...")
    PEPCode: str = Field(None, title="Source Acronym", description="Acronym-type code assigned to the document source e.g., CPS, IJP, ANIJP-EL, ZBK. (The first part of the document ID.)")
    sourceTitle: str = Field(None, title="Source Title", description="The name of the document's source (title) in abbreviated, bibliographic format")
    sourceTitleAbbr: str = Field(None, title="Source Title Abbrev", description="The name of the document's source (title) in abbreviated, bibliographic format")
    sourceType:  str = Field(None, title="Source Type", description="Journal, Book, Videostream")
    kwicList: list = Field(None, title="Key Words in Context", description="The matched terms in the matched document context, set by server config DEFAULT_KWIC_CONTENT_LENGTH ") # a real list, seems better long term
    kwic: str = Field(None, title="Key Words in Context", description="KWIC as text, concatenated, not a list -- the way in v1 (May be deprecated later") # 
    vol: str = Field(None, title="Serial Publication Volume", description="The volume number of the source, can be alphanumeric")
    year: str = Field(None, title="Serial Publication Year", description="The four digit year of publication")
    lang: str = Field(None, title="Language", description="The primary language of this article")
    issn: str = Field(None, title="The ISSN", description="The ISSN for the source") # 2020506 Not sure if we should include this, but we are at least already storing it at article level
    isbn: str = Field(None, title="The ISBN", description="The ISBN for the source") 
    doi: str = Field(None, title="Document object identifier", description="Document object identifier, a standard id system admin by the International DOI Foundation (IDF)")
    issue: str = Field(None, title="Serial Issue Number")
    issueSeqNbr: str = Field(None, title="Serial Issue Sequence Number (continuous count)") 
    issueTitle: str = Field(None, title="Serial Issue Title", description="Issues may have titles, e.g., special topic")
    newSectionName: str = Field(None, title="Name of Serial Section Starting", description="The name of the section of the issue, appears for the first article of a section")
    currSectionName: str = Field(None, title="Current Section Name", description="The name of the section of the issue this article is in")
    #instSectionName: str = Field(None, title="Name of Section from original instance", description="The name of the section of the issue, as represented in each article instance (even repetitively)")
    pgCount: str = Field(None, title="Page Count", description="The number of pages in the document")
    pgRg: str = Field(None, title="Page Range as Published", description="The published start and end pages of the document, separated by a dash")
    pgStart: str = Field(None, title="Starting Page Number as Published", description="The published start page number of the document")
    pgEnd: str = Field(None, title="Ending Page Number as Published", description="The published end page number of the document")
    abstract: str = Field(None, title="Abstract", description="The document abstract, with markup")
    document: str = Field(None, title="Document", description="The full-text document, with markup")
    docPagingInfo: dict = Field(None, title="Requested document page, limit, and offset", description="The document is limited per the call: shows requested page, limit, and offset in a dict")
    # |= the dict (for now, may be better to change to model) allows flexibility in fields, but contains the below for PEP
    #    --document_page: str
    #    --document_page_limit: int
    #    --document_page_offset: int
    docLevel: int = Field(None, title="Document level", description="Top level document=1, subdocument=2")
    docChild: dict = Field(None, title="Child document fields", description="Fields specific to child documents (parent_tag, para)")
    # |= the dict
    #    --parent_tag: str = Field(None, title="The parent of the nested/sub field para, when searching children directly")
    #    --para: str = Field(None, title="The nested/sub field para, when searching children directly")
    updated: datetime = Field(None, title="Source file update date and time", description="The date and time the source file was updated last")
    score: float = Field(None, title="The match score", description="Solr's score for the match in the search")
    hitList: list = Field(None, title="List of hits", description="List of search matches in document, if requested")
    hitCount: int = Field(None, title="Count of hits", description="Count of search matches in documents (counts matchcodes)")
    hitCriteria: str = Field(None, title="Criteria for markup", description="Search criteria for hit markup")
    rank: float = Field(None, title="Document's Search Rank")
    rankfield: str = Field(None, title="Field in rank", description="Which field is in rank")
    referenceCount: str = Field(None, title="Number of references", description="The number of references listed in the document bibliography")
    #instanceCount: int = Field(None, title="Counts", description="Reusable field to return counts requested")
    # |- new v2 field, but removed during cleanup, better ata is in stat.
    stat: dict = Field(None, title="Statistics", description="Reusable field to return counts requested")
    similarityMatch: dict = Field(None, title="Information about similarity matches")
    translationSet: list = Field(None, title="Information about document translations and the original")
    # Search Analysis fields (and shared Glossary term)
    term: str = Field(None, title="Search Analysis Term", description="For search analysis, the clause or term being reported")
    termCount: int = Field(None, title="Search Analysis Term Count", description="For search analysis, the count of occurences of the clause or term being reported")
    # Glossary core specific fields
    termID: str = Field(None, title="Term ID", description="")
    groupID: str = Field(None, title="Group ID", description="")
    groupName: str = Field(None, title="Group Name", description="")
    groupAlso: str = Field(None, title="Group Also", description="")
    groupTermCount: str = Field(None, title="Group Term Count", description="")
    sourcePrevious: str = Field(None, title="Previous part (article)", description="Previous part (article) in a source divided into multiple articles (e.g., journals and some books)")
    sourceNext: str = Field(None, title="Previous part (article)", description="Previous part (article) in a source divided into multiple articles (e.g., journals and some books)")
    termType: str = Field(None, title="", description="")
    termSource: str = Field(None, title="", description="")
    termDefPartXML: str = Field(None, title="", description="")
    termDefRestXML: str = Field(None, title="", description="")
    pdfOriginalAvailable: bool = Field(False, title="", description="")
    embargo: str = Field(False, title="Article Embargoed", description="Article level embargo mechanism") 
    embargotype: str = Field(None, title="Article Embargo Reason", description="Article level embargo reason")
    downloads: bool = Field(None, title="Allow print/downloadng", description="If False, downloads and printing of this document are not allowed.")
    # these are not all currently used
    accessClassification: str = Field(None, title="Document classification, e.g., Archive, Current, Free, OffSite, Special")
    accessChecked: bool = Field(False, title="Access was checked. If false, accessLimited value is just defaulted to False")
    accessLimited: bool = Field(None, title="Access is limited, preventing full-text return.  Only valid when accessChecked is True.")
    accessLimitedCode: int = Field(None, title="If an error code is returned from the server, pass it back here for ease of client processing")
    accessLimitedReason: str = Field(None, title="Explanation of user's access to this")
    accessLimitedDebugMsg: str = Field(None, title="Developer level info for tracing access issues")
    accessLimitedDescription: str = Field(None, title="Description of the access limitation applied")
    accessLimitedClassifiedAsCurrentContent: bool = Field(None, title="Access is limited by embargo to this specific content")
    accessLimitedPubLink: str = Field(None, title="Link to the document or publisher in some cases where doc's not readable on PEP")
    
class DocumentListStruct(BaseModel):
    responseInfo: ResponseInfo
    responseSet: List[DocumentListItem] = []

class DocumentList(BaseModel):
    documentList: DocumentListStruct

#-------------------------------------------------------
class DocumentStruct(BaseModel):
    responseInfo: ResponseInfo
    responseSet: List[DocumentListItem] = []

# modified PEPEasy2020 works with multiple 
class Documents(BaseModel):
    """
    Contains one or more documents (or abstracts) and associated data items.
    
    Note that the current API only supports return of one full-text document at a time, though multiple
      abstracts may be returned via this structure.
      
    """
    # For the GVPi server, it returns a single object not an array of documents.
    # But that's inconsistent with the abstract return.  #ToDo Need to modify PEP-Easy and unify as a list.
    documents: DocumentListStruct

# possible submission and return structure for file items loaded 3/20/2020, subject to change
class FileItem(BaseModel):
    uploadToken: bytes
    sourceCode: str  # enough to generate a PEPCode/DocID
    sourceVol: str
    sourceYear: str
    sourcePageStart: str
    sourcePageEnd: str
    sourceType: str
    title: str
    sourceXML: bytes
    sourcePDF: bytes

class FileItemListStruct(BaseModel):
    responseInfo: ResponseInfo
    responseSet: List[FileItem] = []

class FileItemList(BaseModel):
    fileItemList: FileItemListStruct
    
#-------------------------------------------------------
class GraphicItem(BaseModel):
    documentID: str = Field(None, title="ID of document containing graphic")
    graphic: str = Field(None, title="Graphic filename, or ID, no path")

#-------------------------------------------------------
class ImageURLListItem(BaseModel):    
    PEPCode: str
    imageURL: str
    sourceType: str
    title: str

class ImageURLListStruct(BaseModel):
    responseInfo: ResponseInfo
    responseSet: List[ImageURLListItem] = []

class ImageURLList(BaseModel):
    imageURLList: ImageURLListStruct

#-------------------------------------------------------
class ResponseInfoLoginStatus(BaseModel):
    loggedIn: bool = Field(False, title="Whether the user is logged in or not")
    username: str = Field(None, title="The logged in user's name")
    request: str = Field(None, title="The URL of the request")
    #user: User = Field(None, title="A user object for the user")
    error_message: str = Field(None, title="If an error occurred, description")
    dataSource: str = Field(None, title="Version of the API server software")
    timeStamp: str = Field(None, title="Server timestamp of return data.")   

class LoginReturnItem(BaseModel):    
    session_id: str = Field(None, title="")
    token_type: str = Field(None, title="")
    access_token: str = Field(None, title="")
    session_expires_time: datetime = Field(None, title="")
    authenticated: bool = Field(False, title="")
    keep_active: bool = Field(False, title="Extend the token retention time")
    error_message: str = Field(None, title="Error description if login failed")
    scope: str = Field(None, title="")

#-------------------------------------------------------
class LicenseInfoStruct(BaseModel):
    responseInfo: ResponseInfoLoginStatus
    responseSet: LoginReturnItem = None

class LicenseStatusInfo(BaseModel):
    licenseInfo: LicenseInfoStruct 

# Classes returned by PaDS
class PadsSessionInfo(BaseModel):
    HasSubscription: bool = Field(False, title="")
    IsValidLogon: bool = Field(False, title="")
    IsValidUserName: bool = Field(False, title="")
    ReasonId: int = Field(0, title="")
    ReasonStr = Field("", title="")
    SessionExpires: int = Field(0, title="Session expires time")
    SessionId: str = Field(None, title="Assigned session ID")
    # added session_started to model, not supplied
    session_start_time: datetime = Field(datetime.now(), title="The time the session was started, not part of the model returned")
    pads_status_response: int = Field(0, title="The status code returned by PaDS, not part of the model returned")
    pads_disposition: str = Field(None, title="The disposition of PaDS either from error return or deduction")
    
class PadsUserInfo(BaseModel):
    UserId:int = Field(None, title="")
    UserName:str = Field(None, title="")
    UserType:str = Field(None, title="")
    SubscriptionEndDate:str = Field(None, title="")
    Branding:bool = Field(None, title="")
    ClientSettings:Optional[dict]=Field({}, title="Client app settings.  Type is dict.") # 
    ReasonId:int = Field(None, title="Code corresponding to applicable HTTP error codes")
    ReasonStr:str = Field(None, title="Description of reason for a non 200 return code")
    HasArchiveAccess:bool = Field(False, title="User has subscription to PEP Archive")
    HasCurrentAccess:bool = Field(False, title="User has subscription to PEP Current (rare)")
    # for dummy auth server
    Password:str = Field(None, title="")

class PadsPermitInfo(BaseModel):
    # see https://app.swaggerhub.com/apis/nrshapiro/PEPSecure/1.03#/PermitResponse
    SessionId:str = Field(None, title="session GUID")
    DocId:str = Field(None, title="PEP Document Locator (Document ID: e.g., IJP.082.0215A)")
    HasArchiveAccess:bool = Field(False, title="User has subscription to PEP Archive")
    HasCurrentAccess:bool = Field(False, title="User has subscription to PEP Current (rare)")
    Permit:bool = Field(False, title="True if the user has permission to view fulltext of DocId")
    ReasonId:int = Field(None, title="Code corresponding to applicable HTTP error codes")
    StatusCode: int = Field(None, title="status code returned ")
    ReasonStr:str = Field(None, title="Description of reason for a non 200 return code")

#-------------------------------------------------------
class SessionInfo(BaseModel):    
    session_id: str = Field(None, title="A generated session Identifier number the client passes in the header to identify the session")
    user_id: int = Field(opasConfig.USER_NOT_LOGGED_IN_ID, title="User ID (numeric).  0 for unknown user.  Corresponds to the user table records")
    username: str = Field(opasConfig.USER_NOT_LOGGED_IN_NAME, title="Registered user name, for convenience here")
    user_type: str = Field("Unknown", title="User type, e.g., Admin or Individual")
    is_valid_login: bool = Field(False, title="From PaDS, is valid login")
    has_subscription: bool = Field(False, title="")
    is_valid_username: bool = Field(False, title="")
    authenticated: bool = Field(False, title="True if the user has been authenticated.")
    # the next field allows us to stop asking for permits
    # confirmed_unauthenticated: bool = Field(False, title="True if PaDS has replied to a permit with http code 401 unauthenticated.")
    authorized_peparchive: bool = Field(False, title="New field to simplify permissions - if true this user has access to all of the archive.")
    authorized_pepcurrent: bool = Field(False, title="New field to simplify permissions - if true this user has access to all of the current issues.")
    session_start: datetime = Field(None, title="The datetime when the user started the session")
    session_end: datetime = Field(None, title="The datetime when the user ended the session")
    session_expires_time: datetime = Field(None, title="The limit on the user's session information without renewing")
    admin: bool = Field(False, title="True if the user has been authenticated as admin.")
    api_client_id: int = Field(0, title="Identifies the client APP, e.g., 2 for the PEP-Web client; this is used to look up the client apps unique API_KEY in the database when needed")
    api_direct_login: bool = Field(False, title="True if the session was logged in via the API endpoint.")
    
    # temporary, for debug
    pads_session_info: PadsSessionInfo = None
    pads_user_info: PadsUserInfo = None
    
#-------------------------------------------------------
class ServerStatusContent(BaseModel):
    article_count: int = Field(0, title="")
    journal_count: int = Field(0, title="")
    video_count: int = Field(0, title="")
    book_count: int = Field(0, title="")
    figure_count: int = Field(0, title="")
    year_count: int = Field(0, title="")
    year_first: int = Field(0, title="")
    year_last: int = Field(0, title="")
    vol_count: int = Field(0, title="")   
    page_count: int = Field(0, title="")
    page_height_feet: float = Field(0, title="")
    page_weight_tons: float = Field(0, title="")
    source_count: dict = Field(None, title="")
    description_html: str = Field(None, title="")
    source_count_html: str = Field(None, title="")

class ServerStatusItem(BaseModel):
    db_server_ok: bool = Field(None, title="Database server is online")
    db_server_version: str = Field(None, title="Version of the Database server")
    text_server_ok: bool = Field(None, title="Text server is online")
    text_server_version: str = Field(None, title="Version of the text server")
    opas_version: str = Field(None, title="Version of OPAS")
    dataSource: str = Field(None, title="Version of the API server software")
    timeStamp: str = Field(None, title="Current time")
    serverContent: ServerStatusContent = Field(None, title="Database Content (Counts)")
    session_id: str = Field(None, title="Session ID")
    # admin only
    user_count:  int = Field(0, title="Number of users online")
    user_ip: str = Field(None, title="Requestor's ip")
    config_name: str = Field(None, title="Current Configuration Name")
    text_server_url: str = Field(None, title="Current SOLR URL")
    cors_regex: str = Field(None, title="Current CORS Regex")
    db_server_url: str = Field(None, title="Current DB URL")
    sitemap_path:  str = Field(None, title="Current Sitemap folder location setting")
    google_metadata_path:  str = Field(None, title="Current google metadata folder location setting")
    pdf_originals_path:  str = Field(None, title="Current folder location setting")
    image_source_path:  str = Field(None, title="Current folder location setting")
    image_expert_picks_path:  str = Field(None, title="Current folder location setting")
    xml_originals_path:  str = Field(None, title="Current folder location setting")
    library_versions: dict = Field({}, title="Server Python Library Versions")

#-------------------------------------------------------

class SiteMapInfo(BaseModel):
    siteMapIndex: str = Field(None, title="Site Map Index file name")
    siteMapList: List = Field(None, title="Site Map List of files (filenames)")

#-------------------------------------------------------

class JournalInfoListItem(BaseModel):    # Same as SourceInfoListItem minus a few fields
    sourceType: str = Field(None, title="")
    PEPCode: str = Field(None, title="")
    bannerURL: str = Field(None, title="")
    displayTitle: str = Field(None, title="Reference format for this source")
    srcTitle: str = Field(None, title="Title of this source (from V1. Deprecated)")
    title: str = Field(None, title="Title of this source")
    abbrev: str = Field(None, title="")
    ISSN: str = Field(None, title="")
    language: str = Field(None, title="")
    yearFirst: str = Field(None, title="")
    yearLast: str = Field(None, title="")
    instanceCount: int = Field(None, title="Number of document instances for this source")
    embargoYears: str = Field(None, title="")
    # New fields 2021-11-30
    accessClassification: str = Field(None, title="Document classification, e.g., Archive, Current, Free, OffSite")
    pubSourceURL: str = Field(None, title="URL of the original publication if applicable)")
    PEPRelease: str = Field(None, title="Year this was first published in the database")
    # New fields 2023-10-30
    publisher: str = Field(None, title="Publisher of this source")
    
class JournalInfoStruct(BaseModel):
    responseInfo: ResponseInfo
    responseSet: List[JournalInfoListItem] = []

class JournalInfoList(BaseModel):
    sourceInfo: JournalInfoStruct

#-------------------------------------------------------
class ReportListItem(BaseModel):
    row: dict = Field({}, title="Fully flexible content report row from Database")
    
class ReportStruct(BaseModel):
    responseInfo: ResponseInfo
    responseSet: List[ReportListItem] = []

class Report(BaseModel):
    report: ReportStruct

# Tried this method when the normal mapping method was yielding {} in each row.  
# started yielding empty rows, but worked before: ret_val = [model(**row) for row in curs.fetchall()]
# explicit method worked: ret_val = [model(row=row) for row in curs.fetchall()] 
# 
# This workaround worked but with many complications that kept escalating.
#import copy
#from copy import deepcopy
#from typing import Any, Dict

#class ReportListItem_Alternate_Method(BaseModel):
    #__root__: Dict[Any, Any]

    #def __iter__(self):
        #return iter(self.__root__)

    #def __getattr__(self, item):
        #return self.__root__[item]
    
    #def __deepcopy__(self, item):
        #dpcpy = self.__class__()
        #item[id(self)] = dpcpy
        #for attr in dir(self):
            #if not attr.startswith('_'):
                #value = getattr(self, attr)
                #setattr(dpcpy, attr, copy.deepcopy(value, item))
        #return dpcpy        
 

#-------------------------------------------------------
# This is the model (SolrQuerySpec) 
# populated by parse_search_query_parameters from api parameter or body field requests
   
class SolrQuery(BaseModel):
    # Solr Query Parameters as generated by opasQueryHelper.parse_search_query_parameters
    searchQ: str = Field(None, title="Query in Solr syntax", description="Advanced Query in Solr Q syntax (see Field names)")
    filterQ: str = Field(None, title="Filter query in Solr syntax", description="Filter Query in Solr syntax (see Field names)")
    facetQ: str = Field(None, title="Facet Filter query in Solr syntax", description="Facet Filter Query in Solr syntax (see Field names)")
    semanticDescription: str = Field(None, title="Server's semantic description of the search")
    searchQPrefix: str = Field("", title="Prefix to searchQ", description="Prefix to SearchQ, e.g., for Level 2")
    # returnFields: str = Field(None, title="List of return fields", description="Comma separated list of return fields.  Default=All fields.")
    sort: str=Field(None, title="Fields and direction by which to sort", description="arranges search results in either ascending (asc) or descending (desc) order. The parameter can be used with either numerical or alphabetical content. The directions can be entered in either all lowercase or all uppercase letters (i.e., both asc or ASC).")
    queryTermList: List[str] = None
    # extra fields
    likeThisID: str = Field(None, title="DocumentID to use for reference to find documents like this.")
    analyzeThis: str = ""
    searchAnalysisTermList: List[str] = []

class SolrQueryOpts(BaseModel):
    # these all have proper defaults so technically, it can go into queries as is.
    qOper: str = Field("AND", title="Implied Boolean connector", description="Implied Boolean connector between words in query")
    defType: QueryParserTypeEnum = Field("lucene", title="Query parser to use, e.g., 'edismax'.  Default is 'standard' (lucene)")
    hl: str = Field('true', title="Highlighting (KWIC)", description="Turns on highlighting if specified.")
    hlFields: str = Field('text_xml', title="highlight fields (KWIC)", description="Specific fields to highlight.")
    hlMethod: str = Field("unified", title="Highlighter method", description="Use either unified (fastest) or original.")
    hlFragsize: str = Field(opasConfig.DEFAULT_KWIC_CONTENT_LENGTH, title="highlight fragment size", description="KWIC segment lengths")
    hlMaxAnalyzedChars: int = Field(2520000, title="The character limit to look for highlights, after which no highlighting will be done. This is mostly only a performance concern")
    hlMaxKWICReturns: int = Field(opasConfig.DEFAULT_MAX_KWIC_RETURNS, title="The character limit to look for highlights, after which no highlighting will be done. This is mostly only a performance concern")
    # I think this is redundant with hlMaxKWICReturns - verify
    hlSnippets: str = Field(None, title="Max KWIC Returns", description="Max number of highlights permitted per field")
    hlMultiterm: str = Field('true', title="If set to true, Solr will highlight wildcard queries (and other MultiTermQuery subclasses). If false, they won’t be highlighted at all.")
    hlTagPost: str = Field(opasConfig.HITMARKEREND, title="Markup (tag) after hit term")
    hlTagPre: str = Field(opasConfig.HITMARKERSTART, title="Markup (tag) before hit term")
    hlUsePhraseHighlighter: str = Field('true', title="Solr will highlight phrase queries (and other advanced position-sensitive queries) accurately – as phrases. If false, the parts of the phrase will be highlighted everywhere instead of only when it forms the given phrase.")
    queryDebug: str = Field("off", title="Turn Solr debug info 'on' or 'off' (default=off)")
    # facetFields: str = Field(None, title="Faceting field list (comma separated list)", description="Returns faceting counts if specified.")

    # hlQ: str = Field(None, title="Query to use for highlighting", description="allows you to highlight different terms than those being used to retrieve documents.")
    # maybe move these to a third part of SolrQuerySpec
    # moreLikeThis: bool = Field(False, title="", description="If set to true, activates the MoreLikeThis component and enables Solr to return MoreLikeThis results.")
    moreLikeThisCount: int = Field(0, title="MoreLikeThis count", description="Specifies the number of similar documents to be returned for each result. The default value is 5.")
    moreLikeThisFields: str = Field(None, title="MoreLikeThis fields", description="Specifies the fields to use for similarity. If possible, these should have stored termVectors.")

# Will try a dict approach first, rather than this.
#class FacetSpec(BaseModel):
    #mincount: int = Field(0, title="Minimum count for a facet to be included")
    #sort: str = Field("count", title="Either 'count' (order by count) or 'index' (alphabetical)")
    #prefix: str = Field(None, title="Limit terms to those starting with the prefix (if in the return set otherwise)")
    #contains: str = Field(None, title="Limit terms to those containing the substring (if in the return set otherwise)")
    #excludeTerms: str = Field(None, title="List of terms to exclude (comma separated list)")
    #facetRangesFields: str = Field(None, title="Faceting Range field list (comma separated list)")
    #facetRangesStart: int = Field(0, title="Minimum count for a facet to be included")
    #facetRangesEnd: int = Field(0, title="Minimum count for a facet to be included")
    #limit: int = Field(100, title="Facet Limit for Solr returns")
    #offset: int = Field(0, title="Facet Offset in return set")
    
class SolrQuerySpec(BaseModel):
    label: str = Field("default", title="User defined Label to save query spec")
    urlRequest: str = Field(None, title="URL Request Made")
    # for PEP Opas, this is almost always pepwebdocs
    core: str = Field("pepwebdocs", title="Selected core", description="Selected Solr core")
    fileClassification: str = Field(None, title="File Status: free, current, archive, or offsite", description="File Status: free, current, archive, or offsite")
    fullReturn: bool = Field(False, title="Request full length text return", description="Request full length text (XML) return (otherwise, return field length is capped)")
    abstractReturn: bool = Field(False, title="Request return of abstracts or summaries", description="Request return of abstracts or summaries")
    returnFieldSet: str = Field(None, title="Return field predefined set: DEFAULT, TOC, META, FULL, STAT, CONCORDANCE, applies only to AdvancedSearch. DOCUMENT_ITEM_SUMMARY_FIELDS is default.")
    returnFields: str = Field(None, title="List of return fields (ExtendedSearch Only)", description="Comma separated list of return fields.  Only applies to ExtendedSearch.")
    returnFormat: str = Field("HTML", title="Return type: XML, HTML, TEXT_ONLY", description="Return type applies to abstract and document fields only.")
    returnOptions: dict = Field({}, title="Dictionary of special options for return data (e.g., glossary=False, ...)")
    limit: int = Field(15, title="Record Limit for Solr returns")
    offset: int = Field(0, title="Record Offset in return set")
    page: int = Field(None, title="Page Number in return set")
    page_limit: int = Field(None, title="Page Limit for Solr returns")
    page_offset: int = Field(0, title="Page offset in return set")
    facetFields: str = Field(None, title="Faceting field list (comma separated list)", description="Returns faceting counts if specified.")
    facetMinCount: int = Field(1, title="Minimum count to return a facet")
    facetSort: str = Field(None, title="Fields on which to sort facets")
    facetPivotFields: str = Field(None, title="Fields to pivot on in Faceting (comma separated list)", description="Comma separated list of pivots, NO SPACES.")
    # TODO: facetRanges not yet implemented
    facetRanges: str = Field(None, title="Faceting range list (comma separated list)", description="Returns faceting ranges if specified.")
    # facetSpec can include any Solr facet options, except any that are listed above.
    # option names should use _ instead of Solr's "."
    # unfortunately, because _ is period, you cannot use options with field names embedded, if they have an _,
    #   e.g., f.art_year_int_facet_range_start won't work to set the facet range start for art_year_int
    # You should be able to do this if the field doesn't have an underscore, though we don't have int
    #  fields like that currently.
    facetSpec: dict = Field({}, title="Flexible Dictionary for facet specifications (using _ rather than .) Can include any Solr facet options, except any that are listed as fields above.")
    # sub structures
    solrQuery: SolrQuery = None
    solrQueryOpts: SolrQueryOpts = None

#-------------------------------------------------------
#  used to send individual components of the query, with individual options, to Opas (through API Body) 
#    which then processes it into a SolrQuery and sends to Solr.
class SolrQueryTermSub(BaseModel):
    connector: str = Field(" && ", title="Boolean connector to prev term.  Must be && (default) or ||")
    field: str = Field(None, title="Field to query")
    words: str = Field(None, title="if string field: string or pattern to match; if text field: words, phrase with or without proximity qualifier, boolean connectors")
    parent: str = Field(None, title="default parent to query or None")
    synonyms: bool = Field(False, title="Request thesaurus expansion") # to turn on thesaurus match (appends syn) Default = False
    synonyms_suffix: str = Field(opasConfig.SYNONYM_SUFFIX, title="suffix for field to use thesaurus; may not apply to all fields")

class SolrQueryTerm(BaseModel):
    connector: str = Field(" && ", title="Boolean connector to prev term.  Must be && (default) or ||")
    subClause: List[SolrQueryTermSub] = Field([], title="A sublist of query terms, to aggregate within parentheses; only connector is used with a SolrQueryTerm subclause")
    field: str = Field(None, title="Field to query")
    words: str = Field(None, title="if string field: string or pattern to match; if text field: words, phrase with or without proximity qualifier, boolean connectors")
    parent: str = Field(None, title="default parent to query or None")
    synonyms: bool = Field(False, title="Request thesaurus expansion") # to turn on thesaurus match (appends syn) Default = False
    synonyms_suffix: str = Field(opasConfig.SYNONYM_SUFFIX, title="suffix for field to use thesaurus; may not apply to all fields")
    
class SolrQueryTermList(BaseModel):
    artLevel: int = Field(None, title="1 for main document fields (e.g., text or title); 2 for child fields (e.g., para")
    qt: List[SolrQueryTerm] = []
    qf: List[SolrQueryTerm] = []
    solrQueryOpts: SolrQueryOpts = None
    similarCount: int = Field(0, title=opasConfig.TITLE_SIMILARCOUNT)
    returnFields: str = Field(None, title="List of return fields (ExtendedSearch Only)", description="Comma separated list of return fields.  Only applies to ExtendedSearch.")
    returnFormat: str = Field("HTML", title="Return type: XML, HTML, TEXT_ONLY", description="Return type: XML, HTML, TEXT_ONLY")
    #fullReturn: bool = Field(False, title="Request full length text return", description="Request full length text (XML) return (otherwise, return field length is capped)")
    abstractReturn: bool = Field(False, title="Request return of abstracts or summaries", description="Request return of abstracts or summaries")
    facetFields: str = Field(None, title="Faceting field list (comma separated list)", description="Returns faceting counts if specified.")
    facetMinCount: int = Field(1, title="Minimum count to return a facet")
    # TODO: facetRanges not yet implemented
    facetRanges: str = Field(None, title="Faceting range list (comma separated list)", description="Returns faceting ranges if specified. (Not Yet Implemented)")
    # facetSpec can include any Solr facet options, except any that are listed above.
    # option names should use _ instead of Solr's "."
    facetSpec: dict = Field({}, title="Flexible Dictionary for facet specifications (use _ rather than .) ")

#-------------------------------------------------------
# advanced Solr Raw return
class SolrReturnItem(BaseModel):
    solrRet: dict
        
class SolrReturnStruct(BaseModel):
    responseInfo: ResponseInfo
    responseSet: list # pysolr.results

class SolrReturnList(BaseModel):
    solrRetList: SolrReturnStruct

#-------------------------------------------------------
class SourceInfoListItem(BaseModel):    
    sourceType: str = Field(None, title="")
    PEPCode: str = Field(None, title="")
    bookCode: str = Field(None, title="Like PEPCode (srcCode) but specialized for books where many books fall under the same src_code")
    documentID: str = Field(None, title="OPAS ID for this document")
    bannerURL: str = Field(None, title="Graphical banner/logo for this source (URL)")
    publisher: str = Field(None, title="Publisher name")
    displayTitle: str = Field(None, title="Reference format for this source")
    srcTitle: str = Field(None, title="Title of this source (from V1. Deprecated)")
    title: str = Field(None, title="Title of this source")
    authors: str = Field(None, title="Authors of this source")
    pub_year: str = Field(None, title="Year this was first published in the database")
    abbrev: str = Field(None, title="Short form of the source, e.g., as used in references")
    ISSN: str = Field(None, title="The ISSN for a journal or source that has an ISSN")
    ISBN10: str = Field(None, title="The ISBN10 for a book, if available")
    ISBN13: str = Field(None, title="The ISBN13 for a book, if available")
    language: str = Field(None, title="Publication language (mainly)")
    yearFirst: str = Field(None, title="First year available for this source")
    yearLast: str = Field(None, title="Last year available for this source")
    instanceCount: int = Field(None, title="Number of document instances for this source")
    embargoYears: str = Field(None, title="")
    # these are not all currently used
    accessClassification: str = Field(None, title="Document classification, e.g., archive, current, free, offSite") 
    accessLimited: bool = Field(True, title="Access is limited, preventing full-text return")
    accessLimitedReason: str = Field(None, title="Explanation of user's access to this")
    accessLimitedDebugMsg: str = Field(None, title="Developer level info for tracing access issues")
    accessLimitedDescription: str = Field(None, title="Description of the access limitation applied")
    accessLimitedClassifiedAsCurrentContent: bool = Field(None, title="Access is limited by embargo to this specific content")
    accessLimitedPubLink: str = Field(None, title="Link to the document or publisher in some cases where doc's not readable on PEP")
    # New fields 2021-11-30
    pubSourceURL: str = Field(None, title="URL of the original publication if applicable)")
    PEPRelease: str = Field(None, title="Year this was first published in the database")
    

class SourceInfoStruct(BaseModel):
    responseInfo: ResponseInfo
    responseSet: List[SourceInfoListItem] = []

class SourceInfoList(BaseModel):
    sourceInfo: SourceInfoStruct

#-------------------------------------------------------
class TermIndexItem(BaseModel):
    field: str = Field(None, title="The field where the term indexed was checked.")
    term: str = Field(None, title="The term as indexed by the system.")
    termCount: int = Field(None, title="The number of documents with this term.")
    
class TermIndexStruct(BaseModel):
    responseInfo: ResponseInfo
    responseSet: List[TermIndexItem] = []

class TermIndex(BaseModel):
    termIndex: TermIndexStruct

#-------------------------------------------------------
class VideoInfoListItem(BaseModel):    # Same as SourceInfoListItem minus a few fields
    sourceType: str = Field(None, title="")
    PEPCode: str = Field(None, title="")
    documentID: str = Field(None, title="OPAS ID for this document")
    bannerURL: str = Field(None, title="")
    displayTitle: str = Field(None, title="Reference format for this source")
    title: str = Field(None, title="Title of this source")
    authors: str = Field(None, title="Authors of this source")
    pub_year: str = Field(None, title="Year this was first published in the database")
    abbrev: str = Field(None, title="")
    ISSN: str = Field(None, title="")
    ISBN10: str = Field(None, title="The ISBN10 for a book (or potentially a video), if available")
    ISBN13: str = Field(None, title="The ISBN13 for a book (or potentially a video), if available")
    language: str = Field(None, title="")
    yearFirst: str = Field(None, title="")
    yearLast: str = Field(None, title="")
    instanceCount: int = Field(None, title="Number of document instances for this source")
    embargoYears: str = Field(None, title="")
    # these are not all currently used
    accessClassification: str = Field(None, title="Document classification, e.g., Archive, Current, Free, OffSite")
    accessLimited: bool = Field(True, title="Access is limited, preventing full-text return")
    accessLimitedReason: str = Field(None, title="Explanation of user's access to this")
    accessLimitedDebugMsg: str = Field(None, title="Developer level info for tracing access issues")
    accessLimitedDescription: str = Field(None, title="Description of the access limitation applied")
    accessLimitedClassifiedAsCurrentContent: bool = Field(None, title="Access is limited by embargo to this specific content")
    accessLimitedPubLink: str = Field(None, title="Link to the document or publisher in some cases where doc's not readable on PEP")
    # New fields 2021-11-30
    pubSourceURL: str = Field(None, title="URL of the original publication if applicable)")
    PEPRelease: str = Field(None, title="Year this was first published in the database")

class VideoInfoStruct(BaseModel):
    responseInfo: ResponseInfo
    responseSet: List[VideoInfoListItem] = []

class VideoInfoList(BaseModel):
    sourceInfo: VideoInfoStruct

#-------------------------------------------------------
class VolumeListItem(BaseModel):
    PEPCode: str = Field(None, title="")
    vol: str = Field(None, title="")
    year: str = Field(None, title="")
    years: list = Field([], title="")
    count: int = Field(0, title="")
    
#-------------------------------------------------------

class VolumeListStruct(BaseModel):
    responseInfo: ResponseInfo
    responseSet: List[VolumeListItem] = []   

class VolumeList(BaseModel):
    volumeList: VolumeListStruct

#-------------------------------------------------------
class YearListItem(BaseModel):
    PEPCode: str = Field(None, title="")
    vol: str = Field(None, title="")
    year: str = Field(None, title="")
    vols: list = Field([], title="")
    count: int = Field(0, title="")
    
#-------------------------------------------------------

class YearListStruct(BaseModel):
    responseInfo: ResponseInfo
    responseSet: List[YearListItem] = []   

class YearList(BaseModel):
    yearList: YearListStruct
    
#-------------------------------------------------------
class WhatsNewListItem(BaseModel):
    displayTitle: str = Field(None, title="")
    abbrev: str = Field(None, title="")
    volume: str = Field(None, title="")
    issue: str = Field(None, title="")
    year: str = Field(None, title="")
    PEPCode: str = Field(None, title="")
    srcTitle: str = Field(None, title="")
    updated: str = Field(None, title="")
    volumeURL: str = Field(None, title="")

class BiblioListItem(BaseModel):
    ref_text: str = Field(None, title="")
    ref_local_id: str = Field(None, title="")
    ref_rx: str = Field(None, title="")
    ref_rxcf: str = Field(None, title="")

BiblioList = List[BiblioListItem]

class WhatsNewListStruct(BaseModel):
    responseInfo: ResponseInfo
    responseSet: List[WhatsNewListItem] = []   

class WhatsNewList(BaseModel):
    whatsNew: WhatsNewListStruct

# added 2021-4-17 for advanced smartsearch, but changed method, so not needed
# was in deprecated module (actually never used...will eventually delete)
#class AnalyzedInputString(BaseModel):
    #inputString: str = None
    #textToSearch: str = None    
    #inputStringNoStopWords: list = []
    #stop_words: list = []
    #words: list = []
    #noninitials: list = []
    #nonBooleanWords: list = []
    #booleanWords: list = []
    #capitalized: list = []
    #theNameList: list = []
    #humanNameList: list = []
    #hasStopWords: bool = False
    #isBooleanStrict: bool = False
    #isBooleanLoose: bool = False
    #isBoolean: bool = False
    #wordsAllCapitalized: bool = False
    #hasBooleanWords: bool = False
    
#-------------------------------------------------------
# Perhaps use termindex instead
#class WordIndexItem(BaseModel):
    #word: str = Field(None, title="Word indexed by the system.")
    #wordCount: int = Field(None, title="The number of times the word was found.")
    
#class WordIndexStruct(BaseModel):
    #responseInfo: ResponseInfo
    #responseSet: List[WordIndexItem] = []

#class WordIndex(BaseModel):
    #wordIndex: WordIndexStruct
    
if __name__ == "__main__":
    pass

