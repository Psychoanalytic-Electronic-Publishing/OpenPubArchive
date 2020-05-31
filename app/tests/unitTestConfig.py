#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This file has settings for the server tests, as well as a list of endpoint definitions to aid in
  developing tets.  Of ourse, you can see the OpenAPI Docs for documentation.
  
"""

# use the configured server.
from localsecrets import APIURL
# use this to test with whereever the local config points to 
base_api = APIURL
# or override below.
# base_api = "http://stage.pep.gvpi.net/api"
base_api = "http://127.0.0.1:9100" # local server
base_api = "http://development.org:9100"

# this must be set to the number of unique journals for testing to pass.
JOURNALCOUNT = 77
# this must be set to the exact number of unique books for testing to pass.
BOOKCOUNT = 100 # 100 book in 2020 on PEP-Web including 96 various ZBK, NLP, IPL books, + 4 special books: L&P, SE, GW, Glossary
VIDEOSOURCECOUNT = 12 # Number of video sources (video journal codes)
ARTICLE_COUNT_BJP = 2735 # Right.  2738 in everything with query "BJP (bEXP_ARCH1).xml", but 3 dups.
ARTICLE_COUNT_VOL1_BJP = 49
VOL_COUNT_ALL_JOURNALS = 2554
VOL_COUNT_ALL_BOOKS = 140
VOL_COUNT_ZBK = 70
VOL_COUNT_GW = 18
VOL_COUNT_SE = 24
VOL_COUNT_IPL = 22
VOL_COUNT_ALL_VOLUMES = 2580 #  journals and videos
VOL_COUNT_VIDEOS = 30
VOL_COUNT_VIDEOS_PEPVS = 4
VOL_COUNT_IJPSP = 11 #  source code ended, 11 should always be correct

# Can use constants for endpoints, which solves consistency in the tests, but I find it
#  harder to read (and I'd need to remove the parameters).  Left just for documentation sake
#  and moved to tests
ENDPOINT_V2_ADMIN_CREATEUSER = "/v2/Admin/CreateUser/"
ENDPOINT_V2_ADMIN_SENDALERTS = "/v2/Admin/SendAlerts/"
ENDPOINT_V2_SESSION_STATUS = "/v2/Session/Status/"
ENDPOINT_V2_SESSION_BASICLOGIN = "/v2/Session/BasicLogin/"
ENDPOINT_V2_WHOAMI = "/v2/Session/WhoAmI/"
ENDPOINT_V2_DATABASE_ALERTS = "/v2/Database/Alerts/"
ENDPOINT_V2_DATABASE_REPORTS = "/v2/Database/Reports/"
ENDPOINT_V2_DOCUMENTS_SUBMISSION = "/v2/Documents/Submission/"
ENDPOINT_V1_TOKEN = "/v1/Token/"
ENDPOINT_V1_STATUS_LOGIN = "/v1/Status/Login/"
ENDPOINT_V2_SESSION_LOGIN = "/v2/Session/Login/"
ENDPOINT_V1_LOGIN = "/v1/Login/"
ENDPOINT_V2_SESSION_LOGOUT = "/v2/Session/Logout/"
ENDPOINT_V1_LOGOUT = "/v1/Logout/"
ENDPOINT_V2_DATABASE_MORELIKETHESE = "/v2/Database/MoreLikeThese/"
ENDPOINT_V1_DATABASE_SEARCHANALYSIS = "/v1/Database/SearchAnalysis/"
ENDPOINT_V1_DATABASE_SEARCH = "/v1/Database/Search/"
ENDPOINT_V1_DATABASE_MOSTDOWNLOADED = "/v1/Database/MostDownloaded/"
ENDPOINT_V1_DATABASE_MOSTCITED = "/v1/Database/MostCited/"
ENDPOINT_V1_DATABASE_WHATSNEW = "/v1/Database/WhatsNew/"
ENDPOINT_V1_METADATA_CONTENTS_SOURCECODE = "/v1/Metadata/Contents/{SourceCode}/"
ENDPOINT_V1_METADATA_CONTENTS_SOURCECODE_SOURCEVOLUME = "/v1/Metadata/Contents/{SourceCode}/{SourceVolume}"
ENDPOINT_V1_METADATA_VIDEOS = "/v1/Metadata/Videos/"
ENDPOINT_V1_METADATA_VIDEOS = "/v1/Metadata/Books/"
ENDPOINT_V1_METADATA_JOURNALS = "/v1/Metadata/Journals/"
ENDPOINT_V1_METADATA_VOLUMES_SOURCECODE = "/v1/Metadata/Volumes/{SourceCode}/"
ENDPOINT_V1_AUTHORS_INDEX_AUTHORNAMEPARTIAL = "/v1/Authors/Index/{authorNamePartial}/"
ENDPOINT_V1_DOCUMENTS_ABSTRACTS_DOCUMENTID = "/v1/Documents/Abstracts/{documentID}/"
ENDPOINT_V2_DOCUMENTS_GLOSSARY_TERMID = "/v2/Documents/Glossary/{term_id}/"
ENDPOINT_V2_DOCUMENTS_DOCUMENT_DOCUMENTID = "/v2/Documents/Document/{documentID}/"
ENDPOINT_V1_DOCUMENTS_DOCUMENTID = "/v1/Documents/{documentID}/"
ENDPOINT_V1_DOCUMENTS_DOWNLOADS_RETFORMAT_DOCUMENTID = "/v1/Documents/Downloads/{retFormat}/{documentID}/"

def base_plus_endpoint_encoded(endpoint):
    ret_val = base_api + endpoint
    return ret_val


