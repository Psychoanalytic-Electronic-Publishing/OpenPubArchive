# -*- coding: UTF-8 -*-
#
#  This file is used to export Google Metadata.
#
#		Copyright N. Shapiro, Scilab Inc.
#
# Last Revised: 20211102
#
#

import string, sys, re, os, copy
import opasCentralDBLib
from opasProductLib import SourceInfoDB
import pymysql
#from PEPGlobals import *

#sys.path.append("e:\usr3\py\SCIHL")
#sys.path.append("e:\usr3\py\PEPXML")

#import struct
##import PEPReferenceMetaData
#import PEPReferenceParserBase
#import fileinput
#import sciSupport
#import libPEPBiblioDB
#import PEPArticleListDB
#import PEPLocator
#import sciDocuments
#from PEPGlobals import *
#import MySQLdb
#import sciFilePath
import stat
import time
import codecs

UNKNOWN = 0
LITBKREV = 1
LITJOUR = 2

DEBUG = 0
MAXCOUNT = 100

#============================================================================================
class PEPLibExport:
    """
    """

    tpl1 = """
			  <article>
			    <front>
			      <!-- Information about the journal. (REQUIRED) -->
			      <journal-meta>
			        <!-- Journal title. (REQUIRED; no more than 128 chars) -->
			        <journal-title>&SOURCETITLEFULL;</journal-title>
			        |<!-- Abbreviated Journal title. This can be repeated. (OPTIONAL; no more than 32 chars) -->
			        <abbrev-journal-title>&SOURCETITLESERIES;</abbrev-journal-title>|
			        <!-- ISSN for the journal. This can be repeated (REQUIRED; no more than 128 characters) -->
			        <issn>&ISSN;</issn>
			        |<!-- Metadata for the publisher. (OPTIONAL) -->
			        <publisher>
			           <!-- Name of publisher. (REQUIRED; no more than 128 characters) -->
			           <publisher-name>&PUBLISHER;</publisher-name>
			        </publisher>
			      </journal-meta>|
			      <!-- Information about the article. (REQUIRED) -->
			      <article-meta>
			        <!-- Various identifiers associated with the article. Currently, we use -->
			        <!-- doi, pmid, pmcid, sici, publisher-id. Others are allowed but ignored.  -->
			        <!-- (OPTIONAL; at most five entries) -->
			        <article-id pub-id-type="publisher-id">&KEY;</article-id>
			        <!-- Title of the article. (REQUIRED) -->
			        <title-group>
			          <!-- Title of the article. (REQUIRED; no more than 512 chars) -->
			          <article-title>&TITLE;</article-title>
			        </title-group>
			        |<!-- Information about the contributors. (REQUIRED) -->
			        <contrib-group>
					   &AUTHORMARKUP;
			        </contrib-group>|
			        <!-- Date of publication. (REQUIRED; Gregorian calendar) -->
			        <pub-date pub-type="pub">
			          <!-- Year of publication. Full four digit. (REQUIRED) -->
			          <year>&YEAR;</year>
			        </pub-date>
			        <!-- Volume. (REQUIRED) -->
			        <volume>&VOL;</volume>
			        |<!-- Issue number. (REQUIRED) -->
			        <issue>&ISSUE;</issue>|
			        <!-- First page. (REQUIRED) -->
			        <fpage>&PGSTART;</fpage>
			        <!-- Last page. (REQUIRED) -->
			        <lpage>&PGEND;</lpage>
			        <!-- URLs for the article (REQUIRED; no more than 1024 characters).  -->
			        <!-- Multiple entries are allowed and can refer to multiple formats  -->
			        <!-- It is CRUCIAL that at least one instance of this field be present -->
			        <self-uri xlink:href="https://pep-web.org/browse/document/&KEY;"/>
			      </article-meta>
			    </front>

			    <!-- Type of article. (OPTIONAL. acceptable values:  research-article -->
			    <!--  book, phd-thesis, ms-thesis, bs-thesis, technical-report, unpublished, -->
			    <!-- review-article, patent, other -->
			    <article-type>research-article</article-type>
			  </article>
			"""

    tplBookChapter = """
				<article>
				  <front>
				    <!-- Information about the book. (REQUIRED) -->
					|&BOOKMAINTOCMETA;|
				   |&BOOKCHAPTERMETA;|
				  </front>
				  <!-- Type of book or chapter. (OPTIONAL. acceptable values:  research-article -->
				  <!--  book-chapter, phd-thesis, ms-thesis, bs-thesis, technical-report, unpublished, -->
				  <!-- review-article, patent, other -->
				  <article-type>book-chapter</article-type>
				</article>
				"""

    tplBookMainTOCMeta = """
			<book-meta>
			  <!-- Book title. (REQUIRED; no more than 128 chars) -->
			  <book-title>&TITLE;</book-title>
			  <!-- ISBN for the book.  -->
			  <!-- (REQUIRED; no more than 128 characters) -->
			  <isbn>&ISBN;</isbn>
			  <!-- Metadata for the publisher. (REQUIRED) -->
			  <publisher>
			     <!-- Name of publisher. (REQUIRED; no more than 128 characters) -->
			     <publisher-name>&PUBLISHER;</publisher-name>
			  </publisher>
			  <!-- Author/editor names for the book. (REQUIRED; No more than 1024 entries) -->
			  <!-- If chapters have different authors, do not include them here -->
			  <!-- Acceptable values:  author, editor -->
			     <contrib-group>
			       &BOOKAUTHORMARKUP;
			     </contrib-group>
			  <!-- Date of publication. (REQUIRED; Gregorian calendar) -->
			  <pub-date pub-type="pub">
			    <!-- Year of publication. Full four digit. (REQUIRED) -->
			    <year>&YEAR;</year>
			  </pub-date>
			</book-meta>
	"""

    # this is part of the bookmeta, not for use on it's own
    tplChapterMeta = """
					<!-- Information about the chapter. (REQUIRED) -->
					<chapter-meta>
					    <title-group>
					      <!-- Title of the chapter. (REQUIRED; no more than 512 chars) -->
					      <chapter-title>&TITLE;</chapter-title>
					    </title-group>

					    <!-- Information about the chapter contributors. (OPTIONAL) -->
					    <contrib-group>
					      &AUTHORMARKUP;
					    </contrib-group>

					    |<!-- Chapter number. (REQUIRED) -->
					    <chapter-number>&CHAPTERNUMBER;</chapter-number>|

					    <!-- First page. (REQUIRED) -->
					    <fpage>&PGSTART;</fpage>

					    <!-- Last page. (REQUIRED) -->
					    <lpage>&PGEND;</lpage>

					    <!-- URLs for the chapter (REQUIRED; no more than 1024 characters).  -->
					    <!-- It is CRUCIAL that at least one instance of this field be present -->
					    <self-uri xlink:href="https://pep-web.org/browse/document/&KEY;"/>

					  </chapter-meta>
				"""

    tplAuth = """
					   <!-- Author names. (REQUIRED; No more than 1024 entries) -->
					   <contrib contrib-type="&ROLE;">
					     <name>

					       <!-- Last name of an author. (REQUIRED; No more than 32 characters -->
					       <surname>&NLAST;</surname>

					       <!-- Given names of an author - includes first and middle names if -->
					       <!-- any. (REQUIRED; no more than 48 characters -->
					       <given-names>&NFIRSTMID;</given-names>

					       |<!-- Suffix for the author - Jr/Sr/III etc. (OPTIONAL; no more than 8 chars) -->
					       <suffix>&NSUFX;</suffix>|

					   </name>
					 </contrib>
					  """

    tplNoAuth = """
					   <!-- Author name Template used when there are no authors because its REQUIRED -->
					   <contrib contrib-type="">
					     <name>
					       <surname></surname>
					       <given-names></given-names>
					   </name>
					 </contrib>
					  """

    tplPublisher = """
					<?xml version="1.0" encoding="UTF-8"?>  <!-- encoding must be UTF-8 -->
					<!DOCTYPE publisher SYSTEM "googlepublisher.dtd">
					<!-- Information related to content from one publisher for Google Scholar. -->
					<!-- This includes preferred name, contact person, and links to metadata files. -->

					<publisher>

					   <!-- Name of the publisher. (REQUIRED; no more than 128 chars) -->
					   <publisher-name>Psychoanalytic Electronic Publishing</publisher-name>

					   <!-- Location of the publisher. (OPTIONAL; no more than 32 chars) -->
					   <publisher-location>London, UK and California, USA</publisher-location>

					   <!-- Preferred name for use in Google Scholar. This name will appear -->
					   <!-- in Google Scholar results for urls included in the metadata files. -->
					   <!-- Please note that this name only appears if we are able to index the -->
					   <!-- url for the given result. (OPTIONAL; no more than 24 chars) -->
					   <publisher-result-name>PEP Web</publisher-result-name>

					   <!-- Contact email. (REQUIRED; upto five can be specified) -->
					   <contact>Neil Shapiro &lt;nrshapiro@gmail.com&gt; </contact>
					   <contact>David Tuckett &lt;d.tuckett@ucl.ac.uk&gt;</contact>
					   <contact>Nadine Levinson &lt;nadinelevinson@cs.com&gt;</contact>

					   <!-- Information about metadata files. Upto 10000 files can be specified.  -->
					   <!-- (REQUIRED) -->
					   <metadata-files>
					     &FILELIST;
					   </metadata-files>
					</publisher>

					"""

    tplPublisherFile = """
						<!-- Information about one file. -->
						<file>

						   <!-- Url for a metadata file. (REQUIRED; no more than 1024 chars) -->
						   <url>&FILENAME;</url>

						   <!-- The date on which this file was last updated. (OPTIONAL; -->
						   <!-- Gregorian calendar) -->
						   <change-date>
						      <!-- date of change. (REQUIRED) -->
						      <day>&DAY;</day>

						      <!-- month of change. (REQUIRED; `1-12) -->
						      <month>&MONTH;</month>

						      <!-- year of change. (REQUIRED; four digit) -->
						      <year>&YEAR;</year>
						   </change-date>
						</file>
						"""

    #--------------------------------------------------------------------------------
    def __init__(self, biblioDB, rootDir, startDir="", outBuildName="a1v7"):
        self.biblioDB = biblioDB
        # artSet = cursor.fetchall() # returns empty list if no rows
        self.rootDir = rootDir
        self.startDir = startDir
        self.fullStartDir = os.path.join(rootDir, startDir)
        ptnKeyThere = "#(.+?)@(.+?);"
        ptnKeyNotThere = "!(.+?)@(.+?);"
        self.regKeyThere = re.compile(ptnKeyThere)
        self.regKeyNotThere = re.compile(ptnKeyNotThere)
        self.cTD = "|"	# token delimter
        self.reStartTokenDelim = "&"
        reEndTokenDelim = ";"
        reBaseToken = self.reStartTokenDelim + "[A-Z][A-Z0-9-]*?" + reEndTokenDelim
        reTokenMarked = "\|[^|]*?" + reBaseToken + "[^|]*?\|"
        self.rgcBaseToken = re.compile(reBaseToken, re.IGNORECASE)
        self.rgcTokenMarker = re.compile(reTokenMarked)

    #--------------------------------------------------------------------------------
    def metaDataExport(self, outputFileName="X:\googleMetadata.xml", jrnlCodeWhereClause="", outputBuildName="FO01"):
        """
        Populate the build table from the articles table.   XXX CONTINUE WORK HERE XXX
        
        Need info from Solr similar to the articles table from pepa1db including
           articleID	varchar	24
           arttype	varchar	4
           authorMast	text	
           hdgauthor	text	
           hdgtitle	text	
           srctitleseries	text	
           publisher	varchar	255
           jrnlcode	varchar	14
           year	varchar	5
           vol	int	11
           volsuffix	char	5
           issue	char	5
           pgrg	varchar	20
           pgstart	int	11
           pgend	int	11
           pgcount	int	11
           source	varchar	10
           preserve	int	11
           filename	varchar	255
           maintocID	varchar	20
           newsecname	varchar	255
           bktitle	varchar	255
           bkauthors	varchar	255
           xmlref	text	
           references	int	11
           doi	varchar	255
           artkwds	varchar	384
           artlang	varchar	255

           but may only need these columns:
		     art_id,
             arttype,
             hdgtitle,
             srctitleseries,
             publisher,
             jrnlcode,
             year,
             vol,
             issue,
             pgrg,
             pgstart,
             pgend,
             filename,
             mainTOCID

        Create the include list by calling class member	writeFFFInclude

        The intermediate tables are built, rather than using a transitional query, in order to
        correctly populate the author information, which is used for sorting.


        """
        retVal = 0

        # queries to be used
        selArt = r"""select art_id,
                            art_type,
                            art_title,
                            src_title_abbr,
                            bk_publisher,
                            bk_title,
                            bk_info_xml,
                            src_code,
                            art_year,
                            art_vol,
                            art_vol_suffix,
                            art_issue,
                            art_pgrg,
                            art_pgstart,
                            art_pgend,
                            filename,
                            main_toc_id
                            from api_articles
                            %s
                            order by art_id
            """ % jrnlCodeWhereClause

        selAuth = r"""select a.authorID as authorid,
                             a.last as nlast,
                             a.first as nfirst,
                             a.middle as nmid,
                             arta.authorder as norder,
                             arta.authrole as role,
                             a.suffix as nsufx
                      from artauthorindex arta, authors a
                      where arta.articleID = '%s'
                      and arta.authorid = a.authorid
                      order by authrole, authorder
            """

        # select each article (dbc1)
        #   write initial data
        # 	for each author (dbc2)
        # 	write author data
        #    new record with new parameters
        
        cursor = self.biblioDB.db.cursor(pymysql.cursors.DictCursor)
        row_count = cursor.execute(selArt)
        artSet = cursor.fetchall() # returns empty list if no rows
        #cursor.close()

        #enf = open(outputFileName, "w")
        enf = codecs.open(outputFileName,'w','utf-8')
        header = """<?xml version="1.0" encoding="utf-8" ?>
<!DOCTYPE articles SYSTEM "googlearticles.dtd">
		"""
        enf.write(header)
        enf.write("<articles>\n")
        count = 0
        PEPArticleList = PEPArticleListDB.PEPArticleListDB()
        print("%s Articles for Export." % len(artSet))
        manFixErrors = []
        for art in artSet:
            useBookChap = False
            count += 1
            (articleID, artType, hdgtitle, srctitle, publisher, jrnlcode, year, vol, issue, pgrg, pgstart, pgend, filename, mainTOCID) = art
            artRefList = PEPArticleList.fetchArticleReferenceFromArticleID(articleID)
            if "<" in hdgtitle:
                hdgtitle = sciSupport.doEscapesForPCData(hdgtitle)
                if 0: # not needed anymore
                    print(30*"*", "You need to manually fix this instance.", 30*"*")
                    manFixError = "Warning! Markup characters in heading title: '%s' " % hdgtitle
                    print(sciUnicode.unicode2Console(manFixError))
                    manFixErrors.append(manFixError)
                    print("*"*90)

            aLoc = PEPLocator.Locator(articleID)
            #isBook = aLoc.isBook()
            artRef = artRefList[0]
            #artRef[gConst.ARTICLEID] = articleID
            # blank line separates records

            artRef = self.validateArtLimits(artRef)
            #print artRef
            #raise "Stop"
            # For split books, there's data in another record set for the parent book.  Get it!
            if aLoc.isSplitBook():
                #print "Arttype: ", artType, "MainToc: ", mainTOCID
                if artType == "TOC" and mainTOCID == None:
                    mainTOCID = articleID
                    #print "MainTOC set to ArticleID for TOC instance: ", articleID
                elif mainTOCID == None:
                    print("MainTOC set to ArticleID since there was none, probably TOC instance: ", articleID)
                    mainTOCID = articleID

                bkRef = PEPArticleList.fetchArticleReferenceFromArticleID(mainTOCID)
                #print bkRef[0]
                if bkRef == []:
                    raise "Error, no book matches this mainTOCID: %s" % mainTOCID
                else:
                    # now we have the mainTOC article information, we need the author info
                    dbc2.execute(selAuth % mainTOCID)
                    authSet = dbc2.fetchall()
                    # check to see if it has any authors
                    mainTOCAuthorString = ""
                    if authSet == ():
                        pass  # Leave this out per Darcy
                        #mainTOCAuthorString = self.tplNoAuth + "\n"
                    else:
                        for auth in authSet:
                            self.setPrenames(auth)
                            auth = self.validateAuthLimits(auth)
                            if opasgenlib.is_empty(auth[gConst.AUTHORNAMEROLE]):
                                print("No author role.  Set to 'author'")
                                auth[gConst.AUTHORNAMEROLE] = "author"
                            else:
                                pass
                                #print "Author role: ", auth[gConst.AUTHORNAMEROLE]

                        mainTOCAuthorString += self.applyTemplate(auth, self.tplAuth) + "\n"

                    # now take data and populate template
                    bkRef[0]["BOOKAUTHORMARKUP"] = mainTOCAuthorString
                    artRef["BOOKMAINTOCMETA"] = self.applyTemplate(bkRef[0], self.tplBookMainTOCMeta)
                    useBookChap = True
            elif aLoc.isBook():		# a all-in-one book
                bkRef = artRefList
                # now we have the mainTOC article information, we need the author info
                dbc2.execute(selAuth % articleID)
                authSet = dbc2.fetchall()
                # check to see if it has any authors
                mainTOCAuthorString = ""
                if authSet == ():
                    pass  # Leave this out per Darcy
                    #mainTOCAuthorString = self.tplNoAuth + "\n"
                else:
                    for auth in authSet:
                        self.setPrenames(auth)
                        auth = self.validateAuthLimits(auth)
                        if opasgenlib.is_empty(auth[gConst.AUTHORNAMEROLE]):
                            print("No author role.  Set to 'author'")
                            auth[gConst.AUTHORNAMEROLE] = "author"
                        else:
                            pass

                        mainTOCAuthorString += self.applyTemplate(auth, self.tplAuth) + "\n"

                # now take data and populate template
                bkRef[0]["BOOKAUTHORMARKUP"] = mainTOCAuthorString
                artRef["BOOKMAINTOCMETA"] = self.applyTemplate(bkRef[0], self.tplBookMainTOCMeta)

            # now we have the article information, we need the author info
            dbc2.execute(selAuth % articleID)
            authSet = dbc2.fetchall()
            # check to see if it has any authors
            authorString = ""
            #print "Authset: ", authSet
            if authSet == ():
                pass  # Leave this out per Darcy
                # authorString = self.tplNoAuth + "\n"
            else:
                for auth in authSet:
                    self.setPrenames(auth)
                    if opasgenlib.is_empty(auth[gConst.AUTHORNAMEROLE]):
                        #print "No author role.  Set to 'author'"
                        auth[gConst.AUTHORNAMEROLE] = "author"
                    else:
                        pass
                        #print "Author role: ", auth[gConst.AUTHORNAMEROLE]
                    auth = self.validateAuthLimits(auth)
                    authorString += self.applyTemplate(auth, self.tplAuth) + "\n"

            # now take data and populate template
            artRef["AUTHORMARKUP"] = authorString
            # artRef[gConst.SOURCETITLEFULL] = gJrnlData.getJournalFull(jrnlcode) # TODO: Refactor to replace
            #print artRef[gConst.SOURCETITLEFULL]

            if aLoc.isBook():
                bookChapterMeta = self.applyTemplate(artRef, self.tplChapterMeta)
                artRef["BOOKCHAPTERMETA"] = bookChapterMeta
                result = self.applyTemplate(artRef, self.tplBookChapter)
            else:
                result = self.applyTemplate(artRef, self.tpl1)
            #print result
            enf.write(result+"\n")

            #if count > 2: break

        #print "%d article records exported" % count
        enf.write("</articles>\n")
        dbc1.close()
        dbc2.close()
        enf.close()

        return len(artSet), manFixErrors

    #--------------------------------------------------------------------------------
    def validateArtLimits(self, artRef):
        """
        	<!-- Journal title. (REQUIRED; no more than 128 chars) -->
        	<!-- Abbreviated Journal title. This can be repeated. (OPTIONAL; no more than 32 chars) -->
        	<!-- ISSN for the journal. This can be repeated  (REQUIRED; no more than 128 characters) -->
        	<!-- Name of publisher. (REQUIRED; no more than 128 characters) -->
        	<!-- Title of the article. (REQUIRED; no more than 512 chars) -->
        	<!-- Various identifiers associated with the article. Currently, we use doi, pmid, pmcid, sici, publisher-id. Others are allowed but ignored.  (OPTIONAL; at most five entries) -->
        	<!-- Year of publication. Full four digit. (REQUIRED) -->

        """

        limitTitle = 128
        limitAbbr = 32

        if len(artRef[gConst.TITLE]) > limitTitle: artRef[gConst.TITLE] = artRef[gConst.TITLE][0:limitTitle]
        #print "Issue: ", artRef[gConst.ISSUE], artRef[gConst.TITLE]
        if opasgenlib.is_emptyOrNoneStr(artRef[gConst.ISSUE]):
            artRef[gConst.ISSUE] = ""

        if artRef[gConst.ISSUE] == "S":
            artRef[gConst.ISSUE] = "Suppl."

        # Get rid of arbitrarily assigned volumes
        if artRef[gConst.SOURCEPEPCODE] == "ZBK":
            artRef[gConst.VOL] = ""

        if opasgenlib.is_emptyOrNoneStr(artRef[gConst.PUBLISHER]):
            artLoc = PEPLocator.Locator(artRef[gConst.KEY])
            if artLoc.isBook():
                artBaseCode = artLoc.baseCode()
            else:
                artBaseCode = artRef[gConst.SOURCEPEPCODE]

            #print "artBaseCode:", artBaseCode
            # get Publisher information
            issnRec = self.biblioDB.getISSNRecord(artBaseCode)
            artRef[gConst.PUBLISHER] = issnRec.get(gConst.PUBLISHER, "Psychoanalytic Electronic Publishing")
            #print issnRec
            #print "Publisher: ", issnRec[gConst.PUBLISHER]

        return artRef
    #--------------------------------------------------------------------------------
    def validateAuthLimits(self, auth):
        """
        	<!-- Author names. (REQUIRED; No more than 1024 entries) -->
        	<!-- Last name of an author. (REQUIRED; No more than 32 characters -->
        	<!-- Given names of an author - includes first and middle names if any. (REQUIRED; no more than 48 characters -->
        	<!-- Suffix for the author - Jr/Sr/III etc. (OPTIONAL; no more than 8 chars) -->

        """
        limitLastName = 32
        limitFirstName = 48
        limitSuffix = 8

        if len(auth[gConst.AUTHORNAMEFIRSTMID]) > limitFirstName:
            auth[gConst.AUTHORNAMEFIRSTMID] = auth[gConst.AUTHORNAMEFIRSTMID][0:limitFirstName]
        if len(auth[gConst.AUTHORNAMELAST]) > limitLastName: auth[gConst.AUTHORNAMELAST] = auth[gConst.AUTHORNAMELAST][0:limitLastName]
        if opasgenlib.is_emptyOrNoneStr(auth[gConst.AUTHORNAMESUFX]):
            auth[gConst.AUTHORNAMESUFX] = ""
        else:
            if len(auth[gConst.AUTHORNAMESUFX]) > limitSuffix:
                auth[gConst.AUTHORNAMESUFX] = auth[gConst.AUTHORNAMESUFX][0:limitSuffix]


        return auth

    #--------------------------------------------------------------------------------
    def setPrenames(self, auth):

        auth[gConst.AUTHORNAMEFIRSTMID] = ""
        if not opasgenlib.is_emptyOrNoneStr(auth[gConst.AUTHORNAMEFIRST]):
            if not opasgenlib.is_emptyOrNoneStr(auth[gConst.AUTHORNAMEMID]):
                auth[gConst.AUTHORNAMEFIRSTMID] = auth[gConst.AUTHORNAMEFIRST] + ", " + auth[gConst.AUTHORNAMEMID]
            else:
                auth[gConst.AUTHORNAMEFIRSTMID] = auth[gConst.AUTHORNAMEFIRST]
        else:
            if not opasgenlib.is_emptyOrNoneStr(auth[gConst.AUTHORNAMEMID]):
                auth[gConst.AUTHORNAMEFIRSTMID] = auth[gConst.AUTHORNAMEMID]

        return auth

    #--------------------------------------------------------------------------------
    def writePublisherFile(self, rootDir, doValidate=False):

        #import sciHLXMLParsers

        pubRef = {
            "YEAR": "2006",
            "MONTH": "12",
            "DAY": "15",
        }

        filenames = sciFilePath.getDirFileList(
            baseDir=rootDir,
            baseFilenamePattern=".*",
            baseExtensionPattern="XML")

        filenameBase = "\publisher-info.xml"
        enf = open(rootdir + filenameBase, "w")
        fileInfo = {}
        publisherFileresult = ""
        count = 0
        for filename in filenames:
            count += 1
            #if count == 5: break
            fnv = sciFilePath.fileNameVerSys(filename)
            basename = fnv.getBasename()
            fstatout = os.stat(filename)
            fileTime = fstatout[stat.ST_MTIME]
            #fileTimeAlt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(fileTime))
            year, month, day, hour, minute, second, weekday, day360, dst = time.localtime(fileTime)
            #print "File Date/Time: ", fileTime, fileTimeAlt
            #print year, month, day, hour, minute, second, weekday, day, dst
            fileInfo["DAY"] = day
            fileInfo["YEAR"] = year
            fileInfo["MONTH"] = month
            fileInfo["FILENAME"] = "http://peparchive.org/links/pepwebmeta/%s.xml" % (basename)
            publisherFileresult += pepExport.applyTemplate(fileInfo, self.tplPublisherFile)
            #if doValidate == True:
                #(warningErrors, validationErrors, fatalErrors, statusStr2, errorList)  = sciHLXMLParsers.validateXML(filename, catalogName=r"x:\_PEPA1\catalog.cat")
                #print((warningErrors, validationErrors, fatalErrors, statusStr2, errorList))

        pubRef["FILELIST"] = publisherFileresult
        result = pepExport.applyTemplate(pubRef, self.tplPublisher)
        enf.write(result)
        print("%s files found; written to publisher record." % count)
        enf.close()
        return

    #--------------------------------------------------------------------------------
    def applyTemplate(self, dictData, template):
        """
        Apply the specified template to the data in dictData

          >>> retVal = u'\n\t\t\t  <article>\n\t\t\t    <front>\n\t\t\t      <!-- Information about the journal. (REQUIRED) -->\n\t\t\t      <journal-meta>\n\t\t\t        <!-- Journal title. (REQUIRED; no more than 128 chars) -->\n\t\t\t        <journal-title>&SOURCETITLEFULL;</journal-title>\n\t\t\t        |<!-- Abbreviated Journal title. This can be repeated. (OPTIONAL; no more than 32 chars) -->\n\t\t\t        <abbrev-journal-title>Rev. Psicoan\xe1l. Asoc. Psico. Madrid</abbrev-journal-title>|\n\t\t\t        <!-- ISSN for the journal. This can be repeated (REQUIRED; no more than 128 characters) -->\n\t\t\t        <issn>1135-3171</issn>\n\t\t\t        |<!-- Metadata for the publisher. (OPTIONAL) -->\n\t\t\t        <publisher>\n\t\t\t           <!-- Name of publisher. (REQUIRED; no more than 128 characters) -->\n\t\t\t           <publisher-name>&PUBLISHER;</publisher-name>\n\t\t\t        </publisher>\n\t\t\t      </journal-meta>|\n\t\t\t      <!-- Information about the article. (REQUIRED) -->\n\t\t\t      <article-meta>\n\t\t\t        <!-- Various identifiers associated with the article. Currently, we use -->\n\t\t\t        <!-- doi, pmid, pmcid, sici, publisher-id. Others are allowed but ignored.  -->\n\t\t\t        <!-- (OPTIONAL; at most five entries) -->\n\t\t\t        <article-id pub-id-type="publisher-id">&KEY;</article-id>\n\t\t\t        <!-- Title of the article. (REQUIRED) -->\n\t\t\t        <title-group>\n\t\t\t          <!-- Title of the article. (REQUIRED; no more than 512 chars) -->\n\t\t\t          <article-title>PRESENTACION</article-title>\n\t\t\t        </title-group>\n\t\t\t        |<!-- Information about the contributors. (REQUIRED) -->\n\t\t\t        <contrib-group>\n\t\t\t\t\t   &AUTHORMARKUP;\n\t\t\t        </contrib-group>|\n\t\t\t        <!-- Date of publication. (REQUIRED; Gregorian calendar) -->\n\t\t\t        <pub-date pub-type="pub">\n\t\t\t          <!-- Year of publication. Full four digit. (REQUIRED) -->\n\t\t\t          <year>&YEAR;</year>\n\t\t\t        </pub-date>\n\t\t\t        <!-- Volume. (REQUIRED) -->\n\t\t\t        <volume>&VOL;</volume>\n\t\t\t        |<!-- Issue number. (REQUIRED) -->\n\t\t\t        <issue>&ISSUE;</issue>|\n\t\t\t        <!-- First page. (REQUIRED) -->\n\t\t\t        <fpage>7</fpage>\n\t\t\t        <!-- Last page. (REQUIRED) -->\n\t\t\t        <lpage>&PGEND;</lpage>\n\t\t\t        <!-- URLs for the article (REQUIRED; no more than 1024 characters).  -->\n\t\t\t        <!-- Multiple entries are allowed and can refer to multiple formats  -->\n\t\t\t        <!-- It is CRUCIAL that at least one instance of this field be present -->\n\t\t\t        <self-uri xlink:href="https://pep-web.org/browse/document/&KEY;"/>\n\t\t\t      </article-meta>\n\t\t\t    </front>\n\n\t\t\t    <!-- Type of article. (OPTIONAL. acceptable values:  research-article -->\n\t\t\t    <!--  book, phd-thesis, ms-thesis, bs-thesis, technical-report, unpublished, -->\n\t\t\t    <!-- review-article, patent, other -->\n\t\t\t    <article-type>research-article</article-type>\n\t\t\t  </article>\n\t\t\t'
          >>> keyTemplate = '&SOURCETITLEFULL;'
          >>> n = u'Journal Revista de Psicoan\xc3\xa1lisis'
          >>> retVal = retVal.replace(keyTemplate, unicode(n))

        """

        #if gDbg1: print "Apply template to dictionary: ", dictData

        # assign the specified template
        if template!=None:
            retVal = copy.copy(template)
        else:
            raise "PEPLibExport: No Template Specified."

        for (key, n) in dictData.items():
            if n != None and n!="":	 # added "" clause 12/28/2005, so watch for side effects where caller expects empty string to be inserted! XXX
                keyTemplate = self.reStartTokenDelim + "%s;" % string.upper(key)
                #print "KeyTemplate: %s Val: %s" % (keyTemplate, n)
                retVal = str(retVal)
                try:
                    retVal = retVal.replace(keyTemplate, str(n))
                except Exception as e:
                    try:
                        retVal = retVal.replace(keyTemplate, n)
                    except Exception as e:
                        retVal = retVal.replace(keyTemplate, str(n, "utf8"))


        # Look for "there" conditional formats
        m = self.regKeyThere.search(retVal)
        # are there any conditional formatting strings?
        #print "Apply Conditional Template Changes: Key: %s, val: %s" % (key, n)
        while m != None:
            fullStr = m.group(0)
            fieldName = m.group(1)
            fieldFormat = m.group(2)
            #print "Field Name: %s, Field Format: '%s', Full String: %s" % (fieldName, fieldFormat, fullStr)
            fieldToFind = string.upper(fieldName)
            val = False
            for (key, n) in dictData.items():
                #print "Key, ToFind, Value", string.upper(key), fieldToFind, n
                if string.upper(key) == fieldToFind:
                    # Found unless value is empty
                    if not opasgenlib.is_empty(n):
                        val = True

                    # break in either case once the key is found
                    break

            if val:
                #print "Field %s there, put in conditional!" % fieldName
                repl = fieldFormat
            else:
                repl = ""

            # use string to match group 0 completely
            retVal = string.replace(retVal, fullStr, repl)
            #print "Replaced Format, Template Now: ", retVal

            # now see if we match again
            m = self.regKeyThere.search(retVal)

        # Look for not "there" conditional formats
        m = self.regKeyNotThere.search(retVal)
        # are there any conditional formatting strings?
        #if gDbg1: print "Apply Conditional Template Changes if Key not there. Key: %s, val: %s" % (key, n)
        while m != None:
            fullStr = m.group(0)
            fieldName = m.group(1)
            fieldFormat = m.group(2)
            #print "Field Name: %s, Field Format: '%s', Full String: %s" % (fieldName, fieldFormat, fullStr)
            fieldToFind = string.upper(fieldName)
            val = False
            for (key, n) in dictData.items():
                #print "Key, ToFind, Value", string.upper(key), fieldToFind, n
                if string.upper(key) == fieldToFind:
                    # Found unless value is empty
                    if not opasgenlib.is_empty(n):
                        val = True
                    # break in either case once the key is found
                    break

            if not val:
                #print "Field %s not there, put in conditional!" % fieldName
                repl = fieldFormat
            else:
                repl = ""

            # use string to match group 0 completely
            retVal = string.replace(retVal, fullStr, repl)
            #print "Replaced Format, Template Now: ", retVal

            # now see if we match again
            m = self.regKeyNotThere.search(retVal)


        m = self.rgcBaseToken.search(retVal)
        if m != None:
            # delete all untranslated tokens, between the markers
            retVal = self.rgcTokenMarker.sub("", retVal)
            # ok, if not || remove token
            retVal = self.rgcBaseToken.sub("", retVal)
            # now look again--any left?
            #m = self.rgcBaseToken.search(retVal)
            #if m is not None:
            #	log_error("Still untranslated tokens in template: '%s'" % retVal)

        #Remove any token delimiters (they will still be there from the sub areas that worked
        retVal = string.replace(retVal, self.cTD, "")
        #print "Result: ", retVal
        return retVal

#-------------------------------------------------------------------------------------------------
def doMetaDataExport(pepExport, rootdir, filenameBase, jrnlWhereClause):
    fullname = rootdir + filenameBase
    count, errs = pepExport.metaDataExport(outputFileName=fullname, jrnlCodeWhereClause=jrnlWhereClause, outputBuildName="FO01")
    if count == 0:
        os.remove(rootdir + filenameBase)
    return count, errs

#==================================================================================================
# Main Standalone (Test) Routines
#==================================================================================================

if __name__ == "__main__":
    """
	Run Tests of module routines
	"""

    import sys
    totalErrors = []

    ocd = opasCentralDBLib.opasCentralDB()

    biblioDB = ocd
    biblioDB.open_connection()
    rootdir = r"X:\_PEPa1\GoogleMetadata"
    pepExport = PEPLibExport(biblioDB, rootdir, outBuildName="a1v12")
    # Use these to turn on/off processing of journals, classic books, or SE respectively.
    #doJournals = False
    doJournals = True
    #doBooks = False
    doBooks = True
    #doSE = False
    doSE = True
    #doGW = False
    doGW = True
    productDB = SourceInfoDB()
    # try journal by journal/vol
    if doJournals:
        jrnlCodes = productDB.journalCodes()
        #jrnlCodes = SourceInfoDB.PEPA1JrnlCodesLastVer
        #jrnlCodes = ["IZPA"]  # for testing
        print("Journal Set: ", jrnlCodes)

        totalCount = 0
        for jrnl in jrnlCodes:
            #print "Processing: %s" % jrnl
            if jrnl in ["SE", "GW"]:
                continue

            jrnlCount = 0
            restrictedYears = productDB.volyears(jrnl)
            print("Journal Years: ", restrictedYears)
            #restrictedYears = ["86", "2009"]  # for testing
            for vol, year in restrictedYears:
                if type(vol) == type([]):
                    for volsub in vol:
                        jrnlWhereClause = "where src_code='%s' and art_vol='%s'" % (jrnl, volsub)
                        filenameBase = r"\%s.%s.xml" % (jrnl, year)
                        print("Processing %s: %s/%s to %s" % (jrnl, volsub, year, filenameBase))
                        count, errs = doMetaDataExport(pepExport, rootdir, filenameBase, jrnlWhereClause)
                        jrnlCount += count
                        totalCount += count
                        if errs!=[]: totalErrors.append((filenameBase, errs))

                else:
                    jrnlWhereClause = "where src_code='%s' and art_vol='%s'" % (jrnl, vol)
                    filenameBase = r"\%s.%s.xml" % (jrnl, year)
                    print("Processing %s: %s/%s to %s" % (jrnl, vol, year, filenameBase))
                    count, errs = doMetaDataExport(pepExport, rootdir, filenameBase, jrnlWhereClause)
                    jrnlCount += count
                    totalCount += count
                    if errs!=[]: totalErrors.append((filenameBase, errs))

            print("%s articles had metadata exported for Journal %s." % (jrnlCount, jrnl))

        print("%s total articles had metadata exported." % totalCount)

    if doBooks:
        # try book by book
        #enable this to try a specfic book
        #bookCodes = ["ZBK",]
        books = list(gClassicBookTOCList.keys())
        #books = ["ZBK.002.0001"]
        totalCount = 0
        for bookID in books:
            bookLoc = PEPLocator.Locator(bookID)
            book = bookLoc.jrnlCode
            vol =  bookLoc.jrnlVol
            bookWhereClause = "where jrnlcode='%s' and vol='%s'" % (book, vol)
            filenameBase = r"\%s.%s.xml" % (book, vol)
            print("Processing %s: %s to %s" % (book, vol, filenameBase))
            count, errs = doMetaDataExport(pepExport, rootdir, filenameBase, bookWhereClause)
            print("%s chapters in book %s" % (count, filenameBase))
            totalCount += count
            if errs!=[]: totalErrors.append((filenameBase, errs))

        print("%s total books had metadata exported." % totalCount)


    if doSE:
        # try book by book
        totalCount = 0
        for vol in range(1,25):
            book = "SE"
            bookWhereClause = "where jrnlcode='%s' and vol='%s'" % (book, vol)
            filenameBase = r"\%s.%03d.xml" % (book, vol)
            print("Processing %s: %s to %s" % (book, vol, filenameBase))
            count, errs = doMetaDataExport(pepExport, rootdir, filenameBase, bookWhereClause)
            print("%s chapters in SE %s" % (count, filenameBase))
            totalCount += count
            if errs!=[]: totalErrors.append((filenameBase, errs))

        print("%s total SE Volumes had metadata exported." % totalCount)

    if doGW:
        # try book by book
        totalCount = 0
        for vol in range(1,19):
            book = "GW"
            bookWhereClause = "where jrnlcode='%s' and vol='%s'" % (book, vol)
            filenameBase = r"\%s.%03d.xml" % (book, vol)
            print("Processing %s: %s to %s" % (book, vol, filenameBase))
            count, errs = doMetaDataExport(pepExport, rootdir, filenameBase, bookWhereClause)
            print("%s chapters in GW %s" % (count, filenameBase))
            totalCount += count
            if errs!=[]: totalErrors.append((filenameBase, errs))

        print("%s total GW Volumes had metadata exported." % totalCount)

    pepExport.writePublisherFile(rootdir)

    for err in totalErrors:
        print("%s - %s " % (err[0][1:], err[1]))