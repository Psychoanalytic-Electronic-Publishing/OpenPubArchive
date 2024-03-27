#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=C0321,C0330,R0915,R0914,C0326
# Disable many annoying pylint messages, warning me about variable naming for example.
# yes, in my code I'm caught between two worlds of snake_case and camelCase (transitioning to snake_case).

__author__      = "Neil R. Shapiro"
__copyright__   = "Copyright 2023, Psychoanalytic Electronic Publishing"
__license__     = "Apache 2.0"
__version__     = "2023.0607/v2.1.047"
__status__      = "Development"

# !!! IMPORTANT: Increment opasXMLProcessor version (if version chgd). It's written to the XML !!!

programNameShort = "opasDataLoader"

border = 80 * "*"
print (f"""\n
        {border}
            {programNameShort} - Open Publications-Archive Server (OPAS) XML Compiler/Loader
                            Version {__version__}
                   Document/Authors/References Compiler/Loader
        {border}
        """)

help_text = (
    fr""" 
        - Read the XML KBD3 files specified, process into EXP_ARCH in memory and load to Solr/RDS directly
        - Can also output and save EXP_ARCH (procesed files)
        - Can also load the database (Solr/RD) from EXP_ARCH1 files
        
        See documentation at:
          https://github.com/Psychoanalytic-Electronic-Publishing/OpenPubArchive-Content-Server/wiki/TBD  *** TBD ***
        
        Example Invocation:
                $ python opasDataLoader.py
                
        Important option choices:
         -h, --help         List all help options
         -a                 Force update of files (otherwise, only updated when the data is newer)
         --sub              Start with this subfolder of the root (can add sublevels to that)
         --key:             Do just one file with the specified PEP locator (e.g., --key AIM.076.0309A)
         --nocheck          Don't prompt whether to proceed after showing setting/option choices
         --reverse          Process in reverse
         --halfway          Stop after doing half of the files, so it can be run in both directions
         --whatsnewdays  Use the days back value supplied to write a new article log, rather than the specific files loaded.
                         Note that 1==today.
         --whatsnewfile  To specify the file in which to write the what's new list.
         --nofiles          Can be used in conjunction with whatsnewdays to simply produce the new article log rather than loading files.

         V.2 New Options (see default settings for many in loaderConfig.py)
         --inputbuild        input build name, e.g., (bKBD3) 
         --outputbuild       output build name, e.g., (bEXP_ARCH1) 
         --inputbuildpattern selection by build of what files to include
         --smartload         see if inputbuild file is newer or missing from db,
                             if so then compile and load, or
                             if there's changes to this article's records in api_biblioxml2
                             then compile and load
                             otherwise skip
         --nohelp            Turn off front-matter help (that displays when you run)
         --doctype           Output doctype (defaults to DEFAULT_DOCTYPE setting in loaderConfig.py)
         --rebuild           Rebuild all compiled XML files, then load into the database, even if not changed
         --reload            Reload all compiled XML files into the database, even if not changed
         
         We may not keep these...

         --prettyprint       Format generated XML (bEXP_ARCH1) nicely (currently does not work on AWS S3 since the file system won't let lxml write directly)
         --load              mainly for backwards compatibility, it loads the EXP_ARCH1 files as
                             input files by default, skipping as before if not updated. smartload should be enough.


        Example:
          Update all files from the root (default, pep-web-xml) down.  Starting two runs, one running the file list forward, the other backward.
          As long as you don't specify -a, they will skip the ones the other did when they eventually
          cross

             python opasDataLoader.py 
             python opasDataLoader.py --reverse

          Update all of PEPCurrent

             python opasDataLoader2.py -a --sub _PEPCurrent
             
          Generate a new articles log file for 10 days back
             
             python opasDataLoader.py --nofiles --whatsnewdays=10

          Import single precompiled file (e.g., EXP_ARCH1) only (no processing), verbose

             python opasDataLoader.py --verbose --key BIP.001.0342A --load --inputbuild=(bEXP_ARCH1)

          Import folder of precompiled files, even if the same (--rebuild).
             python opasDataLoader.py --verbose --sub _PEPCurrent\CFP\012.2022 --load --rebuild --inputbuild=(bEXP_ARCH1)
             
          Smart build folder of uncompiled XML files (e.g., bKBD3) if needed.
             python opasDataLoader.py --verbose --sub _PEPCurrent\CFP\012.2022 --smartload
                 

        Note:
          S3 is set up with root=localsecrets.FILESYSTEM_ROOT (default).  The root must be the bucket name.
          
          S3 has subfolders _PEPArchive, _PEPCurrent, _PEPFree, _PEPOffsite
            to allow easy processing of one archive type at a time simply using
            the --sub option (or four concurrently for faster processing).
    """
)

import sys
sys.path.append('../libs')
sys.path.append('../config')
sys.path.append('../libs/configLib')

import time
import pytz # when we get to python 3.9, this isn't needed
import random
import pysolr
import localsecrets
import re
import os
import os.path
import pathlib
import traceback
from opasFileSupport import FileInfo

import datetime as dtime
from datetime import datetime
import logging
logger = logging.getLogger(programNameShort)
from loggingDebugStream import log_everywhere_if    # log as usual, but if first arg is true, also put to stdout for watching what's happening

from optparse import OptionParser

import lxml
from lxml import etree
import pydantic

import configLib.opasCoreConfig
from configLib.opasCoreConfig import solr_authors2, solr_gloss2
import loaderConfig
import opasSolrLoadSupport
import opasArticleIDSupport
import opasBiblioSupport
import opasConfig

import opasXMLHelper as opasxmllib
import opasGenSupportLib as opasgenlib
import opasCentralDBLib

# import opasProductLib
import opasFileSupport
import opasAPISupportLib
import opasDataLoaderIJPOpenSupport
import opasPySolrLib
from opasMetadataCache import metadata_cache
cached_metadata = metadata_cache.get_cached_data()

if sys.version_info[0] < 3:
    raise Exception("Must be using Python 3")

#detect data is on *nix or windows system
if "AWS" in localsecrets.CONFIG or re.search("/", localsecrets.IMAGE_SOURCE_PATH) is not None:
    path_separator = "/"
else:
    path_separator = r"\\"

# for processxml (build XML or update directly without intermediate file)
import opasXMLProcessor

# Module Globals
fs_flex = None

def get_defaults(options, default_build_pattern, default_build):

    if options.input_build is not None:
        selected_build = options.input_build
    else:
        selected_build = default_build           
        
    if options.input_build_pattern is not None:
        build_pattern = options.input_build_pattern
    else:
        build_pattern = default_build_pattern
        
    return (build_pattern, selected_build)

def get_output_defaults(options, default_build):
    if options.output_build is not None:
        selected_build = options.output_build
    else:
        selected_build = default_build           
    return selected_build
#------------------------------------------------------------------------------------------------------
def find_all(name_pat, path):
    result = []
    name_patc = re.compile(name_pat, re.IGNORECASE)
    for root, dirs, files in os.walk(path):
        for filename in files:
            if name_patc.match(filename):
                result.append(os.path.join(root, filename))
    return result

#------------------------------------------------------------------------------------------------------
def derive_output_filename(input_filename, input_build=opasConfig.DEFAULT_INPUT_BUILD,
                           output_build=opasConfig.DEFAULT_OUTPUT_BUILD):
    
    filename = str(input_filename)
    ret_val = filename.replace(input_build, output_build)
    
    return ret_val

#------------------------------------------------------------------------------------------------------
def file_was_created_before(before_date, fileinfo):
    ret_val = False
    try:
        timestamp_str = fileinfo.date_str
        if timestamp_str < before_date:
            ret_val = True
        else:
            ret_val = False
    except Exception:
        ret_val = False # not found or error, return False
        
    return ret_val

#------------------------------------------------------------------------------------------------------
def file_was_created_after(after_date, fileinfo):
    ret_val = False
    try:
        timestamp_str = fileinfo.date_str
        if timestamp_str >  after_date:
            ret_val = True
        else:
            ret_val = False
    except Exception:
        ret_val = False # not found or error, return False
        
    return ret_val
#------------------------------------------------------------------------------------------------------
def file_was_loaded_to_solr_before(solrcore, before_date, art_id):
    ret_val = False
    try:
        result = opasSolrLoadSupport.get_file_dates_solr(solrcore, art_id=art_id)
        timestampstr = result[0]["timestamp"]
        timestamp = opasgenlib.utctimestampstr_to_timestamp(timestampstr)
        timestamp = timestamp.replace(tzinfo=None)
        if timestamp < before_date:
            ret_val = True
        else:
            ret_val = False
    except Exception as e:
        ret_val = True # not found or error, return true
        
    return ret_val

#------------------------------------------------------------------------------------------------------
def file_was_loaded_to_solr_after(solrcore, after_date, art_id):
    ret_val = False
    try:
        result = opasSolrLoadSupport.get_file_dates_solr(solrcore, art_id=art_id)
        timestampstr = result[0]["timestamp"]
        timestamp = opasgenlib.utctimestampstr_to_timestamp(timestampstr)
        timestamp = timestamp.replace(tzinfo=None)
        if timestamp > after_date:
            ret_val = True
        else:
            ret_val = False
    except Exception as e:
        ret_val = True # not found or error, return true
        
    return ret_val

#------------------------------------------------------------------------------------------------------
def output_file_needs_rebuilding(outputfilename, inputfilename=None, inputfilespec=None, art_id=None, ocd=None):
    """
    Checks and returns true if:
       - output (precompiled markup) file doesn't exist
       - input file is dated after the output file
       - if a reference in the biblio (via api_biblioxml2) has been updated
       
    Returns tuple with info:
         input_file_was_updated, infile_exists, outfile_exists, both_same
         
    """
    ret_val = False
    outfile_exists = True
    infile_exists = True
    both_same = False
    
    if inputfilespec is None:
        inputfilespec = FileInfo()
        exists = inputfilespec.mapFS(inputfilename) # if exists, data in fileinfo
        
    if inputfilename is None and inputfilespec is not None:
        inputfilename = inputfilespec.filespec
       
    if inputfilename != outputfilename:
        # see if inputfilename is older           
        try:
            # fileinfoout = FileInfo(fs=fs)
            fileinfoout = FileInfo()
            exists = fileinfoout.mapFS(outputfilename)
            if not exists:
                # need to build
                ret_val = True
                outfile_exists = False
            
            elif fileinfoout.timestamp < inputfilespec.timestamp:
                # need to rebuild
                ret_val = True

            elif art_id is not None:
                # get api_biblioxml2 last add date
                last_update = ocd.get_max_bibrecord_update(art_id)
                    
                if last_update is None:
                    ret_val = False # no biblio records
                else:
                    try:
                        east_tz = pytz.timezone("US/Eastern")
                        utc_tz = pytz.timezone("UTC")
                        file_updated_time = utc_tz.localize(fileinfoout.timestamp)
                        if localsecrets.S3_KEY is None:
                            # nrs - my local db is recording in my time zone ET
                            localized = east_tz.localize(last_update)
                            last_update = localized.astimezone(utc_tz)
                        else: # rds configured for utc time
                            localized = utc_tz.localize(last_update)
                            last_update = localized.astimezone(utc_tz)

                        if last_update >= file_updated_time:
                            ret_val = True
                
                    except Exception as e:
                        print (e)
                            
            else:
                ret_val = False
                    

        except Exception as e:
            print (e)
            logger.error(f"File checking error {e}")
            ret_val = False # no need to rebuild
    else:
        both_same = True
    
    return (ret_val, infile_exists, outfile_exists, both_same)

#------------------------------------------------------------------------------------------------------
def file_needs_reloading_to_solr(solrcore, art_id, timestamp_str, filename=None, fs=None, filespec=None, smartload=False,
                                 input_build=opasConfig.DEFAULT_INPUT_BUILD,
                                 output_build=opasConfig.DEFAULT_OUTPUT_BUILD):
    """
    Now, since Solr may have EXP_ARCH1 and the load 'candidate' may be KBD3, the one in Solr
      can be the same or NEWER, and it's ok, no need to reprocess.
      
      BUT: We need to see if there actually is a bEXP_ARCH1 
    """
    ret_val = True
    if filename is None:
        filename = art_id

    try:
        outputfname = filename.replace(input_build, output_build)
        result = opasSolrLoadSupport.get_file_dates_solr(solrcore, art_id=art_id, filename=outputfname)
        if result[0]["file_last_modified"] >= timestamp_str:
            ret_val = False # newer in solr
        else:
            ret_val = True

        if options.display_verbose: # To refresh or not to refresh
            try:
                filetime = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ")
                filetime = filetime.strftime("%Y-%m-%d %H:%M:%S")
                solrtime = result[0]['file_last_modified']
                solrtime = datetime.strptime(solrtime, "%Y-%m-%dT%H:%M:%SZ")
                solrtime = solrtime.strftime("%Y-%m-%d %H:%M:%S")
                if not ret_val:
                    pass
                    # print (f"Skipped - No refresh needed File {filename}: {filetime} vs Solr: {solrtime}")
                else:
                    print (f"Refresh needed File {filename}: {filetime} vs Solr: {solrtime}")

            except Exception as e:
                msg =f"Can't get file info {filename}"
                logger.error(msg)
                print (msg)

            
    except KeyError as e:
        ret_val = True # not found, return true so it's loaded anyway.
    except Exception as e:
        logger.info(f"File check error: {e}")
        ret_val = True # error, return true so it's loaded anyway.
        
    return ret_val 


#------------------------------------------------------------------------------------------------------
def file_is_same_or_newer_in_solr_by_artid(solrcore, art_id, timestamp_str, filename=None, fs=None,
                                           filespec=None, smartload=False, input_build=opasConfig.DEFAULT_INPUT_BUILD,
                                           output_build=opasConfig.DEFAULT_OUTPUT_BUILD):
    """
    Now, since Solr may have EXP_ARCH1 and the load 'candidate' may be KBD3, the one in Solr
      can be the same or NEWER, and it's ok, no need to reprocess.
      
      BUT: We need to see if there actually is a bEXP_ARCH1 
    """
    ret_val = False
    if filename is None:
        filename = art_id

    if smartload:
        try:
            inputfilename = filespec.fileinfo["name"]
        except KeyError as e:
            inputfilename = str(filespec.filespec)
            
        outputfname = inputfilename.replace(input_build, output_build)
        if inputfilename != outputfname:
            # see if inputfilename is older           
            try:
                # fileinfoout = FileInfo(fs=fs)
                fileinfoout = FileInfo()
                exists = fileinfoout.mapFS(outputfname)
                if not exists:
                    # need to build
                    ret_val = False
                else:    
                    if fileinfoout.date_modified <  filespec.date_modified:
                        # need to rebuild
                        ret_val = False
                    else:
                        ret_val = True

            except Exception as e:
                print (e)
                ret_val = True # no need to recompile
        else:
            result = opasSolrLoadSupport.get_file_dates_solr(solrcore, art_id=art_id)
            if len(result) >= 1:
                if result[0]["file_last_modified"] >= timestamp_str:
                    ret_val = True # newer in solr, no need to reload
                else:
                    ret_val = False
            else:
                ret_val = False # need to build
    else:
        try:
            outputfname = filename.replace(input_build, output_build)
            result = opasSolrLoadSupport.get_file_dates_solr(solrcore, art_id=art_id, filename=outputfname)
            if result[0]["file_last_modified"] >= timestamp_str:
                ret_val = True # newer in solr
            else:
                ret_val = False
    
            if options.display_verbose: # To refresh or not to refresh
                try:
                    filetime = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ")
                    filetime = filetime.strftime("%Y-%m-%d %H:%M:%S")
                    solrtime = result[0]['file_last_modified']
                    solrtime = datetime.strptime(solrtime, "%Y-%m-%dT%H:%M:%SZ")
                    solrtime = solrtime.strftime("%Y-%m-%d %H:%M:%S")
                    if ret_val:
                        print (f"Skipped - No Solr refresh needed File {filename}: {filetime} vs Solr: {solrtime}")
                    else:
                        print (f"Solr Refresh needed File {filename}: {filetime} vs Solr: {solrtime}")
    
                except Exception as e:
                    msg =f"Can't get file info {filename}"
                    logger.error(msg)
                    print (msg)
    
                
        except KeyError as e:
            ret_val = False # not found, return false so it's loaded anyway.
        except Exception as e:
            logger.info(f"File check error: {e}")
            ret_val = False # error, return false so it's loaded anyway.
        
    return ret_val 

#------------------------------------------------------------------------------------------------------
def file_is_same_as_in_solr(solrcore, filename, timestamp_str):
    ret_val = False
    try:
        result = opasSolrLoadSupport.get_file_dates_solr(solrcore, filename)
        if result[0]["file_last_modified"] == timestamp_str:
            ret_val = True
        else:
            ret_val = False
    except KeyError as e:
        ret_val = False # not found, return false so it's loaded anyway.
    except Exception as e:
        logger.info(f"File check error: {e}")
        ret_val = False # error, return false so it's loaded anyway.
        
    return ret_val

#------------------------------------------------------------------------------------------------------
def main():
    
    global options  # so the information can be used in support functions
    
    cumulative_file_time_start = time.time()
    randomizer_seed = None 

    # scriptSourcePath = os.path.dirname(os.path.realpath(__file__))

    art_reference_count = 0
    total_reference_count = 0
    processed_files_count = 0
    rebuild_count = 0
    reload_count = 0
    ocd =  opasCentralDBLib.opasCentralDB(reuse_connection=options.reuse_connection)
    # options.rootFolder defaults to localsecrets.FILESYSTEM_ROOT, unless option is specified otherwise
    # this allows relative paths, by specifying dataroot="", example:
    #       --dataroot="" --only "./../tests/testxml/_PEPSpecial/IJPOpen/IJPOPEN.008.0100A(bKBD3).xml"
    fs = opasFileSupport.FlexFileSystem(key=localsecrets.S3_KEY, secret=localsecrets.S3_SECRET, root=options.rootFolder)

    # set toplevel logger to specified loglevel
    logger = logging.getLogger()
    logger.setLevel(options.logLevel)
    # get local logger
    logger = logging.getLogger(programNameShort)

    logger.info('Started at %s', datetime.today().strftime('%Y-%m-%d %H:%M:%S"'))
    # logging.basicConfig(filename=logFilename, level=options.logLevel)

    solrurl_docs = None
    solrurl_authors = None
    solrurl_glossary = None
    if options.rootFolder == localsecrets.XML_ORIGINALS_PATH or options.rootFolder is None:
        start_folder = pathlib.Path(localsecrets.XML_ORIGINALS_PATH)
    else:
        start_folder = pathlib.Path(options.rootFolder)   
    
    pre_action_verb = "Load"
    post_action_verb = "Loaded"
    if 1: # (options.biblio_update or options.fulltext_core_update or options.glossary_core_update) == True:
        try:
            solrurl_docs = localsecrets.SOLRURL + configLib.opasCoreConfig.SOLR_DOCS  # e.g., http://localhost:8983/solr/    + pepwebdocs'
            solrurl_authors = localsecrets.SOLRURL + configLib.opasCoreConfig.SOLR_AUTHORS
            solrurl_glossary = localsecrets.SOLRURL + configLib.opasCoreConfig.SOLR_GLOSSARY
            # print("Logfile: ", logFilename)
            print("Messaging verbose: ", options.display_verbose)
            print("Input data Root: ", start_folder)
            print("Input data Subfolder: ", options.subFolder)
            print(f"Database: {localsecrets.DBNAME}")

            selected_output_build = opasConfig.DEFAULT_OUTPUT_BUILD

                
            if options.forceReloadAllFiles == True:
                options.smartload = True
                msg = "Forced Reload - All precompiled files reloaded, even if they are the same as in Solr. Smartload recompile is inferred--if the source XML is updated, they will be recompiled and added. "
                logger.info(msg)
                print (msg)

            if options.forceRebuildAllFiles == True:
                input_build_pattern, selected_input_build = get_defaults(options,
                                                                         default_build_pattern=loaderConfig.DEFAULT_INPUT_BUILD_PATTERN,
                                                                         default_build=opasConfig.DEFAULT_INPUT_BUILD)
                selected_output_build = get_output_defaults(options,
                                                            default_build=opasConfig.DEFAULT_OUTPUT_BUILD)
                pre_action_verb = "Compile, save and load"
                post_action_verb = "Compiled, saved and loaded"
                msg = f"Forced Rebuild - All specified files of build {input_build_pattern} recompiled from source XML to precompiled XML {selected_output_build} and loaded."
                logger.info(msg)
                print (msg)

            elif options.loadprecompiled and not options.smartload:
                input_build_pattern, selected_input_build = get_defaults(options,
                                                                         default_build_pattern=loaderConfig.DEFAULT_PRECOMPILED_INPUT_BUILD_PATTERN,
                                                                         default_build=opasConfig.DEFAULT_OUTPUT_BUILD)
                print(f"Precompiled XML of build {selected_input_build} will be loaded to the databases if newer than Solr, without examining source and compiling")
                pre_action_verb = "Load"
                post_action_verb = "Loaded"
                
            elif options.smartload:
                # compiled and loaded if input file is newer than output written file or if there's no output file
                input_build_pattern, selected_input_build = get_defaults(options,
                                                                         default_build_pattern=loaderConfig.DEFAULT_INPUT_BUILD_PATTERN,
                                                                         default_build=opasConfig.DEFAULT_INPUT_BUILD)
                selected_output_build = get_output_defaults(options,
                                                            default_build=opasConfig.DEFAULT_OUTPUT_BUILD)
                print(f"Smartload. XML of build {input_build_pattern} will be compiled and saved and loaded if newer than compiled build {selected_output_build}")
                pre_action_verb = "Smart compile, save and load"
                post_action_verb = "Smart compiled, saved and loaded"
                #selected_input_build = options.input_build
                
            if options.resetCoreData:
                print("Reset Core Data option selected. As of 2023, it does not clear api_articles nor api_biblioxml2. They are cleared via a postprocess program: ", options.resetCoreData)
            
            print(80*"*")
            print(f"Database will be updated. Location: {localsecrets.DBHOST}")
            if not options.glossary_only: # options.fulltext_core_update:
                print("Solr Full-Text Core will be updated: ", solrurl_docs)
                print("Solr Authors Core will be updated: ", solrurl_authors)

            # this is invoked only if you process a glossary core document ZBK.069, so it shouldn't need to be selective.
            if 1: # options.glossary_core_update:
                print("Solr Glossary Core will be updated: ", solrurl_glossary)
        
            print(80*"*")
            print ("Other Options:")
            if options.include_paras:
                print ("--includeparas is selected. Each paragraph will also be stored individually for *Docs* core. Increases core size markedly!")
            else:
                try:
                    print (f"--includeparas is OFF.  Paragraphs only stored for sources indicated in loaderConfig. Currently: [{', '.join(loaderConfig.SRC_CODES_TO_INCLUDE_PARAS)}]")
                except:
                    print ("Paragraphs only stored for sources indicated in loaderConfig.")
    
            if options.parse_only:
                print ("--parseonly is selected. Files will be parsed for xml errors but not processed.")

            if options.halfway:
                print ("--halfway option selected. Including approximately one-half of the files that match.")
                
            if options.run_in_reverse:
                print ("--reverse option selected. Running the files found in reverse order.")

            if options.continue_processing:
                print (f"--continue selected.  Include only files loaded to solr before {opasConfig.CONTINUE_PROCESSING_DAYS} days ago")
                
            if options.file_key:
                print (f"--key supplied.  Including files matching the article id {options.file_key}.\n   ...Automatically implies force rebuild (--smartload) and/or reload (--load) of files.")

            if options.glossary_term_tagging:
                print ("--termtags selected. Glossary terms will be marked up with impx for compiled XML.")

            if not options.add_glossary_term_dict:
                print ("--termdictoff selected. The glossary_term_dict unit will not be added to compiled XML.")

            print(80*"*")
            if not options.no_check:
                cont = input ("The above databases will be updated. Do you want to continue (y/n)?")
                if cont.lower() == "n":
                    print ("User requested exit. No data changed.")
                    sys.exit(0)
                
        except Exception as e:
            msg = f"cores specification error ({e})."
            print((len(msg)*"-"))
            print (msg)
            print((len(msg)*"-"))
            sys.exit(0)

    # import data about the PEP codes for journals and books.
    #  Codes are like APA, PAH, ... and special codes like ZBK000 for a particular book
    # sourceDB = opasProductLib.SourceInfoDB()
    solr_docs2 = None
    # The connection call is to solrpy (import was just solr)
    if localsecrets.SOLRUSER is not None and localsecrets.SOLRPW is not None:
        if 1: # options.fulltext_core_update:
            solr_docs2 = pysolr.Solr(solrurl_docs, auth=(localsecrets.SOLRUSER, localsecrets.SOLRPW))
    else: #  no user and password needed
        solr_docs2 = pysolr.Solr(solrurl_docs)

    # Reset core's data if requested (mainly for early development)
    if options.resetCoreData:
        if not options.glossary_only: # options.fulltext_core_update:
            if not options.no_check:
                cont = input ("The solr cores will be cleared.  Do you want to continue (y/n)?")
                if cont.lower() == "n":
                    print ("User requested exit.  No data changed.")
                    sys.exit(0)
            else:
                print ("Options --nocheck and --resetcore both specified.  Warning: The solr cores and the database artstat table will be cleared.  Pausing 60 seconds to allow you to cancel (ctrl-c) the run.")
                time.sleep(60)
                print ("Second Warning: Continuing the run (and core and database reset) in 20 seconds...")
                time.sleep(20)               

            msg = "*** Deleting all data from the docs and author cores, BUT NOT THE the articles, artstat, and biblio database tables ***"
            logger.warning(msg)
            print (msg)
            # As of 2023, do not delete RDS/MySQL article data during build
            # The api_biblioxml2 table is critical to linking, and is adjusted though opasDataLinker and manually to fix links
            # we may be able to delete the article data from api_articles, but I believe we have referential integrity constraints
            # on online which will delete the records from the bibliotable.
            # DISABLED: ocd.delete_all_article_data()
            solr_docs2.delete(q='*:*')
            solr_docs2.commit()
            solr_authors2.delete(q="*:*")
            solr_authors2.commit()

        # reset glossary core when others are reset, or when --resetcore is selected with --glossaryonly   
        if 1: # options.glossary_core_update:
            msg = "*** Deleting all data from the Glossary core ***"
            logger.warning(msg)
            print (msg)
            solr_gloss2.delete(q="*:*")
            solr_gloss2.commit()
    else:
        pass   # XXX Later - check for missing files and delete them from the core, since we didn't empty the core above

    # Go through a set of XML files
    # ########################################################################
    # Get list of files to process    
    # ########################################################################
    
    if options.subFolder is not None:
        start_folder = start_folder / pathlib.Path(options.subFolder)

    # record time in case options.nofiles is true
    timeStart = time.time()
    archived_files_not_loaded = 0
    files_with_parse_errors = 0
    
    if options.no_files == False: # process and/or load files (no_files just generates a whats_new list, no processing or loading)
        print (f"Locating files for processing at {start_folder} with build pattern {input_build_pattern}. Started at ({time.ctime()}).")
        print (f"Smartbuild Notes: Files matching {loaderConfig.SMARTBUILD_EXCEPTIONS} are load only, no recompile.  Will load from output format {selected_output_build}")
        if options.file_key is not None:  
            # print (f"File Key Specified: {options.file_key}")
            # Changed from opasDataLoader (reading in bKBD3 files rather than EXP_ARCH1)
            pat = fr"({options.file_key})\({input_build_pattern}\)\.(xml|XML)$"
            print (f"Reading {pat} files")
            filenames = fs.get_matching_filelist(filespec_regex=pat, path=start_folder)
            if len(filenames) is None:
                msg = f"File {pat} not found.  Exiting."
                logger.warning(msg)
                print (msg)
                exit(0)
            else:
                # options.forceRebuildAllFiles = True
                options.forceReloadAllFiles = True
        elif options.file_only is not None: # File spec for a single file to process.
            fileinfo = FileInfo()
            filespec = options.file_only
            exists = fileinfo.mapFS(filespec)
            if not exists:
                msg = f"File {filespec} not found.  Exiting. {os.getcwd()}"
                logger.warning(msg)
                print (msg)
                exit(0)
            else:
                filenames = [fileinfo]
                print (f"Filenames: {filespec}")
        else:
            # allow for SMARTBUILD_EXCEPTIONS filenames which are output only and have them in the list.
            pat = fr"(((.*?)\({input_build_pattern}\))|({loaderConfig.SMARTBUILD_EXCEPTIONS}(.*?)\({selected_output_build}\)))\.(xml|XML)$" # should we include the pattern including TOC?
            filenames = []
        
        if filenames == []:
            # get a list of all the XML files that are new
            if options.forceRebuildAllFiles or options.forceReloadAllFiles:
                # get a complete list of filenames for start_folder tree
                filenames = fs.get_matching_filelist(filespec_regex=pat, path=start_folder)
            else:
                filenames = fs.get_matching_filelist(filespec_regex=pat, path=start_folder, revised_after_date=options.created_after)
                
        print((80*"-"))
        files_found = len(filenames)
        if options.forceRebuildAllFiles or options.forceReloadAllFiles:
            #maybe do this only during core resets?
            #print ("Clearing database tables...")
            #ocd.delete_all_article_data()
            print(f"Ready to {pre_action_verb} records from {files_found} files at path {start_folder}")
        else:
            print(f"Ready to {pre_action_verb} {files_found} files *if modified* at path: {start_folder}")
    
        timeStart = time.time()
        print (f"Processing started at ({time.ctime()}).")
    
        print((80*"-"))
        precommit_file_count = 0
        skipped_files = 0
        stop_after = 0
        cumulative_file_time_start = time.time()
        issue_updates = {}
        total_reference_count = 0
        if files_found > 0:
            if options.halfway:
                stop_after = round(files_found / 2) + 5 # go a bit further
                
            if options.run_in_reverse:
                filenames.reverse()
            
            # ----------------------------------------------------------------------
            # Now walk through all the filenames selected
            # ----------------------------------------------------------------------
            print (f"{pre_action_verb} started ({time.ctime()}).  Examining files.")

            includes_glossary = False
            # are we forcing a glossary file to build from kbd3 files?
            if options.file_key is not None and re.search("zbk.069", options.file_key, re.IGNORECASE):
                includes_glossary = True
            if options.file_only is not None and re.search("zbk.069", options.file_only, re.IGNORECASE):
                includes_glossary = True
            if options.subFolder is not None and re.search("069.PEP", options.subFolder, re.IGNORECASE):
                includes_glossary = True

            if not options.glossary_only and not includes_glossary:
                glossary_file_skip_pattern=r"ZBK.069(.*)"
            else: # unmatchable pattern, so everything matches and is processed.
                glossary_file_skip_pattern=r"ZZZX"
            rc_skip_glossary_kbd3_files = re.compile(glossary_file_skip_pattern, re.IGNORECASE)
            insert_date = ocd.get_last_record_insertion_date()
            file_number = 0
            parse_only_count = 0
            for n in filenames:
                file_number += 1
                fullfilename = n.filespec
                fileTimeStart = time.time()

                if loaderConfig.FUTURE_DIRECTORY_NAME in str(n.filespec):
                    print(f"Skipping file in excluded subdirectory: {n.filespec}")
                    skipped_files += 1
                    continue

                input_file_was_updated = False
                output_file_newer_than_solr = False
                smart_file_rebuild = False
                base = n.basename
                art_id_from_filename = opasArticleIDSupport.parse_artid_from_filename(fullfilename)
                #artID = os.path.splitext(base)[0]
                #m = re.match(r"([^ ]*).*\(.*\)", artID)
                #artID = m.group(1)
                #artID = artID.upper()
                #artID = artID.replace(".EMBARGOED", "")
                #m = re.match(r"(.*?)\.", artID)
                #if file_number % 500 == 0 and options.display_verbose and processed_files_count == 0:
                    #print (f"#{file_number} of {len(filenames)}")
                
                try:
                    inputfilename = n.fileinfo["name"]
                except KeyError as e:
                    inputfilename = str(n.filespec)
                
                outputfilename = inputfilename.replace(opasFileSupport.get_build_name(inputfilename),
                                                       selected_output_build) # was opasConfig.DEFAULT_OUTPUT_BUILD)
                # ###############################################################################
                # Check if:
                #   - output (precompiled markup) file doesn't exist
                #   - input file is dated after the output file
                #   - if a reference in the biblio (via api_biblioxml2) has been updated
                # ###############################################################################
                file_status_tuple = output_file_needs_rebuilding(inputfilespec=n,
                                                                 inputfilename=inputfilename,
                                                                 outputfilename=outputfilename,
                                                                 art_id=art_id_from_filename,
                                                                 ocd=ocd)

                input_file_was_updated, infile_exists, outfile_exists, both_same = file_status_tuple
                
                if outfile_exists and not input_file_was_updated and not options.forceRebuildAllFiles and not options.forceReloadAllFiles:
                    timestamp = n.timestamp_str
                    output_file_newer_than_solr = file_needs_reloading_to_solr(solrcore=solr_docs2,
                                                                               art_id=art_id_from_filename,
                                                                               timestamp_str=timestamp,
                                                                               filename=outputfilename,
                                                                               output_build=selected_output_build)
                
                                   
                if not options.forceRebuildAllFiles:  # not forced, but always force processed for single file
                    if not options.parse_only:
                        if not options.display_verbose and processed_files_count % 100 == 0 and processed_files_count != 0: # precompiled xml files loaded progress indicator
                            print (f"Examining file #{file_number} Precompiled XML Files \t ...loaded {processed_files_count} out of {files_found} possible.")
        
                        if not options.display_verbose and skipped_files % 100 == 0 and skipped_files != 0: # xml files loaded progress indicator
                            print (f"Examining file #{file_number} Skipped {skipped_files} so far \t...processed or loaded {processed_files_count} out of {files_found} possible." )
                    else:
                        if options.parse_only and skipped_files % 100 == 0 and skipped_files != 0:
                            print (f"Examining file #{file_number} Parsed {parse_only_count} so far out of {files_found} possible." )
                        
                    # if smartload, this will be kbd3, and it basically only decides whether it needs to be built.
                    
                    if options.forceReloadAllFiles or input_file_was_updated or output_file_newer_than_solr:
                        reload_count += 1
                    else:
                        skipped_files += 1
                        if file_number % 250 == 0 and options.display_verbose:
                            print (f"#Skipped {skipped_files} of {len(filenames)}")
                        continue
                elif options.continue_processing: # don't rebuild or reload anythng newer than Solr
                    # This option can also be used to run simultaneous overlapping builds.  
                    # For example, if you run the same set in reverse and forward, when they overlapp, they 
                    #  will start skipping files since they have already been loaded into Solr in the 
                    #  continuation time period

                    insert_date = datetime.today() - dtime.timedelta(days = opasConfig.CONTINUE_PROCESSING_DAYS)
                    if file_was_loaded_to_solr_after(solr_docs2, insert_date, art_id=art_id_from_filename):
                        msg = f"Examining file #%s of %s: %s (%s bytes). **Already processed.**" % (processed_files_count + skipped_files, files_found, n.basename, n.filesize)
                        log_everywhere_if(options.display_verbose, level="info", msg=msg)
                        skipped_files += 1
                        continue
                    
                # get mod date/time, filesize, etc. for mysql database insert/update
                processed_files_count += 1
                if stop_after > 0:
                    if processed_files_count > stop_after:
                        print (f"Halfway mark reached on file list ({stop_after})...file processing stopped per halfway option")
                        break

                if options.smartload:
                    if options.forceRebuildAllFiles or input_file_was_updated:
                        smart_file_rebuild = True
                    else:
                        smart_file_rebuild = False
                
                msg = f"{80*'-'}\nExamining file #%s of %s: %s (%s bytes)." % (processed_files_count, files_found, n.basename, n.filesize)
                log_everywhere_if(options.display_verbose, level="info", msg=msg)

                final_xml_filename = derive_output_filename(n.filespec, 
                                                            input_build=selected_input_build, 
                                                            output_build=options.output_build)
                separated_input_output = final_xml_filename != n.filespec
                just_compiled = False
                # smart rebuild should not rebuild glossary files, so skip those
                if (smart_file_rebuild or options.forceRebuildAllFiles) and not rc_skip_glossary_kbd3_files.match(n.basename):
                    # make changes to the XML
                    input_filespec = n.filespec
                    fileXMLContents, input_fileinfo = fs.get_file_contents(input_filespec)
                    # print (f"Filespec: {input_filespec}")
                    parser = lxml.etree.XMLParser(encoding='utf-8', recover=True, resolve_entities=True, load_dtd=True, dtd_validation=True)
                    try:
                        parsed_xml = etree.fromstring(opasxmllib.remove_encoding_string(fileXMLContents), parser)
                    except lxml.etree.ParseError as e:
                        log_everywhere_if(True, level="error", msg=f"Parse error: {e}")
                        files_with_parse_errors += 1
                    except Exception as e:
                        if fileXMLContents is None:
                            logger.error(f"Can't parse empty converted XML string")
                        else:
                            logger.error(f"Can't parse XML {final_xml_filename} starting '{fileXMLContents[0:128]}'")
                    else:
                        if parsed_xml is None:
                            logger.error(f"Rebuild failed. Can't parse converted XML! Skipping file {final_xml_filename}")
                            continue
                        
                    # save common document (article) field values into artInfo instance for both databases
                    artInfo = opasArticleIDSupport.ArticleInfo(parsed_xml=parsed_xml, art_id=art_id_from_filename, filename_base=base, fullfilename=input_filespec, logger=logger)
                    # check if artInfo artID matches the one from the filename
                    artInfo.filedatetime = input_fileinfo.timestamp_str
                    # artInfo.filename = base # now done in articleInfo
                    # get artinfo per filename, to see if this is an issue coded with volume suffix
                    artInfo.file_size = input_fileinfo.filesize
                    artInfo.file_updated = input_file_was_updated
                    artInfo.file_create_time = input_fileinfo.create_time

                    # ##########################################################################################################
                    # Special IJPOpen Version Processing Part 1
                    parsed_version_history_unit = None
                    version_history_unit = None
                    if artInfo.src_code == "IJPOPEN":
                        # Check if there are multiple versions, remove old versions, and add version history to each
                        version_history_unit = \
                            opasDataLoaderIJPOpenSupport.version_history_processing(ocd,
                                                                                    artInfo,
                                                                                    solrdocs=solr_docs2, solrauth=solr_authors2,
                                                                                    file_xml_contents=fileXMLContents,
                                                                                    full_filename_with_path=inputfilename,
                                                                                    options=options, 
                                                                                    verbose=options.display_verbose)
                        if version_history_unit is not None:
                            parsed_version_history_unit = version_history_unit["version_section"]
                    # ##########################################################################################################
                    # Process XML to create output (compiled and marked up version)
                    parsed_xml, ret_status = opasXMLProcessor.xml_update(parsed_xml,
                                                                         artInfo,
                                                                         ocd,
                                                                         pretty_print=options.pretty_printed,
                                                                         markup_terms=options.glossary_term_tagging,
                                                                         add_glossary_list=options.add_glossary_term_dict, 
                                                                         verbose=options.display_verbose,
                                                                         no_database_update = options.no_bibdbupdate)
                    
                    # ##########################################################################################################
                    # Special IJPOpen Version Processing Part 2 (Post conversion)
                    # if there's a version history to be appended, do it so it's written to output file!
                    if version_history_unit is not None and parsed_version_history_unit is not None:
                        if options.display_verbose:
                            print("\t...Adding PEP-Web Manuscript history unit to compiled XML")
                        parsed_xml.append(parsed_version_history_unit)
                    # ##########################################################################################################

                    # impx_count = int(pepxml.xpath('count(//impx[@type="TERM2"])'))
                    # write output file
                    fname = str(n.filespec)
                    fname = re.sub("\(b.*\)", options.output_build, fname)
                    file_prefix = f"""{loaderConfig.DEFAULT_XML_DECLARATION}\n{options.output_doctype}\n"""
                    # xml_text version, not reconverted to tree
                    file_text = lxml.etree.tostring(parsed_xml, pretty_print=options.pretty_printed, encoding="utf8").decode("utf-8")
                    file_text = file_prefix + file_text
                    # this is required if running on S3
                    just_compiled = fs.create_text_file(fname, data=file_text, delete_existing=True)
                    if just_compiled:
                        rebuild_count += 1
                        log_everywhere_if(options.display_verbose , level="info", msg=f"\t...Compiled {n.basename} to ...{fname[-40:]}")
                    else:
                        log_everywhere_if(options.display_verbose, level="error", msg=f"\t...There was a problem writing {fname}.")

                # ##########################################################################################################
                # Special IJPOpen Version Processing Part 3 (Post conversion)
                # Check if it's removed (need to check database, not variable because it may be a load, not a build)
                if opasDataLoaderIJPOpenSupport.is_removed_version(ocd, art_id=art_id_from_filename):
                    print ("\t...This version has been archived/removed.  No need to load to Solr.")
                    skipped_files += 1
                    archived_files_not_loaded += 1
                    continue # skip this file
                # ##########################################################################################################
                # 
                # make sure the file we read is the processed file. Should be the output/processed build, e.g. (bEXP_ARCH1).
                # note: if input build is same as processed (output build), then this won't change and the input file/build
                #       will be used
                # 
                # Read processed file format (sometimes it's the input file, sometimes it's the output file)
                fileXMLContents, final_fileinfo = fs.get_file_contents(final_xml_filename)
                if fileXMLContents is None:
                    # skip
                    logger.error(f"Cannot find/or/read {final_xml_filename}...skipping! (Make sure options are correct, is --smartload missing?)")
                    continue
                
                if options.display_verbose: # SmartLoad: File not modified. No need to recompile
                    if separated_input_output and options.smartload and not smart_file_rebuild:
                        print (f"SmartLoad: File not modified. No need to recompile.")
                        
                # import into lxml
                parser = lxml.etree.XMLParser(encoding='utf-8', recover=True, resolve_entities=True, load_dtd=True, dtd_validation=True)
                try:
                    parsed_xml = etree.fromstring(opasxmllib.remove_encoding_string(fileXMLContents), parser)
                except lxml.etree.ParseError as e:
                    log_everywhere_if(True, level="error", msg=f"Parse error: {e}")
                    files_with_parse_errors += 1
                except Exception as e:
                    if fileXMLContents is None:
                        logger.error(f"Can't parse empty converted XML string")
                    else:
                        logger.error(f"Can't parse XML {final_xml_filename} starting '{fileXMLContents[0:128]}'")
                else:
                    if parsed_xml is None:
                        logger.error(f"Rebuild failed. Can't parse converted XML! Skipping file {final_xml_filename}")
                        continue

                if options.parse_only:
                    # just parse but don't act on the file
                    skipped_files += 1
                    parse_only_count += 1
                    continue
                
                #treeroot = pepxml.getroottree()
                #root = pepxml.getroottree()
        
                # save common document (article) field values into artInfo instance for both databases
                try:
                    artInfo = opasArticleIDSupport.ArticleInfo(parsed_xml=parsed_xml,
                                                               art_id=art_id_from_filename,
                                                               filename_base=base,
                                                               fullfilename=final_xml_filename,
                                                               logger=logger)
                except Exception as e:
                    log_everywhere_if(True, "error", f"Error: Article Info Incomplete: Skipping input file {fullfilename} or compiled file {final_xml_filename}. {e}")
                    continue
                
                artInfo.filedatetime = final_fileinfo.timestamp_str
                # artInfo.filename = base
                artInfo.file_size = final_fileinfo.filesize
                artInfo.file_updated = input_file_was_updated
                artInfo.file_create_time = final_fileinfo.create_time
                
                if artInfo.art_orig_rx is None: # fix for the original article 2023-03-01
                    # see if any known article has this article (artID) listed as a translation
                    translationSet, count = opasPySolrLib.quick_docmeta_docsearch(q_str=f"art_origrx:{art_id_from_filename}")
                    if translationSet is not None:
                        # add the article's ID to it's original RX so it can use its IDs to see its translations
                        msg = "\t...This article has translations"
                        log_everywhere_if(options.display_verbose , level="info", msg=msg)
                        artInfo.art_orig_rx = art_id_from_filename

                if artInfo.art_qual is None:
                    # if this doesn't already have related articles...
                    # check if there's any new articles related to this one, and if so, add this
                    #  article's id to art_qual, so it's part of the set which references this one.
                    related, related_id_list = opasPySolrLib.get_articles_related_to_current_via_artqual(art_qual = artInfo.art_id)
                    if not opasgenlib.is_empty(related):
                        if len(related) > 1 or artInfo.art_id not in related_id_list:  
                            msg = f"\t...Other articles point to this article in a set of {len(related)} related articles {related_id_list}. Setting artqual."
                            log_everywhere_if(options.display_verbose, level="info", msg=msg)
                            artInfo.art_qual = artInfo.art_id
                else:
                    related, related_id_list = opasPySolrLib.get_articles_related_to_current_via_artqual(art_qual = artInfo.art_qual)
                    if len(related_id_list) > 1:
                        msg = f"\t...Article is in a set of {len(related)} related articles {related_id_list} via artqual {artInfo.art_qual}"
                        log_everywhere_if(options.display_verbose, level="info", msg=msg)
                    else:
                        msg = f"\t...Article art_qual indicates related articles ({artInfo.art_qual} but none found. Setting to None.)"
                        log_everywhere_if(options.display_verbose, level="info", msg=msg)
                        artInfo.art_qual = None
                    
                artInfo.set_file_classification()
                
                msg = f"\t...Loading precompiled XML file {final_fileinfo.basename} ({final_fileinfo.filesize} bytes) Access: {artInfo.file_classification }"
                log_everywhere_if(options.display_verbose , level="info", msg=msg)
                
                # not a new journal, see if it's a new article.
                if opasSolrLoadSupport.add_to_tracker_table(ocd, artInfo.art_id) and options.write_updates: # if true, added successfully, so new!
                    # don't log to issue updates for journals that are new sources added during the annual update
                    if artInfo.src_code not in loaderConfig.DATA_UPDATE_PREPUBLICATION_CODES_TO_IGNORE:
                        art = f"<article id='{artInfo.art_id}'>{artInfo.art_citeas_xml}</article>"
                        try:
                            issue_updates[artInfo.issue_id_str].append(art)
                        except Exception as e:
                            issue_updates[artInfo.issue_id_str] = [art]
    
                # walk through bib section and add to refs core database
                precommit_file_count += 1
                if precommit_file_count > configLib.opasCoreConfig.COMMITLIMIT:
                    print(f"** Committing info for {configLib.opasCoreConfig.COMMITLIMIT} documents/articles **")
    
                # input to the glossary
                if 1: # options.glossary_core_update:
                    # load the glossary core if this is a glossary item
                    glossary_file_pattern=r"ZBK.069(.*)\(bKBD3|bEXP_ARCH1\)\.(xml|XML)$" # add bKBD3 so smartload also can load the entry
                    if re.match(glossary_file_pattern, n.basename, re.IGNORECASE):
                        opasSolrLoadSupport.process_article_for_glossary_core(parsed_xml, artInfo, solr_gloss2, fileXMLContents, verbose=options.display_verbose)
                
                # input to the full-text and authors cores
                if not options.glossary_only: # options.fulltext_core_update:
                    # load the database
                    opasSolrLoadSupport.add_article_to_api_articles_table(ocd, artInfo, verbose=options.display_verbose)
                    opasSolrLoadSupport.add_to_artstat_table(ocd, artInfo, verbose=options.display_verbose)

                    # 2023-02-08 This may not be needed anymore, if we remove "start of section" flagging from the
                    #  server, which was a workaround for repeating section names.
                    # -----
                    # 2022-04-22 New Section Name Workaround - This works but it means at least for new data, you can't run the load backwards as we currently do
                    #  on a full build.  Should be put into the client instead, really, during table gen.
                    # -----
                    # Uses new views: vw_opasloader_article_firstsectnames which is based on the new view vw_opasloader_article_sectnames
                    #  if an article id is found in that view, it's the first in the section, otherwise it isn't
                    # check database to see if this is the first in the section
                    if 0: # make it switchable for now (newsecnm workaround) in case the workaround needs to stick around a while
                        if not opasSolrLoadSupport.check_if_start_of_section(ocd, artInfo.art_id):
                            # print (f"\t\t...NewSec Workaround: Clearing newsecnm for {artInfo.art_id}")
                            artInfo.start_sectname = None # clear it so it's not written to solr, this is not the first article
                        else:
                            if options.display_verbose: print (f"\t\t...NewSec {artInfo.start_sectname} found in {artInfo.art_id}")
                    # -----

                    # load the docs (pepwebdocs) core
                    if options.display_verbose: print("\t...Loading article for document and author core.")
                    opasSolrLoadSupport.process_article_for_doc_core(parsed_xml, artInfo, solr_docs2, fileXMLContents, include_paras=options.include_paras, verbose=options.display_verbose)
                    # load the authors (pepwebauthors) core.
                    opasSolrLoadSupport.process_info_for_author_core(parsed_xml, artInfo, solr_authors2, verbose=options.display_verbose)
                    
                    if precommit_file_count > configLib.opasCoreConfig.COMMITLIMIT:
                        precommit_file_count = 0
                        try:
                            solr_docs2.commit()
                        except Exception as e:
                            log_everywhere_if(True, "error", f"Docs Core Commit error - Solr internal issue, perhaps lock issue? {e}")
                        
                        try:
                            solr_authors2.commit()
                        except Exception as e:
                            log_everywhere_if(True, "error", f"Authors Core Commit error - Solr internal issue: perhaps lock issue? {e}")
                    
                # Add to the references table
                if not options.no_bibdbupdate and not just_compiled: 
                    # if just_compiled is false, because otherwise, we just loaded these references!
                    # only need to do bib save to db if not done separately via opasDataLinker 
                    if artInfo.ref_count > 0:
                        art_reference_count = artInfo.ref_count
                        total_reference_count += artInfo.ref_count
                        bibReferences = parsed_xml.xpath("/pepkbd3//be")  # this is the second time we do this (also in artinfo, but not sure or which is better per space vs time considerations)
                        if options.display_verbose: print("\t...Loading %s references to the references database." % (artInfo.ref_count))
    
                        for ref in bibReferences:
                            bib_entry = opasBiblioSupport.BiblioEntry(art_id=artInfo.art_id, art_year=artInfo.art_year_int, ref_or_parsed_ref=ref)
                            # perhaps only check if it exists when compiling xml. That would be faster. For now, to update, let it check
                            if bib_entry.link_updated or bib_entry.record_updated or not ocd.exists(table_name=opasConfig.BIBLIO_TABLE, where_conditional=f"art_id='{bib_entry.art_id}' AND ref_local_id='{bib_entry.ref_local_id}'"):
                                ocd.save_ref_to_biblioxml_table(bib_entry)
    
                # close the file, and do the next
                if options.display_verbose: print(f"\t...Time: {time.time() - fileTimeStart:.4f} seconds.")
        
            print (f"{pre_action_verb} process complete ({time.ctime()} ). Time: {time.time() - fileTimeStart:.4f} seconds.")
            if processed_files_count > 0 and not options.parse_only:
                try:
                    print ("Performing final commit.")
                    if not options.glossary_only: # options.fulltext_core_update:
                        solr_docs2.commit()
                        solr_authors2.commit()
                    if 1: # options.glossary_core_update:
                        solr_gloss2.commit()
                except Exception as e:
                    print(("Exception: ", e))
                else:
                    # Use date time as seed, hoping multiple instances don't get here at the same time
                    # but only if caller did not specify
                    if randomizer_seed is None:
                        randomizer_seed = int(datetime.utcnow().timestamp())
    
    if not options.parse_only and options.write_updates:
        opasSolrLoadSupport.garbage_collect_stat(ocd)
        if options.daysback is not None: #  get all updated records
            print (f"Listing updates for {options.daysback} days.")
            issue_updates = {}
            try:
                days_back = int(options.daysback)
            except:
                logger.error("Incorrect specification of days back. Must be integer.")
            else:
                article_list = ocd.get_articles_newer_than(days_back=days_back)
                for art_id in article_list:
                    artInfoSolr = opasAPISupportLib.documents_get_abstracts(art_id)
                    try:
                        art_citeas_xml = artInfoSolr.documents.responseSet[0].documentRefXML
                        src_code = artInfoSolr.documents.responseSet[0].PEPCode
                        art_year = artInfoSolr.documents.responseSet[0].year
                        art_vol_str = artInfoSolr.documents.responseSet[0].vol
                        art_issue = artInfoSolr.documents.responseSet[0].issue
                        issue_id_str = f"<issue_id><src>{src_code}</src><yr>{art_year}</yr><vol>{art_vol_str}</vol><iss>{art_issue}</iss></issue_id>"
                    except IndexError:
                        if re.search("IJPOPEN", art_id, re.IGNORECASE) is None: # IJPOPEN is more dynamic and has archive/removed articles, no need to log these
                            logger.error(f"WhatNewList IndexError: can't find article info for: {art_id}")
                    except Exception as e:
                        logger.error(f"WhatNewList Exception for: {art_id} {e}")
                    else:   
                        if src_code not in loaderConfig.DATA_UPDATE_PREPUBLICATION_CODES_TO_IGNORE:
                            art = f"<article id='{art_id}'>{art_citeas_xml}</article>"
                            try:
                                issue_updates[issue_id_str].append(art)
                            except Exception as e:
                                issue_updates[issue_id_str] = [art]
        if issue_updates != {}:
            random.seed(randomizer_seed)
            try:
                if options.whatsnewfile is None:
                    try:
                        fname = f"{localsecrets.DATA_UPDATE_LOG_DIR}/updated_issues_{dtime.datetime.now().strftime('%Y%m%d_%H%M%S')}({random.randint(1000,9999)}).xml"
                    except Exception as e:
                        fname = f"updated_issues_{dtime.datetime.now().strftime('%Y%m%d_%H%M%S')}({random.randint(1000,9999)}).xml"
                else:
                    fname = options.whatsnewfile
                msg = f"Writing Issue updates.  Writing to file {fname}"
                print (msg)
                logger.info(msg)
                filedata =  f'<?xml version="1.0" encoding="UTF-8"?>\n<issue_updates>\n'
                count_records = 0
                for k, a in issue_updates.items():
                    filedata +=  f"\n\t<issue>\n\t\t{str(k)}\n\t\t<articles>\n"
                    count_records += 1
                    for ref in a:
                        try:
                            filedata +=  f"\t\t\t{ref}\n"
                        except Exception as e:
                            logger.error(f"Issue Update Article Write Error: ({e})")
                    filedata +=  "\t\t</articles>\n\t</issue>"
                filedata +=  '\n</issue_updates>'
    
                success = fs.create_text_file(fname, data=filedata, delete_existing=True, path_is_root_bucket=True)            
    
                if count_records > 0 and success:
                    msg = f"{count_records} issue updates written to whatsnew log file."
                    print (msg)
                    logger.info(msg)
    
            except Exception as e:
                logger.error(f"Issue Update File Write Error: ({e})")
                
        else: # if issue_updates != {}
            if options.daysback is not None:
                msg = f"Note: There was nothing in the whats new request to output for days back == {options.daysback}."
                logger.warning(msg)
            else:
                msg = f"Note: There was nothing new in the batch output whatsnew."
                logger.warning(msg)
    # ---------------------------------------------------------
    # Closing time
    # ---------------------------------------------------------
    timeEnd = time.time()
    ocd.close_connection(force_close=True)
    #currentfile_info.close()

    if not options.no_files and not options.parse_only: # no_files=false
        database_updated = (files_found - skipped_files) != 0
        if database_updated:
            # write database_updated.txt
            try:
                fname = f"{localsecrets.DATA_UPDATE_LOG_DIR}/database_updated.txt"
                success = fs.create_text_file(fname, data='data loaded!\n', delete_existing=False, path_is_root_bucket=True)
                msg = f"Database was updated with {files_found - skipped_files} articles! Wrote {fname} in order to flag changes."
                if success:
                    if 1: # options.display_verbose:
                        print (msg)
                    logger.warning(msg)
                
            except Exception as e:
                # just in case there's a collision of several processes writing, ignore the error
                logger.warning(f"When writing the database updated flag file an error occured: {e}")
            
        # for logging
        if 1: # (options.biblio_update or options.fulltext_core_update) == True:
            # account for archive files skipped
            processed_files_count -= archived_files_not_loaded
            reload_count -= archived_files_not_loaded
            
            elapsed_seconds = timeEnd-cumulative_file_time_start # actual processing time going through files
            elapsed_minutes = elapsed_seconds / 60
            print (80 * "-")
            if options.smartload and not options.forceRebuildAllFiles:
                msg = f"Option --smartload re/compiled {rebuild_count} documents from input source and reloaded to Solr."
                logger.info(msg)
                print (msg)
            
            if options.forceReloadAllFiles:
                msg = f"Option --reload loaded {reload_count} of the total documents to Solr (although the same)."
                logger.info(msg)
                print (msg)
            else:
                msg = f"Of the precompiled files, {skipped_files} files did not need loading."
                logger.info(msg)
                print (msg)
                
            if options.forceRebuildAllFiles:
                msg = f"Option --rebuild loaded recompiled and loaded {rebuild_count} of the total documents to Solr."
                logger.info(msg)
                print (msg)                
                
            if art_reference_count > 0:
                msg = f"Finished! {post_action_verb} {processed_files_count} documents and {total_reference_count} references. Total file inspection/load time: {elapsed_seconds:.2f} secs ({elapsed_minutes:.2f} minutes.) "
                logger.info(msg)
                print (msg)
            else:
                msg = f"Finished! {post_action_verb} {processed_files_count} documents {options.output_build}. Total file load time: {elapsed_seconds:.2f} secs ({elapsed_minutes:.2f} minutes.)"
                logger.info(msg) 
                print (msg)
                

            if processed_files_count > 0:
                msg = f"...Files per Min: {processed_files_count/elapsed_minutes:.4f}"
                logger.info(msg)
                print (msg)
                msg = f"...Files evaluated per Min (includes skipped files): {len(filenames)/elapsed_minutes:.4f}"
                logger.info(msg)
                print (msg)
    
        elapsed_seconds = timeEnd-timeStart # actual processing time going through files
        elapsed_minutes = elapsed_seconds / 60
        msg = f"Note: File load time is not total elapsed time. Total elapsed time is: {elapsed_seconds:.2f} secs ({elapsed_minutes:.2f} minutes.)"
        logger.info(msg)
        print (msg)

        if processed_files_count > 0:
            msg = f"Files per elapsed min: {processed_files_count/elapsed_minutes:.4f}"
            logger.info(msg)
            print (msg)

        print (80 * "-")
            

    else:  # no_files=True or just parsed
        msg = f"{files_with_parse_errors} files had XML parse errors."
        logger.info(msg)
        print (msg)

        print ("Processing finished.")
        elapsed_seconds = timeEnd-timeStart # actual processing time going through files
        elapsed_minutes = elapsed_seconds / 60
        msg = f"Elapsed min: {elapsed_minutes:.4f}"
        logger.info(msg)
        print (msg)
        print (80 * "-")
    
# -------------------------------------------------------------------------------------------------------
# run it!

if __name__ == "__main__":
    global options  # so the information can be used in support functions
    options = None
    description = "Load preprocessed XML Data to the PEP-Web Solr and SQL databases (preprocess and load in one step via --smartbuild). "
    print (f"Running in Python: {sys.version_info[0]}.{sys.version_info[1]}")
    print (f"Configuration used: {localsecrets.CONFIG}")
    print (f"Database Name: {localsecrets.DBNAME}")
    print (f"Version: {__version__}")
    print (f"Pydantic Version {pydantic.__version__}")    
    print (f"lxml Version {lxml.__version__}")    
    print (f"pysolr Version {pysolr.__version__}")   
    
    parser = OptionParser(usage="%prog [options]", version=f"%prog ver. {__version__}", description=description)

    parser.add_option("-a", "--allfiles", action="store_true", dest="forceRebuildAllFiles", default=False,
                      help="Option to force all files to be loaded to the specified cores.")

    parser.add_option("--after", dest="created_after", default=None,
                      help="Load files created or modifed after this datetime (use YYYY-MM-DD format). (May not work on S3)")

    msg = f"""This option can be used to continue a partial/failed run or to run simultaneous overlapping builds.
For example, if you run the same set in reverse and forward, when they overlap, they 
will start skipping files since they have already been loaded into Solr in the continuation time period as configured in opasConfig {opasConfig.CONTINUE_PROCESSING_DAYS} days.
"""
    parser.add_option("--continue", action="store_true", dest="continue_processing", default=False,
                          help=msg)
    
    parser.add_option("-d", "--dataroot", dest="rootFolder", default=localsecrets.FILESYSTEM_ROOT,
                      help="Bucket (Required S3) or Root folder path where input data is located")

    parser.add_option("--doctype", dest="output_doctype", default=opasConfig.PEP_KBD_DOCTYPE,
                      help=f"For output files, default={opasConfig.PEP_KBD_DOCTYPE}.")

    parser.add_option("--glossaryonly", action="store_true", dest="glossary_only", default=False,
                      help="Only process the glossary (quicker).")

    parser.add_option("--halfway", action="store_true", dest="halfway", default=False,
                      help="Only process halfway through (e.g., when running forward and reverse.")

    parser.add_option("--includeparas", action="store_true", dest="include_paras", default=False,
                      help="Don't separately store paragraphs except for sources using concordance (GW/SE).")

    parser.add_option("--inputbuildpattern", dest="input_build_pattern", default=None,
                      help="Pattern of the build specifier to load (input), e.g., (bEXP_ARCH1|bSeriesTOC), or (bKBD3|bSeriesTOC)")
    
    parser.add_option("--inputbuild", dest="input_build", default=opasConfig.DEFAULT_INPUT_BUILD,
                      help=f"Build specifier to load (input), e.g., (bKBD3) or just bKBD3")
    
    parser.add_option("--key", dest="file_key", default=None,
                      help="Key for a single file to load, e.g., AIM.076.0269A.  Use in conjunction with --sub for faster processing of single files on AWS")

    parser.add_option("-l", "--loglevel", dest="logLevel", default=logging.ERROR,
                      help="Level at which events should be logged (DEBUG, INFO, WARNING, ERROR")

    # --load option still the default.  Need to keep for backwards compatibility, at least for now (7/2022)
    parser.add_option("--load", "--loadxml", action="store_true", dest="loadprecompiled", default=True,
                      help="Load precompiled XML, e.g. (bEXP_ARCH1) into database.")

    parser.add_option("--nobibdbupdate", action="store_true", dest="no_bibdbupdate", default=False,
                      help="Turn off save of biblio info to the database (i.e., if done using opasDataLinker")

    parser.add_option("--nocheck", action="store_true", dest="no_check", default=False,
                      help="Don't prompt whether to proceed.")

    parser.add_option("--nofiles", action="store_true", dest="no_files", default=False,
                      help="Don't load any files (use with whatsnewdays to only generate a whats new list).")

    parser.add_option("--nohelp", action="store_true", dest="no_help", default=False,
                      help="Turn off front-matter help")

    parser.add_option("--only", dest="file_only", default=None,
                      help="File spec for a single file to process.")
    
    parser.add_option("--parseonly", action="store_true", dest="parse_only", default=False,
                      help="Parse (check) but don't store in Solr")

    parser.add_option("--outputbuild", dest="output_build", default=opasConfig.DEFAULT_OUTPUT_BUILD,
                      help=f"Specific output build specification, default='{opasConfig.DEFAULT_OUTPUT_BUILD}'. e.g., (bEXP_ARCH1) or just bEXP_ARCH1.")
    
    parser.add_option("--prettyprint", action="store_true", dest="pretty_printed", default=False,
                      help="Pretty format the compiled XML.")

    parser.add_option("--pw", dest="httpPassword", default=None,
                      help="Password for the server")

    parser.add_option("-r", "--reverse", dest="run_in_reverse", action="store_true", default=False,
                      help="Whether to run the selected files in reverse order")

    # redundant add option to use so compatible options to the PEPXML code for manual use
    parser.add_option("--rebuild", action="store_true", dest="forceRebuildAllFiles", default=False,
                      help="Force files to be compiled into precompiled XML and reloaded to the specified cores whether changed or not.")

    parser.add_option("--reload", action="store_true", dest="forceReloadAllFiles", default=False,
                      help="Force reloads to Solr whether changed or not.")

    parser.add_option("--resetcore",
                      action="store_true", dest="resetCoreData", default=False,
                      help="This option (currently disabled) would clear (delete) any data in the selected cores (author core is reset with the fulltext core).")

    parser.add_option("--seed",
                      dest="randomizer_seed", default=None,
                      help="Seed so data update files don't collide if they start writing at exactly the same time.")

    parser.add_option("--writeupdates",
                      action="store_true",
                      dest="write_updates", default=False,
                      help="Turn on XML issue updates.")

    msg = f"""Rebuild from source (e.g., bKBD3) if necessary, and load precompiled XML (e.g., bEXP_ARCH1) to
the databases when new or updated. If inputbuild file is newer, output is missing, or there are changes to 
the api_biblioxml2 records, compile and then load into database. Run opasDataLinker once files have been initially
processed and it will use heuristics to find reference links to articles beyond the basic links added here.
At any time you can manually fix links in api_biblioxml2 and they will be loaded into the precompiled XML when
opasDataLoader is run. Use --smartbuild to automatically check for reference updates in the api_biblioxml2 table.
Manually set rx_confidence to 1 for manually corrected links, or to .01 to ignore, and the automated process
will skip these from then on."""
    parser.add_option("--smartload", "--smartbuild", action="store_true", dest="smartload", default=False,
                      help=msg)
    
    parser.add_option("--sub", dest="subFolder", default=None,
                      help="Sub folder of root folder specified via -d to process")

    parser.add_option("--termtags", action="store_true", dest="glossary_term_tagging", default=False,
                      help="Markup glossary terms in paragraphs when compiling XML")

    parser.add_option("--termdictoff", action="store_false", dest="add_glossary_term_dict", default=True,
                      help=f"""Do not add a glossary term/count dict when compiling XML""")

    parser.add_option("--test", dest="testmode", action="store_true", default=False,
                      help="Run Doctests")

    parser.add_option("--userid", dest="httpUserID", default=None,
                      help="UserID for the server")

    parser.add_option("--verbose", action="store_true", dest="display_verbose", default=False,
                      help="Display status and operational timing info as load progresses.")

    parser.add_option("--whatsnewdays", dest="daysback", default=None,
                      help="Generate a log of files added in the last n days (1==today), rather than for files added during this run.")

    parser.add_option("--whatsnewfile", dest="whatsnewfile", default=None,
                      help="File name to force the file and path rather than a generated name for the log of files added in the last n days.")

    parser.add_option("--reuseconnection", dest="reuse_connection", default=True,
                      help="Reuse the connection to MySQL")

    (options, args) = parser.parse_args()

    
    if options.smartload:
        options.loadprecompiled = False # override default
    
    if not (options.loadprecompiled):
        options.smartload = True
    
    # if you turn off term tagging, always add the term dict.
    if options.glossary_term_tagging:
        options.add_glossary_term_dict = True
    
    if not options.no_help:
        print (help_text)

    if len(options.output_build) < 2:
        logger.error("Bad output buildname. Using default.")
        options.output_build = opasConfig.DEFAULT_OUTPUT_BUILD
        
    if options.output_build is not None and (options.output_build[0] != "(" or options.output_build[-1] != ")"):
        print ("Warning: output build should have parenthesized format like (bEXP_ARCH1). Adding () as needed.")
        if options.output_build[0] != "(":
            options.output_build = f"({options.output_build}"
        if options.output_build[-1] != ")":
            options.output_build = f"{options.output_build})"
    
    if options.input_build is not None and (options.input_build[0] != "(" or options.input_build[-1] != ")"):
        print ("Warning: input build should have parenthesized format like (bKBD3). Adding () as needed.")
        if options.input_build[0] != "(":
            options.input_build = f"({options.input_build}"
        if options.input_build[-1] != ")":
            options.input_build = f"{options.input_build})"

    if options.glossary_only and options.file_key is None:
        options.file_key = "ZBK.069(.*)"

    if options.testmode:
        import doctest
        doctest.testmod()
        print ("Fini. opasDataLoader Tests complete.")
        sys.exit()

    try:
        main()
    except Exception as e:
        logger.error(f"Main hit an exception in opasDataLoader {e}")
        traceback.print_exc()
        sys.exit(1)
    else:
        print ("Normal exit...not using 255 this time")
        sys.exit() # will change later back to 255, but for now back to normal exiting.
