#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#Copyright 2012-2018 Neil R. Shapiro

__author__      = "Neil R. Shapiro"
__copyright__   = "Copyright 2023"
__license__     = "Apache 2.0"
__version__     = "2023.0419/v1.0.019"   
__status__      = "Development"

programNameShort = "opasDataLinker"
border = 80 * "*"
print (f"""\n
        {border}
            {programNameShort} - Open Publications-Archive Server (OPAS) Reference Linker
                            Version {__version__}
        {border}
        """)

help_text = (
    fr""" 
        - Read the api_biblioxml2 table and link articles in PEP.
        
        Example Invocation:
                $ python opasDataLinker.py
                
        Important option choices:
         -h, --help        List all help options
         -a                Process all records
         --halfway         Process only half the records, so you can run two
                              instances, one forward, one in reverse
         
         Filters
         --key             Process biblio records with the key=article-id
                              or regex (e.g., --key AIM.076.0309A, or AIM.*)
         --nightly         Process biblio records added since the day before
         --type            Process type 'books' or 'journals' only (abbr. b or j)
         --unlinked        Process only unlinked (no rx and rxcf data) records
         --where           Process by conditional against api_biblioxml2
                              table, e.g., ref_year > 2010
         
         
         Processing Order
         --reverse         Process biblio records in reverse order by
                              article id, local id
         --oldest          Process oldest (in terms of last updated) biblio
                              records first

        After running opasDataLinker, run opasDataLoader with the
        
              --smartbuild
        
        option to reprocess any articles which have new links
        from opasDataLinker. This will build new compiled (output) xml files for those
        with the link data embedded.
         
""")
    
import sys
sys.path.append('../libs')
sys.path.append('../config')
sys.path.append('../libs/configLib')

import os

# import string, sys, copy, re
import logging
logger = logging.getLogger(__name__)
# import re
import time
import datetime
today = datetime.date.today()  # Get today's date
yesterday = today - datetime.timedelta(days=2)  # Subtract 24 hours to cover 

# from optparse import OptionParser
from loggingDebugStream import log_everywhere_if

import opasConfig
# import opasGenSupportLib as opasgenlib
import opasBiblioSupport
import opasCentralDBLib
import localsecrets

import csv
import boto3

# import opasPySolrLib
# from opasLocator import Locator
#import opasXMLHelper
# import opasXMLProcessor
#import opasArticleIDSupport
# import models

gDbg1 = 0	# details
gDbg2 = 1	# High level

LOWER_RELEVANCE_LIMIT = 35

ocd = opasCentralDBLib.opasCentralDB()
# import lxml.etree as ET
# import lxml
sqlSelect = ""

sns_client = boto3.client('sns', aws_access_key_id=localsecrets.S3_KEY, aws_secret_access_key=localsecrets.S3_SECRET)
s3_client = boto3.client('s3', aws_access_key_id=localsecrets.S3_KEY, aws_secret_access_key=localsecrets.S3_SECRET)


def upload_csv_to_s3(csv_file_path, bucket_name, object_name):
    """Upload CSV file to S3 and return the object URL."""
    s3_client.upload_file(csv_file_path, bucket_name, object_name)
    object_url = f"https://s3.console.aws.amazon.com/s3/object/{bucket_name}?prefix={object_name}"
    return object_url

def send_sns_notification(sns_topic_arn, message, subject):
    """Send an SNS notification."""
    response = sns_client.publish(
        TopicArn=sns_topic_arn,
        Message=message,
        Subject=subject
    )
    return response

def initialise_report_writer(filename, verbose):
    csvFile = open(filename, mode='w', newline='', encoding='utf-8')
    writer = csv.writer(csvFile)

    writer.writerow([
            'Article ID',
            'RX',
            'RX Conf',
            'RXCF',
            'RXCF Conf',
            'Ref text',
            'Source',
    ])

    log_everywhere_if(verbose, "info", f"Report: initialised CSV - {filename}")

    return writer, csvFile

def report_write_changes(bib_entry, writer, verbose=False):
    data_row = [
        bib_entry.art_id,
        bib_entry.ref_rx,
        bib_entry.ref_rx_confidence,
        bib_entry.ref_rxcf,
        bib_entry.ref_rxcf_confidence,
        bib_entry.ref_text,
        bib_entry.ref_link_source,
    ]

    writer.writerow(data_row)
    log_everywhere_if(verbose, "info", f"\t...Report: Database update written to CSV -{data_row}")


def walk_through_reference_set(ocd=ocd,
                               sql_set_select = "select * from api_biblioxml2 where art_id='CPS.031.0617A' and ref_local_id='B022'",
                               set_description = "All",
                               halfway=False, 
                               verbose=True
                               ):
    """
    >> walk_set = "SingleTest1", "select * from api_biblioxml2 where art_id='CPS.031.0617A' and ref_local_id='B024'"
    >> walk_through_reference_set(ocd, sql_set_select=walk_set[1], set_description=walk_set[0])

    >> walk_set = "SingleTest1", "select * from api_biblioxml2 where art_id='CPS.031.0617A' and ref_local_id='B022'"
    >> walk_through_reference_set(ocd, sql_set_select=walk_set[1], set_description=walk_set[0])

    >> walk_set = "Freud", "select * from api_biblioxml2 where ref_rx is NULL and ref_authors like '%Freud%' and ref_rx_confidence=0"
    >> walk_through_reference_set(ocd, sql_set_select=walk_set[1], set_description=walk_set[0])
    
    """
    fname = "walk_through_reference_set"
    ocd.open_connection(caller_name=fname) # make sure connection is open
    ret_val = None
    #artInfo = opasArticleIDSupport.ArticleInfo(art_id="IJP.100.0001A",
                                               #art_year="2022")
    
    # parser = lxml.etree.XMLParser(encoding='utf-8', recover=True, resolve_entities=True, load_dtd=True)
    if ocd.db is not None:
        cumulative_time_start = time.time()
        # rows = self.SQLSelectGenerator(sqlSelect)
        log_everywhere_if(True, "info", "Finding candidate references...")
        total_count = ocd.get_select_count(sql_set_select)
        if halfway:
            count = round(total_count / 2)
            limit_clause = f" \nLIMIT {count}"
            log_everywhere_if(True, "info", f"Limiting to halfway--{count} of {total_count} references.")
        else:
            limit_clause = ""
            count = total_count
            
        biblio_entries = ocd.get_references_select_biblioxml(sql_set_select + limit_clause)
        log_everywhere_if(True, "info", f"Scanning {len(biblio_entries)} references in api_biblioxml2 to find new links.")
        counter = 0
        updated_record_count = 0

        if options.dryrun or options.report_changes or options.report_links_days:
            reportFilename = f"linker_change_report_{cumulative_time_start}.csv"
            reportWriter, reportFile = initialise_report_writer(reportFilename, verbose)

        for ref_model in biblio_entries:
            reference_time_start = time.time()
            counter += 1
            log_everywhere_if(True, "info", 80*"-")
            one_line_text = ""
            if ref_model.ref_text:
                one_line_text = ref_model.ref_text.replace('\n', '')
            last_updated = ref_model.last_update.strftime("%m/%d/%Y")
            log_everywhere_if(True, "info", f"{counter}/{count}:Analyzing Record Last Updated:{last_updated} ID:{ref_model.art_id}/{ref_model.ref_local_id}\nRef:{one_line_text}")
            # parsed_ref = ET.fromstring(ref_model.ref_xml, parser=parser)
            bib_entry = opasBiblioSupport.BiblioEntry(ref_model.art_id, db_bib_entry=ref_model, verbose=verbose)
            # art_id = bib_entry.art_id
            if not bib_entry.ref_title:
                # ignore this one from now on
                log_everywhere_if(verbose, "info", f"\t...No title found, ignoring this one from now on.")
                bib_entry.ref_rx_confidence = opasConfig.RX_CONFIDENCE_NEVERMORE
                bib_entry.link_updated = True                        
            else:
                if options.report_links_days:
                    report_write_changes(bib_entry, reportWriter, verbose)
                    continue

                if bib_entry.ref_rx_confidence == .01 or bib_entry.ref_rx_confidence == opasConfig.RX_CONFIDENCE_POSITIVE:
                    log_everywhere_if(verbose, "info", f"\t...Skipping Marked 'RX_CONFIDENCE_NEVERMORE (.01)' or RX_CONFIDENCE_POSITIVE (1) Reference ID: {bib_entry.ref_rx} Confidence {bib_entry.ref_rx_confidence}")
                    continue
                
                if bib_entry.ref_rx and not ocd.article_exists(bib_entry.ref_rx):
                    log_everywhere_if(verbose, "info", f"\t...Not in PEP. Removing RX link {bib_entry.ref_rx}. Preserving rxcf: {bib_entry.ref_rx}  Will try to match")
                    bib_entry.ref_in_pep = False
                    bib_entry.ref_rx = None
                    bib_entry.ref_rx_confidence = 0
                    bib_entry.link_updated = True

                if bib_entry.ref_rx is None:
                    if verbose and bib_entry.ref_text:
                        one_line_text = bib_entry.ref_text.replace('\n','')
                    # bib_entry.lookup_title_in_db(ocd)
                    bib_entry.identify_heuristic(verbose=verbose)
                    if bib_entry.ref_rx is not None:
                        log_everywhere_if(verbose, "info", f"\t...Matched Reference ID: {bib_entry.ref_rx} Confidence {bib_entry.ref_rx_confidence} Link Updated: {bib_entry.link_updated}")
                        
                elif bib_entry.ref_rx_confidence == .01: # check again
                    log_everywhere_if(verbose, "info", f"\t...Skipping Marked 'RX_CONFIDENCE_NEVERMORE (.01)' Reference ID: {bib_entry.ref_rx} Confidence {bib_entry.ref_rx_confidence}")
                    continue

                if bib_entry.ref_rx is not None:
                    log_everywhere_if(verbose, "info", f"\t...Checking ref_rx from xml for accuracy")
                    bib_entry.lookup_more_exact_artid_in_db(ocd)
                        
                    if bib_entry.ref_is_book and not bib_entry.ref_in_pep and bib_entry.ref_rxcf is not None:
                        log_everywhere_if(verbose, "info", f"\t...Types don't match or not in PEP. Removing RX link {bib_entry.ref_rx}. Preserving rxcf: {bib_entry.ref_rx}")
                        bib_entry.ref_rx = None
                        bib_entry.ref_rx_confidence = 0
                        bib_entry.link_updated = True
                    #elif not bib_entry.record_from_db:
                        #bib_entry.compare_to_database(ocd)
                        #bib_entry.lookup_more_exact_artid_in_db(ocd)

            if bib_entry.link_updated or bib_entry.record_updated or options.forceupdate:
                updated_record_count += 1

                if options.dryrun or options.report_changes:
                    report_write_changes(bib_entry, reportWriter, verbose)
                
                if options.dryrun:
                    continue
     
                # Proceed with actual database update
                success = ocd.save_ref_to_biblioxml_table(bib_entry, bib_entry_was_from_db=True)
                if success:
                    if bib_entry.link_updated or options.forceupdate:
                        log_everywhere_if(verbose, "info", f"\t...Link updated.  Updating DB: rx:{bib_entry.ref_rx} rxcf:{bib_entry.ref_rxcf} source: ({bib_entry.ref_link_source})")
                    else:
                        log_everywhere_if(verbose, "info", f"\t...Record updated. Updating DB.")
                    log_everywhere_if(verbose, "info", f"\t...Time: {time.time() - reference_time_start:.4f} seconds.")
                else:
                    log_everywhere_if(verbose, "error", f"\t...Error saving record.")
                
    ocd.close_connection(caller_name=fname) # make sure connection is closed
    timeEnd = time.time()

    if options.dryrun or options.report_changes or options.report_links_days:
        reportFile.close()
        uploadFilename = f"dry_run_{reportFilename}" if options.dryrun else reportFilename
        uploadFilepath = f"linker/{uploadFilename}"
        object_url = upload_csv_to_s3(reportFilename, os.environ['REPORT_BUCKET'], uploadFilepath)
        message = f"Your CSV file is available in the S3 bucket: {object_url}"
        subject = "Data Linker: Dry Run" if options.dryrun else "Data Linker: Report"
        send_sns_notification(os.environ["SNS_TOPIC_ARN"], message, subject)

    elapsed_seconds = timeEnd-cumulative_time_start # actual processing time going through files
    elapsed_minutes = elapsed_seconds / 60
    log_everywhere_if(True, "info", 80 * "-")
    msg = f"Finished! {updated_record_count} records updated from {counter} references. Total scan/update time: {elapsed_seconds:.2f} secs ({elapsed_minutes:.2f} minutes.) "
    log_everywhere_if(True, "info", msg)
    if counter > 0:
        msg = f"References per elapsed min: {counter/elapsed_minutes:.2f}"
        log_everywhere_if(True, "info", msg)
    
    # return session model object
    return ret_val # None or Session Object

def clean_reference_links(ocd=ocd,
                          sql_set_select = "select * from api_biblioxml2 where bib_rx is not NULL",
                          set_description = "All"
                          ):
    """
    """
    fname = "clean_reference_links"
    ocd.open_connection(caller_name=fname) # make sure connection is open
    ret_val = None
    
    if ocd.db is not None:
        # rows = self.SQLSelectGenerator(sqlSelect)
        curs = ocd.db.cursor(buffered=True, dictionary=True)
        curs.execute(sql_set_select)
        warnings = curs.fetchwarnings()
        if warnings:
            for warning in warnings:
                logger.warning(warning)
                
        counter = 0
        for row in curs.fetchall():
            counter += 1
            art_id = row["art_id"]
            bib_local_id = row["bib_local_id"]
            bib_rx = row["bib_rx"]
            if bib_rx:
                if not ocd.article_exists(bib_rx):
                    print (f"\t...{art_id}/{bib_local_id} - {bib_rx} doesn't exist. Updating record.")
                    success = ocd.update_biblioxml_links(art_id, bib_local_id,
                                                          rx=None,
                                                          rx_confidence=0,
                                                          verbose=True)

    ocd.close_connection(caller_name=fname) # make sure connection is closed
    
    # return session model object
    return ret_val # None or Session Object

def test_runs():
    do_walk_set = True
    do_clean = False
    do_doctest = False
    walk_set = [
                  ("Freud", "select * from api_biblioxml2 where ref_rx is NULL and ref_authors like '%Freud%' and ref_rx_confidence=0"),
                  ("FreudTest", "select * from api_biblioxml2 where art_id LIKE 'APA.017.0421A'")
               ]
        
    if do_walk_set:
        walk_through_reference_set(ocd, set_description=walk_set[1][0], sql_set_select=walk_set[1][1])

    if do_clean:
        clean_set = "Freud", "select * from api_biblioxml2 where ref_rx is not NULL and ref_authors like '%Freud%'"
        clean_reference_links(ocd, clean_set[1])
    
    if do_doctest:
        import doctest
        doctest.testmod(optionflags=doctest.ELLIPSIS|doctest.NORMALIZE_WHITESPACE)
    
    print ("Fini. Tests or Processing complete.")
    
# -------------------------------------------------------------------------------------------------------
# run it!
if __name__ == "__main__":
    global options  # so the information can be used in support functions
    options = None
    description = """Process the api_biblioxml2 table of SQL database opasCentral to find links (and potentially related links) to PEP articles.
    After running opasDataLinker, run opasDataLoader with the --smartbuild option to reprocess any articles which updated links
    from opasDataLinker. This will build new compiled (output) xml files for those with the link data embedded."""
    epilog = """\nNote that the following options are mutually exclusive:
     --after
     --all
     --before
     --key
     --nightly (-n)
     --where
    """
    import optparse
    
    class MyParser(optparse.OptionParser):
        def format_epilog(self, formatter):
            return self.epilog
        
    parser = MyParser(usage="%prog [options]", version=f"%prog ver. {__version__}",
                          description=description, epilog=epilog)

    parser.add_option("-a", "--all", action="store_true", dest="process_all", default=False,
                      help="Option to force all records to be checked.")

    parser.add_option("--after", dest="added_after", default=None,
                      help="Check biblio records last modified AFTER after this datetime (use YYYY-MM-DD format).")

    parser.add_option("--before", dest="scanned_before", default=None,
                      help="Check biblio records last modified BEFORE this datetime (use YYYY-MM-DD format).")

    parser.add_option("-f", "--forcedupdate", action="store_true", dest="forceupdate", default=False,
                      help="Force update to biblio database")

    parser.add_option("--key", dest="file_key", default=None,
                      help="Key for a single file to load, e.g., AIM.076.0269A.  Use in conjunction with --sub for faster processing of single files on AWS")

    parser.add_option("--type, -t", dest="source_type", default=None,
                      help="Source Type to analyze, i.e., book or journal")

    parser.add_option("-u", "--unlinked", action="store_true", dest="unlinked_refs", default=False,
                      help="Check only unlinked (no rx or rxcf) references")

    parser.add_option("-l", "--loglevel", dest="logLevel", default=logging.ERROR,
                      help="Level at which events should be logged (DEBUG, INFO, WARNING, ERROR")

    parser.add_option("--where", dest="where_condition", default=None,
                      help="Add your own SQL clause to the search of api_biblioxml2")
    
    parser.add_option("-n", "--nightly", dest="nightly_includes", action="store_true", default=False,
                      help="Check references in only the new articles added since yesterday")
    
    parser.add_option("-o", "--oldest", dest="run_oldest_first", action="store_true", default=False,
                      help="Whether to run the selected files in reverse order of last update (otherwise by art_id/local_id)")

    parser.add_option("--halfway", action="store_true", dest="halfway", default=False,
                      help="Only process half of the references (e.g., to run two instances, one forward and one reverse.)")

    parser.add_option("-r", "--reverse", dest="run_in_reverse", action="store_true", default=False,
                      help="Whether to run the selected files in reverse order")

    # --load option still the default.  Need to keep for backwards compatibility, at least for now (7/2022)

    parser.add_option("--test", dest="testmode", action="store_true", default=False,
                      help="Run Doctests")

    parser.add_option("--verbose", action="store_true", dest="display_verbose", default=True,
                      help="Display status and operational timing info as load progresses.")

    parser.add_option("--dryrun", action="store_true", dest="dryrun", default=False,
                      help="Output what would be done, but don't do it")

    parser.add_option("--report", action="store_true", dest="report_changes", default=False,
                      help="Generate a report of changes performed")

    parser.add_option("--report-links", dest="report_links_days", type="int", default=None,
                      help="Report on the links added in the past X days without modifying or adding new links.")

    import optparse
    parser.formatter = optparse.IndentedHelpFormatter()
    # Set the width of the help output
    parser.formatter.width = 100
    # Set the width of the option column
    parser.formatter.help_position = 40
    parser.formatter.max_help_position = 40
    (options, args) = parser.parse_args()
    # set toplevel logger to specified loglevel
    logger = logging.getLogger()
    logger.setLevel(options.logLevel)
    # get local logger
    logger = logging.getLogger(programNameShort)
    
    if options.display_verbose: print (help_text)
    if options.unlinked_refs:
        print ("Checking only unlinked--rx and rxcf--records.")
        unlinked_ref_clause = 'and ref_rx is Null and ref_rxcf is Null'
    else:
        unlinked_ref_clause = ''

    #suggested by chatdpt as legal in mysql but doesn't work in mysql
    #limit_clause = ""
    #if options.halfway:
        ## need to see how many references there are and divide by 2
        #limit_clause = "LIMIT (SELECT COUNT(*) * 0.5 FROM api_biblioxml2)"

    type_clause = ""
    if options.source_type:
        if options.source_type[0].upper() == "B":
            type_clause = "and ref_sourcetype = 'book'"
            print ("Checking reference type = books")
        elif options.source_type[0].upper() == "J":
            type_clause = "and ref_sourcetype = 'journal'"
            print ("Checking reference type = journals")

    skip_for_incremental_scans = "and skip_incremental_scans is NULL"

    # scan articles matching art_id (single) or with regex wildcard (multiple)
    biblio_refs_matching_artid = f"""select *
                                     from api_biblioxml2
                                     where art_id RLIKE '%s'
                                     and ref_rx_confidence != {opasConfig.RX_CONFIDENCE_NEVERMORE}
                                     {skip_for_incremental_scans}
                                     {unlinked_ref_clause}
                                     {type_clause}
                                     %s
                                  """

    # scan articles added after date
    biblios_for_articles_after = f"""select *
                                     from api_biblioxml2 as bib
                                     where bib.last_update >= '%s'
                                     {skip_for_incremental_scans}
                                     and ref_rx_confidence != {opasConfig.RX_CONFIDENCE_NEVERMORE}
                                     {unlinked_ref_clause}
                                     {type_clause}
                                     %s
                                  """
    # scan articles with bib entries updated before date
    biblio_refs_updated_before = f"""select *
                                     from api_biblioxml2 as bib
                                     where bib.last_update < '%s'
                                     {skip_for_incremental_scans}
                                     and ref_rx_confidence != {opasConfig.RX_CONFIDENCE_NEVERMORE}
                                     {unlinked_ref_clause}
                                     {type_clause}
                                     %s
                                  """

    # user supplied conditional clause
    biblio_refs_advanced_query = f"""select *
                                     from api_biblioxml2 as bib
                                     where ref_rx_confidence != {opasConfig.RX_CONFIDENCE_NEVERMORE}
                                     {skip_for_incremental_scans}
                                     {unlinked_ref_clause}
                                     {type_clause}
                                     %s
                                  """

    biblio_refs_nightly = f"""SELECT api_biblioxml2.*
                             FROM
                             api_biblioxml2, article_tracker
                             WHERE
                             article_tracker.art_id = api_biblioxml2.art_id
                             and article_tracker.date_inserted >= '{yesterday}'
                             {type_clause}
                             {unlinked_ref_clause}
                           """

    if options.run_in_reverse:
        print ("Reverse order option selected.")
        direction = "DESC"
    else:
        direction = "ASC"

    # run in order of article bib entry updates least recently updated first
    if options.run_oldest_first:
        print ("Scanning biblio table records by last update--oldest first.")
        addon_to_query = f"order by last_update ASC" # oldest first
    else:
        print (f"Scanning biblio table records in {direction} order.")
        addon_to_query = f"order by art_id {direction}, ref_local_id {direction}"
        
    if options.testmode:
        import doctest
        doctest.testmod()
        print ("Fini. opasDataLoader Tests complete.")
        sys.exit()

    if options.report_links_days:
        date_after = datetime.datetime.now() - datetime.timedelta(days=options.report_links_days)
        options.added_after = date_after.strftime('%Y-%m-%d')

    if options.nightly_includes:
        query = biblio_refs_nightly
        print (f"Nightly build option selected. Processing article references added since {yesterday}")
    elif options.where_condition:
        query = biblio_refs_advanced_query % (f" AND {options.where_condition}")
        print (f"Processed records limited by --where condition: ' AND {options.where_condition}'")
    elif options.file_key:
        options.file_key = options.file_key.replace("!", "^")  # on my local machine, can't use ^ on command line arg
        query = biblio_refs_matching_artid % (options.file_key, addon_to_query)
        print (f"Article ID specified. Processing articles matching {options.file_key} ")
    elif options.added_after:
        query = biblios_for_articles_after % (options.added_after, addon_to_query)
        print (f"Nightly build option selected. Processing articles added after {options.added_after}")
    elif options.scanned_before:
        query = biblio_refs_updated_before % (options.scanned_before, addon_to_query)
    elif options.unlinked_refs: # just unlinked, it can still be used with several types above.
        query = biblio_refs_updated_before % (options.scanned_before, addon_to_query)
    else: # options.process_all:
        query = biblio_refs_matching_artid % ('.*', addon_to_query)
        print (f"Processing ALL article references")
    
    walk_through_reference_set(ocd, set_description=f"Key: {options.file_key}", sql_set_select=query, halfway=options.halfway)
    print ("Finished!")
    