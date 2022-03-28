#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=C0321,C0103,C0301,E1101,C0303,E1004,C0330,R0915,R0914,W0703,C0326
# Disable many annoying pylint messages, warning me about variable naming for example.
# yes, in my Solr code I'm caught between two worlds of snake_case and camelCase.

__author__      = "Neil R. Shapiro"
__copyright__   = "Copyright 2019-2021, Psychoanalytic Electronic Publishing"
__license__     = "Apache 2.0"
__version__     = "2022.0107.1" 
__status__      = "Development"

programNameShort = "compareTables"
import sys
if sys.version_info[0] < 3:
    raise Exception("Must be using Python 3")


import sys
sys.path.append('../libs')
sys.path.append('../config')
sys.path.append('../libs/configLib')

import re
import os
import os.path
import time
from datetime import datetime as datetime1

import logging
logger = logging.getLogger(programNameShort)
logger.setLevel(logging.DEBUG)
import mysql.connector

from optparse import OptionParser

import localsecrets
# import opasCentralDBLib
# import opasGenSupportLib as opasgenlib

from localsecrets import STAGE_DB_HOST, STAGE2PROD_PW, STAGE2PROD_USER, PRODUCTION_DB_HOST, AWSDEV_DB_HOST 
DEV_DBHOST = "localhost"
DEV_DBUSER = "root"
DEV_DBPW = ""

def is_date_time(date_text):
    ret_val = True
    try:
        if isinstance(date_text, datetime1):
            ret_val = True
        else:
            val = datetime1.strptime(date_text, '%Y-%m-%d')
            
    except ValueError:
        ret_val = False

    return ret_val

class opasCentralDBMini(object):
    """
    This object should be used and then discarded in any multiuser mode.
    Therefore, keeping session info in the object is ok
    
    """
    connection_count = 0
    
    def __init__(self, session_id=None,
                 host=localsecrets.DBHOST,
                 port=localsecrets.DBPORT,
                 user=localsecrets.DBUSER,
                 password=localsecrets.DBPW,
                 database=localsecrets.DBNAME):

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connected = False
        self.db = None
        self.session_id = session_id # deprecate?
    
    def open_connection(self, dbname=localsecrets.DBNAME, caller_name=""):
        """
        Opens a connection if it's not already open.
        
        If already open, no changes.
        >>> ocd = opasCentralDB()
        >>> ocd.open_connection("my name")
        True
        >>> ocd.close_connection("my name")
        """
        try:
            status = self.db.open
            self.connected = True
        except:
            # not open reopen it.
            status = False
        
        if status == False:
            try:
                opasCentralDBMini.connection_count += 1
                self.db = mysql.connector.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database)
                self.connected = True
                logger.debug(f"Database opened by ({caller_name}) Specs: {self.database} for host {self.host},  user {self.user} port {self.port} Opened connection #{opasCentralDBMini.connection_count}")
                
            except Exception as e:
                self.connected = False
                logger.error(f"compareTablesDBError: Cannot connect to database {self.database} for host {self.host},  user {self.user} port {self.port} ({e})")
                self.db = None

        return self.connected

    def close_connection(self, caller_name=""):
        try:
            self.db.close()
            self.db = None
            opasCentralDBMini.connection_count -= 1
            logger.debug(f"Database closed by ({caller_name})")
                
        except Exception as e:
            logger.error(f"caller: {caller_name} the db is not open ({e}).")

        self.connected = False
        return self.connected

    def get_table_sql(self, sql):
        """
         Returns 0,[] if no rows are returned
        """
        ret_val = []
        row_count = 0
        caller_name = "get_table_sql"
        # always make sure we have the right input value
        self.open_connection(caller_name=caller_name) # make sure connection is open
        
        if self.db is not None:
            cursor = self.db.cursor(mysql.connector.cursor)
            cursor.execute(sql)
            warnings = cursor.fetchwarnings()
            if warnings:
                for warning in warnings:
                    logger.warning(warning)

            ret_val = cursor.fetchall() # returns empty list if no rows
            row_count = cursor.rowcount

            cursor.close()
        else:
            logger.fatal("Connection not available to database.")
        
        self.close_connection(caller_name=caller_name) # make sure connection is closed
        return row_count, ret_val

#----------------------------------------------------------------------------------------
#  End OpasCentralDBMini
#----------------------------------------------------------------------------------------


def compare_critical_columns(table_name, key_col_name, value_col_name):
    #  compare local dev and production before pushing
    #  open databases
    try:
        print ("Comparing local DEV table with Production")
        dev_db = opasCentralDBMini(host=DEV_DBHOST, password=DEV_DBPW, user=DEV_DBUSER)
        prod_db = opasCentralDBMini(host=PRODUCTION_DB_HOST, password=STAGE2PROD_PW[1], user=STAGE2PROD_USER[1])
    except Exception as e:
        logger.error(f"Cannot open dev or production databases: {e}.  Terminating without changes")
    else:
        pass

    sql1=f"select {key_col_name}, {value_col_name} from {table_name} order by 1 ASC"
    dev_row_count, dev_tbl = dev_db.get_table_sql(sql1)
    prod_row_count, prod_tbl = prod_db.get_table_sql(sql1)
    # dev_dict = {}
    prod_dict = {}

    for n in prod_tbl:
        # unpack and store
        key_col_val, value_col_val = n
        prod_dict[key_col_val] = value_col_val
        
    count = 0
    for n in dev_tbl:
        key_col_val, value_col_val = n
        try:
            if value_col_val != prod_dict[key_col_val]:
                count += 1
                print (f"Difference in {value_col_name}: {(value_col_val, value_col_val, prod_dict[value_col_val])}")
        except KeyError:
            print (f"Key: {key_col_val} not on production")

    print (f"{count} differences!")
    return count

def compare_critical_column_lists(table_name, key_col_name, value_col_name_list, db1Name="STAGE", db2Name="PRODUCTION", verbose=False):
    #  compare local dev and production before pushing
    #  open databases

    try:
        if db1Name == "DEV":
            dev_db = opasCentralDBMini(host=DEV_DBHOST, password=DEV_DBPW, user=DEV_DBUSER)
            
        elif db1Name == "STAGE":
            dev_db = opasCentralDBMini(host=STAGE_DB_HOST, password=STAGE2PROD_PW[1], user=STAGE2PROD_USER[1])
                
        if db2Name == "STAGE":
            target_db = opasCentralDBMini(host=STAGE_DB_HOST, password=STAGE2PROD_PW[1], user=STAGE2PROD_USER[1])
        elif db2Name == "PRODUCTION":
            target_db = opasCentralDBMini(host=PRODUCTION_DB_HOST, password=STAGE2PROD_PW[1], user=STAGE2PROD_USER[1])
    except Exception as e:
        logger.error(f"Cannot open dev or production databases: {e}.  Terminating without changes")
    else:
        print (f"\nComparing {db1Name} table {table_name} with {db2Name}")


    for value_col_name in value_col_name_list:
        # if verbose: print (f"\tChecking: {value_col_name}")
        sql1=f"select {key_col_name}, {value_col_name} from {table_name} order by 1 ASC"
        dev_row_count, dev_tbl = dev_db.get_table_sql(sql1)
        target_row_count, target_tbl = target_db.get_table_sql(sql1)
        #dev_dict = {}
        target_dict = {}

        for n in target_tbl:
            # unpack and store
            key_col_val, value_col_val = n
            target_dict[key_col_val] = value_col_val
            
        count = 0
        for n in dev_tbl:
            key_col_val, value_col_val = n
            try:
                if value_col_val != target_dict[key_col_val]:
                    count += 1
                    print (f"Difference in {value_col_name}: {(value_col_val, target_dict[key_col_val])}")
            except KeyError:
                print (f"Key: {key_col_val} not on target")
    
        if count > 0 or verbose: print (f"\t{value_col_name} has {count} differences!")
    
    return count
    

def compare_tables(db_tables=None):

    def_db_tables = [{"name": "api_productbase", "key": "basecode"},
                     #{"name": "vw_api_productbase_instance_counts", "key": "basecode"},
                     {"name": "api_endpoints", "key": "api_endpoint_id"},
                     {"name": "vw_api_messages", "key": "msg_num_code, msg_language"},
                     {"name": "api_client_apps", "key": "api_client_id"}
    ]
    
    if db_tables is None:
        db_tables = def_db_tables

    #  open databases
    try:
        stage_ocd = opasCentralDBMini(host=STAGE_DB_HOST, password=STAGE2PROD_PW[0], user=STAGE2PROD_USER[0])
        prod_ocd = opasCentralDBMini(host=PRODUCTION_DB_HOST, password=STAGE2PROD_PW[1], user=STAGE2PROD_USER[1])
        awsdev = opasCentralDBMini(host=AWSDEV_DB_HOST, password=STAGE2PROD_PW[2], user=STAGE2PROD_USER[2])
        # if local
        dev_ocd = opasCentralDBMini(host=DEV_DBHOST, password=DEV_DBPW, user=DEV_DBUSER)

    except Exception as e:
        logger.error(f"Cannot open stage or production databases: {e}.  Terminating without changes")
    else:
        pass
    
    total_diffs = 0       
    for db_table in db_tables:
        sql1 = f"""SELECT * from {db_table['name']} ORDER BY {db_table['key']} ASC;"""

        try:
            print (80*"=")
            print (f"Evaluating table: {db_table['name']}")
            stage_row_count, stage_tbl = stage_ocd.get_table_sql(sql1)
            dev_row_count, dev_tbl = dev_ocd.get_table_sql(sql1)
            awsdev_row_count, awsdev_tbl = awsdev.get_table_sql(sql1)
            prod_row_count, prod_tbl = prod_ocd.get_table_sql(sql1)
            if stage_row_count != dev_row_count != awsdev_row_count != prod_row_count:
                print (f"\t{db_table['name']} differs!")
                continue
            else:
                row_count = stage_row_count
                print (f"\tRow counts: {(dev_row_count, awsdev_row_count, stage_row_count, prod_row_count)}")
            
            stage_col_count = len(stage_tbl[0])
            dev_col_count = len(dev_tbl[0])
            awsdev_col_count = len(awsdev_tbl[0])
            prod_col_count = len(prod_tbl[0])
            
            diffs = 0
            coldiffs = 0
            if stage_col_count != dev_col_count:
                print (f"Stage column count {stage_col_count} different than Dev column count {dev_col_count}.")
                coldiffs += 1
            if stage_col_count != awsdev_col_count:
                print (f"Stage column count {stage_col_count} different than AWSDev column count {awsdev_col_count}.")
                coldiffs += 1
            if stage_col_count != prod_col_count:
                print (f"Stage column count {stage_col_count} different than Prod column count {prod_col_count}.")
                coldiffs += 1

            if coldiffs > 0:
                print ("Column count differences.  Stopping compare.")
            else:
                for n in range(row_count):
                    if dev_tbl[n] != stage_tbl[n]:
                        print (f"\tLocal Dev vs Stage: {db_table['name']} row {n} differs!")
                        for item in range(len(stage_tbl[n])):
                            if dev_tbl[n][item] !=  stage_tbl[n][item]:
                                print (f"\t\tCol {item} Dev: {dev_tbl[n][item]}")
                                print (f"\t\tCol {item} Stage: {stage_tbl[n][item]}")
                                print (f"\t\t{40*'-'}")
                        #print (f"\t\tDev: {dev_tbl[n]}")
                        #print (f"\t\tStage: {stage_tbl[n]}")
                        diffs += 1
                    if stage_tbl[n] != awsdev_tbl[n]:
                        print (f"\tStage vs AWS Dev: {db_table['name']} row {n} differs!")
                        for item in range(len(stage_tbl[n])):
                            if awsdev_tbl[n][item] !=  stage_tbl[n][item]:
                                if is_date_time(awsdev_tbl[n][item]):
                                    pass
                                else:
                                    print (f"\t\tCol {item} Dev: {awsdev_tbl[n][item]}")
                                    print (f"\t\tCol {item} Stage: {stage_tbl[n][item]}")
                                    print (f"\t\t{40*'-'}")
                        #print (f"\t\tStage: {stage_tbl[n]}")
                        #print (f"\t\tAWSDev: {awsdev_tbl[n]}")
                        diffs += 1
                    if stage_tbl[n] != prod_tbl[n]:
                        print (f"\tStage vs Prod: {db_table['name']} row {n} differs!")
                        for item in range(len(stage_tbl[n])):
                            if prod_tbl[n][item] != stage_tbl[n][item]:
                                if is_date_time(awsdev_tbl[n][item]):
                                    pass
                                else:
                                    print (f"\t\tCol {item} Dev: {prod_tbl[n][item]}")
                                    print (f"\t\tCol {item} Stage: {stage_tbl[n][item]}")
                                    print (f"\t\t{40*'-'}")
                        #print (f"\t\tStage: {stage_tbl[n]}")
                        #print (f"\t\tProd: {prod_tbl[n]}")
                        diffs += 1
                    if diffs > 10:
                        print (f"{diffs} row differences found; compare was discontinued.")
                        break

            if diffs == 0 and coldiffs == 0:
                print(f"\t{db_table['name']} Tables are the same.")
            else:
                print(f"\t{db_table['name']} Tables Differ.  Row diff Count: {diffs}")
    
            total_diffs += diffs
            
        except IndexError:
            pass # column count difference
        
        except Exception as e:
            print (f"Error: {e}")

    return total_diffs

#------------------------------------------------------------------------------------------------------
def main():

    print(
        f""" 
            {programNameShort} - CompareTables
        
            This program compares the important setup tables in the four MySQL/RDS databases
            used in the PEP-Web data preparation and Production process.
            
            The tables compared need to be in sync for the system to operate properly.
            
            Databases:
              dev - localhost development database
              awsdev - Development database used by production process
              stage - stage server for testing builds
              production - PEP-Web end-user site
            
            Tables:
              The tested tables are listed in a list of dicts.  They currently include:
              db_tables = "api_productbase" - List of journals and books and metadata
                          "api_endpoints"   - List of endpoints and ids
                          "api_messages"    - API return messages (from the server)
                          "api_client_apps" - List of registered client apps
              
            Example Invocation:
                    $ python compareTables.py
                    
            Requires Python 3
            
        """
    )

    # set toplevel logger to specified loglevel
    logger = logging.getLogger()
    logger.setLevel(logging.WARN)
    # get local logger
    logger = logging.getLogger(programNameShort)
    logger.info('Started at %s', datetime1.today().strftime('%Y-%m-%d %H:%M:%S"'))
    logger.setLevel(logging.WARN)

    timeStart = time.time()
    print (f"Processing started at ({time.ctime()}).")
    print((80*"-"))

# -------------------------------------------------------------------------------------------------------
# run it!

#if __name__ == "__main__":
    #main()
