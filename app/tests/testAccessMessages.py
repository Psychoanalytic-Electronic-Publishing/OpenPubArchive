#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
logger = logging.getLogger(__name__)

import unittest
import requests
import localsecrets
import pprint
import sys

import opasDocPermissions
from unitTestConfig import base_api, base_plus_endpoint_encoded, headers, session_id, UNIT_TEST_CLIENT_ID, test_login, test_logout 

# Login!
sessID, headers, session_info = test_login(username=localsecrets.PADS_TEST_ARCHIVEONLY, password=localsecrets.PADS_TEST_ARCHIVEONLY_PW)

global message_collection
global pp
pp = pprint.PrettyPrinter(indent=5, width=70) # , stream=sys.stderr) # , sort_dicts=True) sort_dicts requires py 3.8
message_collection = {}

class TestAccessMessageDisplay_To_INSPECT_MANUALLY(unittest.TestCase):
    """
    Note: tests are performed in alphabetical order, hence the function naming
          with forced order in the names.
    
    """
    search_term = "test"
    print (f"User for tests: {localsecrets.PADS_TEST_ID}")
    # Embargoed files (removed IJPOPen for example, xml flag:embargo=true)
    def test_001B_download_embargoed_document_PDF(self):
        full_URL = base_plus_endpoint_encoded(f'/v2/Documents/Downloads/PDF/IJPOPEN.004.0013A/')
        response = requests.get(full_URL, headers=headers)
        r = response.json()
        print (r["detail"])
        assert(response.ok == False)

    def test_001C_download_embargoed_document_PDF_ORIG(self):
        full_URL = base_plus_endpoint_encoded(f'/v2/Documents/Downloads/PDFOrig/IJPOPEN.004.0013A/')
        response = requests.get(full_URL, headers=headers)
        r = response.json()
        print (r["detail"])
        assert(response.ok == False)

    def test_001C_download_embargoed_document_EPUB(self):
        full_URL = base_plus_endpoint_encoded(f'/v2/Documents/Downloads/EPUB/IJPOPEN.004.0013A/')
        response = requests.get(full_URL, headers=headers)
        r = response.json()
        print (r["detail"])
        assert(response.ok == False)

    # Download restricted files (currently long books)
    def test_001D_download_prohibited_documentPDF_ORIG_Page_Restricted(self):
        full_URL = base_plus_endpoint_encoded(f'/v2/Documents/Downloads/PDFOrig/SE.004.R0009A/')
        response = requests.get(full_URL, headers=headers)
        r = response.json()
        print (r["detail"])
        assert(response.ok == False)

    def test_001D_download_prohibited_document_PDF(self):
        full_URL = base_plus_endpoint_encoded(f'/v2/Documents/Downloads/PDF/IPL.064.0001A/')
        response = requests.get(full_URL, headers=headers)
        r = response.json()
        print (r["detail"])
        assert(response.ok == False)

    def test_001D_download_prohibited_document_PDF(self):
        full_URL = base_plus_endpoint_encoded(f'/v2/Documents/Downloads/PDFOrig/IPL.064.0001A/')
        response = requests.get(full_URL, headers=headers)
        r = response.json()
        print (r["detail"])
        assert(response.ok == False)

    def test_001D_download_prohibited_document_EPUB(self):
        full_URL = base_plus_endpoint_encoded(f'/v2/Documents/Downloads/EPUB/IPL.064.0001A/')
        response = requests.get(full_URL, headers=headers)
        r = response.json()
        print (r["detail"])
        assert(response.ok == False)

    def test_001D_download_prohibited_document_PDF_EXCEPTION(self):
        full_URL = base_plus_endpoint_encoded(f'/v2/Documents/Downloads/PDF/IPL.064.0001A/')
        response = requests.get(full_URL, headers=headers)
        r = response.json()
        print (r["detail"])
        assert(response.ok == False)
        usertype = "prohibited.download.pdflong"
        message_collection[usertype] = {r["detail"]}

    def test_001D_download_prohibited_document_PDF_EXCEPTION(self):
        full_URL = base_plus_endpoint_encoded(f'/v2/Documents/Downloads/PDFOrig/IPL.064.0001A/')
        response = requests.get(full_URL, headers=headers)
        r = response.json()
        print (r["detail"])
        assert(response.ok == False)
        usertype = "prohibited.download.pdforig"
        message_collection[usertype] = {r["detail"]}

    def test_001D_download_prohibited_document_EPUB_EXCEPTION(self):
        full_URL = base_plus_endpoint_encoded(f'/v2/Documents/Downloads/EPUB/IPL.064.0001A/')
        response = requests.get(full_URL, headers=headers)
        r = response.json()
        print (r["detail"])
        assert(response.ok == False)
        usertype = "prohibited.download.epub"
        message_collection[usertype] = {r["detail"]}

    def test_002A_Future_Download(self):
        full_URL = base_plus_endpoint_encoded(f'/v2/Documents/Downloads/PDFORIG/PPC.004.0026A/')
        response = requests.get(full_URL, headers=headers)
        r = response.json()
        print (r["detail"])
        assert(response.ok == False)
        usertype = "future.download"
        message_collection[usertype] = {r["detail"]}
        

    def test_002B_Current_Download(self):
        full_URL = base_plus_endpoint_encoded(f'/v2/Documents/Downloads/PDFORIG/JICAP.020.0015A/')
        response = requests.get(full_URL, headers=headers)
        r = response.json()
        print (r["detail"])
        assert(response.ok == False)
        usertype = "subs.current.download"
        message_collection[usertype] = {r["detail"]}

    # now let's get back some errors
    def test_010A_nonexistent_session(self):
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/IJP.056.0303A/?return_format=XML")
        headers = {f"client-session":f"123456789",
                   "client-id": UNIT_TEST_CLIENT_ID, 
                   "Content-Type":"application/json",
                   localsecrets.API_KEY_NAME: localsecrets.API_KEY}
        response = requests.get(full_URL, headers=headers)
        r = response.json()
        print (r["detail"])
        assert(response.ok == False)

    def test_010B_nonexistent_document(self):
        full_URL = base_plus_endpoint_encoded(f'/v2/Documents/Document/APA.064E.6666A/')
        response = requests.get(full_URL, headers=headers)
        r = response.json()
        print (r["detail"])
        assert(response.ok == False)

    def test_010C_nonexistent_document_download(self):
        full_URL = base_plus_endpoint_encoded(f'/v2/Documents/Downloads/PDFOrig/APA.064E.6666A/')
        response = requests.get(full_URL, headers=headers)
        r = response.json()
        print (r["detail"])
        assert(response.ok == False)

    search_term = "test"
    def test_011A_get_current_document_manually_proof_messages(self):
        print ("These next tests will always pass barring an exception encountered.  The return data is for human inspection.")
        # Try to return current content, should only return abstract
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/AJP.080.0001A/?return_format=XML")
        response = requests.get(full_URL, headers=headers)
        # Confirm that the request-response cycle completed successfully.
        assert(response.ok == True)
        r = response.json()
        response_info = r["documents"]["responseInfo"]
        response_set = r["documents"]["responseSet"][0]
        assert (response_set["accessLimited"] == True)
        assert (response_set["accessClassification"] == 'current')
        print(f'Classification: {response_set["accessClassification"]}\n')
        print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
        print(f'Description: {response_set["accessLimitedDescription"]}\n')

    def test_011B_get_future_document_manually_proof_messages(self):
        # Try to return current content, should only return abstract
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/PCAS.024.0100A/?return_format=XML")
        response = requests.get(full_URL, headers=headers)
        # Confirm that the request-response cycle completed successfully.
        assert(response.ok == True)
        r = response.json()
        response_info = r["documents"]["responseInfo"]
        response_set = r["documents"]["responseSet"][0]
        assert (response_set["accessLimited"] == True)   
        assert (response_set["accessClassification"] == 'future')
        print(f'Classification: {response_set["accessClassification"]}')
        print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
        print(f'Description: {response_set["accessLimitedDescription"]}\n')

    def test_011C_get_marked_embargoed_document_manually_proof_messages(self):
        # Try to return current content, should only return abstract
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/IJPOPEN.004.0013A/?return_format=XML")
        response = requests.get(full_URL, headers=headers)
        r = response.json()
        response_info = r["documents"]["responseInfo"]
        response_set = r["documents"]["responseSet"][0]
        print(f'Classification: {response_set["accessClassification"]}')
        print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
        print(f'Description: {response_set["accessLimitedDescription"]}\n')

    # now let's get back some errors
    def test_011D_get_current_document_manually_proof_output(self):
        # Try to return current content, should only return abstract
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/AJP.080.0001A/?return_format=XML")
        response = requests.get(full_URL, headers=headers)
        # Confirm that the request-response cycle completed successfully.
        assert(response.ok == True)
        r = response.json()
        response_info = r["documents"]["responseInfo"]
        response_set = r["documents"]["responseSet"][0]
        assert (response_set["accessLimited"] == True)
        assert (response_set["accessClassification"] == 'current')
        print(f'Classification: {response_set["accessClassification"]}\n')
        print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
        print(f'Description: {response_set["accessLimitedDescription"]}\n')

    def test_012A_get_future_document_manually_proof_messages(self):
        # Try to return current content, should only return abstract
        print("When PCAS goes live (2022-02) this will need to be changed!")
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/PCAS.024.0100A/?return_format=XML")
        response = requests.get(full_URL, headers=headers)
        # Confirm that the request-response cycle completed successfully.
        assert(response.ok == True)
        r = response.json()
        response_info = r["documents"]["responseInfo"]
        response_set = r["documents"]["responseSet"][0]
        assert (response_set["accessLimited"] == True)   
        assert (response_set["accessClassification"] == 'future')
        print(f'Classification: {response_set["accessClassification"]}')
        print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
        print(f'Description: {response_set["accessLimitedDescription"]}\n')

        
    def test_013A_get_ijpopen_only_permission_manually_proof_messages(self):
        print ("Login IJPOPen only user!")
        usertype = "IJPOpen Only subscriber"
        message_collection[usertype] = {}
        
        sessID, headers, session_info = test_login(username=localsecrets.PADS_TEST_IJPOPENONLY, password=localsecrets.PADS_TEST_IJPOPENONLY_PW)
        # Try to return IJPOPEN, should be ok

        # pepcurrent document
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/AJP.080.0001A/?return_format=XML")
        response = requests.get(full_URL, headers=headers)
        # Confirm that the request-response cycle completed successfully.
        assert(response.ok == True)
        r = response.json()
        response_info = r["documents"]["responseInfo"]
        response_set = r["documents"]["responseSet"][0]
        print(f'Classification: {response_set["accessClassification"]}\n')
        print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
        print(f'Description: {response_set["accessLimitedDescription"]}\n')
        assert (response_set["accessLimited"] == True)
        assert (response_set["accessClassification"] == 'current')
        message_collection[usertype][response_set["accessClassification"]] = f'Reason:\n   {response_set["accessLimitedReason"]}\n'

        # peparchive document
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/AJP.024.0017A/?return_format=XML")
        response = requests.get(full_URL, headers=headers)
        # Confirm that the request-response cycle completed successfully.
        assert(response.ok == True)
        r = response.json()
        response_info = r["documents"]["responseInfo"]
        response_set = r["documents"]["responseSet"][0]
        print(f'Classification: {response_set["accessClassification"]}\n')
        print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
        print(f'Description: {response_set["accessLimitedDescription"]}\n')
        assert (response_set["accessLimited"] == True)
        assert (response_set["accessClassification"] == 'archive')
        message_collection[usertype][response_set["accessClassification"]] = f'Reason:\n   {response_set["accessLimitedReason"]}\n'

        # free document
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/PEPGRANTVS.001.0007A/?return_format=XML")
        response = requests.get(full_URL, headers=headers)
        # Confirm that the request-response cycle completed successfully.
        assert(response.ok == True)
        r = response.json()
        response_info = r["documents"]["responseInfo"]
        response_set = r["documents"]["responseSet"][0]
        print(f'Classification: {response_set["accessClassification"]}\n')
        print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
        print(f'Description: {response_set["accessLimitedDescription"]}\n')
        assert (response_set["accessLimited"] == False)
        assert (response_set["accessClassification"] == 'free')
        message_collection[usertype][response_set["accessClassification"]] = f'Reason:\n   {response_set["accessLimitedReason"]}\n'
        
        # ijpopen document
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/IJPOPEN.004.0008A/?return_format=XML")
        response = requests.get(full_URL, headers=headers)
        r = response.json()
        response_info = r["documents"]["responseInfo"]
        response_set = r["documents"]["responseSet"][0]
        print(f'Classification: {response_set["accessClassification"]}')
        print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
        print(f'Description: {response_set["accessLimitedDescription"]}\n')
        assert (response_set["accessLimited"] == False)
        assert (response_set["accessClassification"] == 'special')
        message_collection[usertype][response_set["accessClassification"]] = f'Reason:\n   {response_set["accessLimitedReason"]}\n'

    def test_014A_get_archive_subscriber_only_permission_manually_proof_messages(self):
        print ("Login peparchive subscriber user!")
        usertype = "archive subscriber"
        message_collection[usertype] = {}
        
        sessID, headers, session_info = test_login(username=localsecrets.PADS_TEST_ARCHIVEONLY, password=localsecrets.PADS_TEST_ARCHIVEONLY_PW)
        # Try to return IJPOPEN, should be ok

        # pepcurrent document
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/AJP.080.0001A/?return_format=XML")
        response = requests.get(full_URL, headers=headers)
        # Confirm that the request-response cycle completed successfully.
        assert(response.ok == True)
        r = response.json()
        response_info = r["documents"]["responseInfo"]
        response_set = r["documents"]["responseSet"][0]
        print(f'Classification: {response_set["accessClassification"]}\n')
        print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
        print(f'Description: {response_set["accessLimitedDescription"]}\n')
        assert (response_set["accessLimited"] == True)
        assert (response_set["accessClassification"] == 'current')
        message_collection[usertype][response_set["accessClassification"]] = f'Reason:\n   {response_set["accessLimitedReason"]}\n'

        # peparchive document
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/AJP.024.0017A/?return_format=XML")
        response = requests.get(full_URL, headers=headers)
        # Confirm that the request-response cycle completed successfully.
        assert(response.ok == True)
        r = response.json()
        response_info = r["documents"]["responseInfo"]
        response_set = r["documents"]["responseSet"][0]
        print(f'Classification: {response_set["accessClassification"]}\n')
        print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
        print(f'Description: {response_set["accessLimitedDescription"]}\n')
        assert (response_set["accessLimited"] == False)
        assert (response_set["accessClassification"] == 'archive')
        message_collection[usertype][response_set["accessClassification"]] = f'Reason:\n   {response_set["accessLimitedReason"]}\n'

        # free document
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/PEPGRANTVS.001.0007A/?return_format=XML")
        response = requests.get(full_URL, headers=headers)
        # Confirm that the request-response cycle completed successfully.
        assert(response.ok == True)
        r = response.json()
        response_info = r["documents"]["responseInfo"]
        response_set = r["documents"]["responseSet"][0]
        print(f'Classification: {response_set["accessClassification"]}\n')
        print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
        print(f'Description: {response_set["accessLimitedDescription"]}\n')
        assert (response_set["accessLimited"] == False)
        assert (response_set["accessClassification"] == 'free')
        message_collection[usertype][response_set["accessClassification"]] = f'Reason:\n   {response_set["accessLimitedReason"]}\n'
        
        # ijpopen document
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/IJPOPEN.004.0008A/?return_format=XML")
        response = requests.get(full_URL, headers=headers)
        r = response.json()
        response_info = r["documents"]["responseInfo"]
        response_set = r["documents"]["responseSet"][0]
        print(f'Classification: {response_set["accessClassification"]}')
        print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
        print(f'Description: {response_set["accessLimitedDescription"]}\n')
        assert (response_set["accessLimited"] == False)
        assert (response_set["accessClassification"] == 'special')
        message_collection[usertype][response_set["accessClassification"]] = f'Reason:\n   {response_set["accessLimitedReason"]}\n'

    def test_015A_get_pepall_permission_manually_proof_messages(self):
        print ("Login pep all access user!")
        usertype = "All Access user"
        message_collection[usertype] = {}

        sessID, headers, session_info = test_login(username=localsecrets.PADS_TEST_ARCHIVEANDCURRENT, password=localsecrets.PADS_TEST_ARCHIVEANDCURRENT_PW)
        # pepcurrent document
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/AJP.080.0001A/?return_format=XML")
        response = requests.get(full_URL, headers=headers)
        # Confirm that the request-response cycle completed successfully.
        assert(response.ok == True)
        r = response.json()
        response_info = r["documents"]["responseInfo"]
        response_set = r["documents"]["responseSet"][0]
        print ("***** Test PEPCURRENT (AJP) Permission *****")
        assert (response_set["accessLimited"] == False)
        assert (response_set["accessClassification"] == 'current')
        print(f'Classification: {response_set["accessClassification"]}\n')
        print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
        print(f'Description: {response_set["accessLimitedDescription"]}\n')
        message_collection[usertype][response_set["accessClassification"]] = f'Reason:\n   {response_set["accessLimitedReason"]}\n'

        # peparchive document
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/AJP.024.0017A/?return_format=XML")
        response = requests.get(full_URL, headers=headers)
        # Confirm that the request-response cycle completed successfully.
        assert(response.ok == True)
        r = response.json()
        response_info = r["documents"]["responseInfo"]
        response_set = r["documents"]["responseSet"][0]
        print ("***** Test PEPARCHIVE (AJP) Permission *****")
        print(f'Classification: {response_set["accessClassification"]}\n')
        print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
        print(f'Description: {response_set["accessLimitedDescription"]}\n')
        assert (response_set["accessLimited"] == False)
        assert (response_set["accessClassification"] == 'archive')
        message_collection[usertype][response_set["accessClassification"]] = f'Reason:\n   {response_set["accessLimitedReason"]}\n'

        # free document
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/PEPGRANTVS.001.0007A/?return_format=XML")
        response = requests.get(full_URL, headers=headers)
        # Confirm that the request-response cycle completed successfully.
        assert(response.ok == True)
        r = response.json()
        response_info = r["documents"]["responseInfo"]
        response_set = r["documents"]["responseSet"][0]
        print ("***** Test PEPFREE (PEPGRANTVS) Permission *****")
        print(f'Classification: {response_set["accessClassification"]}\n')
        print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
        print(f'Description: {response_set["accessLimitedDescription"]}\n')
        assert (response_set["accessLimited"] == False)
        assert (response_set["accessClassification"] == 'free')
        message_collection[usertype][response_set["accessClassification"]] = f'Reason:\n   {response_set["accessLimitedReason"]}\n'

        # Try to return IJPOPEN, should be ok
        # ijpopen document
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/IJPOPEN.004.0008A/?return_format=XML")
        response = requests.get(full_URL, headers=headers)
        r = response.json()
        response_info = r["documents"]["responseInfo"]
        response_set = r["documents"]["responseSet"][0]
        print ("***** Test IJPOPEN Permission *****")
        print(f'Classification: {response_set["accessClassification"]}')
        print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
        print(f'Description: {response_set["accessLimitedDescription"]}\n')
        assert (response_set["accessLimited"] == False)
        assert (response_set["accessClassification"] == 'special')
        message_collection[usertype][response_set["accessClassification"]] = f'Reason:\n   {response_set["accessLimitedReason"]}\n'

    def test_016A_get_registered_user_permission_manually_proof_messages(self):
        print ("Login registered user!")
        usertype = "Registered user"
        # global message_collection
        message_collection[usertype] = {}
        
        sessID, headers, session_info = test_login(username=localsecrets.PADS_TEST_REGISTEREDUSER, password=localsecrets.PADS_TEST_REGISTEREDUSER_PW)
        # Try to return IJPOPEN
        # ijpopen document
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/IJPOPEN.004.0008A/?return_format=XML")
        response = requests.get(full_URL, headers=headers)
        r = response.json()
        response_info = r["documents"]["responseInfo"]
        response_set = r["documents"]["responseSet"][0]
        assert (response_set["accessLimited"] == True)
        assert (response_set["accessClassification"] == 'special')
        print(f'Classification: {response_set["accessClassification"]}')
        print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
        print(f'Description: {response_set["accessLimitedDescription"]}\n')
        message_collection[usertype][response_set["accessClassification"]] = f'Reason:\n   {response_set["accessLimitedReason"]}\n'

        # pepcurrent document
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/AJP.080.0001A/?return_format=XML")
        response = requests.get(full_URL, headers=headers)
        # Confirm that the request-response cycle completed successfully.
        assert(response.ok == True)
        r = response.json()
        response_info = r["documents"]["responseInfo"]
        response_set = r["documents"]["responseSet"][0]
        assert (response_set["accessLimited"] == True)
        assert (response_set["accessClassification"] == 'current')
        print(f'Classification: {response_set["accessClassification"]}\n')
        print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
        print(f'Description: {response_set["accessLimitedDescription"]}\n')
        message_collection[usertype][response_set["accessClassification"]] = f'Reason:\n   {response_set["accessLimitedReason"]}\n'

        # peparchive document
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/AJP.024.0017A/?return_format=XML")
        response = requests.get(full_URL, headers=headers)
        # Confirm that the request-response cycle completed successfully.
        assert(response.ok == True)
        r = response.json()
        response_info = r["documents"]["responseInfo"]
        response_set = r["documents"]["responseSet"][0]
        assert (response_set["accessLimited"] == True)
        assert (response_set["accessClassification"] == 'archive')
        print(f'Classification: {response_set["accessClassification"]}\n')
        print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
        print(f'Description: {response_set["accessLimitedDescription"]}\n')
        message_collection[usertype][response_set["accessClassification"]] = f'Reason:\n   {response_set["accessLimitedReason"]}\n'

        # free document
        full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/PEPGRANTVS.001.0007A/?return_format=XML")
        response = requests.get(full_URL, headers=headers)
        # Confirm that the request-response cycle completed successfully.
        assert(response.ok == True)
        r = response.json()
        response_info = r["documents"]["responseInfo"]
        response_set = r["documents"]["responseSet"][0]
        assert (response_set["accessLimited"] == False)
        assert (response_set["accessClassification"] == 'free')
        print(f'Classification: {response_set["accessClassification"]}\n')
        print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
        print(f'Description: {response_set["accessLimitedDescription"]}\n')
        message_collection[usertype][response_set["accessClassification"]] = f'Reason:\n   {response_set["accessLimitedReason"]}\n'

    def test_017A_non_logged_in_user_manually_proof_messages(self):
        usertype = "Non-Logged-in-user"
        # global message_collection
        message_collection[usertype] = {}
        
        sessID, headers, session_info = test_login(username=localsecrets.PADS_TEST_REGISTEREDUSER, password=localsecrets.PADS_TEST_REGISTEREDUSER_PW)
        if test_logout(sessID):
            # Try to return IJPOPEN
            # ijpopen document
            full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/IJPOPEN.004.0008A/?return_format=XML")
            response = requests.get(full_URL, headers=headers)
            r = response.json()
            response_info = r["documents"]["responseInfo"]
            response_set = r["documents"]["responseSet"][0]
            assert (response_set["accessLimited"] == True)
            assert (response_set["accessClassification"] == 'special')
            print(f'Classification: {response_set["accessClassification"]}')
            print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
            print(f'Description: {response_set["accessLimitedDescription"]}\n')
            message_collection[usertype][response_set["accessClassification"]] = f'Reason:\n   {response_set["accessLimitedReason"]}\n'
    
            # pepcurrent document
            full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/AJP.080.0001A/?return_format=XML")
            response = requests.get(full_URL, headers=headers)
            # Confirm that the request-response cycle completed successfully.
            assert(response.ok == True)
            r = response.json()
            response_info = r["documents"]["responseInfo"]
            response_set = r["documents"]["responseSet"][0]
            assert (response_set["accessLimited"] == True)
            assert (response_set["accessClassification"] == 'current')
            print(f'Classification: {response_set["accessClassification"]}\n')
            print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
            print(f'Description: {response_set["accessLimitedDescription"]}\n')
            message_collection[usertype][response_set["accessClassification"]] = f'Reason:\n   {response_set["accessLimitedReason"]}\n'
    
            # peparchive document
            full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/AJP.024.0017A/?return_format=XML")
            response = requests.get(full_URL, headers=headers)
            # Confirm that the request-response cycle completed successfully.
            assert(response.ok == True)
            r = response.json()
            response_info = r["documents"]["responseInfo"]
            response_set = r["documents"]["responseSet"][0]
            assert (response_set["accessLimited"] == True)
            assert (response_set["accessClassification"] == 'archive')
            print(f'Classification: {response_set["accessClassification"]}\n')
            print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
            print(f'Description: {response_set["accessLimitedDescription"]}\n')
            message_collection[usertype][response_set["accessClassification"]] = f'Reason:\n   {response_set["accessLimitedReason"]}\n'
    
            # free document
            full_URL = base_plus_endpoint_encoded(f"/v2/Documents/Document/PEPGRANTVS.001.0007A/?return_format=XML")
            response = requests.get(full_URL, headers=headers)
            # Confirm that the request-response cycle completed successfully.
            assert(response.ok == True)
            r = response.json()
            response_info = r["documents"]["responseInfo"]
            response_set = r["documents"]["responseSet"][0]
            assert (response_set["accessLimited"] == False)
            assert (response_set["accessClassification"] == 'free')
            print(f'Classification: {response_set["accessClassification"]}\n')
            print(f'Reason:\n   {response_set["accessLimitedReason"]}\n')
            print(f'Description: {response_set["accessLimitedDescription"]}\n')
            message_collection[usertype][response_set["accessClassification"]] = f'Reason:\n   {response_set["accessLimitedReason"]}\n'
            pp.pprint(message_collection)
    
if __name__ == '__main__':
    unittest.main()
    #print (message_collection)
    # pp.pprint(message_collection)