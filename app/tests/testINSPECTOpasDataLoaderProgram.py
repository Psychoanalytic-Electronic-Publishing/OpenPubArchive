#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Third-party imports...
#from nose.tools import assert_true

#  This test module is in development...

import sys
import os.path

folder = os.path.basename(os.path.dirname(os.path.abspath(__file__)))
if folder == "tests": # testing from within WingIDE, default folder is tests
    sys.path.append('../libs')
    sys.path.append('../config')
    sys.path.append('../../app')
else: # python running from should be within folder app
    sys.path.append('./libs')
    sys.path.append('./config')


from starlette.testclient import TestClient

import unittest
from localsecrets import CONFIG
import subprocess

class TestOpasLoaderProgram(unittest.TestCase):
    """
    Tests
    
    Note: tests are performed in alphabetical order, hence the function naming
          with forced order in the names.
    
    """
    def test_process_sub(self):
        result = subprocess.run([sys.executable, '../opasDataLoader/opasDataLoader.py', '--sub=_PEPFree', '--nocheck', '--nohelp', '--load'], capture_output=True)
        out = result.stdout.decode("UTF-8")
        err = result.stderr.decode("UTF-8")
        print ("Stdout:")
        print (out[-240:])
        print ("Stderr:")
        print (err[-400:])
        self.assertIn(b'Load process complete', result.stdout)

    def test_process_newroot(self):
        if CONFIG == "Local":
            #--nocheck -d X:\_PEPA1\_PEPa1v --sub=_PEPFree
            print (os.getcwd())
            result = subprocess.run([sys.executable, '../opasDataLoader/opasDataLoader.py', '-de:/usr3/GitHub/openpubarchive/app/tests/testdatasource/_PEPFree', '--nocheck', '--verbose', '--smartload', '--rebuild'], capture_output=True)
        else:
            result = subprocess.run([sys.executable, '../opasDataLoader/opasDataLoader.py', '-dpep-web-xml', '--sub=_PEPFree', '--nocheck', '--nohelp', '--load'], capture_output=True)

        out = result.stdout.decode("UTF-8")
        err = result.stderr.decode("UTF-8")
        print ("Stdout:")
        print (out[-2600:])
        print ("Stderr:")
        print (err[-1600:])
        self.assertIn(b'Compile, save and load process complete', result.stdout)


if __name__ == '__main__':
    unittest.main()
    print ("Tests Complete.")