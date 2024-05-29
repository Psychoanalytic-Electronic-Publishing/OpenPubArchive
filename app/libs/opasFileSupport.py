#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__      = "Neil R. Shapiro"
__copyright__   = "Copyright 2019-2021, Psychoanalytic Electronic Publishing"
__license__     = "Apache 2.0"
__version__     = "2020.09.13"
__status__      = "Development"

# #TODO
    # Ensure full PC Local/S3 file system interchangeability/transparency.

# Revision Notes:
    # 20200913 Reworked for better S3 functionality and speed
    # 20200530 Added front matter.  Fixed doctest reference (should have been doc rather than docs)

import sys
import localsecrets
import s3fs # https://s3fs.readthedocs.io/en/latest/api.html#s3fs.core.S3FileSystem
import os, os.path
import re
import boto3
import logging
import datetime
import time
import pathlib
import opasConfig

logger = logging.getLogger(__name__)

def file_exists(document_id, year, ext, path=localsecrets.PDF_ORIGINALS_PATH):
    flex_fs = FlexFileSystem(key=localsecrets.S3_KEY,
                             secret=localsecrets.S3_SECRET,
                             root=path) 
    filename = flex_fs.get_download_filename(filespec=document_id, path=path, year=year, ext=ext)
    if filename is None:
        ret_val = False
    else:
        ret_val = True

    return ret_val

class FileInfo(object):
    def __init__(self, path=None):
        self.fs = FlexFileSystem(key=localsecrets.S3_KEY,
                                 secret=localsecrets.S3_SECRET,
                                 root=path) 
        self.fs_s3type = localsecrets.S3_KEY is not None
        self.build_date = time.time()
        self.filespec:str = None
        self.basename:str = None
        self.filesize:int = None
        self.filetype = None
        self.etag:str = None
        self.timestamp_str:str = None
        self.timestamp:datetime.datetime = None # datetime.datetime.strptime(self.timestamp_str, opasConfig.TIME_FORMAT_STR)
        self.date_modified:datetime.datetime = None # self.timestamp.date()
        # self.date_modified_str:str = None # str(self.date)
        self.fileinfo:dict = {}       
        
    def mapS3(self, fileinfo: dict):
        ret_val = True
        try:
            self.fileinfo = fileinfo
            self.filespec = fileinfo["Key"]
            self.basename = os.path.basename(self.filespec)
            self.filesize = fileinfo["Size"]
            self.filetype = fileinfo["type"]
            self.build_date = time.time() # current time
            # there's no create time on S3, so use LastModified.
            self.create_time = datetime.datetime.strftime(fileinfo["LastModified"], opasConfig.TIME_FORMAT_STR)
            # modified date
            self.timestamp_str = datetime.datetime.strftime(fileinfo["LastModified"], opasConfig.TIME_FORMAT_STR)
            self.timestamp = datetime.datetime.strptime(self.timestamp_str, opasConfig.TIME_FORMAT_STR)
            self.date_modified = self.timestamp.date()
            # self.date_modified_str = str(self.date_modified)
            self.etag = fileinfo.get("Etag", None)
        except Exception as e:
            logger.error(f"mapS3 exception: {e}")
            ret_val = False
            
        return ret_val
    
    def mapLocalFS(self, filespec: str):
        ret_val = True
        try:
            self.fileinfo = {}
            self.filespec = filespec
            self.basename = self.fileinfo["base_filename"] = os.path.basename(self.filespec)
            self.filesize = self.fileinfo["Size"] = os.path.getsize(filespec)
            self.filetype = self.fileinfo["type"] = "xml" # fileinfo["type"]
            self.build_date = self.fileinfo["build_date"] = time.time() # current time
            self.create_time = datetime.datetime.fromtimestamp(os.path.getctime(self.filespec)).strftime(opasConfig.TIME_FORMAT_STR)
            # modified date
            mod_date = self.fileinfo["fileSize"] = os.path.getmtime(filespec)
            self.timestamp_str = self.fileinfo["LastModified"] = datetime.datetime.utcfromtimestamp(mod_date).strftime(opasConfig.TIME_FORMAT_STR)
            self.timestamp = self.fileinfo["timestamp"] = datetime.datetime.strptime(self.timestamp_str, opasConfig.TIME_FORMAT_STR)
            self.date_modified = self.fileinfo["date"] = self.timestamp.date()
            # self.date_modified_str = str(self.date_modified)
            self.etag = self.fileinfo["Etag"] = None
        except FileNotFoundError:
            ret_val = False
        except Exception as e:
            logger.error(f"mapLocalFS: {e}")
            ret_val = False
            
        return ret_val

    def mapFS(self, filespec, path=None):
        ret_val = False
        if self.fs_s3type:
            fileinfo = self.fs.fileinfo(filespec, path=path)
            if fileinfo is not None:
                ret_val = self.mapS3(fileinfo.fileinfo)
                ret_val = True
        else:
            ret_val = self.mapLocalFS(filespec)        
        
        return ret_val # false if it doesn't exist

class FlexFileSystem(object):
    """
    File access to different types of file systems, 'transparently',
      the only difference being the path and if there's a key, then
      it's accessed via secure S3.
      
    Uses the s3fs library which does the heavy lifting, the only purpose
      of this class is to provide file operations transparently
      wherever the file may be, depending on how the class is initialized

      https://s3fs.readthedocs.io/en/latest/api.html#s3fs.core.S3FileSystem
      
    """

    def __init__(self, key=None, secret=None, anon=False, root=None):
        if key is None:
            self.key = localsecrets.S3_KEY
        else:
            self.key = key

        if secret is None:
            self.secret = localsecrets.S3_SECRET
        else:
            self.secret = secret
        
        # We have potentially many roots (buckets, so don't set by default!)
        if root is None:
            self.root = None # localsecrets.XML_ORIGINALS_PATH
        else:
            self.root = root

        # check if local storage or secure storage is enabled
        # self.source_path = path
        try:
            if self.key is not None:
                self.fs = s3fs.S3FileSystem(anon=anon, key=key, secret=secret)
            else:
                self.fs = None
                
        except Exception as e:
            logger.error(f"FlexFileSystemError: initiation error ({e})")
            raise ConnectionError(e)

    def find(self, name, path_root=None):
        """
        Search for file; return None if not found, filename/path if found.
        """
        ret_val = None
        if path_root is None:
            path_root = self.root
           
        if localsecrets.S3_KEY is None:
            for root, dirs, files in os.walk(path_root):
                if name in files:
                    ret_val = os.path.join(root, name)
                    break
        else:
            found_info = find_s3_file(bucket=path_root, filename=name)
            try:
                ret_val = found_info[0].get("Key", None)
            except Exception as e:
                ret_val = None

        return ret_val
       
    def fullfilespec(self, filespec, path=None, path_is_root_bucket=False):
        """
         Get the full file spec
         
        """
        ret_val = filespec
        if self.key is not None:
            if path is not None:
                ret_val = localsecrets.PATH_SEPARATOR.join((path, ret_val)) 

            if not path_is_root_bucket:
                if self.root is not None:
                    m = re.match(self.root+".*", ret_val, flags=re.IGNORECASE)
                    if m is None:
                        ret_val = localsecrets.PATH_SEPARATOR.join((self.root, ret_val)) # "pep-graphics/embedded-graphics"
        else:
            if path is None or path in ['""', "''"]:
                path = ""
            if self.root is None or self.root in ['""', "''"]:
                root = ""
            else:
                root = self.root
            
            if filespec is None:
                info = f"No filespec supplied! {filespec} root: {root}, path:{path} "
                raise FileNotFoundError(info)
            ret_val = os.path.join(root, path, filespec)
            # print (f"Root: {root}, Path: {path} Filespec: {filespec} ret_val: {ret_val}")

        if localsecrets.PATH_SEPARATOR == "/":
            ret_val = ret_val.replace("\\", localsecrets.PATH_SEPARATOR)
        else: # change forward slash to backslash
            ret_val = ret_val.replace("/", localsecrets.PATH_SEPARATOR)

        return ret_val # full file spec    
    #-----------------------------------------------------------------------------
    def fileinfo(self, filespec, path=None, path_is_root_bucket=False):
        """
         Get the file info if it exists, otherwise return None
         if the instance variable key was set at init, checks s3 otherwise, local file system
         
        """
        fileinfo_dict = {}
        ret_obj = FileInfo()
        try:
            if self.key is not None:
                fullfilespec = self.fullfilespec(filespec=filespec, path=path, path_is_root_bucket=path_is_root_bucket) # "pep-graphics/embedded-graphics"
                ret_obj.filespec = fileinfo_dict["filename"] = fullfilespec
                try:
                    fileinfo_dict = self.fs.info(fullfilespec)
                    ret_obj.mapS3(fileinfo_dict)
                except FileNotFoundError:
                    logger.error(f"FlexFileSystemError: File not found: {fullfilespec}")
                    ret_obj = None
                except Exception as e:
                    logger.error(f"FlexFileSystemError: File access error: {e}")
                    ret_obj = None
                else:
                    ret_obj.fileinfo = fileinfo_dict
                    
            else: # local FS
                fullfilespec = self.fullfilespec(filespec=filespec, path=path, path_is_root_bucket=path_is_root_bucket) # "pep-graphics/embedded-graphics"
                # need to check if it exists
                if self.exists(fullfilespec, path):
                    #stat = os.stat(filespec)
                    ret_obj.basename = fileinfo_dict["base_filename"] = os.path.basename(fullfilespec)
                    ret_obj.filesize = fileinfo_dict["fileSize"]  = os.path.getsize(fullfilespec)
                    ret_obj.timestamp_str = fileinfo_dict["timestamp_str"] = datetime.datetime.utcfromtimestamp(os.path.getmtime(fullfilespec)).strftime(opasConfig.TIME_FORMAT_STR)
                    ret_obj.timestamp = fileinfo_dict["timestamp"] = datetime.datetime.strptime(fileinfo_dict["timestamp_str"], opasConfig.TIME_FORMAT_STR)
                    ret_obj.date = fileinfo_dict["date"] = fileinfo_dict["timestamp"].date()
                    ret_obj.date_str = fileinfo_dict["date_str"] = str(fileinfo_dict["date"])
                    #ret_val["type"] = fileinfo["type"]
                    ret_obj.build_date = fileinfo_dict["buildDate"] = time.time()
                    ret_obj.filename = fileinfo_dict["name"] = fullfilespec
                    ret_obj.fileinfo = fileinfo_dict
                else:
                    ret_obj = None

        except Exception as e:
            logger.error(f"FlexFileSystemError: File access error: ({e})")
        
        return ret_obj
    #-----------------------------------------------------------------------------
    def exists(self, filespec, path=None, path_is_root_bucket=False):
        """
        Find if the filespec exists, otherwise return None
        if the instance variable key was set at init, checks s3 otherwise, local file system
        """
        #  see if the file exists
        ret_val = None
        if path is not None:
            fullfilespec = self.fullfilespec(path=path, filespec=filespec, path_is_root_bucket=path_is_root_bucket) # "pep-graphics/embedded-graphics"
        else: # already a filespec
            fullfilespec = filespec
            
        try:
            if self.key is not None:
                ret_val = self.fs.exists(fullfilespec)
            else:
                ret_val = os.path.exists(fullfilespec)
        except Exception as e:
            logger.error(f"FlexFileSystemError: File access error: ({e})")
        
        return ret_val        

    #-----------------------------------------------------------------------------
    def get_full_name_if_exists(self, filespec, path=None, path_is_root_bucket=False):
        """
        Find if the filespec exists, return it, otherwise return None
        if the instance variable key was set at init, checks s3 otherwise, local file system
        """
        #  see if the file exists
        ret_val = None
        filespec = self.fullfilespec(path=path, filespec=filespec, path_is_root_bucket=path_is_root_bucket) # "pep-graphics/embedded-graphics"
        try:
            if self.key is not None:
                if self.fs.exists(filespec):
                    ret_val = filespec
            else:
                if os.path.exists(filespec):
                    ret_val = filespec
                    
        except Exception as e:
            logger.error(f"FlexFileSystemError: File access error: ({e})")
        
        return ret_val        

    #-----------------------------------------------------------------------------
    def create_local_text_file(self, filespec, path="", data=" ", encoding="utf-8", delete_existing=True):
        """
        Provide a means to write a file using local file system access, even when running on AWS s3 where self.key is defined.
        
         >>> fs = FlexFileSystem(key=localsecrets.S3_KEY, secret=localsecrets.S3_SECRET, root=localsecrets.XML_ORIGINALS_PATH)
         >>> fs.create_local_text_file('test-delete.txt', delete_existing=True)
         True
        """
        #  see if the file exists
        ret_val = False
        fullfilespec = os.path.join(path, filespec)
        try:
            if delete_existing:
                os.remove(fullfilespec)
        except Exception as e:
            logger.info(f"Delete existing selected but file {fullfilespec} was not found, so no change there (not an error).  Exception: {e}")
            pass # ok
            
        try:
            with open(fullfilespec, 'w', encoding=encoding) as out:
                out.write(data)
        except Exception as e:
            logger.error(f"FlexFileSystemError: Local File System write/access error: ({e})")
        else:
            ret_val = True
        
        return ret_val        

    #-----------------------------------------------------------------------------
    def create_text_file(self, filespec, path=None, data=" ", encoding="utf-8", delete_existing=True, path_is_root_bucket=False):
        """
         If a key is configured in the class instance, writes to s3, otherwise writes local.
         
         >>> fs = FlexFileSystem(key=localsecrets.S3_KEY, secret=localsecrets.S3_SECRET, root=localsecrets.XML_ORIGINALS_PATH)
         >>> res = fs.delete(filespec="test-delete.txt", path=localsecrets.XML_ORIGINALS_PATH)
         >>> fs.create_text_file('test-delete.txt')
         True
        """
        #  see if the file exists
        ret_val = False
        fullfilespec = self.fullfilespec(filespec=filespec, path=path, path_is_root_bucket=path_is_root_bucket) 
        if self.exists(fullfilespec, path=None, path_is_root_bucket=path_is_root_bucket):
            if delete_existing:
                self.delete(filespec=fullfilespec, path=None, path_is_root_bucket=path_is_root_bucket)
            #else:
                #logger.error(f"FlexFileSystemError: File {fullfilespec} already exists...exiting.")
                #ret_val = False
        try:
            if self.key is not None:
                with self.fs.open(fullfilespec, 'w', encoding=encoding) as out:
                    out.write(data)
            else:
                with open(fullfilespec, 'w', encoding=encoding) as out:
                    out.write(data)

        except Exception as e:
            logger.error(f"FlexFileSystemError: File write/access error: ({e})")
        else:
            ret_val = True
        
        return ret_val        

    #-----------------------------------------------------------------------------
    def delete(self, filespec, path=None, path_is_root_bucket=False):
        """
        Find if the filespec exists, otherwise return None
        if the instance variable key was set at init, checks s3 otherwise, local file system
        
         >>> fs = FlexFileSystem(key=localsecrets.S3_KEY, secret=localsecrets.S3_SECRET, root=localsecrets.XML_ORIGINALS_PATH)
         >>> fs.delete(filespec="test-delete.txt", path=localsecrets.XML_ORIGINALS_PATH)
         True
        """
        #  see if the file exists
        ret_val = False
        fullfilespec = self.fullfilespec(filespec=filespec, path=path, path_is_root_bucket=path_is_root_bucket) 
        try:
            if self.key is not None:
                ret_val = self.fs.exists(fullfilespec)
            else:
                ret_val = os.path.exists(fullfilespec)
        except Exception as e:
            logger.error(f"FlexFileSystemError: File access error: ({e})")
        
        if ret_val:
            try:
                if self.key is not None:
                    self.fs.rm(fullfilespec)
                else:
                    os.remove(fullfilespec)
            except Exception as e:
                logger.error(f"FlexFileSystemError: Can't remove file: ({e})")
                ret_val = False
            else:
                ret_val = True
                
        return ret_val
    
    #-----------------------------------------------------------------------------
    def rename(self, filespec1, filespec2, path=None, path_is_root_bucket=False):
        """

         >>> fs = FlexFileSystem(key=localsecrets.S3_KEY, secret=localsecrets.S3_SECRET, root=localsecrets.XML_ORIGINALS_PATH)
         >>> fs.rename(filespec1="test.txt", filespec2="test-running.txt", path=localsecrets.XML_ORIGINALS_PATH)
         True
         >>> fs.rename(filespec1="test-running.txt", filespec2="test.txt", path=localsecrets.XML_ORIGINALS_PATH)
         True
        """
        #  see if the file exists
        ret_val = False
        filespec1full = self.fullfilespec(filespec=filespec1, path=path, path_is_root_bucket=path_is_root_bucket) 
        filespec2full = self.fullfilespec(filespec=filespec2, path=path, path_is_root_bucket=path_is_root_bucket) 
        try:
            if self.key is not None:
                self.fs.rename(filespec1full, filespec2full)
            else:
                os.rename(filespec1full, filespec2full)
        except Exception as e:
            logger.error(f"FlexFileSystemError: Can't rename file, File access error: ({e})")
        else:
            ret_val = True
        
        return ret_val        

    #-----------------------------------------------------------------------------
    def get_download_filename(self, filespec, path="", year=None, ext=None, path_is_root_bucket=False):
        """
        Return the file name given the filespec, if it exists
      
        """
        # split name to get folder subpath for downloads
        try:
            filespec = filespec.strip()
            fsplit = filespec.split(".")
            jrnlcode, vol, pagenum = fsplit[0:3]
            # remove any suffix for vol, so we don't need to separate the folders
            vol_clean = ''.join(i for i in vol if i.isdigit())
        except Exception as e:
            # changed to log as debug rather than error.  Sometimes this is ok.
            logger.debug(f"Could not split filespec into path: {filespec}. ({e})")
            subpath = ""
        else:
            subpath = f"/{jrnlcode}/{vol_clean}" #  pad volume to 3 digits with 0

        ret_val = self.fullfilespec(filespec=filespec, path=path + subpath, path_is_root_bucket=path_is_root_bucket) # "pep-graphics/embedded-graphics"
        if ext is not None:
            ret_val = ret_val + ext  
            
        if not self.exists(ret_val):
            # note: this could be called, just to check whether to offer it to the caller who is looking at the html
            #       so this info may be "superfluous"
            logger.debug(f"Download file does not exist: {ret_val}")
            ret_val = None
            
        return ret_val   

    #-----------------------------------------------------------------------------
    def get_imagename_if_exists(self, namestr: str, extensions=(".jpg", ".gif", ".tif"), insensitive=True):
        ret_val = None
        dirname, basename = os.path.split(namestr)
        base, base_ext = os.path.splitext(basename)
        try: # to pull out an extension, it must be in extensions
            if base_ext is not None:
                exts = [a.lower() for a in extensions] + [a.upper() for a in extensions]
                if len(base_ext) > 4 or base_ext not in exts: #  revert to no extension
                    base = basename
                    base_ext = None
        except Exception as e:
            logger.debug(f"Exception checking extension: {e}")
            base = basename
            base_ext = None

        if base_ext is not None and base_ext != '':
            if insensitive:
                exts = (base_ext, base_ext.swapcase())
            else:
                exts = (base_ext, ) # comma to ensure it understands the ()
        else:
            # alway use case insensitive extensions
            if base.isupper():
                exts = [a.upper() for a in extensions] + [a.lower() for a in extensions]
            else:
                exts = [a.lower() for a in extensions] + [a.upper() for a in extensions]
            
        if insensitive:
            if base.isupper():
                names = (base, base.lower())
                # prioritize upper
            elif base.islower():
                names = (base, base.upper())
            else: # mixed
                names = (base, base.upper(), base.lower())
        else:
            names = (base, ) # comma to ensure it understands the ()
            
        # test all permutations, in optimum order
        for name in names:
            for ext in exts:
                ret_val = self.get_full_name_if_exists(name + ext)
                if ret_val:
                    if not insensitive:
                        # fail if they don't match
                        if base != name or (ext != base_ext and base_ext is not None):
                            logger.error(f"FlexFileSystemError: File insensitive match {base} vs {name} found insensitive match but not sensitive match.")
                            ret_val = None

                    return (ret_val)
                

        # if it wasn't found, return None
        return ret_val

    #-----------------------------------------------------------------------------
    def get_image_filename(self, filespec, path=None, insensitive=True, log_errors=True, path_is_root_bucket=False):
        """
        Return the file name given the image id, if it exists
        
        """
    
        # check if local storage or secure storage is enabled
        ret_val = self.fullfilespec(filespec=filespec, path=path, path_is_root_bucket=path_is_root_bucket) # "pep-graphics/embedded-graphics"

        # look to see if the file type has been specified via extension
        #ext = os.path.splitext(ret_val)[-1].lower()
        #if re.match("\.jpg|\.tif|\.gif", ext, flags=re.I):
            #exists = self.exists(ret_val)
            #if not exists:
                #fileandpath, ext = os.path.splitext(ret_val)
                #try2 = exists_filename_case_insensitive(fileandpath)
                #if try2 is not None:
                    #logger.warning(f"File {ret_val} not found. Possible missing or case mismatch.")
                #else:
                    #ret_val = try2
        #else:
        #watch for case sensitive extensions on S3 and other systems
        ret_val = self.get_imagename_if_exists(namestr=ret_val, extensions=(".jpg", ".gif", ".tif", ".svg"), insensitive=insensitive)
        if ret_val is None and log_errors:
            logger.error(f"File {filespec} not found")
    
        return ret_val   
    #-----------------------------------------------------------------------------
    def get_image_binary(self, filespec, path=None):
        """
        Return a binary object of the image, e.g.,

        Note: the current server requires the extension, but it should not.  The server should check
        for the file per the following extension hierarchy: .jpg then .gif then .tif
        
        However, if the extension is supplied, that should be accepted.
    
        The GVPi API implements this:
        
        curl -X GET "http://stage.pep.gvpi.net/api/v1/Documents/Downloads/Images/aim.036.0275a.fig001.jpg" -H "accept: image/jpeg" 
        
        and returns a binary object.
       
        """
        ret_val = None
        image_filename = self.get_image_filename(filespec, path=path)
        if image_filename is not None:
            try:
                if self.fs is not None:
                    f = self.fs.open(image_filename, "rb")
                else:
                    f = open(image_filename, "rb")
            except Exception as e:
                logger.error("GetImageBinaryError: Open: %s", e)
                
            try:
                image_bytes = f.read()
                f.close()    
            except OSError as e:
                logger.error("GetImageBinaryError: Read: %s", e)
            except Exception as e:
                logger.error("GetImageBinaryError: Exception: %s", e)
            else:
                ret_val = image_bytes
        else:
            logger.error("GetImageBinaryError: Image File ID %s not found", filespec)
      
        return ret_val
    #-----------------------------------------------------------------------------
    def get_file_contents(self, filespec, path=None, path_is_root_bucket=False):
        """
        Return the contents of a non-binary file

        The GVPi API implements this:
        
        curl -X GET "http://stage.pep.gvpi.net/api/v1/Documents/Downloads/Images/aim.036.0275a.fig001.jpg" -H "accept: image/jpeg" -H "Authorization: Basic cC5lLnAuYS5OZWlsUlNoYXBpcm86amFDayFsZWdhcmQhNQ=="
        
        and returns a binary object.
       
        """
        ret_val = None
        fileinfoout = FileInfo()
        if not self.exists(filespec, path):
            filespec = self.find(filespec, path_root=path)

        if filespec is None:
            logger.error(f"GetFileError: File {filespec} not found on Path {path}")
        else:
            fileinfoout.mapFS(filespec, path)
                
            fullfilespec = self.fullfilespec(filespec=filespec, path=path, path_is_root_bucket=path_is_root_bucket)
            # print (f"Fullfilespec: {fullfilespec}")
            if fullfilespec is not None:
                try:
                    if self.fs is not None:
                        f = self.fs.open(fullfilespec, "r", encoding="utf-8")
                    else:
                        f = open(fullfilespec, "r", encoding="utf-8")
                except Exception as e:
                    logger.error("GetFileError: Open: %s", e)
                else:    
                    try:
                        ret_val = f.read()
                        f.close()    
                    except OSError as e:
                        logger.error("GetFileError: Read: %s", e)
                    except Exception as e:
                        logger.error("GetFileError: Exception: %s", e)
            else:
                logger.error("GetFileError: File %s not found", fullfilespec)
      
        return ret_val, fileinfoout
    
    def get_matching_filelist(self, path=None, filespec_regex=None, revised_after_date=None, subfolder=None, max_items=None):
        """
        Return a list of matching files, as FileInfo objects

        Args:
         - path - full path to search
         - filespec_regex - regexp pattern with folder name and file name pattern, not including the root.
         - Examples:
            get_matching_filelist_info(match_path="_PEPArchive/BAP/.*\.xml", after_revised_date="2020-09-01")
            get_matching_filelist_info(match_path="_PEPCurrent/.*\.xml")
        
        """
        ret_val = []
        count = 0
        rc_match = re.compile(filespec_regex, flags=re.IGNORECASE)
        if subfolder is not None:
            if subfolder[0] not in ("/", r"\\"):
                subfolder = "/" + subfolder
        else:
            subfolder = ""
                
        if path is None:
            data_folder = pathlib.Path(localsecrets.XML_ORIGINALS_PATH + subfolder) # "pep-web-live-xml"
        else:
            if path == pathlib.Path(localsecrets.XML_ORIGINALS_PATH):
                data_folder = pathlib.Path(localsecrets.XML_ORIGINALS_PATH + subfolder)
            elif self.key is not None:
                # s3 running from local
                data_folder = pathlib.Path(str(path) + subfolder)
            else:
                data_folder = pathlib.Path(str(path) + subfolder)
            
        if self.key is not None:
            data_folder = data_folder.as_posix()
            
        kwargs = {"detail": True, "filter": [filespec_regex]} # fs.walk does not appear to support the filter keyword unfortunately. os.walk does not either.
        if revised_after_date is not None:
            revised_after_date = datetime.datetime.date(datetime.datetime.strptime(revised_after_date, '%Y-%m-%d'))
            
        if self.key is not None:
            for folder, subfolder, files in self.fs.walk(path=data_folder, **kwargs):
                if len(files) > 0:
                    # yes, we have files.
                    for key, val_dict in files.items():
                        # print (key)
                        if rc_match.match(key):
                            fileinfo = FileInfo()
                            fileinfo.mapS3(val_dict)
                            if revised_after_date is not None:
                                if fileinfo.date_modified > revised_after_date:
                                    ret_val.append(fileinfo)
                                    count += 1
                                else:
                                    continue
                            else:
                                ret_val.append(fileinfo)
                                count += 1
                            
                            if max_items is not None:
                                if count >= max_items:
                                    break
    
                if max_items is not None:
                    if count >= max_items:
                        break
        else:
            for folder, subfolder, files in os.walk(data_folder):
                for file in files:
                    # print (key)
                    if rc_match.match(file):
                        fileinfo = FileInfo()
                        filespec = pathlib.Path(folder) / file
                        fileinfo.mapLocalFS(filespec)
                        if revised_after_date is not None:
                            if fileinfo.date_modified > revised_after_date:
                                ret_val.append(fileinfo)
                                count += 1
                            else:
                                continue
                        else:
                            ret_val.append(fileinfo)
                            count += 1
                        
                        if max_items is not None:
                            if count >= max_items:
                                break
    
                if max_items is not None:
                    if count >= max_items:
                        break
            
        return ret_val            
    
    
def find_s3_file(bucket=r'pep-web-xml',
                 filename=None,
                 is_folder=False,
                 max_items=1):
    """
    Walk through an S3 bucket to find a file
    """
    ret_val = []
    count = 0
    for item in iterate_bucket_items(bucket=bucket):
        if filename in item["Key"]:
            if is_folder == True:
                if item.Size !=  0:
                    continue

            count = count + 1
            ret_val.append(item)
            if max_items is not None:
                if count >= max_items:
                    break

    return ret_val            

def get_build_name(filename, with_markup=True):
    """
    Return the build name with the encapsulating markup (parens and b prefix) by default,
    e.g.,
      (bEXP_ARCH1)
      
    >>> get_build_name("GW.001.0000A(bEXP_ARCH1).xml")
    '(bEXP_ARCH1)'
    
    >>> get_build_name("GW.001.0000A(bEXP_ARCH1).xml", with_markup=False)
    'EXP_ARCH1'

    """
    m = re.search(r"\(b(?P<bldname>.*?)\)", filename)
    if m:
        if with_markup:
            ret_val = f'(b{m.group("bldname")})'
        else:
            ret_val = m.group("bldname")
    else:
        ret_val = None
        
    return ret_val

def iterate_bucket_items(bucket):
    """
    Generator that iterates over all objects in a given s3 bucket

    See http://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.list_objects_v2 
    for return data format
    :param bucket: name of s3 bucket
    :return: dict of metadata for an object
    
    >> [i for i in iterate_bucket_items(bucket=r'pep-web-xml\\_PEPArchive\\ADPSA\\001.1926')]
    >> print (i[0])
    >> for i in iterate_bucket_items(bucket=r'pep-web-xml') : print(i)
    
    """

    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket)
    for page in page_iterator:
        if page['KeyCount'] > 0:
            for item in page['Contents']:
                yield item

# -------------------------------------------------------------------------------------------------------
# run it!

if __name__ == "__main__":
    print (40*"*", "opasFileSupport Tests", 40*"*")
    print ("Running in Python %s" % sys.version_info[0])

    import doctest
    doctest.testmod(optionflags=doctest.ELLIPSIS|doctest.NORMALIZE_WHITESPACE)
    print ("Fini. opasFileSupport Tests complete.")
    sys.exit()

    # test S3FileSystem
    remfs = s3fs.S3FileSystem(anon=False, key=localsecrets.S3_KEY, secret=localsecrets.S3_SECRET)
    #fs.ls("embedded-graphics")
    filename_and_path = "pep-web-files/doc/g/BAP.01.0004.FIG001.jpg"
    
    try:
        if remfs.ls(filename_and_path) != []:
            # exists
            with remfs.open(filename_and_path, mode='rb') as f:  # doctest: +SKIP
                image_bytes = f.read()
                f.close()    
        
            print (image_bytes)
    except Exception as e:
        print (f"Error: {e}")

