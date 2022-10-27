
# Configuration file for opasDataLoader
default_input_build_pattern = "(bKBD3|bSeriesTOC)"
default_precompiled_input_build_pattern = "(bEXP_ARCH1|bSeriesTOC)"
default_precompiled_input_build = "(bEXP_ARCH1)"
default_process_pattern = "(bKBD3|bSeriesTOC)"
default_input_build = "(bKBD3)"
default_output_build = "(bEXP_ARCH1)"
default_doctype = '<!DOCTYPE pepkbd3 SYSTEM "http://peparchive.org/pepa1dtd/pepkbd3.dtd">'
default_xml_declaration = "<?xml version='1.0' encoding='UTF-8'?>"

# Global variables (for data and instances)
options = None

# Source codes (books/journals) which should store paragraphs
SRC_CODES_TO_INCLUDE_PARAS = ["GW", "SE"]

# for these codes, do not create update notifications
DATA_UPDATE_PREPUBLICATION_CODES_TO_IGNORE = ["IPL", "ZBK", "NLP", "SE", "GW"] # no update notifications for these codes.

# SmartBuild Exceptions (these have only output builds, no input file to build, this loads output file into file list even with smart build)
SMARTBUILD_EXCEPTIONS = "(ZBK.069)"
