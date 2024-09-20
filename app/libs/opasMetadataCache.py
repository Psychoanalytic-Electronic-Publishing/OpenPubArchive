import datetime
from datetime import timedelta
import sys

sys.path.append("../config")
from configLib.opasCoreConfig import solr_docs2
import opasConfig
import logging
logger = logging.getLogger(__name__)


def metadata_get_sourcecodes(source_type=None):
    """
    >>> all = metadata_get_sourcecodes()
    >>> all_count = len(all)
    >>> all_count > 70
    True

    """
    ret_val = None
    distinct_return = "art_sourcecode"

    query = "bk_subdoc:false"

    if source_type is not None:
        query += f' AND sourcetype:"{source_type}"'

    try:
        logger.info(f"Solr Query: q={query}")
        facet_fields = ["art_sourcecode"]
        facet_pivot_fields = ["art_sourcecode"]

        args = {
            "fl": distinct_return,
            "fq": "*:*",
            "sort": "art_sourcecode asc",
            "facet": "on",
            "facet.fields": facet_fields,
            "facet.pivot": facet_pivot_fields,
            "facet.mincount": 1,
            "facet.limit": opasConfig.MAX_SOURCE_COUNT,
            "facet.sort": "art_sourcecode asc",
        }

        results = solr_docs2.search(query, **args)
        ret_val = [n["value"] for n in results.facets["facet_pivot"]["art_sourcecode"]]
        logger.info(f"Solr Query: q={query}")

    except Exception as e:
        logger.error(f"SourceCodeValues. Query: {query} Error: {e}")

    return ret_val


def metadata_get_split_books():
    """
    Fetches art_ids of split books, defined as books with an art_type of TOC.

    Returns a list of art_ids for books that meet the criteria.

    Example usage:
    >>> split_books = metadata_get_split_books()
    >>> len(split_books) <= 100
    True
    """
    ret_val = {}
    query = "sourcetype:book AND art_type:TOC"

    try:
        logger.info(f"Solr Query: q={query}")
        args = {
            "fl": "sourcecode, art_vol",
            "fq": "*:*",
            "rows": 1000,
            "sort": "art_id asc",
        }

        results = solr_docs2.search(query, **args)
        for doc in results.docs:
            basecode = f"{doc['sourcecode']}{doc['art_vol'].zfill(3)}"
            ret_val[basecode] = 0
        logger.info(f"Solr Query: q={query} returned {len(ret_val)} records")

    except Exception as e:
        logger.error(f"SplitBooks. Query: {query} Error: {e}")

    return ret_val


class MetadataCache:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MetadataCache, cls).__new__(cls)
            cls._instance.initialize_cache()
        return cls._instance

    def initialize_cache(self):
        self.expires = datetime.datetime.now() + timedelta(days=1)
        self.refresh_cache()

    def refresh_cache(self):
        self.BOOK_CODES_ALL = metadata_get_sourcecodes("book")
        self.VIDEOSTREAM_CODES_ALL = metadata_get_sourcecodes("videostream")
        self.JOURNAL_CODES_ALL = metadata_get_sourcecodes("journal")
        self.ALL_CODES = metadata_get_sourcecodes()
        self.ALL_EXCEPT_JOURNAL_CODES = self.BOOK_CODES_ALL + self.VIDEOSTREAM_CODES_ALL
        self.gSplitBooks = metadata_get_split_books()

    def get_cached_data(self, forced_update=False):
        current_time = datetime.datetime.now()
        if current_time >= self.expires or forced_update:
            self.refresh_cache()
            self.expires = current_time + timedelta(days=1)  # Reset expiration

        return {
            "BOOK_CODES_ALL": self.BOOK_CODES_ALL,
            "VIDEOSTREAM_CODES_ALL": self.VIDEOSTREAM_CODES_ALL,
            "JOURNAL_CODES_ALL": self.JOURNAL_CODES_ALL,
            "ALL_CODES": self.ALL_CODES,
            "ALL_EXCEPT_JOURNAL_CODES": self.ALL_EXCEPT_JOURNAL_CODES,
            "gSplitBooks": self.gSplitBooks,
        }


metadata_cache = MetadataCache()
