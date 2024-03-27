import datetime
from datetime import timedelta
from opasPySolrLib import metadata_get_sourcecodes, metadata_get_split_books


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
            "ALL_EXCEPT_JOURNAL_CODES": self.ALL_EXCEPT_JOURNAL_CODES,
            "gSplitBooks": self.gSplitBooks,
        }


metadata_cache = MetadataCache()
