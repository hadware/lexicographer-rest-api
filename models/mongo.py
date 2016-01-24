from operator import itemgetter

__author__ = 'hadware'
import re
from datetime import date

from pymongo import MongoClient


#DB constants (collection names, etc...)
AUTHORS_COLLECTION_NAME = "authors"
BOOKS_COLLECTION_NAME = "books"
TOPICS_COLLECTION_NAME = "subjects"

class DBConnector(object):
    """Automatically connects when instantiated"""

    def __init__(self):
        self.client = MongoClient() # connecting to the db
        self.epub_db = self.client['epub'] # opening a DB


    def _retrieve_books_dates(self):
        self.books_dates = [ {"id" : book["_id"], "date" : date(*map(int,book["metadatas"]["dates"][0].split("-"))) }
                            for book in self.epub_db.books.find({}, {"metadatas.dates" : 1})]
        self.books_dates = sorted(self.books_dates, key=itemgetter("date"))

    @property
    def date_boundaries(self):
        try:
            return self._date_boundaries
        except AttributeError:
            self._retrieve_books_dates()
            self._date_boundaries = {"first_date" : str(self.books_dates[0]["date"]),
                                     "last_date" : str(self.books_dates[-1]["date"])}
            return self._date_boundaries


    @property
    def authors(self):
        try :
            return self._authors
        except AttributeError:
            self._authors = [{ "id" : i, "name" : entry["_id"]}
                             for i, entry
                             in enumerate(self.epub_db["authors"].find())]
            return self._authors

    def get_authors_list(self, query_str):
        return [author for author in self.authors
                if re.search(query_str, author["name"], re.IGNORECASE)]