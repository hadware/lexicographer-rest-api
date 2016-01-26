from operator import itemgetter

__author__ = 'hadware'
import re
from os.path import abspath
from datetime import date

from pymongo import MongoClient

from .cache import cached

#DB constants (collection names, etc...)
AUTHORS_COLLECTION_NAME = "authors"
BOOKS_COLLECTION_NAME = "books"
TOPICS_COLLECTION_NAME = "subjects"

DB_ADDRESS = "mongodb://localhost:27017/"

def publication_datestring_to_date(datetestring):
    return date(*map(int, datetestring.split("-")))

class DBConnector(object):
    """Automatically connects when instantiated"""

    def __init__(self):
        self.client = MongoClient(DB_ADDRESS) # connecting to the db
        self.epub_db = self.client['epub'] # opening a DB

    @cached("books_dates_list")
    def _retrieve_books_dates(self):
        print("Fecthing books dates")
        books_dates = [ {"id" : book["_id"], "date" : publication_datestring_to_date(book["metadatas"]["dates"][0]) }
                            for book in self.epub_db[BOOKS_COLLECTION_NAME].find({}, {"metadatas.dates" : 1})]
        return sorted(books_dates, key=itemgetter("date"))

    @cached("books_dates_dict")
    def _retrieve_books_dates_dict(self):
        return { entry["id"] : entry["date"] for entry in self._retrieve_books_dates()}

    @property
    def date_boundaries(self):
        try:
            return self._date_boundaries
        except AttributeError:
            self.books_dates = self._retrieve_books_dates()
            self._date_boundaries = {"first_date" : str(self.books_dates[0]["date"]),
                                     "last_date" : str(self.books_dates[-1]["date"])}
            return self._date_boundaries

    @cached("authors_list")
    def _retrieve_authors(self):
        return { i:  entry["_id"]
                for i, entry
                in enumerate(self.epub_db[AUTHORS_COLLECTION_NAME].find())}

    @property
    def authors(self):
        try :
            return self._authors
        except AttributeError:
            self._authors = self._retrieve_authors()
            return self._authors

    def get_authors_list(self, query_str):
        return [ {"id" : author_id, "name" : name} for author_id, name in self.authors.items()
                if re.search(query_str, name, re.IGNORECASE)]

    def _compute_book_filter(self, **kwargs):

        args_dict = {}
        no_filter = True
        # since the filters are all not very required, we have to test if the values are present, and if not,
        # fall back to "default" values. There is also a "no_filters" flag. If it's not raised,
        # it means there will be no filter at all, and the whole books database is used
        try:
            args_dict["start_date"] = kwargs["start_date"]
            no_filter = False
        except KeyError:
            args_dict["start_date"] = self.date_boundaries["first_date"]

        try:
            args_dict["start_date"] = kwargs["end_date"]
            no_filter = True
        except KeyError:
            args_dict["start_date"] = self.date_boundaries["last_date"]

        try:
            args_dict["author_id"] = kwargs["author_id"]
            no_filter = True
        except KeyError:
            args_dict["author_id"] = None

        try:
            args_dict["genre_id"] = kwargs["genre_id"]
            no_filter = True
        except KeyError:
            args_dict["genre_id"] = None


        return None if no_filter else args_dict

    def get_filtered_book_set(self, args_dict):
        """Retrieves the filtered book set's objectid for a non-None args_dict"""

        #TODO: handle genre_id

        if "author_id" in args_dict:
            author_book_query = self.epub_db[AUTHORS_COLLECTION_NAME].find_one({"_id" : self.authors[args_dict["author_id"]]})
            books_objectid = author_book_query["idRef"] #only one element
        else:
            books_objectid = [ entry["_id"] for entry in self.epub_db.books.find({}, {"_id" : 1})]

        #retrieving the publication dates, and filtering out the ones that are before/after the date limits
        # the dirty way for now, using the cached date dict
        books_date_dict = self._retrieve_books_dates_dict()
        return [ objectid for objectid in books_objectid
                 if (books_date_dict[objectid] > args_dict["start_date"]
                     and books_date_dict[objectid] < args_dict["end_date"])]


    def compute_dashboard_stats(self, **kwargs):
        """Renders the message for the dashboard dada"""
        response = {}

        # first, we send the kwargs to this method, which figures out the filters to use
        args_dict = self._compute_book_filter(**kwargs)
        if args_dict is None:
            # first, we update the response according to the filter's parameters
            response.update({"nb_authors" : self.epub_db[AUTHORS_COLLECTION_NAME].count(),
                             "nb_genres" : self.epub_db[TOPICS_COLLECTION_NAME].count(),
                             "date_first_book" : self._date_boundaries["first_date"],
                             "date_last_book" : self._date_boundaries["last_date"]})

        else:
            # first, we update the response according to the filter's parameters
            response.update({"nb_authors" : 0 if args_dict["author_id"] is None else 1,
                             "nb_genres" : 0 if args_dict["genre_id"] is None else 1,
                             "date_first_book" : args_dict["start_date"],
                             "date_last_book" : self._date_boundaries["end_date"]})

            filtered_books_ids = self.get_filtered_book_set(args_dict)

        word_counts_pipeline = [
            {"$project" : { '_id' : 1, 'glossary_count' : { '$size' : "$glossary" }}},
            { "$group" : { "_id" : 1,
                           "vocab_total" : { "$sum" : "$glossary_count"},
                           "avg_words" : { "$avg" : "$glossary_count"} }}
        ]

        total_words_pipeline = [
            { "$unwind" : "$glossary"},
            { "$group" : { "_id" : 1,
                           "words_total" : { "$sum" : "$glossary.occ"}}}
        ]

        response["nb_books"] = self.epub_db.books.count()
        vocab_query_result = next(self.epub_db.glossaries.aggregate(word_counts_pipeline))
        word_query_result = next(self.epub_db.glossaries.aggregate(total_words_pipeline))
        response["vocabulary_size"] = vocab_query_result["vocab_total"]
        response["words_avg_per_book"] = vocab_query_result["avg_words"]
        response["nb_words"] = word_query_result["words_total"]

        return response

    def compute_advanced_stats(self, **kwargs):
        response = {"words" : {}, "sentences" : {}}

        # first, we send the kwargs to this method, which figures out the filters to use
        args_dict = self._compute_book_filter(**kwargs)


        word_counts_pipeline = [
            { "$group" : { "_id" : 1,
                           "words_total" : { "$sum" : "$stats.nbrWord"},
                           "words_avg_in_books" : {"$avg" : "$stats.nbrWord"},
                           "words_avg_in_sentence" : {"$avg" : "$stats.nbrWordBySentence"}}}
        ]



        word_counts_query_result = next(self.epub_db.bookStats.aggregate(word_counts_pipeline))
        response["words"] = {"count": word_counts_query_result["words_total"],
                             "avg_in_sentence": int(word_counts_query_result["words_avg_in_sentence"]),
                             "avg_in_books": int(word_counts_query_result["words_avg_in_books"])
                             }

        return response