from math import floor, log
from operator import itemgetter

from scipy.spatial.distance import cosine

__author__ = 'hadware'
import re
from os.path import isfile, join, dirname
from datetime import date

from pymongo import MongoClient
from bson.objectid import ObjectId
import numpy as np
from scipy.sparse import coo_matrix
import numpy.linalg as LA

from .cache import cached
from .utils import Pipeline

#DB constants (collection names, etc...)
AUTHORS_COLLECTION_NAME = "authors"
BOOKS_COLLECTION_NAME = "books"
TOPICS_COLLECTION_NAME = "subjects"
GLOSSARIES_COLLECTION_NAME = "glossaries"
BOOKSTATS_COLLECTION_NAME = "bookStats"

if isfile(join(dirname(__file__), "db_address.txt")):
    with open(join(dirname(__file__), "db_address.txt")) as db_address_config_file:
        DB_ADDRESS = db_address_config_file.read()
else:
    DB_ADDRESS = "mongodb://localhost:27017/"

class WordNotFound(Exception):
    pass

def publication_datestring_to_date(datetestring):
    return date(*map(int, datetestring.split("-")))

class DBConnector(object):
    """Automatically connects when instantiated"""

    def __init__(self):
        self.client = MongoClient(DB_ADDRESS) # connecting to the db
        self.epub_db = self.client['epub'] # opening a DB
        self.genres = self.epub_db[TOPICS_COLLECTION_NAME]
        self.authors = self.epub_db[AUTHORS_COLLECTION_NAME]
        self.glossaries = self.epub_db[GLOSSARIES_COLLECTION_NAME]
        self.books = self.epub_db[BOOKS_COLLECTION_NAME]
        self.bookstats = self.epub_db[BOOKSTATS_COLLECTION_NAME]


    @cached("books_dates_list")
    def _retrieve_books_dates(self):
        print("Fetching books dates")
        books_dates = [ {"id" : book["_id"], "date" : publication_datestring_to_date(book["metadatas"]["dates"][0]) }
                            for book in self.books.find({}, {"metadatas.dates" : 1})]
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
                in enumerate(self.authors.find())}

    @property
    def cached_authors(self):
        try :
            return self._authors
        except AttributeError:
            self._authors = self._retrieve_authors()
            return self._authors

    def get_authors_list(self, query_str):
        return [ {"id" : author_id, "name" : name} for author_id, name in self.cached_authors.items()
                if re.search(query_str, name, re.IGNORECASE)]

    def _compute_book_filter(self, **kwargs):

        args_dict = {}
        no_filter = True
        # since the filters are all not very required, we have to test if the values are present, and if not,
        # fall back to "default" values. There is also a "no_filters" flag. If it's not raised,
        # it means there will be no filter at all, and the whole books database is used
        if kwargs["start_date"] is not None:
            args_dict["start_date"] = kwargs["start_date"]
            no_filter = False
        else :
            args_dict["start_date"] = self.date_boundaries["first_date"]

        if kwargs["end_date"] is not None:
            args_dict["end_date"] = kwargs["end_date"]
            no_filter = False
        else:
            args_dict["end_date"] = self.date_boundaries["last_date"]

        if kwargs["author"] is not None:
            args_dict["author_id"] = kwargs["author"]
            no_filter = False
        else :
            args_dict["author_id"] = None

        if kwargs["genre"] is not None:
            args_dict["genre_id"] = kwargs["genre"]
            no_filter = False
        else :
            args_dict["genre_id"] = None


        return None if no_filter else args_dict

    def get_filtered_book_set(self, args_dict=None):
        """Retrieves the filtered book set's objectid for a non-None args_dict"""

        #TODO: handle genre_id
        if args_dict is None:
            return None, None, None
        else:
            if args_dict["author_id"] is not None:
                author_book_query = self.authors.find_one({"_id" : self.cached_authors[args_dict["author_id"]]})
                books_objectid = author_book_query["idRef"] #only one element
            else:
                books_objectid = [ entry["_id"] for entry in self.books.find({}, {"_id" : 1})]

            #retrieving the publication dates, and filtering out the ones that are before/after the date limits
            # the dirty way for now, using the cached date dict
            books_date_dict = self._retrieve_books_dates_dict()
            ouput_booksid_list = []
            min_date, max_date = None, None

            for objectid in books_objectid:
                if (books_date_dict[objectid] > publication_datestring_to_date(args_dict["start_date"])
                         and books_date_dict[objectid] < publication_datestring_to_date(args_dict["end_date"])):
                    ouput_booksid_list.append(objectid)
                if min_date is None:
                    max_date, min_date = books_date_dict[objectid], books_date_dict[objectid]
                else:
                    if max_date < books_date_dict[objectid]:
                        max_date = books_date_dict[objectid]
                    if min_date > books_date_dict[objectid]:
                        min_date = books_date_dict[objectid]

            return ouput_booksid_list, max_date, min_date

    def _get_authors_counts_in_bookids(self, books_ids):
        total_books_authors_ppln = [
            {"$unwind" : "$idRef"},
            {"$match" : { "idRef" : { "$in" : books_ids}}},
            {"$group" : { "_id" : "$_id"}},
            {"$group" : { "_id" : 1, "count" : {"$sum" : 1}}}
        ]

        return next(self.authors.aggregate(total_books_authors_ppln))["count"]


    def _get_genres_counts_in_bookids(self, books_ids):
        total_books_genres_ppln = [
            {"$unwind" : "$idRef"},
            {"$match" : { "idRef" : { "$in" : books_ids}}},
            {"$group" : { "_id" : "$_id"}},
            {"$group" : { "_id" : 1, "count" : {"$sum" : 1}}}
        ]

        return next(self.genres.aggregate(total_books_genres_ppln))["count"]


    def compute_dashboard_stats(self, **kwargs):
        """Renders the message for the dashboard data"""
        response = {}

        # first, we send the kwargs to this method, which figures out the filters to use
        args_dict = self._compute_book_filter(**kwargs)
        if args_dict is None:
            # first, we update the response according to the filter's parameters
            response.update({"nb_authors" : self.authors.count(),
                             "nb_genres" : self.genres.count(),
                             "date_first_book" : self.date_boundaries["first_date"],
                             "date_last_book" : self.date_boundaries["last_date"]})
            filtered_books_ids = None
        else:
            # first, we update the response according to the filter's parameters
            filtered_books_ids, max_date, min_date = self.get_filtered_book_set(args_dict)
            response.update({
                "nb_authors" : self._get_authors_counts_in_bookids(filtered_books_ids)
                                if args_dict["author_id"] is None else 1,
                "nb_genres" : self._get_genres_counts_in_bookids(filtered_books_ids)
                                if args_dict["genre_id"] is None else 1,
                "date_first_book" : str(min_date),
                "date_last_book" : str(max_date)
            })

        vocab_count_ppln = Pipeline([
            {"$unwind" : "$glossary"},
            {"$group" : { "_id" : "$glossary.word" }},
            { "$group" : { "_id" : 1, "vocab_total" : { "$sum" : 1}}}
        ], filtered_books_ids)

        total_words_ppln = Pipeline([
            { "$unwind" : "$glossary"},
            { "$group" : { "_id" : 1,
                           "words_total" : { "$sum" : "$glossary.occ"}}}
        ], filtered_books_ids)

        response["nb_books"] = self.books.count() if filtered_books_ids is None else len(filtered_books_ids)
        total_vocab_query_result = next(self.glossaries.aggregate(vocab_count_ppln.pipeline))
        word_query_result = next(self.glossaries.aggregate(total_words_ppln.pipeline))
        response["vocabulary_size"] = total_vocab_query_result["vocab_total"]

        # this is to display a "shortened" count for words, using "1235K" notation
        if word_query_result["words_total"] < 100000:
            response["nb_words"] = word_query_result["words_total"]
        else:
            response["nb_words"] = str(floor(word_query_result["words_total"] / 1000)) + "K"
        return response

    def compute_advanced_stats(self, **kwargs):
        """Retrieving the data dfor the 'statistics' tab"""
        response = {"words" : {}, "sentences" : {}}

        # first, we send the kwargs to this method, which figures out the filters to use
        args_dict = self._compute_book_filter(**kwargs)
        filtered_books_ids, max_date, min_date = self.get_filtered_book_set(args_dict)

        #computes various statistics, moslty summing over the bookstats objects
        word_counts_ppln = Pipeline([
            { "$group" : { "_id" : 1,
                           "words_total" : { "$sum" : "$stats.nbrWord"},
                           "words_avg_in_books" : {"$avg" : "$stats.nbrWord"},
                           "words_avg_in_sentence" : {"$avg" : "$stats.nbrWordBySentence"},
                           "sentences_total" : { "$sum" : "$stats.nbrSentence"},
                           "sentences_avg_in_book" : { "$avg" : "$stats.nbrSentence"}}}
        ], filtered_books_ids)

        #computes the avegare number of unique words per books
        avg_words_ppln = Pipeline([
            {"$project" : { '_id' : 1, 'glossary_count' : { '$size' : "$glossary" }}},
            { "$group" : { "_id" : 1,
                           "avg_words" : { "$avg" : "$glossary_count"} }}
        ], filtered_books_ids)

        avg_words_query_result = next(self.glossaries.aggregate(avg_words_ppln.pipeline))
        word_counts_query_result = next(self.bookstats.aggregate(word_counts_ppln.pipeline))
        response["words"] = {"count": word_counts_query_result["words_total"],
                             "avg_in_sentence": int(word_counts_query_result["words_avg_in_sentence"]),
                             "avg_in_books": int(word_counts_query_result["words_avg_in_books"]),
                             "avg_book_vocab" : int(avg_words_query_result["avg_words"])
                             }
        response["sentences"] = { "count" : int(word_counts_query_result["sentences_total"]),
                                  "avg_in_books" : int(word_counts_query_result["sentences_avg_in_book"])}

        return response

    def retrieve_word_cloud(self, **kwargs):
        """Retrieves the word cloud for a given book set"""
        response = []

        # first, we send the kwargs to this method, which figures out the filters to use
        args_dict = self._compute_book_filter(**kwargs)
        filtered_books_ids, max_date, min_date = self.get_filtered_book_set(args_dict)

        # unwinding the glossaries from the set, then grouping the elements by words, while summing the occurences
        # then, sorting it descrecendo, and getting only the 20 firsts
        words_occ_pipeline = Pipeline([
            {"$unwind" : "$glossary"},
            {"$group" : { "_id" : "$glossary.word" , "occ" : { "$sum" : "$glossary.occ"}}},
            {"$sort" : {"occ" : -1}},
            {"$limit" : 20}
        ], filtered_books_ids)

        return { word["_id"]: word["occ"] for word in
                 self.glossaries.aggregate(words_occ_pipeline.pipeline)}

    @cached("full_glossary")
    def _get_full_glossary(self):
        """Cached. Retrieves the list of words contained in the books"""
        idf_table = self.epub_db.idf.find_one()
        del idf_table["_id"]
        return [word for word in idf_table]

    def check_if_word_exists(self, word_query):
        """Checks if a word actually is in the books"""
        return word_query in self._get_full_glossary()

    def get_matching_words(self, word_query):
        """Returns the words that contain the word query word"""
        return [word for word in self._get_full_glossary() if word_query in word]

    def _get_set_vocab(self, book_id_list):
        """Using a set of Books ObjecId's, retrieves the vocab for this set"""
        document_set_words = [
            { "$match" : {"_id" : { "$in" : book_id_list}}},
            {"$unwind" : "$glossary"},
            {"$group" : { "_id" : "$glossary.word"}},
        ]

        vocab_dict = {}
        vocab_list = []
        for i, entry in enumerate(self.glossaries.aggregate(document_set_words)):
            vocab_dict[entry["_id"]] = i
            vocab_list.append(entry["_id"])
        return vocab_dict, vocab_list


    def _get_glossary_dict(self, book_id_list):
        glossaries_query_result = self.glossaries.find({ "_id" : { "$in" : book_id_list}})
        glossaries_dict = {}
        for glossary in glossaries_query_result:
            glossaries_dict[glossary["_id"]] = {entry["word"] : entry["occ"] for entry in glossary["glossary"]}

        return glossaries_dict

    def _get_idf(self, books_dict):
        """returns the full IDF table, with books not in the book set filetered out"""
        idf_table = self.epub_db.idf.find_one()
        del idf_table["_id"]
        for k in idf_table:
            #casting keys to ObjectId, all the while checking if they're part of the book set
            idf_table[k] = [ ObjectId(object_id) for object_id in idf_table[k] if ObjectId(object_id) in books_dict]
        return idf_table

    def retrieve_semantic_field(self, **kwargs):
        """Retrieving the 5 words in the semantic field of a given word"""

        # first, we send the kwargs to this method, which figures out the filters to use
        args_dict = self._compute_book_filter(**kwargs)

        #then we build the book set (using they objectid's)
        if args_dict is None:
            books_ids_list = [book["_id"] for book in self.books.find({}, {"_id" : 1})] #for now, for the full book list
        else:
            books_ids_list, max_date, min_date = self.get_filtered_book_set(args_dict)

        #first step : we gather the vocabulary for the given request
        books_count = len(books_ids_list)
        print("%i books used for analysis" % books_count)
        books_id_dict = { bookid : i for i, bookid in enumerate(books_ids_list)}
        vocab_dict, vocab_list = self._get_set_vocab(books_ids_list)

        if kwargs["word"] not in vocab_dict:
            raise WordNotFound()

        #retrieving the glossaries for all concerned books
        glossaries_dict = self._get_glossary_dict(books_ids_list)

        #retrieving the IDF table, which is a dictionary of the form { "word" : [ ObjectId's]}
        idf_table = self._get_idf(books_ids_list)

        #computing three lists to build a sparse matrix: colmun, row and the computed tfidf
        row = []
        column = []
        tfidf = []
        for word, i in vocab_dict.items():
            for book_id in idf_table[word]:
                if book_id in glossaries_dict:
                    row.append(i),
                    column.append(books_id_dict[book_id])
                    tfidf.append((1 + log(glossaries_dict[book_id][word])) * (books_count / len(idf_table[word])))

        tfidf_matrix = coo_matrix((tfidf, (row, column)), shape=(len(vocab_dict), books_count))
        tfidf_matrix = tfidf_matrix.todense()

        print("Done computing tfidf table")

        query_word_row = tfidf_matrix[vocab_dict[kwargs["word"]]]
        query_word_norm = LA.norm(query_word_row)
        ESA_results = []
        for word in vocab_dict:
            # basically, computing the scalar product, divingin it by the product of norms, and then
            # simply extracting item 0

            # ESA_results.append((np.dot(query_word_row,
            #                            tfidf_matrix.getrow(vocab_dict[word]).todense().T)
            #                    /
            #                     (
            #                         LA.norm(tfidf_matrix.getrow(vocab_dict[word]).todense())
            #                         *
            #                         query_word_norm
            #                     )).item(0)
            #                    )
            ESA_results.append(cosine(query_word_row, tfidf_matrix[vocab_dict[word]]))

        print("Finished computing the scalar products")
        # casting the results to array, then, finding the 10 biggest coefficients
        ESA_results_array = np.array(ESA_results)
        closest_elements_indices = ESA_results_array.argsort()[:5]

        return [vocab_list[i] for i in closest_elements_indices]