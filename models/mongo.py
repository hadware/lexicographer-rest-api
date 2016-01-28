from math import floor, log

from scipy.spatial.distance import cosine

from models.config_db import AUTHORS_COLLECTION_NAME, BOOKS_COLLECTION_NAME, TOPICS_COLLECTION_NAME, \
    GLOSSARIES_COLLECTION_NAME, BOOKSTATS_COLLECTION_NAME, DB_ADDRESS

from pymongo import MongoClient
from bson.objectid import ObjectId
import numpy as np
from scipy.sparse import coo_matrix
import numpy.linalg as LA

from .cache import cached
from .helpers import Pipeline, FilteringHelper

class WordNotFound(Exception):
    pass


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
        self.filtering_helper = FilteringHelper(self.epub_db)


    def compute_dashboard_stats(self, **kwargs):
        """Renders the message for the dashboard data"""
        response = {}

        # first, we send the kwargs to this method, which figures out the filters to use
        args_dict = self.filtering_helper.compute_book_filter(**kwargs)
        if args_dict is None:
            # first, we update the response according to the filter's parameters
            response.update({"nb_authors" : self.authors.count(),
                             "nb_genres" : self.genres.count(),
                             "date_first_book" : self.filtering_helper.date_boundaries["first_date"],
                             "date_last_book" : self.filtering_helper.date_boundaries["last_date"]})
            filtered_books_ids = None
        else:
            # first, we update the response according to the filter's parameters
            filtered_books_ids, max_date, min_date = self.filtering_helper.get_filtered_book_set(args_dict)
            response.update({
                "nb_authors" : self.filtering_helper.get_unique_count(self.authors, filtered_books_ids)
                                if args_dict["author_id"] is None else 1,
                "nb_genres" : self.filtering_helper.get_unique_count(self.genres, filtered_books_ids)
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
        args_dict = self.filtering_helper.compute_book_filter(**kwargs)
        filtered_books_ids, max_date, min_date = self.filtering_helper.get_filtered_book_set(args_dict)

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

        # first, we send the kwargs to this method, which figures out the filters to use
        args_dict = self.filtering_helper.compute_book_filter(**kwargs)
        filtered_books_ids, max_date, min_date = self.filtering_helper.get_filtered_book_set(args_dict)

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
        args_dict = self.filtering_helper.compute_book_filter(**kwargs)

        #then we build the book set (using they objectid's)
        if args_dict is None:
            books_ids_list = [book["_id"] for book in self.books.find({}, {"_id" : 1})]
        else:
            books_ids_list, max_date, min_date = self.filtering_helper.get_filtered_book_set(args_dict)
        books_count = len(books_ids_list)
        print("%i books used for analysis" % books_count)
        books_id_dict = { bookid : i for i, bookid in enumerate(books_ids_list)}

        # then we gather the vocab for the given book
        vocab_dict, vocab_list = self._get_set_vocab(books_ids_list)

        # checking if the vocabulary isn't empty
        if kwargs["word"] not in vocab_dict:
            raise WordNotFound()

        # then, retrieving the glossaries for all concerned books
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
        print("Finished sorting results")
        
        return [vocab_list[i] for i in closest_elements_indices]