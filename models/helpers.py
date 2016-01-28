from datetime import date
import re

from .config_db import AUTHORS_COLLECTION_NAME, BOOKS_COLLECTION_NAME, TOPICS_COLLECTION_NAME, GLOSSARIES_COLLECTION_NAME
from operator import itemgetter
from .cache import cached


class Pipeline(object):

    def __init__(self,pipeline, books_ids = None):
        self.pipeline = pipeline
        if books_ids is not None:
            self.pipeline.insert(0, {"$match" : { "_id" : { "$in" : books_ids}}})

class NoBookFound(Exception):
    pass

def publication_datestring_to_date(datetestring):
    return date(*map(int, datetestring.split("-")))

class FilteringHelper(object):
    """Takes care of all the dirty filtering business"""

    def __init__(self, db_epub):
        self.epub_db = db_epub
        self.genres = self.epub_db[TOPICS_COLLECTION_NAME]
        self.authors = self.epub_db[AUTHORS_COLLECTION_NAME]
        self.glossaries = self.epub_db[GLOSSARIES_COLLECTION_NAME]
        self.books = self.epub_db[BOOKS_COLLECTION_NAME]

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

    @cached("genres_list")
    def _retrieve_genres(self):
        return { i:  entry["_id"]
                for i, entry
                in enumerate(self.genres.find())}

    @property
    def cached_genres(self):
        try :
            return self._genres
        except AttributeError:
            self._genres = self._retrieve_genres()
            return self._genres

    def get_genres_list(self):
        return [ {"id" : genre_id, "name" : name} for genre_id, name in self.cached_genres.items()]


    def compute_book_filter(self, **kwargs):

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

    def _retrieve_corresponing_bookids(self, collection, element_id):
        return collection.find_one({"_id" : element_id})["idRef"]

    def get_filtered_book_set(self, args_dict=None):
        """Retrieves the filtered book set's objectid for a non-None args_dict"""

        if args_dict is None:
            return None, None, None
        else:

            if args_dict["author_id"] is None and args_dict["genre_id"] is None:
                books_objectid = [ entry["_id"] for entry in self.books.find({}, {"_id" : 1})]

            elif args_dict["author_id"] is not None and args_dict["genre_id"] is not None:
                books_id_for_author = self._retrieve_corresponing_bookids(self.authors,
                                                                          self.cached_authors[args_dict["author_id"]])
                books_id_for_genre = self._retrieve_corresponing_bookids(self.genres,
                                                                         self.cached_genres[args_dict["genre_id"]])
                # intersecting both sets
                books_objectid = list(set(books_id_for_author).intersection(set(books_id_for_genre)))

            elif args_dict["author_id"] is not None:
                books_objectid = self._retrieve_corresponing_bookids(self.authors,
                                                                     self.cached_authors[args_dict["author_id"]])
            else:
                books_objectid = self._retrieve_corresponing_bookids(self.genres,
                                                                     self.cached_genres[args_dict["genre_id"]])

            # retrieving the publication dates, and filtering out the ones that are before/after the date limits
            books_date_dict = self._retrieve_books_dates_dict()
            ouput_booksid_list = []
            min_date, max_date = None, None

            # we're also taking advantage of the date tests to figure out the date brackets for all books
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

            if not ouput_booksid_list:
                raise NoBookFound()

            return ouput_booksid_list, max_date, min_date


    def get_unique_count(self,collection, books_ids):
        """Functioned used to count the number of unique authors or genre for a given set of bookids"""
        metadata_count_pipeline = [
            {"$unwind" : "$idRef"},
            {"$match" : { "idRef" : { "$in" : books_ids}}},
            {"$group" : { "_id" : "$_id"}},
            {"$group" : { "_id" : 1, "count" : {"$sum" : 1}}}
        ]

        return next(collection.aggregate(metadata_count_pipeline))["count"]


