from time import sleep
from flask_restful import Resource, reqparse
from pymongo.errors import AutoReconnect
from models.mongo import DBConnector

def failsafe(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except AutoReconnect:
            sleep(5)
            return func(*args, **kwargs)

    return wrapper

class RetrieveDateBracketsHandler(Resource):
    """Return the start and the end dates of all the books (the outer date boundaries)"""
    def get(self):
        self.db_connector = DBConnector()
        return self.db_connector.filtering_helper.date_boundaries


class BaseDateFilteredHandler(Resource):
    """This handler is the base handler for all requests where there is a filter on date,
    in other words, when there is a date bracket"""
    @failsafe
    def __init__(self):
        """The constructor for this abstract class just creates an request parser
         that checks for the needed date brackets"""
        super().__init__()
        self.db_connector = DBConnector()
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument("start_date", type=str)
        self.reqparse.add_argument("end_date", type=str)

class RetrieveAuthorsHandler(BaseDateFilteredHandler):
    """Returns the list of all genres available"""
    @failsafe
    def get(self):
        self.reqparse.add_argument("name_query", type=str, required=True)
        args = self.reqparse.parse_args()
        return self.db_connector.filtering_helper.get_authors_list(args["name_query"])

class RetrieveGenresHandler(BaseDateFilteredHandler):
    """Returns the list of all authors available """
    @failsafe
    def get(self):
        return self.db_connector.filtering_helper.get_genres_list()