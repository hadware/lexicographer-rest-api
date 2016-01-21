from flask_restful import Resource, reqparse
import re

from models.stubs import *


class RetrieveDateBracketsHandler(Resource):
    """Return the start and the end dates of all the books (the outer date boundaries)"""
    def get(self):
        return DATE_BRACKETS_STUB


class BaseDateFilteredHandler(Resource):
    """This handler is the base handler for all requests where there is a filter on date,
    in other words, when there is a date bracket"""
    def __init__(self):
        """The constructor for this abstract class just creates an request parser
         that checks for the needed date brackets"""
        super().__init__()
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument("startdate", type=int)
        self.reqparse.add_argument("enddate", type=int)

class RetrieveAuthorsHandler(BaseDateFilteredHandler):
    """Returns the list of all genres available"""
    def get(self):
        self.reqparse.add_argument("name_query", type=str, required=True)
        args = self.reqparse.parse_args()
        return [ author for author in AUTHOR_LIST_STUB
                 if re.search(args["name_query"], author["name"], re.IGNORECASE)]

class RetrieveGenresHandler(BaseDateFilteredHandler):
    """Returns the list of all authors available """
    def get(self):
        return {}