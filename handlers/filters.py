from flask_restful import Resource, reqparse


class RetrieveDateBracketsHandler(Resource):
    """Return the start and the end dates of all the books (the outer date boundaries)"""
    def get(self):
        return {}


class BaseDateFilteredHandler(Resource):
    """This handler is the base handler for all requests where there is a filter on date,
    in other words, when there is a date bracket"""
    def __init__(self):
        """The constructor for this abstract class just creates an request parser
         that checks for the needed date brackets"""
        super().__init__()
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument("startdate", type=int, required=True)
        self.reqparse.add_argument("enddate", type=int, required=True)

class RetrieveAuthorsHandler(BaseDateFilteredHandler):
    """Returns the list of all genres available"""
    def get(self):
        args = self.reqparse.parse_args()
        return { k : v for k,v in args.items()}

class RetrieveGenresHandler(BaseDateFilteredHandler):
    """Returns the list of all authors available """
    def get(self):
        return {}