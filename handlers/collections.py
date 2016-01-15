from .filters import BaseDateFilteredHandler

class BaseMetadataFilterHandler(BaseDateFilteredHandler):
    """This abstract handler ensures that there the """
    def __init__(self):
        super().__init__()
        self.reqparse.add_argument("id_genre", type=int, required=True)
        self.reqparse.add_argument("id_author", type=int, required=True)


class RetrieveDashboardHandler(BaseMetadataFilterHandler):
    """Retrieves the dashboard data for a given collection"""
    def get(self):
        args = self.reqparse.parse_args()
        return { k : v for k,v in args.items()}

class RetrieveStatisticsHandler(BaseMetadataFilterHandler):
    """Returns the base statistics for a given collection"""
    def get(self):
        args = self.reqparse.parse_args()
        return { k : v for k,v in args.items()}

class RetrieveWordcloudHandler(BaseDateFilteredHandler):
    """Return the wordcloud for a given collection"""
    def get(self):
        return {}