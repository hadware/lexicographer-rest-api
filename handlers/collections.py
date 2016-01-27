from .filters import BaseDateFilteredHandler
from models.stubs import ADVANCED_STATS_STUB


class BaseMetadataFilterHandler(BaseDateFilteredHandler):
    """This abstract handler ensures that there the genre"""
    def __init__(self):
        super().__init__()
        self.reqparse.add_argument("genre", type=int)
        self.reqparse.add_argument("author", type=int)


class RetrieveDashboardHandler(BaseMetadataFilterHandler):
    """Retrieves the dashboard data for a given collection"""
    def get(self):
        args = self.reqparse.parse_args()
        return self.db_connector.compute_dashboard_stats(**args)

class RetrieveStatisticsHandler(BaseMetadataFilterHandler):
    """Returns the base statistics for a given collection"""
    def get(self):
        args = self.reqparse.parse_args()
        return self.db_connector.compute_advanced_stats(**args)

class RetrieveWordcloudHandler(BaseMetadataFilterHandler):
    """Return the wordcloud for a given collection"""
    def get(self):
        args = self.reqparse.parse_args()
        return self.db_connector.retrieve_word_cloud(**args)