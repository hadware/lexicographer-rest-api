from .filters import BaseDateFilteredHandler
from models.stubs import ADVANCED_STATS_STUB, ADVANCED_STATS_EMPTY_RESPONSE, DASHBOARD_STATS_EMPTY_RESPONSE
from models.helpers import NoBookFound


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
        try:
            return self.db_connector.compute_dashboard_stats(**args)
        except NoBookFound:
            return DASHBOARD_STATS_EMPTY_RESPONSE

class RetrieveStatisticsHandler(BaseMetadataFilterHandler):
    """Returns the base statistics for a given collection"""
    def get(self):
        args = self.reqparse.parse_args()
        try:
            return self.db_connector.compute_advanced_stats(**args)
        except NoBookFound:
            return ADVANCED_STATS_EMPTY_RESPONSE

class RetrieveWordcloudHandler(BaseMetadataFilterHandler):
    """Return the wordcloud for a given collection"""
    def get(self):
        args = self.reqparse.parse_args()
        try:
            return self.db_connector.retrieve_word_cloud(**args)
        except NoBookFound:
            return []