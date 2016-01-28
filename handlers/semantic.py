from .collections import BaseMetadataFilterHandler
from models.mongo import WordNotFound
from .filters import failsafe

class RetrieveMatchingWordsList(BaseMetadataFilterHandler):
    """Retrieves a list of words matching the given query"""

    @failsafe
    def get(self):
        self.reqparse.add_argument("query", type=str, required=True)
        args = self.reqparse.parse_args()

        if len(args["query"]) > 3:
            matching_words_list = self.db_connector.get_matching_words(args["query"])
            if matching_words_list:
                return matching_words_list
            else:
                return {}
        else:
            return {}


class RetrieveWordSemanticField(BaseMetadataFilterHandler):
    """Retrieves the semantic field of 5 words for a given query"""

    @failsafe
    def get(self):
        self.reqparse.add_argument("word", type=str, required=True)
        args = self.reqparse.parse_args()

        if self.db_connector.check_if_word_exists(args["word"]):
            try:
                return self.db_connector.retrieve_semantic_field(**args)
            except WordNotFound:
                return {}
        else:
            return {}