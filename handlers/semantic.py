from .collections import BaseMetadataFilterHandler


class RetrieveMatchingWordsList(BaseMetadataFilterHandler):

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

    def get(self):
        self.reqparse.add_argument("word", type=str, required=True)
        args = self.reqparse.parse_args()

        if self.db_connector.check_if_word_exists(args["word"]):
            return self.db_connector.retrieve_semantic_field(**args)
        else:
            return {}