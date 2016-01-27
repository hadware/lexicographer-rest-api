from .collections import BaseMetadataFilterHandler

class RetrieveWordSemanticField(BaseMetadataFilterHandler):
    def get(self):
        self.reqparse.add_argument("query", type=str, required=True)
        args = self.reqparse.parse_args()

        if self.db_connector.check_if_word_exists(args["query"]):
            return self.db_connector.retrieve_semantic_field(**args)
        else:
            return {}