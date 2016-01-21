from .collections import BaseMetadataFilterHandler

class RetrieveWordSemanticField(BaseMetadataFilterHandler):
    def get(self):
        self.reqparse.add_argument("word", type=str, required=True)
        return {}