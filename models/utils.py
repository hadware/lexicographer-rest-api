__author__ = 'hadware'


class Pipeline(object):

    def __init__(self,pipeline, books_ids = None):
        self.pipeline = pipeline
        if books_ids is not None:
            self.pipeline.insert(0, {"$match" : { "_id" : { "$in" : books_ids}}})

