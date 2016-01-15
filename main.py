from flask import Flask
from flask_restful import Api

from handlers import *

app = Flask(__name__)
api = Api(app)

### This is the api's routing table

#Retrieving filters
api.add_resource(RetrieveDateBracketsHandler, '/api/date_brackets')
api.add_resource(RetrieveGenresHandler, '/api/genres')
api.add_resource(RetrieveAuthorsHandler, '/api/authors')

#retrieving data for a given collection
api.add_resource(RetrieveDashboardHandler, '/api/dashboard')
api.add_resource(RetrieveStatisticsHandler, '/api/statistics')
api.add_resource(RetrieveWordcloudHandler, '/api/wordcloud')


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0") # served on the local network