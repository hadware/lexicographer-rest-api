from flask_restful import fields, marshal

""" This module defines the output messages of the API
"""

author = {
    'id': fields.String, # objectid in the DB
    'name': fields.String, # actual name
}

author_list = {
    fields.List(fields.Nested(author)),
}

genre ={
    'id': fields.String, # objectid in the DB
    'name': fields.String, # actual name
}

genre_list = {
    fields.List(fields.Nested(author))
}

### For the Dashboard

dashboard_data = {
    'nb_books' : fields.Integer,# nombre de livres dans la collection
    'nb_authors' : fields.Integer, # nombre d'auteurs différents dans la collection
    'nb_genres' : fields.Integer, # nombre de genres littéraires dans la collection
    'nb_words' : fields.Integer, # nombre de mots dans la collection
    'vocabulary_size' : fields.Integer , # taille du vocabulaire (nombre de mots distincts dans la collection)
    'date_first_book': fields.String, # date du premier livre de la collection
    'date_last_book': fields.String # date du dernier livre de la collection
}


# a single word
word_stat = {
    "id" : fields.String,
    "value" : fields.String, # the actual word (stemmed)
    "count" : fields.Integer, # optionnal, for the wordcloud
    "relation_score" : fields.Float # optionnal, for semantic field
}

### For the Wordcloud

word_cloud_data = {
    "words" : fields.List(fields.Nested(word_stat))
}

### for the words research

words_list ={
    "words" : fields.List(fields.Nested(word_stat))
}

### For the Semantic field

semantic_field = {
    "words" : fields.List(fields.Nested(word_stat))
}