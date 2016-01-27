from os.path import isfile, join, dirname

AUTHORS_COLLECTION_NAME = "authors"
BOOKS_COLLECTION_NAME = "books"
TOPICS_COLLECTION_NAME = "subjects"
GLOSSARIES_COLLECTION_NAME = "glossaries"
BOOKSTATS_COLLECTION_NAME = "bookStats"

if isfile(join(dirname(__file__), "db_address.txt")):
    with open(join(dirname(__file__), "db_address.txt")) as db_address_config_file:
        DB_ADDRESS = db_address_config_file.read()
else:
    DB_ADDRESS = "mongodb://localhost:27017/"