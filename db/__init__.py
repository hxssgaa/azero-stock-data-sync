from pymongo import MongoClient

_db = MongoClient('127.0.0.1', 8081)['azero-stock']