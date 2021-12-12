from pymongo import MongoClient


client = MongoClient(
    host='127.0.0.1',
    port=27017,
)

db = client["covid"]
collection = db["result"]

collection.delete_many({})
