from pymongo import MongoClient

from decouple import config

import certifi

ca = certifi.where()

def conectar_mongo():
    try:
        client = MongoClient(config("MONGO_URL"), tlsCAFile=ca)
        db = client.get_database()
        return db
    except Exception as e:
        print(e)