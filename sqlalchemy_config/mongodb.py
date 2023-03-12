from pymongo import mongo_client

client = mongo_client.MongoClient(host='localhost', port=27017)
client.get_database(name='metawiki')
print(client.get_database(name='metawiki'))