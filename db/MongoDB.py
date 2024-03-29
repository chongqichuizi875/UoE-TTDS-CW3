from typing import List
from db.DBInterface import DBInterface
from pymongo import MongoClient, DESCENDING
import datetime


# default thresholds for lengths of individual tokens
TOKEN_MIN_LEN = 2
TOKEN_MAX_LEN = 15
MAX_INDEX_SPLITS = 30

class MongoDB(DBInterface):
    def __init__(self) -> None:
        # client = MongoClient("mongodb://192.168.224.1:27017/")
        # print('initialize MongoDB......')
        # print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
        client = MongoClient("mongodb://127.0.0.1:27017/")
        # client = MongoClient("mongodb://192.168.224.1:27017/")
        self.wiki = client.subwiki
        self.pages = self.wiki.pages
        self.inverted_index = self.wiki.inverted_index
        self.tfs = self.wiki.tfs
        self.inverted_index.create_index("token")
        self.avg_page_len = self.get_avg_page_len()
        self.total_page_count = self.get_page_count()
        self.token_freqs = self.create_token_freqs_dict()
        # print('avg_page_len:::' + str(self.avg_page_len))
        # print('page_count:::'+str(self.page_count))
        # print('initialize MongoDB done.')
        # print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))

    """  
        id: page_id
        return: page={"_id": num, "title": str, "text": str}
    """
    def get_page_by_page_id(self, id: str):
        return self.pages.find_one({'_id': id}, {"_id":0}) # exclude id

    """      
        ids: list of page_id
        return: [page, ...]
    """
    def get_pages_by_list_of_ids(self, ids: List[str]):
        pages= list(self.pages.find({"_id": {"$in": ids}}))  
        page_dict = {p['_id']: p for p in pages}
        sorted_pages = [page_dict[id] for id in ids]
        return sorted_pages

    """ 
        return: iterator
        e.g.: 
            page_cursor = db.get_indexed_pages_by_token('sunday', skip=0, limit=1)
            for page in page_cursor:
                print(page)
            output: {'token': 'sunday', 'page_count': 2057, 'page': [{'_id': 7, 'pos': [0]}, {'_id': 184, 'pos': [540]}, {'_id': 684, 'pos': [1289, 1568]}, {'_id': 1611, 'pos': [1638, 1643]}, {'_id': 1712, 'pos': [57, 69]}, {'_id': 5570, 'pos': [145, 1280]}, {'_id': 5571, 'pos': [1977]}, ...]}
    """
    def get_indexed_pages_by_token(self, token: str, batch_size=MAX_INDEX_SPLITS):
        doc_curser = self.inverted_index.find({"token": token}, {"_id": 0})
        for i in range(0, MAX_INDEX_SPLITS, batch_size):
            doc_curser = doc_curser.skip(i).limit(batch_size)
            for doc in doc_curser:
                yield doc

    def get_page_count(self):
        return self.pages.count_documents({})

    def get_avg_page_len(self):
        return next(self.pages.aggregate([
            {'$project': {'avg': {'$avg': '$page_len'}}}
        ]))['avg']

    def get_page_titles(self):
        return self.pages.find({},{"_id":0, "title":1})
    
    def get_token_freqs(self, tf_lower_bound=5):
        self.tfs.insert_many(self.inverted_index.aggregate([
            {"$unwind": "$pages"},
            {'$match': {'pages.tf': {'$gt':tf_lower_bound}}},
            {"$project": {
                "token": "$token",
                "tfs.pageid": "$pages._id",
                "tfs.tf": "$pages.tf",
                "page_count": "$page_count"
            }},
            {'$group': {
                "_id": "$token",
                "page_count" :{"$first":"$page_count"},
                "tfs": {"$push": "$tfs"}
            }},
            {"$sort": {'page_count':-1}}

        ]))
    def create_token_freqs_dict(self):
        # return {doc['_id']: [{x['pageid']:x['tf'] for x in doc['tfs'] }, doc['page_count']] for doc in self.tfs.find()}
        return {doc['_id']: [{x['pageid']:x['tf'] for x in doc['tfs'] }, doc['page_count']] for doc in self.tfs.find()}
