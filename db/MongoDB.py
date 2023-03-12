from typing import List
# from db.DBInterface import DBInterface

from pymongo import MongoClient

class MongoDB():
    def __init__(self) -> None:
        client = MongoClient("mongodb://192.168.224.1:27017/")
        self.wiki = client.metawiki
        self.pages = self.wiki.pages
        self.inverted_index = self.wiki.inverted_index

    def get_page_by_page_id(self, id: str):
        return self.pages.find_one({'_id': id}, {"_id":0}) # exclude id

    # return a list of dictionary
    def get_pages_by_list_of_ids(self, ids: List[str]):
        pages= list(self.pages.find({"_id": {"$in": ids}}))  
        page_dict = {p['_id']: p for p in pages}
        sorted_pages = [page_dict[id] for id in ids]
        return sorted_pages

    def get_indexed_pages_by_token(self, token: str, skip:int, limit:int):
        doc_curser = self.inverted_index.find({"token": token}, {"_id": 0})
        doc_curser = doc_curser.skip(skip).limit(limit)
        return doc_curser