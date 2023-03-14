from typing import List
from db.DBInterface import DBInterface

from pymongo import MongoClient
from gensim import utils

# default thresholds for lengths of individual tokens
TOKEN_MIN_LEN = 2
TOKEN_MAX_LEN = 15

class MongoDB(DBInterface):
    def __init__(self) -> None:
        # client = MongoClient("mongodb://192.168.224.1:27017/")
        client = MongoClient("mongodb://127.0.0.1:27017/")
        self.wiki = client.metawiki
        self.pages = self.wiki.pages
        self.inverted_index = self.wiki.inverted_index

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
    def get_indexed_pages_by_token(self, token: str, skip:int, limit:int):
        doc_curser = self.inverted_index.find({"token": token}, {"_id": 0})
        doc_curser = doc_curser.skip(skip).limit(limit)
        return doc_curser

def tokenize(content, token_min_len=TOKEN_MIN_LEN, token_max_len=TOKEN_MAX_LEN, lower=True):
    """Tokenize a piece of text from Wikipedia.

    Set `token_min_len`, `token_max_len` as character length (not bytes!) thresholds for individual tokens.

    Parameters
    ----------
    content : str
        String without markup (see :func:`~gensim.corpora.wikicorpus.filter_wiki`).
    token_min_len : int
        Minimal token length.
    token_max_len : int
        Maximal token length.
    lower : bool
        Convert `content` to lower case?

    Returns
    -------
    list of str
        List of tokens from `content`.

    """
    return [
        utils.to_unicode(token) 
            for token in utils.tokenize(content, lower=lower, errors='ignore')
            if token_min_len <= len(token) <= token_max_len and not token.startswith('_')
    ]