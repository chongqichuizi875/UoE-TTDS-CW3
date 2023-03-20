import datrie
import string
import numpy as np
import time
from db.MongoDB import MongoDB
from nltk.corpus import stopwords
""" Term Completion
    Result Cache
"""
class Trie_Tree():
    """ {key:hit}
    """
    def __init__(self, path=None) -> None:
        self.init_trie()
        if path:
            self.path = path
            self.load()
    
    """ search by prefix
        return the matched result 
        descending sorting 
    """
    def search(self, key):
        items = self.trie.items(key)
        if items:
            results = [x for x, y in sorted(items, key=lambda item: item[1], reverse=True)]
            # if len(results) > limit:
            #     return results[:limit]
            # else:
            return results
        else:
            return items # empty list

    def update(self, key, val):
        self.trie.update([(key, val)])

    def add(self, key):
        self.trie[key] = 0
    
    def delete(self, key):
        self.trie.pop(key)

    def clear(self):
        self.trie.clear()

    def save(self, path=None):
        if not path:
            path = self.path
        if path:
            self.trie.save(path)
        else:
            print("Save path not defined")

    def init_trie(self):
        self.trie= datrie.BaseTrie(string.ascii_lowercase)
    def load(self, path=None):
        if not path:
            path = self.path
        if path:
            self.trie= self.trie.load(path)
        else:
            print("load path not defined")

class Trie_Hit(Trie_Tree):
    """ User clicks the suggested query
        add the query if not exist
        increase the hit by one if exist
    """
    def __init__(self, path=None) -> None:
        super().__init__(path)

    def hit(self, key):
        if key in self.trie:
            hit = self.trie[key]
            if hit < 2147483647:
                hit += 1
            self.trie.update([(key, hit)])
        else:
            self.add(key)
        self.save()
    def load_from_db(self):
        stop_words = set(stopwords.words('english'))
        for info in MongoDB().get_page_titles():
            title = info["title"].lower()
            if len(title) and title not in stop_words:
                self.add(title)
    def init_trie(self):
        self.trie = datrie.BaseTrie(string.ascii_lowercase+' ')
class Trie_Log(Trie_Tree):
    def __init__(self, path=None) -> None:
        super().__init__(path)
    def hit(self, key):
        self.trie.update([(key, int(time.time()))])
        self.save()

    def load(self, path=None):
        if not path:
            path = self.path
    def init_trie(self):
        self.trie = datrie.BaseTrie(string.ascii_letters+string.digits+string.punctuation+string.whitespace)