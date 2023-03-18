import datrie
import string
import numpy as np

from db.MongoDB import MongoDB
""" Term Completion
    Result Cache
"""
class Tire_Tree():
    """ {key:hit}
    """
    def __init__(self, path) -> None:
        self.trie = datrie.BaseTrie(string.ascii_lowercase+' ')
        self.path = path
    
    """ search by prefix
        return the matched result 
        sorted by hit
    """
    def search(self, key):
        items = self.trie.items(key)
        if items:
            results = [x for x, y in sorted(items, key=lambda item: item[1], reverse=True)]
            if len(results) > 5:
                return results[:5]
            else:
                return results
        else:
            return items # empty list

    """ User clicks the suggested query
        add the query if not exist
        increase the hit by one if exist
    """
    def hit(self, key):
        if key in self.trie:
            hit = self.trie[key]
            if hit < 2147483647:
                hit += 1
            self.trie.update([(key, hit)])
        else:
            self.add(key)
        self.save()
    def add(self, key):
        self.trie[key] = 0

    
    def delete(self, key):
        self.trie.pop(key)

    def clear(self):
        self.trie.clear()

    def save(self, path=None):
        if not path:
            path = self.path
        self.trie.save(path)
    def load(self, path=None):
        if not path:
            path = self.path
        self.trie= datrie.BaseTrie(string.ascii_lowercase).load(path)
    def load_from_db(self):
        for title in MongoDB().get_page_titles():
            self.add(title["title"].lower())

