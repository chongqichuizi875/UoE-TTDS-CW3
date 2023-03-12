from gensim.corpora import Dictionary, WikiCorpus
from pymongo import MongoClient

client = MongoClient("mongodb://192.168.224.1:27017/")
pages = client.wiki.pages
inverted_index = client.wiki.inverted_index

class Wiki_Corpus:
    def __init__(self, dict_path, wiki_path):
        self.token_dictionary = Dictionary.load_from_text(dict_path)
        self.wiki = WikiCorpus(wiki_path, dictionary=self.token_dictionary)
        self.wiki.metadata = True
        self.page_list = list()
        self.inverted_index_dict = dict()

    """     
        iterate over compressed wiki file 
        load pages and create inverted index
        transfer to mongodb
    """ 
    def _process_wiki(self, load_func, save_func, batch_size):
        for i, params in enumerate(self.wiki.get_texts()):
            load_func(params)
            if i % batch_size == batch_size-1:
                save_func()
        save_func()

    def process_pages(self, batch_size=100000):
        self._process_wiki(self.load_page, self.save_pages, batch_size)

    def process_inverted_index(self, batch_size=100000):
        self._process_wiki(self.load_inverted_index, self.save_inverted_index, batch_size)

    def load_inverted_index(self, params):
        tokens, (pageid, title, text) = params
        idx = 0
        for tokenid, token in zip(self.token_dictionary.doc2idx(tokens), tokens):
            if tokenid != -1:
                self.create_inverted_index(tokenid, token, pageid, idx)
                idx += 1
    def load_page(self, params):
        tokens, (pageid, title, text) = params
        self.page_list.append({"_id": pageid, 
                "title": title,
                "text": text})

    def create_inverted_index(self, tokenid, token, pageid, idx):
        temp = self.inverted_index_dict
        temp[tokenid] = temp.get(tokenid, {
            '_id': tokenid,
            'token': token,
            'page_count': self.token_dictionary.dfs.get(tokenid),
            'pages': dict()
        })
        temp[tokenid]['pages'][pageid] = temp[tokenid]['pages'].get(pageid, {
            '_id': pageid,
            'pos': []
        })
        temp[tokenid]['pages'][pageid]['pos'].append(idx) 
        idx += 1

    def save_pages(self):
        pages.insert_many(self.page_list)
        self.page_list.clear()

    def save_inverted_index(self):
        idx_dict = self.inverted_index_dict
        for v in idx_dict.values():
            v['pages'] = list(v['pages'].values)
            
        inverted_index.insert_many(list(idx_dict.values()))
        self.inverted_index_dict.clear()