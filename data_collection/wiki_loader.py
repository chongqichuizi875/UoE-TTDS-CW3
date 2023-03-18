from pymongo import MongoClient, UpdateOne
from data_collection.wikicorpus import MyWikiCorpus
from tqdm import tqdm
import logging
import logging
from tqdm import tqdm
import os

class Wiki_Loader:
    def __init__(self, wiki_path):
        # self.token_dictionary = Dictionary.load_from_text(dict_path)
        # self.wiki = WikiCorpus(wiki_path, dictionary=self.token_dictionary)
        # self.wiki = MyWikiCorpus(wiki_path, dictionary=self.token_dictionary)
        client = MongoClient("mongodb://192.168.224.1:27017/")
        self.pages = client.diuawiki.pages
        self.inverted_index = client.subwiki.inverted_index

        self.wiki = MyWikiCorpus(wiki_path)
        # self.wiki.metadata = True
        self.page_list = list()
        self.inverted_index_dict = dict()
        self.logger = logging.getLogger(__name__)

    # def set_logger(self):
        # if not os.path.exists("Logs"):
        #     os.makedirs("Logs")
        # logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S', filename='Logs/wiki_loader_progress.log', filemode='w')

    """     
        iterate over compressed wiki file 
        load pages and create inverted index
        transfer to mongodb
    """ 
    def _batch_process_wiki(self, load_func, save_func, batch_size, page_limit):
        pbar = tqdm(enumerate(self.wiki.get_texts(), 1))
        for i, params in pbar:
            info = "Processed %d pages"%i
            load_func(params)
            if page_limit is not None and i == page_limit:
                break
            if i % batch_size == 0:
                save_func()
                self.logger.info(info)
            pbar.set_description(info)

        save_func()

    def batch_process_pages(self, batch_size=100000, page_limit=None):
        if page_limit:
            page_limit -= 1
        self._batch_process_wiki(self.load_page, self.save_pages, batch_size, page_limit)

    def batch_process_inverted_index(self, batch_size=100000, page_limit=None):
        if page_limit:
            page_limit -= 1
        self._batch_process_wiki(self.load_inverted_index, self.save_inverted_index, batch_size, page_limit)

    def process_single_wiki(self, f):
        params = self.wiki.process_wiki_page(f)
        # paramas = tokens, (title, pageid, text)
        self.load_page(params)
        self.load_inverted_index(params)
        self.save_inverted_index()
        self.save_pages()

    def load_inverted_index(self, params):
        tokens, (pageid, title, text) = params
        idx = 0
        # for tokenid, token in zip(self.token_dictionary.doc2idx(tokens), tokens):
        #     if tokenid != -1:
        #         self.create_inverted_index(tokenid, token, pageid, idx)
        #         idx += 1
        for token in tokens:
            self.create_inverted_index(token, pageid, idx)
            idx += 1
    def load_page(self, params):
        tokens, (pageid, title, text) = params
        self.page_list.append({"_id": pageid, 
                "title": title,
                "page_len": len(tokens),
                "text": text})

    def create_inverted_index(self, token, pageid, idx):
        temp = self.inverted_index_dict
        temp[token] = temp.get(token, {
            'token': token,
            'page_count': 0,
            'pages': dict()
        })
        if pageid not in temp[token]['pages']:
            temp[token]['pages'][pageid] = {
                '_id': pageid,
                'tf': 0,
                'pos': []
            }
            temp[token]['page_count'] += 1
        temp[token]['pages'][pageid]['pos'].append(idx) 
        temp[token]['pages'][pageid]['tf'] += 1


    # def create_inverted_index(self, token, pageid, idx):
    #     temp = self.inverted_index_dict
    #     temp[tokenid] = temp.get(tokenid, {
    #         '_id': tokenid,
    #         'token': token,
    #         'page_count': self.token_dictionary.dfs.get(tokenid),
    #         'pages': dict()
    #     })
    #     temp[tokenid]['pages'][pageid] = temp[tokenid]['pages'].get(pageid, {
    #         '_id': pageid,
    #         'pos': []
    #     })
    #     temp[tokenid]['pages'][pageid]['pos'].append(idx) 
    #     idx += 1

    def save_pages(self):
        if self.page_list:
            self.pages.insert_many(self.page_list)
            self.page_list.clear()

    def save_inverted_index(self):
        if self.inverted_index_dict:
            idx_dict = self.inverted_index_dict
            for v in idx_dict.values():
                v['pages'] = list(v['pages'].values())
                
            self.inverted_index.insert_many(list(idx_dict.values()))
            self.inverted_index_dict.clear()
    def save_single_inverted_index(self):
        op_list = list()
        for doc in self.inverted_index_dict.values():
            op_list.append(UpdateOne(
                        {'token': doc['token']},
                        {
                            '$push': { 'pages': doc['pages'] }, 
                            '$inc': {'page_count': 1}
                        },
                        upsert= True
            ))
        self.inverted_index.bulk_write(op_list, ordered=False)
    #                
    # def save_inverted_index(self):
    #     if self.inverted_index_dict:
    #         idx_dict = self.inverted_index_dict
    #         print(idx_dict)
    #         for v in idx_dict.values():
    #             v['pages'] = list(v['pages'].values())

    #             op = UpdateOne(
    #                 { 'token': v['token'], '$bsonSize': { '$gt' :BSON.MAX} },
    #                 {
    #                     '$push': { 'pages': v['pages'] }, 
    #                 },
    #                 upsert= True
    #             )
                
    #         inverted_index.insert_many(list(idx_dict.values()))
    #         self.inverted_index_dict.clear()
        # print(list(idx_dict.values()))
        # inverted_index.update_many(
        #     { name: "SensorName1", count { $lt : 500} },
        #     {
        #         //Your update. $push your reading and $inc your count
        #         $push: { readings: [ReadingDocumentToPush] }, 
        #         $inc: { count: 1 }
        #     },
        #     { upsert: true }
        # )