#%%
from db.MongoDB import MongoDB
db = MongoDB()
#%%
from db.MongoDB import tokenize
tokenize("I love you")
#%% 
page_cursor = db.get_indexed_pages_by_token('sunday', skip=0, limit=1)
for page in page_cursor:
    print(page)
    break
#%%
from abc import ABC,abstractmethod

class one(ABC):
    @abstractmethod
    def move(self):
        pass
class child(one):
    def move(self):
        print('move')
obj_one=child()
obj_one.move()
#%%
for i, (tokens, (pageid, title, text)) in enumerate(wiki.get_texts()):

                temp[tokenid]['page'].append= temp[tokenid]['page'].get(pageid, {
                '_id': pageid,
                'pos': []
            })
    # for idx, (tokenid, token) in enumerate(zip(dictionary.doc2idx(tokens), tokens)):
    #     if tokenid != -1:
    #         # pos = [idx for idx, id in enumerate(ids) if id==tokenid]
    #         temp[tokenid] = temp.get(tokenid, {
    #             '_id': tokenid,
    #             'token': token,
    #             'page_count': dictionary.dfs.get(tokenid),
    #             'page': dict()
    #         })
    #         temp[tokenid]['page'][pageid] = temp[tokenid]['page'].get(pageid, {
    #             '_id': pageid,
    #             'pos': []
    #         })
    #         temp[tokenid]['page'][pageid]['pos'].append(idx) 
    pages_list.append({"_id": pageid, 
                       "title": title,
                       "text": text})
    if len(pages_list) == 100000:
        pages.insert_many(pages_list)
        pages_list.clear()


#%%
from data_collection.wiki import Wiki_Corpus
dict_path = "/mnt/e/wiki/results/enwiki/enwiki_wordids.txt.bz2"
wiki_path = "/mnt/e/wiki/enwiki-latest-pages-articles-multistream.xml.bz2"
wiki = Wiki_Corpus(dict_path, wiki_path)

wiki.process_pages()
#%%
from gensim.corpora import Dictionary
outp = "
dictionary = Dictionary.load_from_text(outp + '_wordids.txt.bz2')

# %%
