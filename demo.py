#%%
from db.MongoDB import MongoDB
db = MongoDB()
#%%
from db.MongoDB import tokenize
from gensim.utils import to_unicode
to_unicode(tokenize("I love you")[0])
#%%
import bz2
wiki_path = "/mnt/e/wiki/enwiki-latest-pages-articles-multistream.xml.bz2"
f = bz2.BZ2File(wiki_path)
#%%
from xml.etree.ElementTree import iterparse
elems = (elem for _, elem in iterparse(f, events=("end",)))
#%%
elem = next(elems)
#%%
import re
def get_namespace(tag):
    """Get the namespace of tag.

    Parameters
    ----------
    tag : str
        Namespace or tag.

    Returns
    -------
    str
        Matched namespace or tag.

    """
    m = re.match("^{(.*?)}", tag)
    namespace = m.group(1) if m else ""
    if not namespace.startswith("http://www.mediawiki.org/xml/export-"):
        raise ValueError("%s not recognized as MediaWiki dump namespace" % namespace)
    return namespace

namespace = get_namespace(elem.tag)
ns_mapping = {"ns": namespace}
page_tag = "{%(ns)s}page" % ns_mapping
text_path = "./{%(ns)s}revision/{%(ns)s}text" % ns_mapping
title_path = "./{%(ns)s}title" % ns_mapping
ns_path = "./{%(ns)s}ns" % ns_mapping
pageid_path = "./{%(ns)s}id" % ns_mapping
cat_path = "./{%(ns)s}category" % ns_mapping
#%%
for elem in elems:
        if elem.tag == page_tag:
            cat = elem.find(cat_path).text
            # text = elem.find(text_path).text
            print(cat)
            break
            # Prune the element tree, as per
            # http://www.ibm.com/developerworks/xml/library/x-hiperfparse/
            # except that we don't need to prune backlinks from the parent
            # because we don't use LXML.
            # We do this only for <page>s, since we need to inspect the
            # ./revision/text element. The pages comprise the bulk of the
            # file, so in practice we prune away enough.
#%%
for i, line in enumerate(f):
    print(line)
    if i == 200:
        break
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
# wiki.process_inverted_index()
#%%
from gensim.corpora import Dictionary
outp = "
dictionary = Dictionary.load_from_text(outp + '_wordids.txt.bz2')

# %%
from data_collection.wikicorpus import MyWikiCorpus 
wiki_path = "/mnt/e/wiki/enwiki-latest-pages-articles-multistream.xml.bz2"

wiki = MyWikiCorpus(wiki_path)
#%%
wiki_path = "/mnt/e/wiki/enwiki-latest-pages-articles-multistream.xml.bz2"
dict_path = "/mnt/e/wiki/results/enwiki1/enwiki_wordids.txt.bz2"
from data_collection.wiki_loader import Wiki_Loader

from data_collection.wikicorpus import MyWikiCorpus 

wiki = MyWikiCorpus(wiki_path)
# wiki.save_Dictionary("/mnt/e/wiki/results/enwiki1/enwiki")
loader = Wiki_Loader(wiki_path)
loader.batch_process_pages(100)
# loader.batch_process_inverted_index(50000)
#%%
loader.batch_process_pages(100)
#%%
wiki.save_Dictionary("/mnt/e/wiki/results/enwiki1/enwiki")
#%%
for t in wiki.get_texts():
    print(t)
    break

#%%
from pymongo import MongoClient
client = MongoClient("mongodb://192.168.224.1:27017/")
pages = client.wiki.pages
inverted_index = client.wiki.inverted_index
#%%
pages.insert_one({'x':1})
# %%
import nltk
import ssl
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
from nltk.tokenize import TweetTokenizer

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

nltk.download('stopwords')
nltk.download('punkt')

stop_words = set(stopwords.words('english'))

def preprocessing(text, stemming=True, stop=True):
    """This functioon will preprocess the text passed in, remove the stopping words of English, stemming words,
    and return a list of tokens after processing. """
    token_list = TweetTokenizer().tokenize(text)
    filtered_list = [term.lower() for term in token_list if term not in stop_words or not stop]
    if stemming:
        filtered_list = [PorterStemmer().stem(token) for token in filtered_list]
    return list(filter(lambda x: x.isalnum(), filtered_list))

#%%
from pymongo import MongoClient
client = MongoClient("mongodb://192.168.224.1:27017/")

# client = MongoClient("mongodb://127.0.0.1:27017/")
client.metawiki.pages.count_documents({})
#%%
from db.MongoDB import MongoDB
db = MongoDB()
db.pages.count()
# %%
from data_collection.wikicorpus import MyWikiCorpus
wiki = MyWikiCorpus("")
wiki.wiki_tokenize(r"Сopy of `s` with all the 'File:' and 'Image:' markup replaced by their `corresponding captions <http://www.mediawiki.org/wiki/Help:Images>`_.")
# %%
from db.MongoDB import MongoDB
from data_collection.wikicorpus import MyWikiCorpus
tokens =MyWikiCorpus("").wiki_tokenize("sunday")
gen = MongoDB().get_indexed_pages_by_token(tokens[0])
for x in gen:
    print(x)
#%%
from data_collection.wiki_loader import Wiki_Loader

wiki_path = "/mnt/e/wiki/enwiki-latest-pages-articles-multistream.xml.bz2"
loader = Wiki_Loader(wiki_path)
# loader.batch_process_pages(batch_size=50000, page_limit=500000)
# loader.batch_process_inverted_index(batch_size=50000, page_limit=500000)
loader.batch_process_inverted_index(batch_size=100, page_limit=1000)
# loader.batch_process_inverted_index()
#%%
loader.batch_process_inverted_index(batch_size=50000, page_limit=500000)
# %%
from db.MongoDB import MongoDB

db = MongoDB()
#%%
for i in db.get_indexed_pages_by_token('anarch'):
    print(i)
#%%
# db.avg_page_len
# %%
import sys
from pathlib import Path
import math
import time
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db import MongoDB
from data_collection.preprocessing import Preprocessing

from ranking.ir_rankings import calculate_sorted_bm25_score_of_query

mongoDB = MongoDB.MongoDB()
preprocessing = Preprocessing()
avg_page_len = 868
N = 127332
ts = time.time()
print(ts)
bm25_results = calculate_sorted_bm25_score_of_query("sunday")
ts2 = time.time()
print(ts2)
print("bm25_results:::")
print(bm25_results)
the_first_bm25_returned_page = mongoDB.get_pages_by_list_of_ids(ids=list(bm25_results.keys()))[0]
the_first_bm25_returned_page_title = the_first_bm25_returned_page['title']
the_first_bm25_returned_page_content = the_first_bm25_returned_page['text']
print("title::::bm25")
print(the_first_bm25_returned_page_title)
# %%
class Node:
    def __init__(self):
        self.children = {}
        self.is_leaf = False
        self.words = []

class Trie:
    def __init__(self):
        self.root = Node()

    def add_word(self, word):
        node = self.root
        for char in word:
            if char not in node.children:
                node.children[char] = Node()
            node = node.children[char]
        node.is_leaf = True
        node.words.append(word)

    def search(self, prefix):
        node = self.root
        for char in prefix:
            if char not in node.children:
                return []
            node = node.children[char]
        return node.words

# Example usage
trie = Trie()
trie.add_word("apple")
trie.add_word("application")
trie.add_word("apartment")
trie.add_word("apex")
trie.add_word("apostle")

prefix = "ap"
suggestions = trie.search(prefix)
print(suggestions)

# %%
#%%
import marisa_trie
from db.MongoDB import MongoDB
db = MongoDB()
trie = marisa_trie.Trie([x['title'] for x in db.get_page_titles() ])
trie.save("subwiki.marisa")
# %%
trie.keys('Compu')
#%%
import string
import datrie
trie = datrie.BaseTrie(string.ascii_lowercase)
#%%
trie['abc'] = 0
trie['abcd'] = 1
trie['acde'] = 2

#%%
trie.update([('abc', 10)])
# trie['acde']
#%%
trie.values('ab')
#%%
import torch
from transformers import T5Tokenizer, T5ForConditionalGeneration

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

tokenizer = T5Tokenizer.from_pretrained('castorini/doc2query-t5-base-msmarco')
model = T5ForConditionalGeneration.from_pretrained('castorini/doc2query-t5-base-msmarco')
model.to(device)
# %%
doc_text = 'The presence of communication amid scientific minds was equally important to the success of the Manhattan Project as scientific intellect was. The only cloud hanging over the impressive achievement of the atomic researchers and engineers is what their success truly meant; hundreds of thousands of innocent lives obliterated.'

input_ids = tokenizer.encode(doc_text, return_tensors='pt').to(device)
outputs = model.generate(
    input_ids=input_ids,
    max_length=64,
    do_sample=True,
    top_k=10,
    num_return_sequences=3)

for i in range(3):
    print(f'sample {i + 1}: {tokenizer.decode(outputs[i], skip_special_tokens=True)}')
#%%
from db.MongoDB import MongoDB
from time import time
db = MongoDB()
s = time()
db.get_page_by_page_id("12")
e = time()
print(e-s)
# %%
from trie_search.tire_tree import Trie_Tree
path = "./data/my.trie"
tt = Trie_Tree(path)
tt.load_from_db()
# tt.load()
# %%
# tt.save()
results = tt.search('computer')
#%%
if results:
    tt.hit(results[2])

results = tt.search('computer')
#%%

# %%
import datrie
import string

# trie = datrie.BaseTrie(string.ascii_lowercase+' ')

trie.load()
#%%
# trie["computerscience"] = 0
for i in trie.items():
    print(i)
#%%
from db.MongoDB import MongoDB
db =MongoDB()

x = db.get_high_freq_tokens()
# %%
for i in x:
    print(i)
# %%
ex = ["my quest",
      r"my 'que",
      "my question i2",
      "aoiu2 [fo2lo",
      "oaiu: "
      ]

import re
reg_ex = r"""[\w\s]+\w+$"""
for e in ex:
    res = re.search(reg_ex, e)
    if res:
        print(True, res[0])
#%%
from trie_search.tire_tree import Trie_Tree

tt = Trie_Tree("data/my.trie")
tt.load()
#%%
tt.trie.items()