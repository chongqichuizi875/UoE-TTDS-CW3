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
loader.process_inverted_index(50000)
#%%
loader.process_pages(100)
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