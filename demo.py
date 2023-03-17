#%%
from data_collection.wiki_loader import Wiki_Loader

wiki_path = "/mnt/e/wiki/enwiki-latest-pages-articles-multistream.xml.bz2"
loader = Wiki_Loader(wiki_path)
# loader.batch_process_pages(batch_size=50000, page_limit=500000)
loader.batch_process_inverted_index(batch_size=50000, page_limit=500000)
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

from ranking.ir_rankings_2 import calculate_sorted_bm25_score_of_query

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
