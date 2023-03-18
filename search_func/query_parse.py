import math
import re
import time
from cachetools import cached
import numpy as np
from nltk.corpus import stopwords
from db.MongoDB import MongoDB
import sys
from data_collection.preprocessing import Preprocessing
from functools import wraps

# from ranking.ir_rankings_2 import calculate_sorted_bm25_score_of_query
VERBOSE = False
# nltk.download('stopwords')
# nltk.download('punkt')
stop_words = set(stopwords.words('english'))
k1 = 2
b = 0.75


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        sys.stdout.write(f'Function {func.__name__}{args} {kwargs} Took {total_time:.4f} seconds\n')
        return result

    return timeit_wrapper


class DBSearch(object):
    """
    1: preprocess the query
    2: implement all the search method(single word, phrase, boolean, proximity, free)
    3: link to the database
    """

    def __init__(self, inverted_index_db: MongoDB, verbose=VERBOSE) -> None:
        self.token2index = {}
        self.freq_dicts = {}
        self.inverted_index_db = inverted_index_db
        self.verbose = verbose

    def preprocessing(self, text, stemming=True, stopping=True):
        """This functioon will preprocess the text passed in,
        remove the stopping words of English, stemming words,
        and return a list of tokens after processing.
        """
        return Preprocessing().wiki_tokenize(text)

    def single_search(self, token: str) -> list:
        """
        return the doc_list:
            [{'_id': 7, 'pos': [0]},
             {'_id': 184, 'pos': [540]},
             {'_id': 684, 'pos': [1289, 1568]},
             {'_id': 1611, 'pos': [1638, 1643]},
             {'_id': 1712, 'pos': [57, 69]},
             {'_id': 5570, 'pos': [145, 1280]},
             {'_id': 5571, 'pos': [1977]}]
        """

        try:
            token = self.preprocessing(token)[0]
            if self.verbose:
                sys.stdout.write(f"token after pre \'{token}\'")
        except:
            return []
        doc_list = []
        try:
            doc_curser = self.inverted_index_db.get_indexed_pages_by_token(token)
            for doc in doc_curser:
                doc_list += doc['pages']
            return doc_list
        except:
            sys.stderr.write(f"token {token} not in database")

        return []

    @cached(cache={})
    def boolean_search(self, token: str) -> list:
        """
        wrap the single search because it only search for one token now
        return the id_list of content containing token
        """
        doc_list = self.single_search(token)
        id_list = []
        for location in doc_list:
            id_list.append(location['_id'])
        return id_list

    @cached(cache={})
    def phrase_search(self, phrase: str) -> list:
        """
        phrase -> 'token1 token2'
        return the id_list of content containing token1 & token2
        and the pos of token2 = pos of token1 + 1
        """
        phrase_list = phrase.split()
        output_list = []
        op1 = self.single_search(phrase_list[0])
        op2 = self.single_search(phrase_list[1])
        # turn (list of dicts) -> (dict with list values)
        dict1 = {_['_id']: _['pos'] for _ in op1}
        dict2 = {_['_id']: _['pos'] for _ in op2}
        # find the common '_id' of the two token
        common_list = set(dict1.keys()) & set(dict2.keys())

        for id in common_list:
            for pos1 in dict1[id]:
                if pos1 + 1 in dict2[id] and id not in output_list:
                    output_list.append(id)
        return output_list

    @cached(cache={})
    def proximity_search(self, token1: str, token2: str, distance: int) -> list:
        """
        return the id_list of content containing token1 & token2
        and the abs(pos of token1 and token2) <= distance
        """
        output_list = []
        op1 = self.single_search(token1)
        op2 = self.single_search(token2)
        # turn (list of dicts) -> (dict with list values)
        dict1 = {_['_id']: _['pos'] for _ in op1}
        dict2 = {_['_id']: _['pos'] for _ in op2}
        # find the common '_id' of the two token
        common_list = set(dict1.keys()) & set(dict2.keys())

        for id in common_list:
            for pos1 in dict1[id]:
                for pos2 in dict2[id]:
                    if abs(pos1 - pos2) <= distance:
                        output_list.append(id)
        return output_list

    @cached(cache={})
    def free_search(self, query: str):
        sorted_score_map = self.bm25_sorted(query)
        # sorted_score_map = calculate_sorted_bm25_score_of_query(query)
        result_list = []
        for key in sorted_score_map.keys():
            result_list.append([key, sorted_score_map[key]])
        return result_list

    @timeit
    @cached(cache={})
    def calculate_freq(self, token, minimal_freq=5, max_chunk_size=1000):

        try:
            freq_dict = {}
            doc_curser = self.inverted_index_db.get_indexed_pages_by_token(token)
            page_count = 0
            for doc in doc_curser:
                page_count += doc['page_count']
                for id_pos_dict in doc['pages']:
                    freq = id_pos_dict['tf']
                    if freq > minimal_freq:
                        freq_dict[id_pos_dict['_id']] = freq

            # if len(freq_dict) > max_chunk_size:
            #     freq_dict = dict(sorted(freq_dict.items(), key=lambda x: x[1], reverse=True)[:max_chunk_size])
            return freq_dict, page_count
        except:
            return {}, 0

    def token_to_num(self, tokens):
        for index, token in enumerate(list(set(tokens))):
            self.token2index[token] = index

    @timeit
    def bm25_sorted(self, query: str):
        """

        :param query:
        :return: sorted dict
                 {page_id1: weight1
                  page_id2: weight2
                  page_id3: weight3
                  page_id4: weight4}
        """
        tokens = self.preprocessing(query)
        # tokens = query.split()
        score_dict = {}

        # freq_dict_list
        # idf_list = np.log10((self.inverted_index_db.page_count + 0.5) / (page_count_list + 0.5))
        # print(idf_list.shape)
        for token in tokens:
            freq_dict, page_count = self.calculate_freq(token)
            if len(freq_dict) > 0:
                print(len(freq_dict))
                time1 = time.time()
                idf = math.log10((self.inverted_index_db.page_count - page_count + 0.5) / (page_count + 0.5))
                for page_id in freq_dict.keys():
                    page_dict = self.inverted_index_db.get_page_by_page_id(page_id)
                    tf = freq_dict[page_id]  # term freq
                    page_len = page_dict['page_len']
                    k = k1 * (1 - b + (b * (page_len / self.inverted_index_db.avg_page_len)))
                    relevance = (tf * (k1 + 1)) / (tf + k)
                    if page_id not in score_dict.keys():
                        score_dict[page_id] = 0
                    score_dict[page_id] += idf * relevance
                time2 = time.time()
                print(f"{time2-time1}s")
            else:
                continue
        return dict(sorted(score_dict.items(), key=lambda x: x[1], reverse=True))


class QuerySelection(object):
    """
    determine which search method to use according to the syntact of query
    """

    def __init__(self, query: str, dbsearch: DBSearch, recur=False, verbose=VERBOSE):
        self.queryList = query.split()
        self.result = []
        self.dbsearch = dbsearch
        self.fuzzy = False  # default exact query
        self.recur = recur  # if created recursively -> must be exact search
        self.verbose = verbose

        if (len(query) == 0):
            return

        if not self.recur:
            # distinguish Exact query or fuzzy query
            if "[" not in query:
                self.fuzzy = True
                if self.verbose:
                    sys.stdout.write("Fuzzy query!")
            else:
                query.replace('[', '').replace(']', '')
                if self.verbose:
                    sys.stdout.write("Exact query! ")
        # boolean search
        if "AND NOT" in query:
            q = query.split(' AND NOT ')
            if self.verbose:
                sys.stdout.write('parsing AND NOT: ')
            op1 = QuerySelection(q[0], dbsearch, True).result
            op2 = QuerySelection(q[1], dbsearch, True).result
            self.result = sorted(list(set(op1) - set(op2)))

            return
        elif " AND" in query:
            q = query.split(' AND ')
            if self.verbose:
                sys.stdout.write('parsing AND: ')
            op1 = QuerySelection(q[0], dbsearch, True).result
            op2 = QuerySelection(q[1], dbsearch, True).result
            self.result = sorted(list(set(op1) & set(op2)))

            return
        elif " OR" in query:
            q = query.split(' OR ')
            if self.verbose:
                sys.stdout.write('parsing OR:')
            op1 = QuerySelection(q[0], dbsearch, True).result
            op2 = QuerySelection(q[1], dbsearch, True).result
            self.result = sorted(list(set(op1) | set(op2)))

            return

        # proximity search
        if '#' in query:
            r = re.split(r'[^\w]', query)
            r = list(filter(None, r))
            if self.verbose:
                sys.stdout.write('proximity search')
            self.result = self.dbsearch.proximity_search(r[1], r[2], int(r[0]))

            return

        # phrase search (only 2-word phrase)
        elif '\"' in query:
            if self.verbose:
                sys.stdout.write("phrase search")
            r = re.split(r'[^\w]', query)
            # turn list -> str
            s = str()
            for i in r:
                if len(i) != 0:
                    s += i + ' '
            self.result = self.dbsearch.phrase_search(s)

            return
        # free search
        else:
            if self.fuzzy:
                self.result = self.dbsearch.free_search(query)
                return
            else:
                self.result = self.dbsearch.boolean_search(query)

    def __call__(self, *args, **kwargs):
        return self.result


# query = '["Academy Award" AND NOT Oscars]'
# query = 'how to learn python learn'
query = 'sunday'
mongodb = MongoDB()
time1 = time.time()
dbsearch = DBSearch(inverted_index_db=mongodb)
time2 = time.time()
if VERBOSE:
    sys.stdout.write(f"DB initialize: {time2 - time1}s")
    sys.stdout.write("-----------------------------------------------------------------------")
time1 = time.time()
result = QuerySelection(query, dbsearch)()
time2 = time.time()
if VERBOSE:
    sys.stdout.write(f"first run: {time2 - time1}s")
    sys.stdout.write(f"query: {query}")
    sys.stdout.write(f"result: {result[:10]}")
    sys.stdout.write("-----------------------------------------------------------------------")
sys.stdout.write(f"result: {result[:10]}")
time1 = time.time()
result = QuerySelection(query, dbsearch)()
time2 = time.time()
if VERBOSE:
    print(f"second run: {time2 - time1}s")
    # print(f"query: {query}")
    # print(f"result: {result}")
