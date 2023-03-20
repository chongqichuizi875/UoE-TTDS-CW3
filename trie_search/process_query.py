# from trie_tree import Trie_Hit, Trie_Log
from trie_search.trie_tree import Trie_Hit, Trie_Log
import re

MAX_SUFFIX_WORDS_N = 3
MAX_TITLE_RESULT_N = 3
MAX_LOG_RESULT_N = 2

class Query_Completion():
    def __init__(self, trie_query_log, trie_page_title) -> None:
        # history query log
        self.trie_query_log = trie_query_log
        self.trie_page_title = trie_page_title
        self.suffix_pattern = re.compile(r"""[\w\s]+$""")
    def get_info_list(self, query):
        results = self.parse_query(query.lower())
        return [{"title": s} for i, s in enumerate(results)]

    def parse_query(self, query):
        # 1. history query log (whole match)
        # 2. suffix start with ()
        # 3. one word prefix
        results = []
        query_log = self.trie_query_log.search(query)
        if query_log:
            print('query log', query_log)
            if len(query_log) > MAX_LOG_RESULT_N:
                results.extend(query_log[:MAX_LOG_RESULT_N])
            else:
                results.extend(query_log)
        
        suffix_match = re.search(self.suffix_pattern, query)

        if suffix_match:
            prefix = query[:suffix_match.start()]
            words = suffix_match[0].split(' ')
            if len(words) > MAX_SUFFIX_WORDS_N:
                pre_words = words[:-MAX_SUFFIX_WORDS_N]
                words = words[-MAX_SUFFIX_WORDS_N:]
            else:
                pre_words = []
            
            for i in range(MAX_SUFFIX_WORDS_N):
                tmp_str =' '.join(words[i:]) 
                if tmp_str:
                    titles = self.trie_page_title.search(tmp_str)
                    if titles:
                        results.extend([prefix+' '.join(pre_words+words[:i]+[t]) for t in titles])
                    if len(results) >= MAX_TITLE_RESULT_N:
                        results = results[:MAX_TITLE_RESULT_N] 
                        break

        return results
        
            