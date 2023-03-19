from tire_tree import Trie_Hit, Trie_Log
import re

MAX_SUFFIX_WORDS_N = 3
MAX_TITLE_RESULT_N = 3
MAX_LOG_RESULT_N = 2
class Query_Completion():
    def __init__(self, path) -> None:
        # history query log
        self.trie_query_log = Trie_Log(path)
        self.trie_page_title = Trie_Hit(path)
        self.suffix_pattern = re.compile(r"""[\w\s]+\w+$""")

    def parse_query(self, query):
        # 1. history query log (whole match)
        # 2. suffix start with ()
        # 3. one word prefix
        results = []
        query_log = self.trie_query_log.search(query)
        if query_log:
            if len(query_log) > MAX_LOG_RESULT_N:
                results.extend(query_log[:MAX_LOG_RESULT_N])
            else:
                results.extend(query_log)
        
        suffix = re.search(self.suffix_pattern, query)

        if suffix:
            words = suffix[0].split(' ')
            if len(words) > MAX_SUFFIX_WORDS_N:
                words = words[-MAX_SUFFIX_WORDS_N]
            
            for i in range(MAX_SUFFIX_WORDS_N):
                titles = self.trie_page_title.search(' '.join(words[i:]))
                if titles:
                    results.extend(titles)
                if len(results) >= MAX_TITLE_RESULT_N:
                    results = results[:MAX_TITLE_RESULT_N] 
                    break

        return results
        
            