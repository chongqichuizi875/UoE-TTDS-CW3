import re
import nltk
from nltk.corpus import stopwords
from ranking.ir_rankings import calculate_sorted_bm25_score_of_query
from db.MongoDB import MongoDB
from nltk.tokenize import TweetTokenizer
from nltk.stem import PorterStemmer

nltk.download('stopwords')
nltk.download('punkt')
stop_words = set(stopwords.words('english'))


class DBSearch(object):
    """
    1: preprocess the query
    2: implement all the search method(single word, phrase, boolean, proximity, free)
    3: link to the database
    """

    def __init__(self, inverted_index_db: MongoDB) -> None:
        self.inverted_index_db = inverted_index_db

    def preprocessing(self, text, stemming=True, stopping=True):
        """This functioon will preprocess the text passed in, 
        remove the stopping words of English, stemming words,
        and return a list of tokens after processing. 
        """
        token_list = TweetTokenizer().tokenize(text)
        filtered_list = [term.lower() for term in token_list if term not in stop_words or not stopping]
        if stemming:
            filtered_list = [PorterStemmer().stem(token) for token in filtered_list]
        return list(filter(lambda x: x.isalnum(), filtered_list))

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
            # token = self.preprocessing(token)[0]
            print(f"token after pre \'{token}\'")
        except:
            return []
        doc_list = []
        try:
            doc_curser = self.inverted_index_db.get_indexed_pages_by_token(token=token)
            # 这边需要添加doc_curser的处理->list:token出现的文档id
            for doc in doc_curser:
                doc_list += doc['page']
            return doc_list
        except:
            print(f"token {token} not in database")

        return []

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
                    if (abs(pos1 - pos2) <= distance):
                        output_list.append(id)
        return output_list

    def free_search(self, query: str):
        return calculate_sorted_bm25_score_of_query(query)


class QuerySelection(object):
    """
    determine which search method to use according to the syntact of query 
    """

    def __init__(self, query: str, dbsearch: DBSearch) -> list:
        self.queryList = query.split()
        self.result = []
        self.dbsearch = dbsearch

        if (len(query) == 0):
            return

        # boolean search
        if "AND NOT" in query:
            q = query.split(' AND NOT ')
            print('parsing AND NOT: ')
            op1 = self.dbsearch.boolean_search(q[0])
            op2 = self.dbsearch.boolean_search(q[1])
            self.result = sorted(list(set(op1) - set(op2)))

            return
        elif " AND" in query:
            q = query.split(' AND ')
            print('parsing AND: ')
            op1 = self.dbsearch.boolean_search(q[0])
            op2 = self.dbsearch.boolean_search(q[1])
            self.result = sorted(list(set(op1) & set(op2)))

            return
        elif " OR" in query:
            q = query.split(' OR ')
            print('parsing OR:')
            op1 = self.dbsearch.boolean_search(q[0])
            op2 = self.dbsearch.boolean_search(q[1])
            self.result = sorted(list(set(op1) | set(op2)))

            return

        # proximity search
        if '#' in query:
            r = re.split(r'[^\w]', query)
            r = list(filter(None, r))
            print('proximity search')
            self.result = self.dbsearch.proximity_search(r[1], r[2], int(r[0]))

            return

        # phrase search (only 2-word phrase)  
        elif '\"' in query:
            print("phrase search")
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
            print("free search")
            self.result = self.dbsearch.free_search(query)
            return

    def __call__(self, *args, **kwargs):
        return self.result


query = '#15(participated, commentary)'
mongodb = MongoDB()
dbsearch = DBSearch(inverted_index_db=mongodb)
search = QuerySelection(query, dbsearch)
result = search()
print(f"query: {query}")
print(f"result: {result}")
