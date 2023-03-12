import sys
import pickle

import argparse
import logging

from gensim.corpora import Dictionary, HashDictionary, MmCorpus, WikiCorpus

logging.basicConfig(filename='result.log', level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger()
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

BATCH_SIZE = 100000

class IndexGenerator:
    def __init__(self, dict_path, wiki_path, start=0):
        '''
        Parameters:
            start: where to begin (restore indexing)
        '''
        self.temp = dict()
        self.start = start
        self.token_dictionary = Dictionary.load_from_text(dict_path)
        self.wiki_path = wiki_path
    
    def run_indexing(self, wiki):
        for i, (tokens, page) in enumerate(wiki.get_texts()):
            logging.info("loading: " + str(i+1))

                self.__load_tempfile(tokens, page)

            if (i+1)%15 == 0:
                self.__regularize_dict()
                self.__save_pickle(str(i-14) + "-" + str(i+1) + "-insert")

        self.__regularize_dict()
        self.__save_pickle("last")

    def __load_tempfile(self, tokens, page):
        pageid, title, text = page
        
        preprocessed = preprocessing.preprocess(sentence, stemming=self.activate_stemming, stop=self.activate_stop)
        preprocessed = list(filter(None, preprocessed))

        word_count = len(preprocessing.preprocess(sentence, stemming=False, stop=False))
        
        for term in set(preprocessed):
            positions = [n for n,item in enumerate(preprocessed) if item==term]
            self.temp[term] = self.temp.get(term, {
                'term': term,
                'doc_count': 0,
                'movies': dict()
            })
            self.temp[term]['doc_count'] += 1
            self.temp[term]['movies'][movie_id] = self.temp[term]['movies'].get(movie_id, {'_id': movie_id, 'doc_count': 0, 'sentences': list()})
            self.temp[term]['movies'][movie_id]['doc_count'] += 1
            self.temp[term]['movies'][movie_id]['sentences'].append({
                        '_id': doc_id,
                        'len': word_count,
                        'pos': positions
                    })
                    
    def __regularize_dict(self):
        for value in self.temp.values():
            value['movies'] = list(value['movies'].values())

    def __save_pickle(self, name):
        with open(name + '.pickle', 'wb') as handle:
            pickle.dump(list(self.temp.values()), handle, protocol=pickle.HIGHEST_PROTOCOL)
        self.temp.clear()

def run_with_arguments(stem, stop, start):
    indexGen = IndexGenerator(activate_stop=stop, activate_stemming=stem, start_index=start)
    indexGen.run_indexing()

parser = argparse.ArgumentParser(description='Inverted Index Generator')
parser.add_argument('--stemming', nargs="?", type=str, default='True', help='Activate stemming')
parser.add_argument('--remove_stopwords', nargs="?", type=str, default='False', help='Remove stopwords')
parser.add_argument('--start', nargs="?", type=int, default=0, help='Start batch index')
args = parser.parse_args()

run_with_arguments(eval(args.stemming), eval(args.remove_stopwords), args.start)

