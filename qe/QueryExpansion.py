import nltk
from nltk.corpus import wordnet as wn
from data_collection.preprocessing import Preprocessing
from nltk import pos_tag
from nltk.corpus import stopwords
import re

class QueryExpansion():
    def __init__(self) -> None:
        self.pos_tag_map = {
            'NN': [ wn.NOUN ],
            'JJ': [ wn.ADJ, wn.ADJ_SAT ],
            'RB': [ wn.ADV ],
            'VB': [ wn.VERB ]
        }
        self.stopwords = set(stopwords.words('english'))
        self.preprocessor = Preprocessing()
    
    def download_nltk_pakages(self):
        nltk.download('wordnet')
        nltk.download('punkt')
        nltk.download('averaged_perceptron_tagger')
        nltk.download('omw-1.4')
    
    def remove_stopwords(self, tokens):
        results = []
        for token in tokens:
            if token[0] not in self.stopwords:
                results.append(token)
        return results

    def pos_tag_converter(self, nltk_pos_tag):
        root_tag = nltk_pos_tag[0:2]
        try:
            self.pos_tag_map[root_tag]
            return self.pos_tag_map[root_tag]
        except KeyError:
            return ''
        
    def get_synsets(self, tokens):
        synsets = []
        for token in tokens:
            wn_pos_tag = self.pos_tag_converter(token[1])
            if wn_pos_tag == '':
                continue
            else:
                synsets.append(wn.synsets(token[0], wn_pos_tag))
        return synsets
    
    def get_tokens_from_synsets(self, synsets):
        tokens = {}
        for synset in synsets:
            for s in synset:
                if s.name() in tokens:
                    tokens[s.name().split('.')[0]] += 1
                else:
                    tokens[s.name().split('.')[0]] = 1
        return tokens

    def get_hypernyms(self, synsets):
        hypernyms = []
        for synset in synsets:
            for s in synset:
                hypernyms.append(s.hypernyms())
        return hypernyms
    
    def get_tokens_from_hypernyms(self, synsets):
        tokens = {}
        for synset in synsets:
            for ss in synset:
                if ss.name().split('.')[0] in tokens:
                    tokens[(ss.name().split('.')[0])] += 1
                else:
                    tokens[(ss.name().split('.')[0])] = 1
        return tokens
    
    def underscore_replacer(self,tokens):
        new_tokens = {}
        for key in tokens.keys():
            mod_key = re.sub(r'_', ' ', key)
            new_tokens[mod_key] = tokens[key]
        return new_tokens
    
    def generate_tokens(self, query):
        tokens = self.preprocessor.wiki_tokenize(query,stemming=False,stop=False)
        tokens_tag = pos_tag(tokens)
        tokens_tag = self.remove_stopwords(tokens_tag)
        synsets = self.get_synsets(tokens_tag)
        synonyms = self.get_tokens_from_synsets(synsets)
        synonyms = self.underscore_replacer(synonyms)
        hypernyms = self.get_hypernyms(synsets)
        hypernyms = self.get_tokens_from_hypernyms(hypernyms)
        hypernyms = self.underscore_replacer(hypernyms)
        new_tokens = {**synonyms, **hypernyms}
        print(new_tokens)
        new_tokens = self.preprocessor.stemming_tokens(list(new_tokens.keys()))
        return list(new_tokens)

    
    
