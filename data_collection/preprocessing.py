from nltk.stem import PorterStemmer
from nltk.corpus import stopwords

from gensim.utils import tokenize
from gensim.corpora.wikicorpus import TOKEN_MAX_LEN, TOKEN_MIN_LEN


class Preprocessing():
    def __init__(self) -> None:
        self.stop_words = set(stopwords.words('english'))

    def wiki_tokenize(self, content, token_min_len=TOKEN_MIN_LEN, token_max_len=TOKEN_MAX_LEN, lower=True, stop=True,
                      stemming=True, len_filter=True):
        if len_filter:
            gen = (token for token in tokenize(content, lower=lower, errors='ignore')
                   if token_min_len <= len(token) <= token_max_len and not token.startswith('_'))
        else:
            gen = (token for token in tokenize(content, lower=lower, errors='ignore')
                   if not token.startswith('_'))

        if stop:
            gen = self.remove_stop_words(gen)

        if stemming:
            gen = self.stemming_tokens(gen)
        return list(gen)

    def remove_stop_words(self, gen):
        return (token for token in gen if token not in self.stop_words)

    def stemming_tokens(self, gen):
        return (PorterStemmer().stem(token) for token in gen)
