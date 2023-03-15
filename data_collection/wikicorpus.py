import bz2
import logging
import multiprocessing
import signal
from gensim import utils
import nltk
import ssl
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords

from nltk.tokenize import TweetTokenizer
from gensim.corpora.wikicorpus import WikiCorpus, extract_pages, tokenize, IGNORED_NAMESPACES, TOKEN_MAX_LEN, TOKEN_MIN_LEN, init_to_ignore_interrupt, logger, PicklingError, filter_wiki

from gensim.corpora import Dictionary

class MyWikiCorpus(WikiCorpus):
    def __init__(self, fname, dictionary={}, lower=True):
        super().__init__(fname, dictionary=dictionary, lower = lower)
        self.metadata = True
        try:
            _create_unverified_https_context = ssl._create_unverified_context
        except AttributeError:
            pass
        else:
            ssl._create_default_https_context = _create_unverified_https_context

        nltk.download('stopwords')
        nltk.download('punkt')
        self.stop_words = set(stopwords.words('english'))
    def get_texts(self):
        articles, articles_all = 0, 0
        positions, positions_all = 0, 0

        tokenization_params = (self.tokenizer_func, self.token_min_len, self.token_max_len, self.lower)
        texts = (
            (text, title, pageid, tokenization_params)
            for title, text, pageid
            in extract_pages(bz2.BZ2File(self.fname), self.filter_namespaces, self.filter_articles)
        )
        pool = multiprocessing.Pool(self.processes, init_to_ignore_interrupt)

        try:
            # process the corpus in smaller chunks of docs, because multiprocessing.Pool
            # is dumb and would load the entire input into RAM at once...
            for group in utils.chunkize(texts, chunksize=10 * self.processes, maxsize=1):
                for tokens, title, pageid, text in pool.imap(self.process_article, group):
                    articles_all += 1
                    positions_all += len(tokens)
                    # article redirects and short stubs are pruned here
                    if len(tokens) < self.article_min_tokens or \
                            any(title.startswith(ignore + ':') for ignore in IGNORED_NAMESPACES):
                        continue
                    articles += 1
                    positions += len(tokens)
                    if self.metadata:
                        yield (tokens, (pageid, title, text))
                    else:
                        yield tokens

        except KeyboardInterrupt:
            logger.warning(
                "user terminated iteration over Wikipedia corpus after %i documents with %i positions "
                "(total %i articles, %i positions before pruning articles shorter than %i words)",
                articles, positions, articles_all, positions_all, self.article_min_tokens
            )
        except PicklingError as exc:
            raise PicklingError(
                f'Can not send filtering function {self.filter_articles} to multiprocessing, '
                'make sure the function can be pickled.'
            ) from exc
        else:
            logger.info(
                "finished iterating over Wikipedia corpus of %i documents with %i positions "
                "(total %i articles, %i positions before pruning articles shorter than %i words)",
                articles, positions, articles_all, positions_all, self.article_min_tokens
            )
            self.length = articles  # cache corpus length
        finally:
            pool.terminate()


    def get_tokens(self):
        for tokens, metadata in self.get_texts():
            yield tokens, metadata[0] # tokens, pageid

    def process_article(self, args):
        tokenizer_func, token_min_len, token_max_len, lower = args[-1]
        args = args[:-1]
        text, title, pageid = args
        text = filter_wiki(text)
        result = self.wiki_tokenize(text, token_min_len, token_max_len, lower)
        return result, title, pageid, text

    def wiki_tokenize(self, content, token_min_len=TOKEN_MIN_LEN, token_max_len=TOKEN_MAX_LEN, lower=True, stop=True, stemming=True):
        gen = (token for token in utils.tokenize(content, lower=lower, errors='ignore')
        if token_min_len <= len(token) <= token_max_len and not token.startswith('_'))

        if stop:
            gen = self.remove_stop_words(gen)

        if stemming:
            gen = self.stemming_tokens(gen)
        return list(gen)

    def remove_stop_words(self,gen):
        return (token for token in gen if token not in self.stop_words)

    def stemming_tokens(self, gen):
        return (PorterStemmer().stem(token) for token in gen)

    def save_Dictionary(self, f_path):
        Dictionary((params[0] for params in self.get_tokens())).save_as_text(f_path+"_wordids.txt.bz2")
    def load_Dictionary(self, f_path):
        return Dictionary.load_from_text(f_path+"_wordids.txt.bz2")
from gensim.scripts import make_wiki