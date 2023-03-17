import bz2
import multiprocessing
from gensim import utils

from gensim.corpora.wikicorpus import WikiCorpus, extract_pages, IGNORED_NAMESPACES, init_to_ignore_interrupt, logger, PicklingError, filter_wiki
from gensim.corpora import Dictionary

from data_collection.preprocessing import Preprocessing

class MyWikiCorpus(WikiCorpus):
    def __init__(self, fname, dictionary={}, lower=True):
        super().__init__(fname, dictionary=dictionary, lower = lower)
        self.metadata = True
        self.wiki_tokenize = Preprocessing().wiki_tokenize

    def get_texts(self):
        articles, articles_all = 0, 0
        positions, positions_all = 0, 0

        texts = (
            texts for texts in extract_pages(bz2.BZ2File(self.fname), self.filter_namespaces, self.filter_articles)
        )
        pool = multiprocessing.Pool(self.processes, init_to_ignore_interrupt)

        try:
            # process the corpus in smaller chunks of docs, because multiprocessing.Pool
            # is dumb and would load the entire input into RAM at once...
            for group in utils.chunkize(texts, chunksize=10 * self.processes, maxsize=1):
                for (tokens, (pageid, title, text)) in pool.imap(self.process_article, group):
                    articles_all += 1
                    positions_all += len(tokens)
                    # article redirects and short stubs are pruned here
                    if len(tokens) < self.article_min_tokens or \
                        any(title.startswith(ignore + ':') for ignore in IGNORED_NAMESPACES):
                        continue
                    articles += 1
                    positions += len(tokens)
                    yield (tokens, (pageid, title, text))

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

    def process_article(self, texts):
        title, text, pageid = texts
        text = filter_wiki(text)
        tokens = self.wiki_tokenize(text, self.token_min_len, self.token_max_len, self.lower)

        return  (tokens, (pageid, title, text))

    """ process single page
    """
    def process_wiki_page(self, f):
        texts = next(extract_pages(bz2.BZ2File(self.fname), self.filter_namespaces, self.filter_articles))
        (tokens, (pageid, title, text)) = self.process_article(texts)
        if len(tokens) < self.article_min_tokens or \
            any(title.startswith(ignore + ':') for ignore in IGNORED_NAMESPACES):
            return (tokens, (pageid, title, text))
        else:
            return None

    def save_Dictionary(self, f_path):
        Dictionary((params[0] for params in self.get_tokens())).save_as_text(f_path+"_wordids.txt.bz2")
    def load_Dictionary(self, f_path):
        return Dictionary.load_from_text(f_path+"_wordids.txt.bz2")