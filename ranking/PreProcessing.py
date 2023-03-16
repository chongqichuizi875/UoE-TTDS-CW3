import nltk
import ssl
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
from nltk.tokenize import TweetTokenizer

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

nltk.download('stopwords')
nltk.download('punkt')

stop_words = set(stopwords.words('english'))


def preprocessing(text, stemming=True, stopping=True):
    """This functioon will preprocess the text passed in, remove the stopping words of English, stemming words,
        and return a list of tokens after processing. """
    token_list = TweetTokenizer().tokenize(text)
    filtered_list = [term.lower() for term in token_list if term not in stop_words or not stopping]
    if stemming:
        filtered_list = [PorterStemmer().stem(token) for token in filtered_list]
    return list(filter(lambda x: x.isalnum(), filtered_list))
