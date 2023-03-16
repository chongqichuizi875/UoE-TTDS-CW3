import sys
from pathlib import Path
import math
import time
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db import MongoDB
from data_collection.preprocessing import Preprocessing


mongoDB = MongoDB.MongoDB()
preprocessing = Preprocessing()
avg_page_len = 868
N = 127332


def get_freq_from_index(term):
    freq_dict = {}
    page_cursor = mongoDB.get_indexed_pages_by_token(token=term)
    for page in page_cursor:
        occurred_page_list = page['page']
        for occurrence in occurred_page_list:
            # only store the page ids where the term occurrences are greater than 3
            if len(list(occurrence['pos'])) > 3:
                freq_dict[occurrence['_id']] = len(list(occurrence['pos']))
    return freq_dict


def calculate_tfidf_weight_of_term_in_page(term, pageId):
    page_cursor = mongoDB.get_indexed_pages_by_token(token="sunday")
    # page_dict = mongoDB.get_page_by_page_id(pageId)
    # page_len = len(PreProcessing.preprocessing(page_dict['text']))
    df = 0
    for page in page_cursor:
        df += page['page_count']
    # N = mongoDB.pages_count
    idf = math.log((N/df), 10)
    freq_dict = get_freq_from_index(term)
    if dict(freq_dict).__contains__(pageId):
        tf = freq_dict[pageId]
    else:
        tf = 0
    weight = (1 + math.log(tf, 10)) * idf
    return weight

# TF(t,d): number of times term t appeared in document d / the size of the document
# DF(t): number of documents term t appeared in.
# IDF: log((total number of docs / DF), 10)
# weight = (1 + log(TF, 10)) * IDF
def calculate_sorted_tfidf_score_of_query(query_text):
    """
    :param query_text: the contents of query
    :return: the calculated score list for each doc page.
    """
    score_map = {}
    terms = preprocessing.wiki_tokenize(query_text)
    print("terms::" + ','.join(terms))
    occurred_page_id_list = []
    for term in terms:
        freq_dict = dict(get_freq_from_index(term))
        # print(freq_dict)
        occurred_page_id_list.extend(freq_dict.keys())

    occurred_page_id_list = list(set(occurred_page_id_list))
    # print(len(occurred_page_id_list))
    for i in range(len(occurred_page_id_list)):
        score = 0
        for term in terms:
            score += calculate_tfidf_weight_of_term_in_page(term, occurred_page_id_list[i])

        score_map[occurred_page_id_list[i]] = float(format(score, '.4f'))

    sorted_score_map = dict(sorted(score_map.items(), key=lambda i: i[1], reverse=True))
    return sorted_score_map

# tfidf_results = calculate_sorted_tfidf_score_of_query("sunday")
# # for page_id, score in zip(results.keys(), results.values()):
# print("tfidf_results:::")
# print(tfidf_results)
#
# the_first_tfidf_returned_page = mongoDB.get_pages_by_list_of_ids(ids=list(tfidf_results.keys()))[0]
# the_first_tfidf_returned_page_title = the_first_tfidf_returned_page['title']
# the_first_tfidf_returned_page_content = the_first_tfidf_returned_page['text']
# print("title::::tfidf")
# print(the_first_tfidf_returned_page_title)
# print("text::::tfidf")
# print(the_first_tfidf_returned_page_content)


def get_tfidf_results(query_text):
    infos_list = []
    results_dict = calculate_sorted_tfidf_score_of_query(query_text)
    pages_returned = mongoDB.get_pages_by_list_of_ids(ids=list(results_dict.keys()))
    for page in pages_returned:
        infos_list.append({'title': page['title'], 'introduce': page['text']})
    return infos_list

# IDF: log((total number of docs / DF), 10)
# R(t, d) = tf*(k1 + 1) / (tf+K)
# K = k1 * (1 - b + b*(page_len/avg_page_len))
# k1 = 2, b=0.75
# BM25(t, d) = IDF * R(t, d)
# https://blog.csdn.net/Tink1995/article/details/104745144
k1 = 2
b = 0.75
def calculate_bm25_weight_of_term_in_page(term, page_id):
    page_cursor = mongoDB.get_indexed_pages_by_token(token=term)
    page_dict = mongoDB.get_page_by_page_id(page_id)
    df = 0
    for page in page_cursor:
        df += page['page_count']
    # N = mongoDB.pages_count
    idf = math.log(((N+0.5)/(df+0.5)), 10)
    freq_dict = get_freq_from_index(term)
    if dict(freq_dict).__contains__(page_id) and page_dict:
        tf = freq_dict[page_id]
        # page_len = len(MongoDB.tokenize(page_dict['text']))
        page_len = len(preprocessing.wiki_tokenize(page_dict['text'], lower=False, stop=False, stemming=False, len_filter=False))
        K = k1 * (1 - b + (b * (page_len / avg_page_len)))
        relevance = (tf * (k1 + 1)) / (tf + K)
        weight = idf * relevance
    else:
        weight = 0
    return weight


def calculate_sorted_bm25_score_of_query(query_text):
    """
        :param query_text: the contents of query
        :return: the calculated score list for each doc page.
    """
    score_map = {}
    terms = preprocessing.wiki_tokenize(query_text)
    occurred_page_id_list = []
    for term in terms:
        freq_dict = dict(get_freq_from_index(term))
        # print(freq_dict)
        occurred_page_id_list.extend(freq_dict.keys())

    occurred_page_id_list = list(set(occurred_page_id_list))
    # print(len(occurred_page_id_list))
    for i in range(len(occurred_page_id_list)):
        score = 0
        for term in terms:
            score += calculate_bm25_weight_of_term_in_page(term, occurred_page_id_list[i])

        score_map[occurred_page_id_list[i]] = score
    sorted_score_map = dict(sorted(score_map.items(), key=lambda i: i[1], reverse=True))
    return sorted_score_map

# ts = time.time()
# print(ts)
# bm25_results = calculate_sorted_bm25_score_of_query("sunday")
# ts2 = time.time()
# print(ts2)
# print("bm25_results:::")
# print(bm25_results)
# the_first_bm25_returned_page = mongoDB.get_pages_by_list_of_ids(ids=list(bm25_results.keys()))[0]
# the_first_bm25_returned_page_title = the_first_bm25_returned_page['title']
# the_first_bm25_returned_page_content = the_first_bm25_returned_page['text']
# print("title::::bm25")
# print(the_first_bm25_returned_page_title)

# print("text::::bm25")
# print(the_first_bm25_returned_page_content)
