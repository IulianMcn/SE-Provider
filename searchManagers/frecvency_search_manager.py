from rulesProviders.string_stemming_rule_provider import StringStemmingRuleProvider
from rulesProviders.string_split_rule_provider import StringSplitRuleProvider
from db.index_service import IndexService
from searchManagers.boolean_search_manager import BooleanSearchManager
from searchManagers.search_manager import SearchManager
from db.search_engine_variables_service import SearchEngineVariablesService
from db.posts_service import PostsService
import math


class FrecvencySearchManager(SearchManager):
    k = 0.5
    avg_doc_len = None
    collection_size = None

    def __init__(self, mongo_db):
        super(FrecvencySearchManager, self).__init__(mongo_db)
        self.boolean_search_manager = BooleanSearchManager(mongo_db)
        search_engine_variables_service = SearchEngineVariablesService(
            mongo_db)
        self.avg_doc_len = search_engine_variables_service.get_avarages_variables()[
            'content_avg_len']
        self.posts_service = PostsService(mongo_db)
        self.collection_size = self.posts_service.get_total_nr_of_posts()

    def process_query(self, query):
        terms_to_compute = self.get_terms(query)
        nr_terms_to_compute = len(terms_to_compute)

        terms_index = list(self.index_service.get_index_in(
            list(terms_to_compute.keys())))
        documents_lens = self.get_documents_len(terms_index)
        documents_scores_accumulator = dict()

        for term_index in terms_index:
            word = term_index['_id']
            docs = term_index['documents']
            nr_docs = len(docs)

            for doc in docs:
                tf = self.compute_term_frecvency(
                    doc['freq'], documents_lens.get(doc['_id']))

                idf = self.compute_inverse_document_frequency(nr_docs)

                if doc['_id'] not in documents_scores_accumulator:
                    documents_scores_accumulator[doc['_id']] = 0
                documents_scores_accumulator[doc['_id']
                                             ] += (terms_to_compute[word] / nr_terms_to_compute) * tf * idf

        documents_scores = list(documents_scores_accumulator.items())
        documents_scores.sort(key=lambda x: x[1], reverse=True)

        return list(map(lambda x: x[0], documents_scores))

    def compute_term_frecvency(self, tf, doc_len):
        return tf/(tf+self.k*(doc_len/self.avg_doc_len))

    def compute_inverse_document_frequency(self, dfw):
        return math.log(self.collection_size/dfw)

    def get_documents_len(self, terms_index):
        docs_ids = self.boolean_search_manager._compute_OR(terms_index)
        return self.posts_service.get_posts_content_len(docs_ids)
