from rulesProviders.string_stemming_rule_provider import StringStemmingRuleProvider
from rulesProviders.string_split_rule_provider import StringSplitRuleProvider
from db.index_service import IndexService
from searchManagers.boolean_search_manager import BooleanSearchManager
from searchManagers.search_manager import SearchManager
from db.search_engine_variables_service import SearchEngineVariablesService
from db.posts_service import PostsService
from utils.custom_merge_sort import merge_k_sorted_lists
import math
import bisect


class PPM_search_manager(SearchManager):
    default_score = 1
    avg_doc_len = None
    collection_size = None
    dist_weight = 1
    k1 = 1
    b = 0.5

    # TODO: move this from here.
    def reverse_proximity(self, x):
        return 1/(self.dist_weight*x + 1)

    # TODO: move this from here.
    def proximity_kernel_function(self, x):
        return self.reverse_proximity(x)

    # TODO: move this from here.
    def get_terms_weight_and_position_tuple(self, terms, terms_index):
        N = self.collection_size
        result = dict()

        for i, term_data in enumerate(terms_index):
            term = term_data['_id']
            dfi = len(term_data['documents'])
            wi = math.log((N-dfi + 0.5)/(dfi+0.5))
            result[term] = (i, wi)

        return result

    def compute_K(self, document_len):
        return self.k1*((1-self.b) + (self.b*document_len/self.avg_doc_len))

    def __init__(self, mongo_db):
        super(PPM_search_manager, self).__init__(mongo_db)
        self.boolean_search_manager = BooleanSearchManager(mongo_db)
        search_engine_variables_service = SearchEngineVariablesService(
            mongo_db)
        self.avg_doc_len = search_engine_variables_service.get_avarages_variables()[
            'content_avg_len']
        self.posts_service = PostsService(mongo_db)
        self.collection_size = self.posts_service.get_total_nr_of_posts()

    # TODO: break this up, too long.
    def process_query(self, query):
        terms_to_compute = self.get_terms(query)
        terms_index = list(self.index_service.get_index_in(
            list(terms_to_compute.keys())))
        terms_index_and_weight = self.get_terms_weight_and_position_tuple(
            terms_to_compute, terms_index)
        documents_to_score = self.get_documents_data_from_index(terms_index)
        documents_lens = self.get_documents_len(terms_index)
        result=[]

        for doc_id, doc_to_score in documents_to_score.items():
            fti = dict()
            document_len = documents_lens.get(doc_id)

            for word, positions in doc_to_score.items():
                fti[word] = self.default_score

            terms_relative_pos = merge_k_sorted_lists(
                list(self.get_positional_array_map(doc_to_score)), lambda x: x[0])

            for index, term_relative_pos in enumerate(terms_relative_pos):
                min_distance_element_index = 0

                if index == 0:
                    min_distance_element_index = 0 if len(terms_relative_pos)==1 else 1
                elif (index == (len(terms_relative_pos)-1)):
                    min_distance_element_index = (len(terms_relative_pos)-2)
                else:
                    is_left = abs(terms_relative_pos[index-1][0] - terms_relative_pos[index][0]) <= abs(
                        terms_relative_pos[index+1][0] > terms_relative_pos[index][0])
                    min_distance_element_index = (
                        index-1) if is_left else (index+1)

                doc_offset = terms_relative_pos[index][0] - \
                    terms_relative_pos[min_distance_element_index][0]
                query_offset = terms_index_and_weight[terms_relative_pos[index][1]][0] - \
                    terms_index_and_weight[terms_relative_pos[min_distance_element_index][1]][0]

                dist = abs(doc_offset-query_offset)

                w1 = terms_index_and_weight[terms_relative_pos[index][1]][1]
                w2 = terms_index_and_weight[terms_relative_pos[min_distance_element_index][1]][1]

                fti[term_relative_pos[1]] += w1 * \
                    w2 * self.reverse_proximity(dist)

            K = self.compute_K(document_len)

            document_score=0

            for term,ft in fti.items():
                document_score+=terms_index_and_weight[term][1]*ft/(K+ft)

            bisect.insort(result,(document_score,doc_id),lo=len(documents_to_score),hi=0)

        return result

    # TODO: move this from here.
    def get_positional_array_map(self, words_to_positions):
        result = []
        for word, positions in words_to_positions.items():
            result.append(list(map(lambda x: (x, word), positions)))

        return result

    # TODO: move this from here.
    def get_documents_data_from_index(self, terms_index):
        documents_data = dict()

        for term_index in terms_index:
            word = term_index['_id']
            docs = term_index['documents']

            for doc in docs:
                doc_id = doc['_id']
                if doc['_id'] not in documents_data:
                    documents_data[doc_id] = dict()

                if word not in documents_data[doc_id]:
                    documents_data[doc_id][word] = doc['pos']

        return documents_data
