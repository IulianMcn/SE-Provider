from abc import ABCMeta, abstractmethod
from db.index_service import IndexService
from rulesProviders.string_stemming_rule_provider import StringStemmingRuleProvider
from rulesProviders.string_split_rule_provider import StringSplitRuleProvider


class SearchManager:
    __metaclass__ = ABCMeta

    def __init__(self, mongo_db):
        self.index_service = IndexService(mongo_db)

    @abstractmethod
    def process_query(self, query): raise NotImplementedError

    def get_terms(self, query):
        result = dict()

        words_to_compute = StringSplitRuleProvider.alpha_numeric_splitting(
            query)
        words_to_compute = map(
            lambda x: StringStemmingRuleProvider.snowball(x.lower()), words_to_compute)

        terms_to_compute = list()

        for words in words_to_compute:
            terms_to_compute.extend(words)

        for term_to_compute in terms_to_compute:
            if term_to_compute not in result:
                result[term_to_compute]=0

            result[term_to_compute]=1
                

        return result
