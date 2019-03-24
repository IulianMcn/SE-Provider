from rulesProviders.string_split_rule_provider import StringSplitRuleProvider
from bson.objectid import ObjectId
from rulesProviders.string_stemming_rule_provider import StringStemmingRuleProvider
from rulesProviders.fields_indexing_rule_provider import FiledsIndexingRuleProvider
from utils.dictionary_helper import DictionaryHelper

class DocumentParser:

    def parse_document(self, document):

        inverse_indexes_dicts = []

        for field in FiledsIndexingRuleProvider.filetsToIndex:
            inverse_indexes_dicts.append(DocumentParser.compute_inverse_index_on_column(
                document, field))

        aggregation = DictionaryHelper.aggregate_dicts(inverse_indexes_dicts)

        return aggregation

    @staticmethod
    def split_string(str):
        return StringSplitRuleProvider.alpha_numeric_splitting(str)

    @staticmethod
    def compute_inverse_index_on_column(document, column):
        words_to_index = DocumentParser.split_string(document[column])
        words_count = dict()

        for index, word_to_index_raw in enumerate(words_to_index):
            if(len(word_to_index_raw) <= 2):
                continue

            terms_to_index = StringStemmingRuleProvider.snowball(
                word_to_index_raw.lower())

            for term_to_index in terms_to_index:
                if term_to_index in words_count:
                    words_count[term_to_index].append(index)
                else:
                    words_count[term_to_index] = [index]

        return words_count
