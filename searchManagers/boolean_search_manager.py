from rulesProviders.string_stemming_rule_provider import StringStemmingRuleProvider
from rulesProviders.string_split_rule_provider import StringSplitRuleProvider
from db.index_service import IndexService

class BooleanSearchManager:

    def __init__(self, mongos_client):
        self.indexer_db = mongos_client['Indexer-Database']

    def process_query(self, query):
        words_to_compute = StringSplitRuleProvider.alpha_numeric_splitting(
            query)
        result = []

        for word_to_compute in words_to_compute:
            word_to_query = StringStemmingRuleProvider.snowball(
                word_to_compute.lower())
            documents = self.process_word(word_to_query[0])

            if(len(result) == 0):
                result.extend(documents)
            else:
                result = [value for value in result if value in documents] if len(result) > len(
                    documents) else [value for value in documents if value in result]

        return result

    def process_word(self, word):
        index = self.get_index(word)

        if(index == None):
            return []

        documents = set(list(map(lambda x: x['_id'], index['documents'])))

        return documents

    def get_index(self, word):
        index_collection = self.indexer_db.index
        word_index = index_collection.find_one({
            '_id': word
        })

        return word_index
