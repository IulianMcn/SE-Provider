from abc import ABCMeta, abstractmethod

class SearchManager:
    __metaclass__ = ABCMeta

    def __init__(self, mongos_client):
        self.indexer_db = mongos_client['Indexer-Database']
        pass

    @abstractmethod
    def process_query(self, query): raise NotImplementedError

    def get_index(self, word):
        index_collection = self.indexer_db.index
        word_index = index_collection.find_one({
            '_id': word
        })

        return word_index
