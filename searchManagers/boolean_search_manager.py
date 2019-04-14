
from db.index_service import IndexService
from searchManagers.search_manager import SearchManager


class BooleanSearchManager(SearchManager):
    index_service = None

    def __init__(self, mongo_db):
        super(BooleanSearchManager, self).__init__(mongo_db)

    def _OR(self, terms_to_compute):
        terms_index = self.index_service.get_index_in(terms_to_compute)
        return self._compute_OR(terms_index)

    def _compute_OR(self, terms_index):
        documents_ids = set()

        for term_index in terms_index:
            documents_ids.update(
                map(lambda x: x['_id'], term_index['documents']))

        return documents_ids

    def _AND(self, terms_to_compute):
        terms_index = self.index_service.get_index_in(terms_to_compute)

        if(terms_index == None):
            return []

        documents_ids = set(
            map(lambda x: x['_id'], terms_index[0]['documents']))

        iter_terms = iter(terms_index)
        next(iter_terms)

        for term_index in iter_terms:
            documents_ids = documents_ids.intersection(
                set(map(lambda x: x['_id'], term_index['documents'])))

        return documents_ids

    def process_query(self, query):
        return self._AND(list(self.get_terms(query).keys()))
