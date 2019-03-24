from document_parser import DocumentParser

from db.posts_service import PostsService
from db.index_service import IndexService
from bson.objectid import ObjectId


class Indexer:

    def __init__(self, mongo_db):
        self.document_parser = DocumentParser()
        self.posts_service = PostsService(mongo_db)
        self.index_service = IndexService(mongo_db)

    def index_all_current_data(self):

        posts_to_compute = self.posts_service.get_posts_cursor()

        for post_to_compute in posts_to_compute:
            aggregation = self.document_parser.parse_document(post_to_compute)
            for key, value in aggregation.items():
                for idx, positional_indexes in enumerate(value):
                    for positional_index in positional_indexes:
                        self.index_service.upsert_index({
                            'word': key,
                            'document_id': ObjectId(post_to_compute['_id']),
                            'idx': idx,
                            'positional_index': positional_index
                        })
