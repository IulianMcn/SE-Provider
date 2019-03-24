from bson.objectid import ObjectId


class IndexService:
    index_collection=None

    def __init__(self, mongo_db):
        self.index_collection = mongo_db.index

    def get_index(self, word):
        word_index = self.index_collection.find_one({
            '_id': word
        })

        return word_index

    def upsert_index(self, entry):
        self.index_collection.update_one({
            '_id': entry.word
        },
            {
            '$push': {'documents': {
                '_id': ObjectId(entry.document_id),
                'where': entry.idx,
                'position': entry.positional_index
            }}
        },
            upsert=True
        )
