from bson.objectid import ObjectId


class IndexService:
    index_collection = None

    def __init__(self, mongo_db):
        self.index_collection = mongo_db.index

    def get_index(self, word):
        word_index = self.index_collection.find_one({
            '_id': word
        })

        return word_index

    def get_index_in(self, terms_to_compute):
        word_index = self.index_collection.find({
            '_id': {'$in': terms_to_compute}
        })

        return word_index

    def upsert_many_in_index(self, entries):

        for entry in entries:

            self.index_collection.update_many(
                filter={
                    "_id": {"$eq": entry['_id']}},
                update={
                    "$push": {'documents': {'$each': entry['documents']}}
                },
                upsert=True
            )

    def upsert_index(self, entry):
        self.index_collection.update_one({
            '_id': entry['word']
        },
            {
            '$push': {'documents': {
                '_id': ObjectId(entry['document_id']),
                'freq': entry['freq'],
                'positions': entry['positions']
            }}
        },
            upsert=True
        )
