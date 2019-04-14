from bson.objectid import ObjectId
import hashlib


class DirectMapService:
    direct_map_collection = None

    def __init__(self, mongo_db):
        self.direct_map_collection = mongo_db.direct_map

    def direct_map(self, entries):

        for entry in entries:

            docs = list()

            for key, value in entry['docs'].items():
                docs.append({
                    "_id": key,
                    "terms": value
                })

            self.direct_map_collection.update_many(
                filter={"_id": {"$eq": entry["_id"]}},
                update={'$push': {'docs': {'$each': docs}}},
                upsert=True
            )

    def get_map(self, id):
        return self.direct_map_collection.find_one({
            '_id': str(id)
        })
