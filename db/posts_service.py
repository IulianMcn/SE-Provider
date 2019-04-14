from bson.objectid import ObjectId


class PostsService:
    posts_collection = None

    def __init__(self, mongo_db):
        self.posts_collection = mongo_db.posts

    def get_post(self, document_id):
        return self.posts_collection.find_one({
            '_id': document_id
        })

    def get_posts_in(self, documents_ids):
        return self.posts_collection.find({
            '_id': {'$in': list(map(lambda x: x, documents_ids))}
        })

    def get_posts_cursor(self):
        return self.posts_collection.find()

    def get_total_nr_of_posts(self):
        return self.posts_collection.count()

    def get_posts_content_len(self, documents_ids):
        result = dict()
        posts = self.get_posts_in(documents_ids)

        for post in posts:
            result[post['_id']] = post['content_len']

        return result

    def insert_post(self, dbEntry):
        self.posts_collection.insert_one(dbEntry)

        return dbEntry
    
    def insert_posts(self,dbEntries):
        self.posts_collection.insert_many(dbEntries)
