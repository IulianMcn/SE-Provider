class PostsService:
    posts_collection = None

    def __init__(self, mongo_db):
        self.posts_collection = mongo_db.posts

    def get_post(self, document_id):
        return self.posts_collection.find_one({
            '_id': document_id
        })

    def get_posts_cursor(self):
        return self.posts_collection.find()

    def get_total_nr_of_posts(self):
        return self.posts_collection.count()

    def insert_post(self, dbEntry):
        self.posts_collection.insert_one(dbEntry)

        return dbEntry
