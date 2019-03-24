class SubredditsService:
    subreddits_collection = None

    def __init__(self, mongo_db):
        self.subreddits_collection = mongo_db.subreddits

    def check_if_exists(self, subreddit_name):
        entry = self.subreddits_collection.find({
            'subreddit_name': subreddit_name
        })

        return entry is None

    def insert_subreddit(self, subreddit_name):
        self.subreddits_collection.insert({
            'subreddit_name': subreddit_name
        })

