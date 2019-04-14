from mpi4py import MPI

import threading
from dataCrawl.reddit_provider import RedditProvider
# Warning-PY: maybe Singleton?


class MasterJob(threading.Thread):
    data_provider = None

    def __init__(self, mongo_db, manager):
        threading.Thread.__init__(self)
        self.data_provider = RedditProvider(mongo_db)
        self.manager = manager

    def run(self):
        iterator = self.data_provider.provide_data()

        for submission in iterator:
            self.manager.add_submission(submission)
