import praw
import pprint

from db.search_engine_variables_service import SearchEngineVariablesService
from db.posts_service import PostsService

from abc import ABCMeta, abstractmethod


class DataProvider(object):
    __metaclass__ = ABCMeta

    variables_service = None
    posts_service = None

    @abstractmethod
    def __init__(self, mongo_db):
        self.variables_service = SearchEngineVariablesService(mongo_db)
        self.posts_service = PostsService(mongo_db)

    @abstractmethod
    def provide_data(self): pass
