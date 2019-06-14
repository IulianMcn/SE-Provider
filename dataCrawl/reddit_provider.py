import time
import praw
from dataCrawl.data_provider import DataProvider

from db.search_engine_variables_service import SearchEngineVariablesService
from db.subreddits_service import SubredditsService
from db.posts_service import PostsService


class RedditProvider(DataProvider):

    number_of_subreddits_to_parse = None
    limit_of_reddits_to_parse = None
    variables_service = None
    posts_service = None
    subreddits_service = None
    include_comments = False

    def __init__(self, mongo_db, number_of_subreddits_to_parse=30, limit_of_reddits_to_parse=100):
        super(RedditProvider, self).__init__(mongo_db)
        self.subreddits_service = SubredditsService(mongo_db)
        self.client = praw.Reddit(client_id='-GYWVXI4AgNJgw', cache_timeout=0,
                                  client_secret='8TJSFXgWgWO_dAKwCr2pPB5EUAs',
                                  user_agent='my user agent?'  
                                  # Warning-PY: wtf is this?
                                  )  # Warning-PY: use a praw.ini file when moving to github
        self.number_of_subreddits_to_parse = number_of_subreddits_to_parse
        self.limit_of_reddits_to_parse = limit_of_reddits_to_parse

    def provide_data(self):
        parsed_subreddits = 0
        avg_variables_dict = self.variables_service.get_avarages_variables()
        total_nr_of_documents = self.posts_service.get_total_nr_of_posts()
        posts_to_insert = []

        while(parsed_subreddits != self.number_of_subreddits_to_parse):
            subreddit = self.client.random_subreddit()
            print("Subreddit " + subreddit.display_name)
            if(self.subreddits_service.check_if_exists(subreddit.display_name)):
                continue
            else:
                parsed_subreddits += 1
                self.subreddits_service.insert_subreddit(
                    subreddit.display_name)

            for submission in list(subreddit.hot(limit=self.limit_of_reddits_to_parse)):
                if(self.include_comments):
                    submission._fetch()
                dbEntry = RedditProvider.create_db_entry_from_submission(
                    submission)
                posts_to_insert.append(dbEntry)
                total_nr_of_documents += 1

                for column_to_index in SearchEngineVariablesService.columns_to_index:
                    column_name = column_to_index+SearchEngineVariablesService.avg_len_suffix
                    avg_variables_dict[column_name] += (
                        dbEntry[column_to_index+'_len']-avg_variables_dict[column_name])/total_nr_of_documents
                yield dbEntry

        self.posts_service.insert_posts(posts_to_insert)
        self.variables_service.set_variables(avg_variables_dict)

    @staticmethod
    def create_db_entry_from_submission(submission):
        comments = ""
        if RedditProvider.include_comments:
            comments = ' '.join(
                list(map(lambda x: x.body if hasattr(x, 'body') else "", submission.comments[:100] or [])))
        if(len(comments) != 0):
            comments = ' '.join(['<COMMENTS>', comments, '</COMMENTS>'])

        title = ('<TITLE>'+submission.title +
                 '</TITLE>') if len(submission.title or "") > 0 else ''
        body = ('<BODY>'+submission.selftext +
                '</BODY>') if len(submission.selftext or "") > 0 else ''
        lens = [len(submission.title or ""), len(
            submission.selftext or ""), len(comments or "")]

        ret = {
            '_id': submission.id,
            'content': title + body + comments,
            'num_comments': submission.num_comments,
            'upvotes': submission.ups,
            'downvotes': submission.downs,
            'created_at': submission.created_utc,
            'title_len': lens[0],
            'body_len': lens[1],
            'comments_len': lens[2],
            'content_len': lens[0]+lens[1]+lens[2],
            'url': submission.url}

        return ret
