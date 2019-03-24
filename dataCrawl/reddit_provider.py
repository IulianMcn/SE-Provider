import praw
from dataCrawl.data_provider import DataProvider

from db.search_engine_variables_service import SearchEngineVariablesService
from db.subreddits_service import SubredditsService


class RedditProvider(DataProvider):

    number_of_subreddits_to_parse = 5
    limit_of_reddits_to_parse = 10
    variables_service = None
    posts_service = None
    subreddits_service = None

    def __init__(self, mongo_db):
        super(RedditProvider, self).__init__(mongo_db)
        self.subreddits_set = SubredditsService(mongo_db)
        self.client = praw.Reddit(client_id='-GYWVXI4AgNJgw',
                                  client_secret='8TJSFXgWgWO_dAKwCr2pPB5EUAs',
                                  user_agent='my user agent?'  # Warning-PY: wtf is this?
                                  )  # Warning-PY: use a praw.ini file when moving to github

    def provide_data(self):
        parsed_subreddits = 0
        avg_variables_dict = self.variables_service.get_avarages_variables()
        total_nr_of_documents = self.posts_service.get_total_nr_of_posts()

        while(parsed_subreddits != self.number_of_subreddits_to_parse):
            subreddit = self.client.random_subreddit()
            print("Subreddit " + subreddit.display_name)

            if(self.subreddits_service.check_if_exists(subreddit.display_name)):
                continue
            else:
                parsed_subreddits += 1
                self.subreddits_service.insert_subreddit(
                    subreddit.display_name)

            for submission in subreddit.hot(limit=self.limit_of_reddits_to_parse):
                dbEntry = RedditProvider.create_db_entry_from_submission(submission)
                total_nr_of_documents += 1

                for column_to_index in SearchEngineVariablesService.columns_to_index:
                    column_name = column_to_index+SearchEngineVariablesService.avg_len_suffix
                    avg_variables_dict[column_name] += (
                        len(dbEntry[column_to_index])-avg_variables_dict[column_name])/total_nr_of_documents

            self.variables_service.set_variables(avg_variables_dict)

    @staticmethod
    def create_db_entry_from_submission(submission):
        comments = ' '.join(
            list(map(lambda x: x.body if hasattr(x, 'body') else "", submission.comments or [])))

        return{
            'source_id': submission.id,
            'title': submission.title,
            'body': submission.selftext,
            'comments': comments,
            'num_comments': submission.num_comments,
            'upvotes': submission.ups,
            'downvotes': submission.downs,
            'created_at': submission.created_utc,
            'title_len': len(submission.title or ""),
            'body_len': len(submission.selftext or ""),
            'comments_len': len(comments or "")
        }
