from dataCrawl.reddit_provider import RedditProvider
from pymongo import MongoClient
from document_parser import DocumentParser
from rulesProviders.string_stemming_rule_provider import StringStemmingRuleProvider
from indexer import Indexer
from searchManagers.boolean_search_manager import BooleanSearchManager

def main():

    client = MongoClient('localhost', 27017)

    search_manager = BooleanSearchManager(client)

    document_ids=search_manager.process_query("feel free")

    for document_id in document_ids:
        result = client['Indexer-Database'].posts.find_one({
            '_id': document_id
        })
        print(result['title'])
        print(result['body'])
        print(result['comments'])

    # redditProvider = RedditProvider(client)

    # redditProvider.provide_data()

    # client['Indexer-Database'].index.remove({})

    # indxr = Indexer(client)

    # indxr.parse_current_data()


if(__name__ == '__main__'):
    main()
