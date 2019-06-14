from dataCrawl.reddit_provider import RedditProvider
from pymongo import MongoClient
from document_parser import DocumentParser
from rulesProviders.string_stemming_rule_provider import StringStemmingRuleProvider
from indexer import Indexer

from searchManagers.boolean_search_manager import BooleanSearchManager
from searchManagers.frecvency_search_manager import FrecvencySearchManager
from searchManagers.proximity_probabilistic_model_search_manager import PPM_search_manager

from mapReduce.map_reduce_manager import MapReduceManager
from utils.custom_merge_sort import merge_k_sorted_lists


def main():

    client = MongoClient('localhost', 27017)
    mongo_db = client["Indexer-Database"]

    print("Choose what you wanna do 1-Index 0-Search: ")
    ch = '1'

    if(ch == '1'):
        mongo_db['direct_map'].remove({})
        mongo_db['index'].remove({})

        test = MapReduceManager(mongo_db, 8)
        test.start()
        print("Good we re done")
    else:
        search_manager = PPM_search_manager(mongo_db)
        print("What you wanna search?")
        query = input()

        document_ids = search_manager.process_query(query)

        for document_id in document_ids[:10]:
            result = client['Indexer-Database'].posts.find_one({
                '_id': document_id[1]
            })
            print(result['url'])


if(__name__ == '__main__'):
    main()
