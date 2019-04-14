from mpi4py import MPI
from document_parser import DocumentParser
from db.direct_map_service import DirectMapService
from utils.dictionary_helper import aggregate_dicts
from db.index_service import IndexService

import threading
import time
import queue


class ReduceWorkerJob(threading.Thread):
    bulk_count = 2

    def __init__(self, mongo_db, id):
        threading.Thread.__init__(self)
        self.direct_map_service = DirectMapService(mongo_db)
        self.index_service = IndexService(mongo_db)
        self.id = id

    def run(self):
        direct_map = self.direct_map_service.get_map(self.id)
        if direct_map is None:
            exit()

        word_index = dict()
        parsed_docs_count = 0

        for doc in direct_map['docs']:
            for term in doc['terms']:
                if term["_id"] in word_index:
                    word_index[term["_id"]]["documents"].append({
                        "_id": doc['_id'],
                        "freq": term['freq'],
                        "pos": term['pos']
                    })

                else:
                    word_index[term["_id"]] = {
                        "_id": term["_id"],
                        "documents": [{
                            "_id": doc['_id'],
                            "freq":term['freq'],
                            "pos":term['pos']
                        }
                        ]
                    }
                # for

            parsed_docs_count += 1
            if(parsed_docs_count == self.bulk_count):
                self.index_service.upsert_many_in_index(
                    list(word_index.values()))
                word_index = dict()
                parsed_docs_count = 0
        print('STOPPED')
