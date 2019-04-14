from mpi4py import MPI
from document_parser import DocumentParser
from db.direct_map_service import DirectMapService
from utils.dictionary_helper import aggregate_dicts

import threading
import time
import queue


class MapWorkerJob(threading.Thread):
    bulk_count = 2

    def __init__(self, mongo_db, queue, event, stop_flag,num_threads):
        threading.Thread.__init__(self)
        self.queue = queue
        self.event = event
        self.stop_flag = stop_flag
        self.direct_map_service = DirectMapService(mongo_db)
        self.num_threads=num_threads

    def run(self):
        count_docs = 0
        direct_position_map = dict()

        while not self.stop_flag.is_set():
            if(self.queue.empty()):
                self.event.wait(timeout=1)

            document = None
            try:
                document = self.queue.get(False)
            except queue.Empty:
                continue
            direct_position_map_temp = DocumentParser.compute_direct_positional_map(
                document,self.num_threads)
            aggregate_dicts(direct_position_map, direct_position_map_temp)

            count_docs += 1
            if(count_docs == self.bulk_count):
                self.direct_map_service.direct_map(
                    list(direct_position_map.values()))
                direct_position_map = dict()
                count_docs = 0

            self.event.clear()

        self.direct_map_service.direct_map(
            list(direct_position_map.values()))

        print('STOPPED')
