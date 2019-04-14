from mapReduce.master_job import MasterJob
from mapReduce.map_worker_job import MapWorkerJob
from mapReduce.reduce_worker_job import ReduceWorkerJob
from utils.queue_extension import CustomQueue
import heapq
import threading


class MapReduceManager:
    map_workers_queues = None
    map_workers = None
    stop_flag = None

    def __init__(self, mongo_db, num_threads):
        self.num_workers_map = int((num_threads-1)/2)
        self.num_workers_reduce = num_threads-self.num_workers_map-1
        self.mongo_db = mongo_db
        self.stop_flag = threading.Event()

        self.map_workers_queues = []
        self.map_workers = list()
        self.reduce_workers = list()

        for _ in range(self.num_workers_map):
            queue = CustomQueue(maxsize=100)
            event = threading.Event()
            heapq.heappush(self.map_workers_queues,
                           (0, queue, event))
            self.map_workers.append(
                MapWorkerJob(mongo_db, queue, event, self.stop_flag,self.num_workers_reduce)
            )

        for i in range(self.num_workers_reduce):
            self.reduce_workers.append(
                ReduceWorkerJob(mongo_db, i)
            )

        self.master_job = MasterJob(mongo_db, manager=self)

    def add_submission(self, submission):
        print("ADDING " + str(submission['_id']))
        map_worker_queue_ent = heapq.heappop(self.map_workers_queues)

        map_worker_queue_ent[1].put(submission)  # add to queue
        map_worker_queue_ent[2].set()

        heapq.heappush(self.map_workers_queues,
                       (map_worker_queue_ent[0]+submission['content_len'],
                        map_worker_queue_ent[1],
                        map_worker_queue_ent[2]))

    def stop_all_map_workers(self):
        for map_worker in self.map_workers:
            map_worker.event.set()

    def start(self):
        self.master_job.start()
        for map_worker in self.map_workers:
            map_worker.start()

        self.master_job.join()
        self.stop_flag.set()
        self.stop_all_map_workers()
        for map_worker in self.map_workers:
            map_worker.join()

        for reduce_worker in self.reduce_workers:
            reduce_worker.start()
        
        for reduce_worker in self.reduce_workers:
            reduce_worker.join()

