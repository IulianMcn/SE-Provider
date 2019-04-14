import queue


class CustomQueue(queue.Queue):
    def __lt__(self, other):
        return True
