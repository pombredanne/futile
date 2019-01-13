import heapq


class HeapSet:
    def __init__(self):
        self._set = set()
        self._heap = []

    def add(self, key, score=0):
        if key in self._set:
            return
        self._set.add(key)
        heapq.heappush(self._heap, (0, key))

    def update(self, key, score):
        heapq.heapreplace(self._heap, (score, key))

    def peek(self):
        return self._heap[0]
