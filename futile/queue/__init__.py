import queue


def queue_mget(q, n, timeout=None):
    result = [q.get(timeout=timeout)]  # block until at least 1
    try:  # add more until `q` is empty or `n` items obtained
        while len(result) < n:
            result.append(q.get(block=False))
    except queue.Empty:
        pass
    return result



# TODO select on queues
