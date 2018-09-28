class KeyedWorkderPool:
    """
    根据 queue 中的 message 的 key 分配到对应 worker 处理，保证需要使用对应资源
    的消息都被同一个 worker 处理
    """
    def __init__(self, worker_num, thread_num, hash_func=hash):
        raise NotImplementedError
        self.worker_num = worker_num
        self.thread_num = thread_num
        self.hash_func = hash_func


