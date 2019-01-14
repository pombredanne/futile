import os
import threading
from queue import LifoQueue, Empty, Full


class ConnectionPool:
    def __init__(
        self,
        max_connections=50,
        timeout=20,
        connection_class=None,
        queue_class=LifoQueue,
        **connection_kwargs,
    ):

        self.queue_class = queue_class  # 使用一个队列来存放连接
        self.timeout = timeout  # 增加了超时功能
        self.max_connections = max_connections
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs

        self.reset()  # 调用 reset 初始化一些属性

    def reset(self):
        self.pid = os.getpid()
        self._check_lock = threading.Lock()

        # 首先在队列中填满 None，后面会用到，这里很关键
        self.pool = self.queue_class(self.max_connections)
        while True:
            try:
                self.pool.put_nowait(None)
            except Full:
                break
        # Keep a list of actual connection instances so that we can
        # disconnect them later.
        self._connections = []

    def _checkpid(self):
        # 如果当前的 connection 是 fork 来的，直接关闭链接
        if self.pid != os.getpid():
            with self._check_lock:
                if self.pid == os.getpid():
                    # 另一个线程已经检查了，直接返回
                    return
                self.disconnect()
                self.reset()

    def make_connection(self):
        # 创建一个链接，貌似和上面的函数没有什么区别。。
        connection = self.connection_class(**self.connection_kwargs)
        # 一直往这个数组里面怼可能有内存泄露风险
        self._connections.append(connection)
        return connection

    def get_connection(self):
        """
        获取一个新的连接，最长等待 timeout 秒

        如果我们读取到的新连接是 None 的话，就会创建一个新的连接。因为我们使用的
        是 LIFO 队列，也就是栈，所以我们优先得到的是已经创建的链接，而不是最开始
        放进去的 None。也就是我们只有在需要的时候才会创建新的连接，也就是说连接
        数量是按需增长的。
        """
        # 确保没有更换进程
        self._checkpid()

        # 尝试获取一个连接，如果在 timeout 时间内失败的话，抛出 ConnectionError
        connection = None
        try:
            connection = self.pool.get(block=True, timeout=self.timeout)
        except Empty:
            # 需要注意的是这个错误并不会被 redis 捕获，需要用户自己处理
            raise ConnectionError("No connection available.")

        # 如果真的没有连接可用了，直接创建一个新的连接
        if connection is None:
            connection = self.make_connection()

        return connection

    def release(self, connection):
        # 释放连接到连接池
        self._checkpid()
        if connection.pid != self.pid:
            return

        # Put the connection back into the pool.
        try:
            self.pool.put_nowait(connection)
        except Full:
            # perhaps the pool has been reset() after a fork? regardless,
            # we don't want this connection
            pass

    def disconnect(self):
        # 释放所有的连接
        for connection in self._connections:
            connection.disconnect()
