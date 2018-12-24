import time
import threading


class TokenBucket:

    """
    令牌桶算法
    """

    def __init__(self, capacity: int, rate: int) -> None:
        """
        :param capacity: 桶的最大容量
        :param rate: 填充速度(个/s)
        """
        self.capacity = capacity
        self.rate = rate

        self.tokens = 0  # 当前桶中的令牌数量
        self.last_run_time = 0  # 上次获取令牌时间
        self.lock = threading.Lock()

    def _update_tokens(self) -> None:
        """
        更新当前桶中的令牌数量, 并返回
        """
        current = int(time.time())
        time_passed = current - self.last_run_time
        self.tokens += int(time_passed * self.rate)
        self.last_run_time = current
        if self.tokens > self.capacity:  # 最大不能超过容量
            self.tokens = self.capacity

    def get_tokens(self) -> int:
        with self.lock:
            self._update_tokens()
            return self.tokens

    def consume(self, count: int) -> int:
        """
        return (ok, wait) 是否可以执行, 如果不可以执行, 至少需要等待的时间
        """
        with self.lock:
            self._update_tokens()
            if self.tokens >= count:
                self.tokens -= count
                return 0
            else:
                return (count - self.tokens) / self.rate


if __name__ == "__main__":
    bucket = TokenBucket(80, 1)
    assert bucket.get_tokens() == 80
    print("consume(10) =", bucket.consume(10))
    print("consume(10) =", bucket.consume(10))
    print("sleep 1 sec")
    time.sleep(1)
    print("tokens =", bucket.get_tokens())
    print("sleep 1 sec")
    time.sleep(1)
    print("tokens =", bucket.get_tokens())
    print("consume(90) =", bucket.consume(90))
    print("tokens =", bucket.get_tokens())
