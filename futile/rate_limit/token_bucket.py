import time


class TokenBucket:

    def __init__(self, capacity: int, fill_rate: int, fill_interval: int) -> None:
        self.capacity = capacity
        self.tokens = 0
        self.fill_rate = fill_rate
        self.fill_interval = fill_interval
        self.last_run_time = 0

    def get_tokens(self) -> int:
        # update tokens
        current = int(time.time())
        time_passed = current - self.last_run_time
        self.tokens += int(time_passed * self.fill_rate / self.fill_interval)
        self.last_run_time = current
        if self.tokens > self.capacity:
            self.tokens = self.capacity
        return self.tokens

    def consume(self, count: int) -> bool:
        if self.get_tokens() >= count:
            allowed = True
            self.tokens -= count
        else:
            allowed = False
        return allowed


if __name__ == '__main__':
    bucket = TokenBucket(80, 1, 1)
    assert bucket.get_tokens() == 80
    print("consume(10) =", bucket.consume(10))
    print("consume(10) =", bucket.consume(10))
    print('sleep 1 sec')
    time.sleep(1)
    print("tokens =", bucket.get_tokens())
    print('sleep 1 sec')
    time.sleep(1)
    print("tokens =", bucket.get_tokens())
    print("consume(90) =", bucket.consume(90))
    print("tokens =", bucket.get_tokens())
