import time
import threading

from datetime import datetime
from croniter import croniter
from concurrent.futures import ThreadPoolExecutor

from futile.log import get_logger
from futile.strings import ensure_str


logger = get_logger("cron")


class Cron:

    PREFIX = "cron"

    def __init__(self, redis_client, run):
        self._redis_client = redis_client
        self._executor = ThreadPoolExecutor()
        self._run = run

    def schedule(self):
        while True:
            dues = self._redis_client.zrangebyscore(
                self.PREFIX + ':due', 0, int(time.time()), withscores=True
            )
            for cron_id, due_time in dues:
                # 更新下次运行时间
                expr = self._redis_client.hget(self.PREFIX + ':expr', cron_id)
                next_due = croniter(ensure_str(expr), base=due_time).get_next()
                self._redis_client.zadd(self.PREFIX + ':due', {cron_id: next_due})
                # 执行命令
                cmd = self._redis_client.hget(self.PREFIX + ':cmd', cron_id)
                self._executor.submit(self._run, cmd)
            time.sleep(1)

    def add(self, expression, command, cron_id):
        if not self.is_valid(expression):
            return False
        next_due = croniter(expression).get_next()
        p = self._redis_client.pipeline(transaction=False)
        p.zadd(self.PREFIX + ':due', {cron_id: next_due})
        p.hset(self.PREFIX + ':expr', cron_id, expression)
        p.hset(self.PREFIX + ':cmd', cron_id, command)
        p.execute()
        return True

    def delete(self, cron_id):
        pass

    def is_valid(self, expression):
        return croniter.is_valid(expression)
