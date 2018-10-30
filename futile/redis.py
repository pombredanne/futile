import os
import time
import threading
import queue
import redis

from .log import get_logger
from .consul import lookup_service


MAX_PENDING_SIZE = 1024
BATCH_SIZE = 128
BLOCK_MS = 500
IDLE_TIME = 30 * 60 * 1000  # 30 min


def _response_to_dict(l):
    return dict(zip(l[::2], l[1::2]))


class RedisStream:
    def __init__(self, client, stream):
        self._client = client
        self._stream = stream

    def stream_info(self, steram):
        pass

    def groups_info(self, groups):
        pass

    def consumers_info(self, consumers):
        pass

    def __len__(self):
        return self._client.execute_command("xlen", self._stream)


class RedisProducer:
    def __init__(self, client):
        self._client = client

    def create_stream(self, stream, maxlen):
        self._client.execute_command(
            "xadd", stream, "maxlen", "~", str(maxlen), "*", "_", "_noop"
        )

    def delete_message(self, stream, message_id):
        self._client.xdel("xdel", stream, message_id)

    def publish(self, stream, message, message_id="*"):
        """
        发布消息到 redis stream
        """
        _message = []
        for k, v in message.items():
            _message.append(k)
            _message.append(v)
        self._client.execute_command("xadd", stream, message_id, *_message)


class RedisConsumer:
    def __init__(self, client, stream, group, consumer, handle, max_workers):
        """
        client: a StrictRedis client
        group: consumer group name
        consumer: consumer name
        """
        self._client = client
        self._stream = stream
        self._group = group
        self._consumer = consumer
        self._handle = handle
        self._local_queue = queue.Queue(maxsize=MAX_PENDING_SIZE)
        self._logger = get_logger("redis_consumer")
        self._max_workers = max_workers
        self._should_stop = False
        self._idle_time = IDLE_TIME
        self._threads = []

    def create_group(self, offset_id="$"):
        self._client.execute_command(
            "xgroup", "create", self._stream, self._group, offset_id
        )

    def setid(self, message_id="$"):
        self._client.execute_command(
            "xgroup", "setid", self._stream, self._group, message_id
        )

    def destroy_group(self):
        self._client.execute_command("xgroup", "destroy", self._stream, self._group)

    def delete_consumer(self):
        self._client.execute_command(
            "xgroup", "delconsumer", self._stream, self._group, self._consumer
        )

    def peek(self, start, end, count=None):
        # TODO implement
        self._client.execute_comand("xrange", self._stream, start, end, "count", count)

    def _process_message(self):
        while not self._should_stop:
            try:
                message_id, message = self._local_queue.get_nowait()
                self._do_process(message_id, message)
            except queue.Empty:
                self._logger.debug("worker got no work to do")
                time.sleep(0.5)
                continue

    def _do_process(self, message_id, message):
        try:
            self._handle(message)
            rsp = self._client.execute_command(
                "xack", self._stream, self._group, message_id
            )
            if rsp != 1:
                raise Exception("ack failed, rsp=%s" % rsp)
        except Exception as e:
            self._logger.exception("handle message %s error %s", message, e)

    def start_consuming(self):
        for i in range(self._max_workers):
            worker_thread = threading.Thread(
                target=self._process_message, name=f"worker-{i}"
            )
            self._threads.append(worker_thread)
            worker_thread.daemon = True
            worker_thread.start()

        # 首先处理 pending
        while not self._should_stop:
            rsp = self._client.execute_command(
                "xreadgroup",
                "group",
                self._group,
                self._consumer,
                "count",
                BATCH_SIZE,
                "block",
                BLOCK_MS,
                "streams",
                self._stream,
                "0",
            )
            if rsp is None:
                self._logger.debug("no message in pending")
                break
            _, messages = rsp[0]
            if not messages:
                self._logger.debug("no message in pending")
                break
            for message_id, message in messages:
                if message is None:
                    self._logger.error("message no longer available, %s", message_id)
                    rsp = self._client.execute_command(
                        "xack", self._stream, self._group, message_id
                    )
                    continue
                message = _response_to_dict(message)
                if message.get(b'_') == b'_noop':
                    rsp = self._client.execute_command(
                        "xack", self._stream, self._group, message_id
                    )
                    continue
                self._do_process(message_id, message)

        while not self._should_stop:
            rsp = self._client.execute_command(
                "xreadgroup",
                "group",
                self._group,
                self._consumer,
                "count",
                BATCH_SIZE,
                "block",  # TODO block 在 redis 还是这里?
                BLOCK_MS,
                "streams",
                self._stream,
                ">",
            )
            if rsp is None:
                self._logger.debug("not message in redis")
                continue
            _, messages = rsp[0]
            for message_id, message in messages:
                message = _response_to_dict(message)
                if message.get(b'_') == b'_noop':
                    rsp = self._client.execute_command(
                        "xack", self._stream, self._group, message_id
                    )
                    continue
                self._local_queue.put((message_id, message))

    def stop_consuming(self):
        self._should_stop = True
        for thread in self._threads:
            thread.join()

    def start_recycling(self):
        """
        回收 consumer group 中未被处理的消息
        """
        while not self._should_stop:
            rsp = self._client.execute_command(
                "xpending", self._stream, self._group, "-", "+", BATCH_SIZE
            )
            if rsp is None:
                break
            message_ids = []
            for message_id, consumer, *message in rsp:
                message = _response_to_dict(message)
                self._local_queue.put((message_id, message))
            self._client.execute_command(
                "xclaim",
                self._stream,
                self._group,
                self._consumer,
                "idle",
                self._idle_time,
                *message_ids,
                "justid",
            )


def make_redis_client(conf=None) -> redis.StrictRedis:
    # addresses = lookup_service('inf.db.redis')
    # ip, port = addresses[0]
    ip = os.environ.get("REDIS_IP")
    port = os.environ.get("REDIS_PORT")
    return redis.StrictRedis(ip, port)


def make_redis_stream_client(conf=None) -> redis.StrictRedis:
    ip = os.environ.get("REDIS_STREAM_IP")
    port = os.environ.get("REDIS_STREAM_PORT")
    return redis.StrictRedis(ip, port)


def make_pika_client(conf=None) -> redis.StrictRedis:
    # addresses = lookup_service('inf.db.pika')
    # ip, port = addresses[0]
    ip = os.environ.get("PIKA_IP")
    port = os.environ.get("PIKA_PORT")
    return redis.StrictRedis(ip, port)
