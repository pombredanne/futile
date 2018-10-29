import os
import json
import time
import threading
from queue import Queue, Empty as QueueEmpty, Full as QueueFull
from functools import partial

import pika  # pika is the python client lib for AMQP, not to be confused with pika the kv server

from google.protobuf.text_format import MessageToString

from futile.log import get_logger

# from futile.consul import lookup_kv, lookup_service


# TODO auto reconnect or use connection pooling


class AmqpClient:
    def __init__(self, ip, port, username="guest", password="guest"):
        credentials = pika.PlainCredentials(username, password)
        parameters = pika.ConnectionParameters(ip, port, credentials=credentials)
        self._parameters = parameters
        self._connection = pika.BlockingConnection(parameters)

    def __getattr__(self, name):
        """
        for example:

        >>> client = Amqpclient()
        >>> channel = client.channnel()
        """

        def wrapped(*args, **kwargs):
            for _ in range(3):
                method = getattr(self._connection, name)
                try:
                    return method(*args, **kwargs)
                except pika.exceptions.AMQPConnectionError:
                    self._connection = pika.BlockingConnection(self._parameters)

        return wrapped

    def sleep(self, dur):
        self._connection.sleep(dur)


class AmqpProducer:
    # TODO drain the queue when stopping

    def __init__(self, name, client):
        """
        主线程发送消息，后台线程读取 rabbitmq 消息
        """
        self._name = name
        self._client = client
        self._queue = Queue()
        self._channel = self._client.channel()

        communicate_thread = threading.Thread(
            name=f"{name}-communicate", target=self._communicate
        )
        communicate_thread.daemon = True
        communicate_thread.start()
        self._communicate_thread = communicate_thread

    def publish(
        self, exchange, routing_key, body, mandatory=False, immediate=False, **kwargs
    ):
        """
        发送消息，首先缓存到队列中
        """
        meta = dict(
            exchange=exchange,
            routing_key=routing_key,
            body=body,
            mandatory=mandatory,
            immediate=immediate,
            **kwargs,
        )
        try:
            self._queue.put_nowait(meta)
            return True
        except QueueFull:
            return False

    def _publish(
        self, exchange, routing_key, body, mandatory=False, immediate=False, **kwargs
    ):
        properties = pika.BasicProperties(delivery_mode=2, **kwargs)
        for _ in range(3):
            try:
                return self._channel.basic_publish(
                    exchange, routing_key, body, properties, mandatory, immediate
                )
            except pika.exceptions.AMQPChannelError:
                self._channel = self._client.channel()

    def _communicate(self):
        """
        发送消息到 rabbitmq
        """
        while True:
            try:
                meta = self._queue.get_nowait()
                self._publish(**meta)
            except QueueEmpty:
                time.sleep(0.5)
                self._client.process_data_events()


class Worker:
    def __init__(self, amqp_client, message_type=None):
        self._client = amqp_client
        self._should_stop = False
        self._message_type = message_type

    def stop(self):
        self._should_stop = True

    def work(self, thread_queue, handle):
        logger = get_logger("Worker")
        while True:
            if self._should_stop:
                logger.info("receive stop signal, stopping worker...")
                break
            try:
                ch, method, props, message = thread_queue.get_nowait()
            except QueueEmpty:
                logger.debug("worker got no work to do")
                time.sleep(0.5)
                continue
            try:
                handle(message)
            except Exception as e:
                # 如果处理消息出错就不会 ACK
                if self._message_type is not None:
                    parsed_message = self._message_type()
                    parsed_message.ParseFromString(message)
                    logger.exception(
                        "handle message %s error %s",
                        MessageToString(parsed_message, as_one_line=True),
                        e,
                    )
                else:
                    logger.exception("handle message %s error %s", message, e)
            # finally:
            #     acker = partial(ch.basic_ack, delivery_tag=method.delivery_tag)
            #     self._client.add_callback_threadsafe(acker)


class AmqpConsumer:
    def __init__(self, name, client):
        self._name = name
        self._client = client
        self._channel = self._client.channel()
        self.logger = get_logger(f"pipeline-{self._name}")
        self._workers = []
        self._worker_threads = []

    def start_pipeline(
        self, handle, *, message_type=None, queue="", max_workers=1, **kwargs
    ):
        """
        主线程消费消息，worker线程实际处理消息
        """

        self.logger.info("using queue %s", queue)

        # thread queue to pass events to workers
        thread_queue = Queue(maxsize=50000)

        # start workers
        for i in range(max_workers):
            worker = Worker(self._client, message_type)
            self._workers.append(worker)
            self.logger.info("spawn worker number %s", i)
            worker_thread = threading.Thread(
                target=worker.work,
                name=f"{self._name}-{i}",
                args=(thread_queue, handle),
            )
            self._worker_threads.append(worker_thread)
            worker_thread.daemon = True
            worker_thread.start()
            self.logger.info("worker %s started", i)

        # 这里有严重的 bug, 直接抛异常实际上还是在读取, 但是 block 之后又会导致链接挂掉,
        # pika 实在太烂了
        # on message arrive callback, put message to thread queue
        def on_message(ch, method, props, message):
            try:
                thread_queue.put_nowait((ch, method, props, message))
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except QueueFull:
                self.logger.info('thread queue is full!')
                time.sleep(.5)

        # retry for 3 times
        for i in range(3):
            try:
                self._channel.queue_declare(queue=queue, durable=True)
                self._channel.basic_consume(on_message, queue, **kwargs)
                # start to poll messages
                self.logger.info("start consuming...")
                return self._channel.start_consuming()
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.logger.exception("amqp connection error, %s", e)
                time.sleep(0.5 * (2 ** i))
                self._channel = self._client.channel()

    def stop_pipeline(self):
        ret = self._channel.stop_consuming()
        self.logger.info("stop consuming, waiting for workers to quit")
        for worker in self._workers:
            worker.stop()
        # for thread in self._worker_threads:
        #     thread.join()
        return ret


def make_rabbitmq_client():
    # endpoints = lookup_service('inf.mq.rabbit')
    # ip, port = endpoints[0]
    # username = lookup_kv('inf.mq.rabbit/username')
    # password = lookup_kv('inf.mq.rabbit/password')
    ip = os.environ.get("RABBITMQ_IP")
    port = os.environ.get("RABBITMQ_PORT")
    username = os.environ.get("RABBITMQ_USERNAME")
    password = os.environ.get("RABBITMQ_PASSWORD")
    return AmqpClient(ip, port, username, password)
