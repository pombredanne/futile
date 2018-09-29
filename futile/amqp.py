import json
import time
import threading
from queue import Queue, Empty as QueueEmpty, Full as QueueFull
from functools import partial

import pika  # pika is the python client lib for AMQP, not to be confused with pika the kv server

from google.protobuf.text_format import MessageToString

from futile.log import get_logger
from futile.consul import lookup_kv, lookup_service


# TODO auto reconnect or use connection pooling


class AmqpClient:

    def __init__(self, ip, port, username='guest', password='guest'):
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

    def declare_and_bind(self, exchange, routing_key):
        channel = self.channel()
        queue_name = channel.declare_queue(exclusive=True).method.queue
        channel.bind(exchange, queue_name, routing_key)
        return queue_name

    def sleep(self, dur):
        self._connection.sleep(dur)

    def set_handle(self, handle, queue, **kwargs):
        channel = self.channel()

        def callback(ch, method, props, message):
            message = json.loads(message)
            handle(message)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        return channel.basic_consume(callback, queue, **kwargs)


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

        communicate_thread = threading.Thread(name=f'{name}-communicate',
                                              target=self._communicate
                                              )
        communicate_thread.daemon = True
        communicate_thread.start()
        self._communicate_thread = communicate_thread

    def publish(self, exchange, routing_key, body, mandatory=False,
                immediate=False, **kwargs):
        """
        发送消息，首先缓存到队列中
        """
        meta = dict(
            exchange=exchange,
            routing_key=routing_key,
            body=body,
            mandatory=mandatory,
            immediate=immediate,
            **kwargs
        )
        try:
            self._queue.put_nowait(meta)
            return True
        except QueueFull:
            return False

    def _publish(self, exchange, routing_key, body, mandatory=False,
                 immediate=False, **kwargs):
        properties = pika.BasicProperties(**kwargs)
        for _ in range(3):
            try:
                return self._channel.basic_publish(exchange, routing_key, body,
                                                   properties, mandatory, immediate)
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
                time.sleep(.5)
                self._client.process_data_events()


class Worker:

    def __init__(self, amqp_client):
        self._client = amqp_client
        self._should_stop = False

    def stop(self):
        self._should_stop = True

    def work(self, queue, handle):
        L = get_logger('Worker')
        while True:
            try:
                ch, method, props, message = queue.get_nowait()
            except QueueEmpty:
                if self._should_stop:
                    break
                L.debug('worker got no work to do')
                time.sleep(.5)
                continue
            L.debug('handle message %s', message)
            try:
                handle(message)
                acker = partial(ch.basic_ack, delivery_tag=method.delivery_tag)
                self._client.add_callback_threadsafe(acker)
            except Exception as e:
                # 如果处理消息出错就不会 ACK
                L.exception('handle message %s error %s',
                            MessageToString(message, as_one_line=True), e)


class AmqpConsumer:

    def __init__(self, name, client):
        self._name = name
        self._client = client
        self._channel = self._client.channel()
        self.L = get_logger(f'pipeline-{self._name}')
        self._workers = []
        self._worker_threads = []

    def start_pipeline(self, handle, *, queue='', thread_num=1, **kwargs):
        """
        主线程消费消息，worker线程实际处理消息
        """

        self.L.info('using queue %s', queue)

        # thread queue to pass events to workers
        thread_queue = Queue()

        # start workers
        for i in range(thread_num):
            worker = Worker(self._client)
            self._workers.append(worker)
            self.L.info('spawn worker number %s', i)
            worker_thread = threading.Thread(target=worker.work,
                                             name=f'{self._name}-{i}',
                                             args=(thread_queue, handle)
                                             )
            self._worker_threads.append(worker_thread)
            worker_thread.daemon = True
            worker_thread.start()
            self.L.info('worker %s started', i)

        # on message arrive callback, put message to thread queue
        def on_message(ch, method, props, message):
            thread_queue.put_nowait((ch, method, props, message))

        # retry for 3 times
        for i in range(3):
            try:
                self._channel.queue_declare(queue=queue)
                self._channel.basic_consume(on_message, queue, **kwargs)
                # start to poll messages
                self.L.info('start consuming...')
                return self._channel.start_consuming()
            except pika.exceptions.AMQPChannelError:
                time.sleep(.5 * (2 ** i))
                self._channel = self._client.channel()

    def stop_pipeline(self):
        ret = self._channel.stop_consuming()
        self.L.info('stop consuming, waiting for workers to quit')
        for worker in self._workers:
            worker.stop()
        for thread in self._worker_threads:
            thread.join()
        return ret


def make_rabbitmq_client():
    endpoints = lookup_service('inf.mq.rabbit')
    ip, port = endpoints[0]
    username = lookup_kv('inf.mq.rabbit/username')
    password = lookup_kv('inf.mq.rabbit/password')
    return AmqpClient(ip, port, username, password)
