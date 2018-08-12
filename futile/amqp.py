import json
import time
import threading
from queue import Queue, Empty as QueueEmpty, Full as QueueFull
from functools import partial

import pika

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
            except QueueEmpty:
                time.sleep(.5)
                self._client.process_data_events()
                continue
            self._publish(**meta)


class Worker:

    def __init__(self, amqp_client):
        self._client = amqp_client

    def work(self, queue, handle):
        L = get_logger('[Worker]')
        while True:
            try:
                ch, method, props, message = queue.get_nowait()
            except QueueEmpty:
                L.info('worker got no work to do')
                time.sleep(.5)
                continue
            L.debug('handle message %s', message)
            try:
                handle(message)
            except Exception as e:
                L.info('handle message %s error %s', message, e)
            acker = partial(ch.basic_ack, delivery_tag=method.delivery_tag)
            self._client.add_callback_threadsafe(acker)


class AmqpConsumer:

    def __init__(self, name, client):
        self._name = name
        self._client = client
        channel = self._client.channel()
        self._channel = channel

    def start_pipeline(self, handle, *,
                       exchange='', queue='', routing_key='', thread_num=1, **kwargs):
        """
        主线程消费消息，worker线程实际处理消息
        """

        L = get_logger(f'[Pipeline-{self._name}]')
        channel = self._channel

        # TODO support more scenario
        if exchange == '':
            # directly consume from a queue
            amqp_queue = queue
        else:
            # decalre a exclusive queue to consume from exchange
            r = channel.queue_declare(exclusive=True)
            amqp_queue = r.method.queue
            channel.queue_bind(exchange=exchange, queue=amqp_queue,
                               routing_key=routing_key)

        L.info('using queue %s with exchange %s', amqp_queue, exchange)

        # thread queue to pass events to workers
        thread_queue = Queue()

        # start workers
        worker = Worker(self._client)
        for i in range(thread_num):
            L.info('spawn worker %s', i)
            worker_thread = threading.Thread(target=worker.work,
                                             name=f'{self._name}-{i}',
                                             args=(thread_queue, handle)
                                             )
            worker_thread.daemon = True
            worker_thread.start()
            L.info('worker %s started', i)

        # on message arrive callback, put message to thread queue
        def on_message(ch, method, props, message):
            thread_queue.put_nowait((ch, method, props, message))

        for i in range(3):
            try:
                self._channel.basic_consume(on_message, amqp_queue, **kwargs)

                # start to poll messages
                L.info('start to consume')
                return self._channel.start_consuming()
            except pika.exceptions.AMQPChannelError:
                time.sleep(.5 * i)
                self._channel = self._client.channel()

    def stop_pipeline(self):
        return self._channel.stop_consuming()


def make_rabbitmq_client():
    endpoints = lookup_service('mq.rabbit')
    ip, port = endpoints[0]
    username = lookup_kv('mq.rabbit/username')
    password = lookup_kv('mq.rabbit/password')
    return AmqpClient(ip, port, username, password)
