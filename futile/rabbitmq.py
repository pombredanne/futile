import pika
from futile.consul import lookup_kv, lookup_service


class RabbitMqClient:

    def __init__(self, ip, port, username='guest', password='guest'):
        credentials = pika.PlainCredentials(username, password)
        parameters = pika.ConnectionParameters(ip, port, credentials=credentials)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

    def publish(self, exchange, routing_key, message, **kwargs):
        properties = pika.BasicProperties(**kwargs)
        self.channel.basic_publish(exchange, routing_key, message, properties)

    def consume(self, callback, queue, **kwargs):
        return self.channel.basic_consume(callback, queue, **kwargs)

    def start_consuming(self):
        return self.channel.start_consuming()


def make_rabbitmq_client():
    endpoints = lookup_service('mq.rabbit')
    ip, port = endpoints[0]
    username = lookup_kv('mq.rabbit/username')
    password = lookup_kv('mq.rabbit/password')
    return RabbitMqClient(ip, port, username, password)


class RabbitMqExchange:

    def __init__(name, type, **kwargs):
        self.name = name
        self.type = type
        self.kwargs = kwargs

    def declare(self, client):
        client.channel.exchange_declare(exchange=self.name,
                                        exchange_type=self.type,
                                        **self.kwargs)


class RabbitMqQueue:

    def __init__(self, name, routing_key=None, **kwargs):
        self.name = name
        self.routing_key = name if routing_key is None else routing_key
        self.kwargs = kwargs

    def declare(self, client):
        client.channel.queue_declare(queue=self.name,
                                     routing_key=self.routing_key,
                                     **self.kwargs)

    def bind(self, exchange):
        client.channel.queue_bind(exchange.name, self.name)

