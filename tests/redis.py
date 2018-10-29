import redis
from futile.redis import RedisProducer, RedisConsumer
from futile.log import get_logger, init_log


def handle(message):
    print(message)


def main():
    init_log("test", console_level="debug")

    client = redis.StrictRedis(host="localhost", port=6380)
    producer = RedisProducer(client)
    producer.create_stream("futile_test", 100)
    for i in range(100):
        producer.publish("futile_test", {"foo": i})

    consumer = RedisConsumer(
        client, "futile_test", "test_group", "test_consumer", handle, 1
    )

    # consumer.create_group()
    try:
        consumer.start_consuming()
    except KeyboardInterrupt:
        consumer.stop_consuming()


if __name__ == "__main__":
    main()
