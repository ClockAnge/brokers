from confluent_kafka import Producer
import msgpack

bootstrap_servers = 'localhost:29092'
topic_name = 'test_topic'

conf = {
    'bootstrap.servers': bootstrap_servers
}

producer = Producer(conf)

def produce_message(message):
    serialized_message = msgpack.packb(message)
    producer.produce(topic_name, value=serialized_message)
    producer.flush()


message_to_send = {'key': 'value'}
produce_message(message_to_send)
print(f"Sent: '{message_to_send}'")

