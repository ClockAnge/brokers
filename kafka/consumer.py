from confluent_kafka import Consumer, KafkaException
import msgpack

bootstrap_servers = 'localhost:29092'
topic_name = 'test_topic'
group_id = 'g.makhmudov'

conf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': 'earliest',
}

consumer = Consumer(conf)

consumer.subscribe([topic_name])

def process_message(msg):
    decoded_message = msgpack.unpackb(msg.value(), raw=False)
    print(f"Received: {decoded_message}")

try:
    while True:
        msg = consumer.poll(timeout=1)
        if msg is None:
             print("Message is None")
             continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
                break

        process_message(msg)

except KeyboardInterrupt:
    pass

finally:
    consumer.close()

