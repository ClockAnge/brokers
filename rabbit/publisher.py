import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672))
channel = connection.channel()

channel.exchange_declare(exchange='logs', exchange_type='fanout')

message = "Сообщение"
channel.basic_publish(exchange='logs', routing_key='', body=message)

print(f"Sent: '{message}'")

connection.close()
