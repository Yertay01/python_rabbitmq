import pika
import sys

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(
    exchange='logs',
    exchange_type='fanout',
)

result = channel.queue_declare(queue="", durable= True, exclusive=True)

channel.queue_bind(
    exchange="logs",
    queue=result.method.queue,
    routing_key="black",
    )

message = "testing message"
channel.basic_publish(
    exchange='logs',
    routing_key='',
    body=message,
    properties=pika.BasicProperties(
        delivery_mode=pika.DeliveryMode.Persistent
    ))

print(f" [x] Send {message}")

connection.close()