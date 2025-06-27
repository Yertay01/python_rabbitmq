import pika
import sys
import os
import time


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.exchange_declare(
        exchange="logs",
        exchange_type="fanout",
    )

    result = channel.queue_declare(queue="", durable=True, exclusive=True)

    queue_name = result.method.queue

    channel.queue_bind(
        exchange="logs",
        queue=queue_name,
    )
    
    print(' [*] Waiting for logs.To exit press command+C')
    def callback(
            ch,
            method,
            properties,
            body,
    ):
        # print(f" [x] Received {body.decode()}")
        # time.sleep(body.count(b'.'))
        # print(f" [x] Done")
        # ch.basic_ack(delivery_tag = method.delivery_tag)

        print(' [x] {body}')


    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=True,
        )
    
    #channel.basic_qos(prefetch_count=1)
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)