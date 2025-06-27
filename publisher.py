from config import (
    get_connection,
    configure_logging,
    MQ_EXCHANGE,
    MQ_ROUTING_KEY,
)
import logging
import time

from rabbit.common import SimpleRabbit


log = logging.getLogger(__name__)

class Publisher(SimpleRabbit):
    
    def produce_message(self, idx: int) -> None:
        message_body = f"New message #{idx:02d}"
        log.info("Publish message %s", message_body)
        self.channel.basic_publish(
            exchange=MQ_EXCHANGE,
            routing_key=MQ_ROUTING_KEY,
            body=message_body,
        )
        log.warning("Publish message %s", message_body)

def main():
    configure_logging()
    with Publisher() as publisher:
            publisher.declare_queue()
            for idx in range(1, 21):
                publisher.produce_message(idx=idx)
                time.sleep(0.5)
            

    while True:
        pass

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Ты кто такой? Давай, до свидания!")