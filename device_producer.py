import threading
import time
import random

from kafka import KafkaProducer
from settings import BOOTSTRAP_SERVERS, KAFKA_TOPIC


class DeviceProducer(threading.Thread):
    def __init__(self, device_id: str, producer: KafkaProducer, topic: str, stop_event: threading.Event):
        super().__init__()
        self.device_id = device_id
        self.producer = producer
        self.topic = topic
        self.stop_event = stop_event


    def run(self):
        while not self.stop_event.is_set():
            value = random.randint(0, 100)
            self.producer.send(topic=self.topic, key=self.device_id, value=value)
            self.producer.flush()
            print(f"Produced: {self.device_id} : {value}")
            time.sleep(10)


if __name__ == '__main__':
    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'key_serializer': lambda key: str(key).encode(),
        'value_serializer': lambda value: str(value).encode()
    }

    kafka_producer = KafkaProducer(**config)

    stop_event = threading.Event()
    
    producer1 = DeviceProducer('device1', kafka_producer, KAFKA_TOPIC, stop_event)
    producer2 = DeviceProducer('device2', kafka_producer, KAFKA_TOPIC, stop_event)
    
    producer1.start()
    producer2.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("KeyboardInterrupt received. Stopping...")
        stop_event.set()

        producer1.join()
        producer2.join()
        kafka_producer.close()
