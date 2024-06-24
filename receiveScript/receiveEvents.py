from confluent_kafka import Consumer, KafkaError
import uuid

# Kafka config
config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': uuid.uuid1()
}
consumer = Consumer(config)

# Events config
eventsToSuscribe = ['HospitalFridgeAlert', 'HospitalLabAlert', 'HospitalFireAlert']
timeout = 120 # in seconds

def consumeEvents():
    consumer.subscribe(eventsToSuscribe)
    while True:
        message = consumer.poll(timeout)

        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(message.error())
                break
        print("{}: {}".format(message.topic(), message.value().decode('utf-8')))
    consumer.close()

if __name__ == "__main__":
    consumeEvents()