from confluent_kafka import Producer
import socket
import random
import time
import json

# Kafka config
config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname()
}
producer = Producer(config)

# Events simulation config
nEventsToSend = 20 # number of events to send of each event type
simpleEventsSchema = {
    'HospitalFridgeTemp': {
        'value': {
            'min': -4,
            'max': 10
        },
        'unit': 'C'
    }
}
sensorId = "sensor1"

def get_event(eventSchema, timestamp, sensorId) -> dict:
    event = {'timestamp': timestamp, 'sensorId': sensorId}

    for key in eventSchema:
        value = eventSchema[key]
        if isinstance(value, dict):
            event[key] = random.uniform(value['min'], value['max'])
        else:
            event[key] = value
    
    return event

def init_simulation():
    for key in simpleEventsSchema:
        for i in range(nEventsToSend):
            event = get_event(simpleEventsSchema[key], int(time.time()), sensorId)
            producer.produce(key, key="event {0}".format(i), value=json.dumps(event))
    producer.flush()

if __name__ == "__main__":
    init_simulation()