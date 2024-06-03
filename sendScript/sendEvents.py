from confluent_kafka import Producer
import socket

config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname()
}

producer = Producer(config)