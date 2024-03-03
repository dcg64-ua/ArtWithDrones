import socket
import sys
import time
import threading
import secrets
import string
from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaError


conf = {'bootstrap.servers': "localhost" + ":" + str(9092),
    'group.id': "my-group",
    'auto.offset.reset': 'earliest'}

consumer = Consumer(conf)

tp = TopicPartition("drones", int(1))
print("Partición: drones", int(1))
consumer.assign([tp])
mensaje = ""

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        print("La partición está vacía")
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break
    if msg.value() is not None:
        msg = msg.value().decode('utf-8')
        print('Received message: {}'.format(msg))
        consumer.commit()

consumer.close()

