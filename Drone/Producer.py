from confluent_kafka import Producer, KafkaError

conf = {
    'bootstrap.servers': 'localhost:9092',
    'security.protocol': 'PLAINTEXT',  # Asegúrate de que este protocolo coincida con la configuración del broker
    'api.version.request': True,
}

producer = Producer(**conf)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# Produce message
producer.produce('test-topic', key='key', value='value', callback=delivery_report)
producer.poll(0)
producer.flush()
