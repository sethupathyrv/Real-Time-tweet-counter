from kafka import KafkaProducer
bootstrap_servers=['localhost:9092']
topicName='umbrella'
producer=KafkaProducer(bootstrap_servers=['localhost:9092'])

producer.send(topicName, b'Hello World')

producer.flush()