from kafka import KafkaConsumer
import sys
bootstrap_servers = ['localhost:9092']
topicName = 'umbrella'
consumer = KafkaConsumer (topicName)
try:
    for message in consumer:
        print(message)
except KeyboardInterrupt:
    sys.exit()