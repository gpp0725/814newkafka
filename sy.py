from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
for _ in range(100,200):
    # producer.send('test1', b'little_message_bytes')
    producer.send('test3',bytes(_))
# from kafka import KafkaConsumer
#
# server = ['localhost:9092']
# consumer1 = KafkaConsumer('test1',
#                           group_id='consumer-2018819',
#                           bootstrap_servers=server,
#                           auto_offset_reset='earliest',
#                           enable_auto_commit=False,
#                           consumer_timeout_ms=1000
#                           )
# consumer2 = KafkaConsumer('test1',
#                           group_id='consumer-2018819',
#                           bootstrap_servers=server,
#                           auto_offset_reset='earliest',
#                           enable_auto_commit=False,
#                           consumer_timeout_ms=1000
#
#                           )
# cons_umer = []
# cons_umer.append(consumer1)
# cons_umer.append(consumer2)
# print(cons_umer)
