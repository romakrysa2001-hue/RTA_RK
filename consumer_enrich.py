from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'lab1',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    data = message.value
    if data['amount'] > 3000:
        data['risk_level'] = "HIGH"
    elif data['amount'] > 1000:
        data['risk_level'] = "MEDIUM"
    else:
        data['risk_level'] = "LOW"

    print(data)
