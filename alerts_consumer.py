from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'alerts',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='alerts-viewer',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print('Nasłuchuję tematu alerts...')
for message in consumer:
    a = message.value
    print(f"[ALERT] {a['tx_id']} | {a['amount']:.2f} PLN | {a.get('category','?')} | "
          f"h={a.get('hour','?')} | score={a['score']} | reguły: {a['rules']}")
