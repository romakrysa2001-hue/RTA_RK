from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'lab1',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

# TWÓJ KOD
for message in consumer:
    data = message.value
    store = data['store']
    amount = data['amount']

    # 1. Zlicz transakcje per sklep
    store_counts[store] += 1

    # 2. Sumuj kwoty per sklep
    total_amount[store] += amount

    # 3. Co 10 wiadomości wypisz podsumowanie
    msg_count += 1
    if msg_count % 10 == 0:
        print(f"\n--- Podsumowanie po {msg_count} wiadomościach ---")
        for s in store_counts:
            print(f"  {s}: {store_counts[s]} transakcji, suma: {total_amount[s]:.2f}")
