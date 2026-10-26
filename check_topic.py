import json
from collections import Counter
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "wiki_changes",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    consumer_timeout_ms=10000,   # stop after 10 seconds of silence
    value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
)

per_hour = Counter()
ids = set()
total = 0

for message in consumer:
    row = message.value
    total = total + 1
    ids.add(row["event_id"])
    per_hour[row["event_time"][:13]] += 1   # e.g. 2026-10-22T12

print("Total messages:", total)
print("Unique event_ids:", len(ids))
print("Duplicates:", total - len(ids))
print()
for hour in sorted(per_hour):
    print(hour, per_hour[hour])