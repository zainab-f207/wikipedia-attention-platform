import json
import sys
import time
import requests
from kafka import KafkaProducer

URL = "https://stream.wikimedia.org/v2/stream/recentchange"
HEADERS = {"User-Agent": "de-learning-project (github.com/zainab-f207)"}
TOPIC = "wiki_changes"

# Usage: py produce_edits.py 2026-10-22T11:55:00Z 2026-10-22T16:05:00Z
# With no arguments it follows the live stream forever.
since = None
until = None
if len(sys.argv) > 2:
    since = sys.argv[1]
    until = sys.argv[2]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda row: json.dumps(row).encode("utf-8"),
)


def make_row(event):
    length = event.get("length") or {}
    old = length.get("old")
    new = length.get("new")
    if old is not None and new is not None:
        byte_change = new - old
    else:
        byte_change = None

    # No username on purpose: we do not need it, and it can be an IP address
    return {
        "event_id": event["meta"]["id"],
        "event_time": event["meta"]["dt"],
        "wiki": event.get("wiki"),
        "namespace": event.get("namespace"),
        "type": event.get("type"),
        "title": event.get("title"),
        "is_bot": event.get("bot"),
        "byte_change": byte_change,
    }


count = 0
reconnects = 0
last_id = None         # the stream's own bookmark for the last event we handled
pending_id = None
finished = False
after_end = 0          # events in a row that are past the end time
start = time.time()
print("Sending events to Kafka. Press Ctrl+C to stop.")

try:
    while not finished:
        try:
            headers = dict(HEADERS)
            params = {}
            if last_id is not None:
                headers["Last-Event-ID"] = last_id   # continue exactly where we stopped
            elif since is not None:
                params["since"] = since              # first connection only

            response = requests.get(URL, headers=headers, params=params, stream=True, timeout=60)
            response.raise_for_status()

            for line in response.iter_lines():
                line = line.decode("utf-8")

                if line.startswith("id: "):
                    pending_id = line[4:]
                    continue
                if not line.startswith("data: "):
                    continue

                event = json.loads(line[6:])
                event_time = event["meta"]["dt"]

                # Past the end time? Skip it. Stop only after 500 in a row.
                if until is not None and event_time >= until:
                    last_id = pending_id
                    after_end = after_end + 1
                    if after_end >= 500:
                        finished = True
                        break
                    continue
                after_end = 0

                producer.send(TOPIC, make_row(event))
                last_id = pending_id
                count = count + 1
                if count % 20000 == 0:
                    print("Sent", count, "events, now at", event_time)

            if not finished:
                time.sleep(5)   # the server closed the stream: wait, then continue
        except requests.exceptions.RequestException as error:
            reconnects = reconnects + 1
            print("Connection lost:", type(error).__name__, "- reconnecting in 5 seconds")
            time.sleep(5)
except KeyboardInterrupt:
    print("Stopping...")
finally:
    producer.flush()
    seconds = round(time.time() - start)
    print("Sent", count, "events in", seconds, "seconds,", reconnects, "reconnects")
