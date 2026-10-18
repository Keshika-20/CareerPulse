"""Real-time alert consumer: tails the ``job-alerts`` Kafka topic.

A minimal downstream "real-time app": it prints every hiring-surge and
data-quality alert as it is published, de-duplicating redeliveries by
``alert_id``.

    python -m src.streaming.alert_consumer --bootstrap-servers localhost:9092
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def format_alert(alert: dict) -> str:
    severity = str(alert.get("severity", "info")).upper()
    when = str(alert.get("window_start") or alert.get("created_at") or "")[:19].replace("T", " ")
    return f"[{severity:8}] {alert.get('alert_type', 'ALERT'):13} {when}  {alert.get('message', '')}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Print CareerPulse alerts as they arrive.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="job-alerts")
    parser.add_argument("--from-beginning", action="store_true", help="Replay alerts already in the topic")
    parser.add_argument("--max-messages", type=int, default=None, help="Exit after N alerts (for demos/tests)")
    args = parser.parse_args()

    from kafka import KafkaConsumer
    from kafka.serializer import Deserializer

    from src.streaming.kafka_producer import ensure_topics

    class JsonDeserializer(Deserializer):
        def deserialize(self, *args):
            return json.loads(args[-1].decode("utf-8"))

    ensure_topics(args.bootstrap_servers, [args.topic])
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=[server.strip() for server in args.bootstrap_servers.split(",")],
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        enable_auto_commit=False,
        value_deserializer=JsonDeserializer(),
        consumer_timeout_ms=-1 if args.max_messages is None else 30000,
    )
    print(f"Listening for alerts on '{args.topic}' ({args.bootstrap_servers}). Ctrl+C to stop.", flush=True)
    seen = set()
    received = 0
    try:
        for message in consumer:
            alert = message.value
            if alert.get("alert_id") in seen:
                continue
            seen.add(alert.get("alert_id"))
            print(format_alert(alert), flush=True)
            received += 1
            if args.max_messages is not None and received >= args.max_messages:
                break
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
