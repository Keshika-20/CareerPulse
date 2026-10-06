from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, List

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.generator.job_generator import generate_batch

try:
    from kafka import KafkaProducer
except ImportError:  # pragma: no cover
    KafkaProducer = None


class KafkaJobProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092", topic: str = "job_events") -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self._producer = None

        if KafkaProducer is not None:
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers.split(","),
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                api_version=(0, 10, 2),
            )

    def send_event(self, event: dict) -> None:
        if self._producer is None:
            print(json.dumps(event))
            return
        self._producer.send(self.topic, event)

    def send_batch(self, events: Iterable[dict]) -> None:
        for event in events:
            self.send_event(event)

    def close(self) -> None:
        if self._producer is not None:
            self._producer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish generated job events to Kafka or stdout.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka broker list")
    parser.add_argument("--topic", default="job_events", help="Kafka topic")
    parser.add_argument("--count", type=int, default=25, help="Events to send")
    args = parser.parse_args()

    producer = KafkaJobProducer(bootstrap_servers=args.bootstrap_servers, topic=args.topic)
    try:
        events = generate_batch(count=args.count)
        producer.send_batch(events)
        print(f"Produced {len(events)} events to topic '{args.topic}'")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
