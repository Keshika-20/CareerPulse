
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.generator.job_generator import generate_batch

try:
    from kafka import KafkaProducer
    from kafka.serializer import Serializer
except ImportError:  # pragma: no cover
    KafkaProducer = None
    Serializer = None


# class JsonSerializer(Serializer):
#     """Serialize Python event dictionaries into UTF-8 JSON bytes."""

#     def serialize(self, value: dict) -> bytes:
#         return json.dumps(value, ensure_ascii=False).encode("utf-8")

class JsonSerializer(Serializer):
    """Serialize Python event dictionaries into UTF-8 JSON bytes."""

    def serialize(self, *args) -> bytes:
        # Supports kafka-python versions that pass (topic, data),
        # (topic, headers, data), or just data.
        value = args[-1]
        return json.dumps(value, ensure_ascii=False).encode("utf-8")


class KafkaJobProducer:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "job-events",
        timeout_seconds: float = 10,
    ) -> None:
        if KafkaProducer is None or Serializer is None:
            raise RuntimeError(
                "Kafka publishing requires kafka-python. "
                "Install it with: pip install kafka-python"
            )

        servers = [
            server.strip()
            for server in bootstrap_servers.split(",")
            if server.strip()
        ]
        if not servers:
            raise ValueError("At least one Kafka bootstrap server is required.")

        if not topic.strip():
            raise ValueError("Kafka topic must not be empty.")

        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive.")

        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.timeout_seconds = timeout_seconds

        self._producer = KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=JsonSerializer(),
        )

    def send_event(self, event: dict) -> None:
        future = self._producer.send(self.topic, event)
        future.get(timeout=self.timeout_seconds)

    def send_batch(self, events: Iterable[dict]) -> None:
        for event in events:
            self.send_event(event)

    def close(self) -> None:
        if self._producer is not None:
            self._producer.flush(timeout=self.timeout_seconds)
            self._producer.close(timeout=self.timeout_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publish generated job events to Kafka."
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="job-events")
    parser.add_argument("--count", type=int, default=25)
    args = parser.parse_args()

    if args.count < 0:
        parser.error("--count must be non-negative")

    producer = KafkaJobProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
    )

    try:
        events = generate_batch(count=args.count)
        producer.send_batch(events)
        print(
            f"Kafka acknowledged {len(events)} events on '{args.topic}'"
        )
    finally:
        producer.close()


if __name__ == "__main__":
    main()
