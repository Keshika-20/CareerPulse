
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.generator.job_generator import TITLES, JobEventSimulator, generate_batch

try:
    from kafka import KafkaProducer
    from kafka.serializer import Serializer
except ImportError:  # pragma: no cover
    KafkaProducer = None
    # Keeps the module importable so KafkaJobProducer can raise a clear error.
    Serializer = object


class JsonSerializer(Serializer):
    """Serialize Python event dictionaries into UTF-8 JSON bytes."""

    def serialize(self, *args) -> bytes:
        # Supports kafka-python versions that pass (topic, data),
        # (topic, headers, data), or just data.
        value = args[-1]
        if isinstance(value, str):
            # Pre-serialized payloads (e.g. deliberately malformed demo
            # records) are sent verbatim.
            return value.encode("utf-8")
        return json.dumps(value, ensure_ascii=False).encode("utf-8")


class KeySerializer(Serializer):
    """Serialize message keys (job IDs) as UTF-8."""

    def serialize(self, *args) -> bytes | None:
        key = args[-1]
        return None if key is None else str(key).encode("utf-8")


class KafkaJobProducer:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "job-events",
        timeout_seconds: float = 10,
    ) -> None:
        if KafkaProducer is None:
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
            key_serializer=KeySerializer(),
            # acks=all + idempotence: broker-side de-duplication of retried
            # sends and no reordering within a partition even with several
            # requests in flight, so each job's events keep their order.
            acks="all",
            enable_idempotence=True,
            # Small linger + larger batches + compression trade a few ms of
            # latency for far fewer, smaller produce requests.
            linger_ms=5,
            batch_size=64 * 1024,
            compression_type="gzip",
        )

    @staticmethod
    def message_key(event: Any) -> Optional[str]:
        # Keying by job_id keeps every event of one posting on one partition,
        # so consumers see that posting's lifecycle in order.
        if isinstance(event, dict) and event.get("job_id") is not None:
            return str(event["job_id"])
        return None

    def send_event(self, event: Any) -> None:
        """Send one event and wait for the broker acknowledgement.

        Used where the caller must know the event is durable before moving on
        (the ingestion outbox marks events published only after this returns).
        """
        future = self._producer.send(self.topic, value=event, key=self.message_key(event))
        future.get(timeout=self.timeout_seconds)

    def send_async(self, event: Any, key: Optional[str] = None):
        """Queue one event without waiting; returns the delivery future."""
        return self._producer.send(self.topic, value=event, key=key or self.message_key(event))

    def flush(self) -> None:
        self._producer.flush()

    def send_batch(self, events: Iterable[Any]) -> int:
        """Pipeline many sends, then wait once for every acknowledgement.

        Raises on the first failed delivery after flushing, so a batch is only
        reported as sent when all of it is acknowledged.
        """
        futures = [self.send_async(event) for event in events]
        self._producer.flush()  # bounded by the producer's delivery timeout
        for future in futures:
            future.get(timeout=self.timeout_seconds)
        return len(futures)

    def close(self) -> None:
        if self._producer is not None:
            self._producer.flush(timeout=self.timeout_seconds)
            self._producer.close(timeout=self.timeout_seconds)


def ensure_topics(
    bootstrap_servers: str,
    topics: Iterable[str],
    *,
    partitions: int = 3,
    replication_factor: int = 1,
    retries: int = 20,
    retry_delay_seconds: float = 3.0,
) -> None:
    """Create Kafka topics that do not exist yet (idempotent).

    Consumers call this before subscribing: Spark's Kafka source fails if its
    topic does not exist, so starting the stream before the first producer
    would otherwise crash it.
    """
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    servers = [server.strip() for server in bootstrap_servers.split(",") if server.strip()]
    last_error: Optional[Exception] = None
    for attempt in range(retries):
        try:
            admin = KafkaAdminClient(bootstrap_servers=servers, client_id="careerpulse-admin")
            try:
                missing = [topic for topic in topics if topic not in set(admin.list_topics())]
                if missing:
                    try:
                        admin.create_topics([
                            NewTopic(name=topic, num_partitions=partitions, replication_factor=replication_factor)
                            for topic in missing
                        ])
                    except TopicAlreadyExistsError:
                        pass  # created concurrently by another client
                return
            finally:
                admin.close()
        except Exception as exc:  # broker may still be starting
            last_error = exc
            if attempt + 1 < retries:
                time.sleep(retry_delay_seconds)
    raise RuntimeError(f"Unable to ensure Kafka topics {list(topics)}: {last_error}")


def stream_events(
    producer: KafkaJobProducer,
    simulator: JobEventSimulator,
    *,
    rate: float,
    duration_seconds: Optional[float] = None,
    max_events: Optional[int] = None,
    surge_title: Optional[str] = None,
    surge_after: float = 60.0,
    surge_duration: float = 120.0,
) -> int:
    """Publish simulator events continuously at ``rate`` events per second.

    With ``surge_title``, a hiring surge for that title runs from
    ``surge_after`` seconds for ``surge_duration`` seconds.
    """
    interval = 1.0 / rate
    started = time.monotonic()
    sent = 0
    pending = []
    while (max_events is None or sent < max_events) and (
        duration_seconds is None or time.monotonic() - started < duration_seconds
    ):
        if surge_title:
            elapsed = time.monotonic() - started
            active = surge_after <= elapsed < surge_after + surge_duration
            if active != (simulator.surge_title == surge_title):
                simulator.surge_title = surge_title if active else None
                print(f"Hiring surge for {surge_title} {'started' if active else 'ended'}", flush=True)
        pending.append(producer.send_async(simulator.next_event()))
        sent += 1
        if sent % 50 == 0:
            # Surface delivery errors periodically instead of blocking per event.
            for future in pending:
                future.get(timeout=producer.timeout_seconds)
            pending.clear()
            print(f"Published {sent} events", flush=True)
        delay = started + sent * interval - time.monotonic()
        if delay > 0:
            time.sleep(delay)
    for future in pending:
        future.get(timeout=producer.timeout_seconds)
    return sent


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publish generated job events to Kafka."
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="job-events")
    parser.add_argument("--count", type=int, default=None, help="Events to send (default 25), or an optional cap in --stream mode")
    parser.add_argument("--stream", action="store_true", help="Send simulated lifecycle events continuously")
    parser.add_argument("--rate", type=float, default=5.0, help="Events per second in --stream mode")
    parser.add_argument("--duration", type=float, default=None, help="Stop --stream mode after N seconds")
    parser.add_argument("--backfill-hours", type=float, default=0, help="Send --count simulated events spread over the past N hours")
    parser.add_argument("--invalid-rate", type=float, default=0.0, help="Fraction of malformed records to include")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--surge-title", default=None, help="Simulate a hiring surge for this job title (e.g. 'Data Engineer')")
    parser.add_argument("--surge-after", type=float, default=60.0, help="Stream mode: seconds before the surge starts")
    parser.add_argument("--surge-duration", type=float, default=120.0, help="Stream mode: surge length in seconds")
    args = parser.parse_args()

    if args.count is not None and args.count < 0:
        parser.error("--count must be non-negative")
    batch_count = 25 if args.count is None else args.count
    if args.rate <= 0:
        parser.error("--rate must be positive")
    if args.surge_title and args.surge_title not in TITLES:
        parser.error(f"--surge-title must be one of: {', '.join(TITLES)}")

    producer = KafkaJobProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
    )
    simulator = JobEventSimulator(seed=args.seed, invalid_rate=args.invalid_rate)

    try:
        if args.backfill_hours:
            span = timedelta(hours=args.backfill_hours)
            # In a backfill the surge occupies 90-95% of the time span.
            surge = (args.surge_title, 0.90, 0.95) if args.surge_title else None
            events = simulator.generate(
                batch_count, start=datetime.now(timezone.utc) - span, span=span, surge=surge
            )
            producer.send_batch(events)
            sent = len(events)
            if args.stream:
                sent += stream_events(
                    producer, simulator, rate=args.rate, duration_seconds=args.duration,
                    surge_title=args.surge_title, surge_after=args.surge_after,
                    surge_duration=args.surge_duration,
                )
        elif args.stream:
            sent = stream_events(
                producer, simulator, rate=args.rate, duration_seconds=args.duration,
                max_events=args.count, surge_title=args.surge_title,
                surge_after=args.surge_after, surge_duration=args.surge_duration,
            )
        elif args.invalid_rate:
            events = simulator.generate(batch_count)
            producer.send_batch(events)
            sent = len(events)
        else:
            events = generate_batch(count=batch_count)
            producer.send_batch(events)
            sent = len(events)
        print(f"Kafka acknowledged {sent} events on '{args.topic}'")
    except KeyboardInterrupt:
        print("Stopped publishing.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
