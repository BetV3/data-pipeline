import argparse
import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_event(event_type: str, source: str) -> dict:
    # Simple, stable event envelope we can evolve later (Avro comes in T1)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "source": source,
        "occurred_at": utc_now_iso(),
        "payload": {
            "user_id": random.randint(1, 1_000_000),
            "value": round(random.random() * 100, 4),
            "tags": ["t0", "demo"],
        },
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"[DELIVERY-ERROR] {err}", file=sys.stderr)
    else:
        # Keep it terse but useful
        print(f"[DELIVERED] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


def main() -> int:
    parser = argparse.ArgumentParser(description="T0 Kafka producer: emits JSON events to Kafka.")
    parser.add_argument("--brokers", default=os.getenv("KAFKA_BROKERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "events"))
    parser.add_argument("--event-type", default=os.getenv("EVENT_TYPE", "event_received"))
    parser.add_argument("--source", default=os.getenv("EVENT_SOURCE", "producer-python"))
    parser.add_argument("--rate", type=float, default=float(os.getenv("EVENTS_PER_SEC", "25")))
    parser.add_argument("--seconds", type=int, default=int(os.getenv("RUN_SECONDS", "10")))
    args = parser.parse_args()

    conf = {
        "bootstrap.servers": args.brokers,
        # Dev-friendly reliability knobs (we’ll revisit for exactly-once later)
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,
        "batch.num.messages": 1000,
    }

    producer = Producer(conf)

    stop = False

    def handle_sig(_sig, _frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    interval = 1.0 / max(args.rate, 0.0001)
    end_time = time.time() + args.seconds

    print(
        f"Producing to topic='{args.topic}' brokers='{args.brokers}' "
        f"rate={args.rate}/s for {args.seconds}s"
    )

    sent = 0
    while not stop and time.time() < end_time:
        event = build_event(args.event_type, args.source)
        key = event["event_type"].encode("utf-8")
        value = json.dumps(event, separators=(",", ":")).encode("utf-8")

        producer.produce(args.topic, key=key, value=value, on_delivery=delivery_report)
        sent += 1

        # Serve delivery callbacks
        producer.poll(0)

        time.sleep(interval)

    print("Flushing...")
    producer.flush(10)
    print(f"Done. Sent={sent}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
