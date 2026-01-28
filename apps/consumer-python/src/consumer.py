import argparse
import json
import os
import signal
import sys
import time
from typing import Any, Dict, Optional

import psycopg
from psycopg.rows import dict_row
from confluent_kafka import Consumer, KafkaException


def parse_json(value: Optional[bytes]) -> Dict[str, Any]:
    if value is None:
        raise ValueError("message value is None")
    return json.loads(value.decode("utf-8"))


def required_str(obj: Dict[str, Any], key: str) -> str:
    v = obj.get(key)
    if not isinstance(v, str) or not v:
        raise ValueError(f"missing/invalid '{key}'")
    return v


def connect_db(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn, row_factory=dict_row)


def insert_raw_event(conn: psycopg.Connection, event: Dict[str, Any]) -> None:
    # Our producer emits an envelope with payload nested under "payload"
    event_id = required_str(event, "event_id")
    event_type = required_str(event, "event_type")
    source = required_str(event, "source")
    occurred_at = required_str(event, "occurred_at")

    payload = event.get("payload", {})
    if not isinstance(payload, dict):
        payload = {"_raw_payload": payload}

    payload_json = json.dumps(payload)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw_events (event_id, event_type, source, occurred_at, payload_json)
            VALUES (%s, %s, %s, %s, %s::jsonb)
            """,
            (event_id, event_type, source, occurred_at, payload_json),
        )
    conn.commit()


def main() -> int:
    parser = argparse.ArgumentParser(description="T0 Kafka consumer: writes events to Postgres raw_events.")
    parser.add_argument("--brokers", default=os.getenv("KAFKA_BROKERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "events"))
    parser.add_argument("--group", default=os.getenv("KAFKA_GROUP_ID", "raw-events-writer"))
    parser.add_argument("--db-dsn", default=os.getenv("POSTGRES_DSN", "postgresql://platform:platform@localhost:5432/platform"))
    parser.add_argument("--poll-ms", type=int, default=int(os.getenv("POLL_MS", "1000")))
    args = parser.parse_args()

    # Consumer is at-least-once in this step; we'll harden later with idempotent sinks / EOS story.
    conf = {
        "bootstrap.servers": args.brokers,
        "group.id": args.group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # we will commit only after DB write succeeds
    }

    consumer = Consumer(conf)
    consumer.subscribe([args.topic])

    conn = connect_db(args.db_dsn)

    stop = False

    def handle_sig(_sig, _frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    print(
        f"Consuming topic='{args.topic}' brokers='{args.brokers}' group='{args.group}' "
        f"db='{args.db_dsn}'"
    )

    written = 0
    try:
        while not stop:
            msg = consumer.poll(timeout=args.poll_ms / 1000.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            try:
                event = parse_json(msg.value())
                insert_raw_event(conn, event)

                # Commit after successful DB commit to avoid skipping messages.
                consumer.commit(message=msg, asynchronous=False)

                written += 1
                if written % 100 == 0:
                    print(f"[OK] written={written} last_offset={msg.offset()} partition={msg.partition()}")
            except Exception as e:
                # For T0: log and continue. DLQ comes in T1.
                print(f"[ERROR] failed to process message: {e}", file=sys.stderr)
                # small sleep to avoid tight error loops
                time.sleep(0.1)

    finally:
        try:
            consumer.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    print(f"Done. written={written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
