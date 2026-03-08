from __future__ import annotations

import json
import os
import signal
from typing import Any

from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaException, KafkaError
import psycopg2
from psycopg2.extras import execute_batch


# Load .env
load_dotenv()

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")

# Postgres
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

BATCH_SIZE = 200
POLL_TIMEOUT = 1.0

running = True

def shutdown_handler(signum, frame):
    global running
    print("Shutting down consumer...")
    running = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


def get_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def create_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })


INSERT_SQL = """
INSERT INTO roulette_events (
    event_id,
    event_name,
    event_ts,
    user_id,
    session_id,
    table_id,
    round_id,
    bet_id,
    bet_type,
    selection,
    stake,
    currency,
    country,
    device,
    kafka_partition,
    kafka_offset
)
VALUES (
    %(event_id)s,
    %(event_name)s,
    %(event_ts)s,
    %(user_id)s,
    %(session_id)s,
    %(table_id)s,
    %(round_id)s,
    %(bet_id)s,
    %(bet_type)s,
    %(selection)s,
    %(stake)s,
    %(currency)s,
    %(country)s,
    %(device)s,
    %(kafka_partition)s,
    %(kafka_offset)s
)
ON CONFLICT (event_id) DO NOTHING
"""


def flush_batch(cursor, rows: list[dict[str, Any]]):
    if rows:
        execute_batch(cursor, INSERT_SQL, rows, page_size=len(rows))


def main():
    print("Starting Kafka → Postgres consumer")

    print(f"Kafka server: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print(f"Consumer group: {KAFKA_GROUP_ID}")

    print(f"Postgres: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

    consumer = create_consumer()
    consumer.subscribe([KAFKA_TOPIC])

    conn = get_postgres_connection()
    conn.autocommit = False

    batch = []

    try:
        with conn.cursor() as cur:

            while running:

                msg = consumer.poll(POLL_TIMEOUT)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                payload = json.loads(msg.value().decode("utf-8"))

                row = {
                    "event_id": payload["event_id"],
                    "event_name": payload["event_name"],
                    "event_ts": payload["event_ts"],
                    "user_id": payload["user_id"],
                    "session_id": payload["session_id"],
                    "table_id": payload["table_id"],
                    "round_id": payload["round_id"],
                    "bet_id": payload["bet_id"],
                    "bet_type": payload["bet_type"],
                    "selection": payload.get("selection"),
                    "stake": payload["stake"],
                    "currency": payload["currency"],
                    "country": payload["country"],
                    "device": payload["device"],
                    "kafka_partition": msg.partition(),
                    "kafka_offset": msg.offset()
                }

                batch.append(row)

                if len(batch) >= BATCH_SIZE:

                    flush_batch(cur, batch)
                    conn.commit()
                    consumer.commit(asynchronous=False)

                    print(f"Inserted batch of {len(batch)} rows")

                    batch.clear()

    finally:

        try:
            if batch:
                with conn.cursor() as cur:
                    flush_batch(cur, batch)
                    conn.commit()
        except Exception:
            conn.rollback()

        consumer.close()
        conn.close()

        print("Consumer stopped")


if __name__ == "__main__":
    main()