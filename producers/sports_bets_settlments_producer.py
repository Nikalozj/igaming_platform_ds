from __future__ import annotations

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

import psycopg2
from confluent_kafka import Producer
from dotenv import load_dotenv


load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_SPORTS_SETTLEMENTS")

DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

BATCH_SIZE = 10


producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "sports-bet-settlements-producer",
    "acks": "all",
    "retries": 5,
    "retry.backoff.ms": 1000,
    "message.timeout.ms": 10000
})


RESULT_STATUSES = ["won", "lost", "void", "cancelled", "cashout"]
RESULT_STATUS_WEIGHTS = [42, 45, 4, 3, 6]


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def q2(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def weighted_choice(values: list[str], weights: list[float]) -> str:
    return random.choices(values, weights=weights, k=1)[0]


def calculate_win_amount(
    result_status: str,
    stake_amount: Decimal,
    odds: Decimal
) -> Decimal:
    if result_status == "won":
        return q2(stake_amount * odds)

    if result_status in ("void", "cancelled"):
        return q2(stake_amount)

    if result_status == "cashout":
        lower_bound = stake_amount * Decimal("0.30")
        upper_bound = min(stake_amount * odds, stake_amount * Decimal("1.20"))
        value = Decimal(str(random.uniform(float(lower_bound), float(upper_bound))))
        return q2(value)

    return Decimal("0.00")


def claim_open_bets(conn, batch_size: int) -> list[dict]:
    """
    Atomically claim open bets and mark them as processing.
    Safe for multiple workers because of SKIP LOCKED.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH picked AS (
                SELECT bet_id, payload
                FROM sports_bets_registry
                WHERE status = 'open'
                ORDER BY created_at
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE sports_bets_registry r
            SET status = 'processing',
                picked_at = CURRENT_TIMESTAMP
            FROM picked
            WHERE r.bet_id = picked.bet_id
            RETURNING r.bet_id, picked.payload::text
            """,
            (batch_size,)
        )

        rows = cur.fetchall()
        conn.commit()

    claimed = []
    for bet_id, payload_text in rows:
        claimed.append({
            "bet_id": bet_id,
            "payload": json.loads(payload_text)
        })

    return claimed


def mark_bet_settled(conn, bet_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sports_bets_registry
            SET status = 'settled',
                settled_at = CURRENT_TIMESTAMP
            WHERE bet_id = %s
            """,
            (bet_id,)
        )
    conn.commit()


def reset_bet_to_open(conn, bet_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE sports_bets_registry
            SET status = 'open',
                picked_at = NULL
            WHERE bet_id = %s
            """,
            (bet_id,)
        )
    conn.commit()


def generate_settlement(bet_event: dict) -> dict:
    stake_amount = Decimal(str(bet_event["stake_amount"]))
    odds = Decimal(str(bet_event["odds"]))

    result_status = weighted_choice(RESULT_STATUSES, RESULT_STATUS_WEIGHTS)
    win_amount = calculate_win_amount(result_status, stake_amount, odds)

    return {
        "settlement_id": str(uuid.uuid4()),
        "bet_id": bet_event["bet_id"],
        "result_status": result_status,
        "win_amount": str(win_amount),
        "settled_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    }


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Delivery failed for key={msg.key()}: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")


def main() -> None:
    print(f"Producing to topic: {KAFKA_TOPIC}")
    print(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")

    conn = get_db_connection()

    try:
        while True:
            claimed_bets = claim_open_bets(conn, BATCH_SIZE)

            if not claimed_bets:
                print("No open bets available for settlement...")
                time.sleep(3)
                continue

            for item in claimed_bets:
                bet = item["payload"]
                bet_id = item["bet_id"]

                try:
                    settlement_event = generate_settlement(bet)

                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key=settlement_event["bet_id"],
                        value=json.dumps(settlement_event).encode("utf-8"),
                        callback=delivery_report
                    )

                    producer.poll(0)
                    producer.flush()

                    mark_bet_settled(conn, bet_id)

                    print("Settled bet:")
                    print(settlement_event)

                except Exception as e:
                    print(f"Failed to process bet_id={bet_id}: {e}")
                    reset_bet_to_open(conn, bet_id)

            time.sleep(2)

    except KeyboardInterrupt:
        print("Stopping settlements producer...")

    finally:
        producer.flush()
        conn.close()


if __name__ == "__main__":
    main()