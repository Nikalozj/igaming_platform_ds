from __future__ import annotations

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_WALLET_WITHDRAWALS")

producer = Producer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "wallet-withdrawals-producer",
    }
)

USER_ID_MIN = 1
USER_ID_MAX = 100_000

CURRENCIES = ["USD", "EUR", "GEL"]
PAYOUT_METHODS = ["bank_transfer", "card_refund", "wallet_transfer"]
PROVIDERS = ["tbc_bank", "bog", "wise", "paypal"]
EUROPE_COUNTRY_CODES = [
    "AL","AD","AT","BY","BE","BA","BG","HR","CY","CZ",
    "DK","EE","FI","FR","DE","GI","GR","HU","IS","IE",
    "IT","XK","LV","LI","LT","LU","MT","MD","MC","ME",
    "NL","MK","NO","PL","PT","RO","RU","SM","RS","SK",
    "SI","ES","SE","CH","TR","UA","GB","VA"
]
DEVICES = ["mobile", "desktop", "tablet"]

STATUS_WEIGHTS = [
    ("completed", 70),
    ("pending", 20),
    ("failed", 5),
    ("rejected", 5),
]

AMOUNTS = [
    Decimal("10.00"),
    Decimal("20.00"),
    Decimal("30.00"),
    Decimal("50.00"),
    Decimal("100.00"),
    Decimal("150.00"),
    Decimal("250.00"),
    Decimal("500.00"),
]


def choose_weighted(values: list[tuple[str, int]]) -> str:
    items = [v[0] for v in values]
    weights = [v[1] for v in values]
    return random.choices(items, weights=weights, k=1)[0]


def money(value: Decimal) -> str:
    return str(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def generate_withdrawal_event() -> dict:
    withdrawal_status = choose_weighted(STATUS_WEIGHTS)
    amount = random.choice(AMOUNTS)

    return {
        "withdrawal_id": str(uuid.uuid4()),
        "user_id": random.randint(USER_ID_MIN, USER_ID_MAX),
        "amount": money(amount),
        "currency": random.choice(CURRENCIES),
        "payout_method": random.choice(PAYOUT_METHODS),
        "provider_name": random.choice(PROVIDERS),
        "provider_txn_id": str(uuid.uuid4()),
        "withdrawal_status": withdrawal_status,
        "country_code": random.choice(EUROPE_COUNTRY_CODES),
        "device_type": random.choice(DEVICES),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered to topic={msg.topic()} partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


def main() -> None:
    print(f"Producing to topic: {KAFKA_TOPIC}")

    try:
        while True:
            event = generate_withdrawal_event()

            producer.produce(
                topic=KAFKA_TOPIC,
                key=str(event["user_id"]),
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)

            print(json.dumps(event, ensure_ascii=False))
            time.sleep(random.uniform(1.0, 3.0))

    except KeyboardInterrupt:
        print("Stopping producer...")

    finally:
        producer.flush()


if __name__ == "__main__":
    main()