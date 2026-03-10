from __future__ import annotations

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv


load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_SPORTS_BETS")

BET_REGISTRY_FILE = Path("../runtime/sports_bets_registry.jsonl")

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "sports-bets-producer",
    "acks": "all",
    "retries": 5,
    "retry.backoff.ms": 1000,
    "message.timeout.ms": 10000
})


EUROPE_COUNTRY_CODES = [
    "AL","AD","AT","BY","BE","BA","BG","HR","CY","CZ",
    "DK","EE","FI","FR","DE","GI","GR","HU","IS","IE",
    "IT","XK","LV","LI","LT","LU","MT","MD","MC","ME",
    "NL","MK","NO","PL","PT","RO","RU","SM","RS","SK",
    "SI","ES","SE","CH","TR","UA","GB","VA"
]

CURRENCIES = ["EUR", "USD", "GBP"]
CURRENCY_WEIGHTS = [80, 15, 5]

DEVICE_TYPES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [70, 27, 3]

BET_STATUSES = ["open", "cancelled", "error"]
BET_STATUS_WEIGHTS = [99.2, 0.5, 0.3]


def q2(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def weighted_choice(values: list[str], weights: list[float]) -> str:
    return random.choices(values, weights=weights, k=1)[0]


def generate_stake_amount() -> Decimal:
    r = random.random()

    if r < 0.70:
        return q2(Decimal(str(random.uniform(1.00, 10.00))))
    if r < 0.92:
        return q2(Decimal(str(random.uniform(10.01, 50.00))))
    if r < 0.99:
        return q2(Decimal(str(random.uniform(50.01, 200.00))))
    return q2(Decimal(str(random.uniform(200.01, 1000.00))))


def generate_odds() -> Decimal:
    r = random.random()

    if r < 0.55:
        odds = random.uniform(1.20, 1.80)
    elif r < 0.85:
        odds = random.uniform(1.81, 3.50)
    elif r < 0.97:
        odds = random.uniform(3.51, 7.00)
    else:
        odds = random.uniform(7.01, 20.00)

    return Decimal(str(round(odds, 3)))


def generate_ticket_id() -> str:
    return f"TKT_{uuid.uuid4().hex[:12].upper()}"


def save_bet_for_settlement(event: dict) -> None:
    with BET_REGISTRY_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")


def generate_sports_bet() -> dict:
    stake_amount = generate_stake_amount()
    odds = generate_odds()
    bet_status = weighted_choice(BET_STATUSES, BET_STATUS_WEIGHTS)

    event = {
        "bet_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 1_000_000),
        "ticket_id": generate_ticket_id(),
        "event_id": random.randint(100000, 200000),
        "market_id": random.randint(1, 50),
        "selection_id": random.randint(1, 3),
        "odds": str(odds),
        "stake_amount": str(stake_amount),
        "currency": weighted_choice(CURRENCIES, CURRENCY_WEIGHTS),
        "bet_status": bet_status,
        "is_live": random.choices([True, False], weights=[30, 70])[0],
        "device_type": weighted_choice(DEVICE_TYPES, DEVICE_WEIGHTS),
        "country_code": random.choice(EUROPE_COUNTRY_CODES),
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    }

    return event


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Delivery failed for key={msg.key()}: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")


def main() -> None:
    print(f"Producing to topic: {KAFKA_TOPIC}")
    print(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")

    counter = 0

    try:
        while True:
            event = generate_sports_bet()

            producer.produce(
                topic=KAFKA_TOPIC,
                key=event["bet_id"],
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report
            )

            producer.poll(1)

            if event["bet_status"] == "open":
                save_bet_for_settlement(event)

            print(event)

            counter += 1
            if counter % 100 == 0:
                producer.flush()

            time.sleep(1)

    except KeyboardInterrupt:
        print("Stopping producer...")

    finally:
        producer.flush()


if __name__ == "__main__":
    main()