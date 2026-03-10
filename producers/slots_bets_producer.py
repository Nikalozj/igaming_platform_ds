from __future__ import annotations

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from dotenv import load_dotenv

from confluent_kafka import Producer

load_dotenv()
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_SLOTS_BETS")

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "slots-bets-producer"
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
DEVICE_WEIGHTS = [72, 25, 3]

BET_STATUSES = ["lost", "won", "cancelled", "error"]
BET_STATUS_WEIGHTS = [70.0, 25.0, 0.3, 0.2]


def q2(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def weighted_choice(values: list[str], weights: list[float]) -> str:
    return random.choices(values, weights=weights, k=1)[0]


def generate_stake_amount() -> Decimal:
    r = random.random()

    if r < 0.70:
        return q2(Decimal(str(random.uniform(0.20, 5.00))))
    if r < 0.92:
        return q2(Decimal(str(random.uniform(5.01, 20.00))))
    if r < 0.99:
        return q2(Decimal(str(random.uniform(20.01, 100.00))))

    return q2(Decimal(str(random.uniform(100.01, 500.00))))


def generate_win_amount(stake: Decimal, status: str) -> Decimal:
    if status == "lost":
        return Decimal("0.00")

    if status in {"refund", "cancelled"}:
        return q2(stake)

    if status == "error":
        return Decimal("0.00")

    r = random.random()

    if r < 0.55:
        multiplier = Decimal(str(random.uniform(0.10, 0.99)))
    elif r < 0.88:
        multiplier = Decimal(str(random.uniform(1.00, 3.00)))
    elif r < 0.98:
        multiplier = Decimal(str(random.uniform(3.01, 10.00)))
    else:
        multiplier = Decimal(str(random.uniform(10.01, 50.00)))

    return q2(stake * multiplier)


def generate_slots_bet() -> dict:
    stake_amount = generate_stake_amount()
    bet_status = weighted_choice(BET_STATUSES, BET_STATUS_WEIGHTS)
    win_amount = generate_win_amount(stake_amount, bet_status)

    event = {
        "bet_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 1_000_000),
        "game_id": random.randint(1, 300),
        "provider_id": random.randint(1, 25),
        "stake_amount": str(stake_amount),
        "win_amount": str(win_amount),
        "currency": weighted_choice(CURRENCIES, CURRENCY_WEIGHTS),
        "bet_status": bet_status,
        "device_type": weighted_choice(DEVICE_TYPES, DEVICE_WEIGHTS),
        "country_code": random.choice(EUROPE_COUNTRY_CODES),
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    }

    return event


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")


def main() -> None:
    print(f"Producing to topic: {KAFKA_TOPIC}")

    try:
        while True:
            event = generate_slots_bet()

            producer.produce(
                topic=KAFKA_TOPIC,
                key=event["bet_id"],
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report
            )

            producer.poll(0)
            print(event)

            time.sleep(1)

    except KeyboardInterrupt:
        print("Stopping producer...")

    finally:
        producer.flush()


if __name__ == "__main__":
    main()