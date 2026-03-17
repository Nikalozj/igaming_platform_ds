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
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_LC_BETS")

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "live-casino-bets-producer",
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

BET_STATUSES = ["lost", "won", "cancelled", "error"]
BET_STATUS_WEIGHTS = [50.5, 48.8, 0.4, 0.3]

GAME_TYPES = ["baccarat", "roulette", "blackjack"]
GAME_TYPE_WEIGHTS = [45, 35, 20]

PROVIDERS = list(range(1, 16))
DEALER_IDS = [f"DLR_{i:04d}" for i in range(1, 201)]

GAME_ID_MAP = {
    "baccarat": list(range(1001, 1021)),
    "roulette": list(range(2001, 2021)),
    "blackjack": list(range(3001, 3021))
}

TABLE_ID_MAP = {
    "baccarat": [f"BAC_{i:03d}" for i in range(1, 31)],
    "roulette": [f"ROU_{i:03d}" for i in range(1, 31)],
    "blackjack": [f"BJ_{i:03d}" for i in range(1, 31)]
}


def q2(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def weighted_choice(values: list[str], weights: list[float]) -> str:
    return random.choices(values, weights=weights, k=1)[0]


def generate_stake_amount(game_type: str) -> Decimal:
    r = random.random()

    if game_type == "blackjack":
        if r < 0.65:
            return q2(Decimal(str(random.uniform(1.00, 20.00))))
        if r < 0.92:
            return q2(Decimal(str(random.uniform(20.01, 100.00))))
        if r < 0.99:
            return q2(Decimal(str(random.uniform(100.01, 300.00))))
        return q2(Decimal(str(random.uniform(300.01, 1000.00))))

    if game_type == "baccarat":
        if r < 0.70:
            return q2(Decimal(str(random.uniform(1.00, 25.00))))
        if r < 0.93:
            return q2(Decimal(str(random.uniform(25.01, 150.00))))
        if r < 0.99:
            return q2(Decimal(str(random.uniform(150.01, 500.00))))
        return q2(Decimal(str(random.uniform(500.01, 2000.00))))

    if r < 0.75:
        return q2(Decimal(str(random.uniform(0.50, 10.00))))
    if r < 0.94:
        return q2(Decimal(str(random.uniform(10.01, 50.00))))
    if r < 0.995:
        return q2(Decimal(str(random.uniform(50.01, 200.00))))
    return q2(Decimal(str(random.uniform(200.01, 1000.00))))


def generate_bet_type(game_type: str) -> str:
    if game_type == "baccarat":
        return random.choices(
            ["player", "banker", "tie"],
            weights=[44, 50, 6],
            k=1
        )[0]

    if game_type == "roulette":
        return random.choices(
            ["red", "black", "odd", "even", "straight"],
            weights=[26, 26, 20, 20, 8],
            k=1
        )[0]

    return random.choices(
        ["main", "side"],
        weights=[88, 12],
        k=1
    )[0]


def pick_status(game_type: str, bet_type: str) -> str:
    if game_type == "roulette" and bet_type == "straight":
        return random.choices(
            ["lost", "won", "cancelled", "error"],
            weights=[97.3, 2.0, 0.4, 0.3],
            k=1
        )[0]

    if game_type == "blackjack" and bet_type == "side":
        return random.choices(
            ["lost", "won", "cancelled", "error"],
            weights=[89.3, 10.0, 0.4, 0.3],
            k=1
        )[0]

    return weighted_choice(BET_STATUSES, BET_STATUS_WEIGHTS)


def generate_win_amount(stake: Decimal, status: str, game_type: str, bet_type: str) -> Decimal:
    if status == "lost":
        return Decimal("0.00")

    if status == "cancelled":
        return q2(stake)

    if status == "error":
        return Decimal("0.00")

    if game_type == "baccarat":
        if bet_type == "tie":
            multiplier = Decimal("8.00")
        elif bet_type == "banker":
            multiplier = Decimal(str(random.choice([0.95, 1.00])))
        else:
            multiplier = Decimal("1.00")
        return q2(stake * multiplier)

    if game_type == "roulette":
        if bet_type == "straight":
            multiplier = Decimal("35.00")
        else:
            multiplier = Decimal("1.00")
        return q2(stake * multiplier)

    if game_type == "blackjack":
        if bet_type == "side":
            multiplier = Decimal(str(random.choice([2.00, 5.00, 10.00])))
        else:
            multiplier = Decimal(str(random.choice([1.00, 1.50])))
        return q2(stake * multiplier)

    return Decimal("0.00")


def generate_live_casino_bet() -> dict:
    game_type = weighted_choice(GAME_TYPES, GAME_TYPE_WEIGHTS)
    game_id = random.choice(GAME_ID_MAP[game_type])
    table_id = random.choice(TABLE_ID_MAP[game_type])
    round_id = f"RND_{uuid.uuid4().hex[:16].upper()}"
    dealer_id = random.choice(DEALER_IDS)

    bet_type = generate_bet_type(game_type)
    stake_amount = generate_stake_amount(game_type)
    bet_status = pick_status(game_type, bet_type)
    win_amount = generate_win_amount(stake_amount, bet_status, game_type, bet_type)

    event = {
        "bet_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 1_000_000),
        "game_id": game_id,
        "provider_id": random.choice(PROVIDERS),
        "table_id": table_id,
        "round_id": round_id,
        "bet_type": bet_type,
        "stake_amount": str(stake_amount),
        "win_amount": str(win_amount),
        "currency": weighted_choice(CURRENCIES, CURRENCY_WEIGHTS),
        "bet_status": bet_status,
        "device_type": weighted_choice(DEVICE_TYPES, DEVICE_WEIGHTS),
        "country_code": random.choice(EUROPE_COUNTRY_CODES),
        "dealer_id": dealer_id,
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
            event = generate_live_casino_bet()

            producer.produce(
                topic=KAFKA_TOPIC,
                key=event["bet_id"],
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report
            )

            producer.poll(1)

            print(event)

            counter += 1
            if counter % 10 == 0:
                producer.flush()

            time.sleep(1)

    except KeyboardInterrupt:
        print("Stopping producer...")

    finally:
        producer.flush()


if __name__ == "__main__":
    main()