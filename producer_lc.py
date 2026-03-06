import time
import uuid
import random
from datetime import datetime, timezone

import requests


API_URL = "http://127.0.0.1:8000/events"

TARGET_EVENTS_PER_SEC = 100
DUPLICATE_PROBABILITY = 0.01  # 1% duplicates

COUNTRIES = ["GE", "TR", "DE", "PL", "ES"]
DEVICES = ["MOBILE", "DESKTOP"]

BET_TYPES = ["RED", "BLACK", "ODD", "EVEN", "STRAIGHT"]


def now():
    return datetime.now(timezone.utc).isoformat()


def generate_event():
    bet_type = random.choice(BET_TYPES)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_name": "roulette_bet_placed",
        "event_ts": now(),

        "user_id": f"user_{random.randint(1, 10000)}",
        "session_id": f"s_{random.randint(1, 100000)}",

        "table_id": f"table_{random.randint(1,5)}",
        "round_id": f"round_{random.randint(1,100000)}",
        "bet_id": f"bet_{uuid.uuid4().hex[:8]}",

        "bet_type": bet_type,
        "selection": random.randint(0, 36) if bet_type == "STRAIGHT" else None,
        "stake": round(random.uniform(1, 50), 2),

        "currency": "EUR",
        "country": random.choice(COUNTRIES),
        "device": random.choice(DEVICES)
    }

    return event


def send_event(event):
    try:
        requests.post(API_URL, json=event, timeout=2)
    except Exception as e:
        print("send error:", e)


def run():
    print("Producer started (~100 rows/sec)")

    while True:
        start = time.time()

        for _ in range(TARGET_EVENTS_PER_SEC):

            event = generate_event()

            send_event(event)

            # simulate retry / duplicates
            if random.random() < DUPLICATE_PROBABILITY:
                send_event(event)

        elapsed = time.time() - start

        # ensure ~100 events per second
        if elapsed < 1:
            time.sleep(1 - elapsed)


if __name__ == "__main__":
    run()