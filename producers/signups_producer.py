from __future__ import annotations

import json
import os
import random
import string
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_USER_SIGNUPS")

producer = Producer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "user-signups-producer",
    }
)

USER_ID_MIN = 1
USER_ID_MAX = 100_000

REGISTRATION_METHODS = ["email", "google", "apple", "facebook"]
EUROPE_COUNTRY_CODES = [
    "AL","AD","AT","BY","BE","BA","BG","HR","CY","CZ",
    "DK","EE","FI","FR","DE","GI","GR","HU","IS","IE",
    "IT","XK","LV","LI","LT","LU","MT","MD","MC","ME",
    "NL","MK","NO","PL","PT","RO","RU","SM","RS","SK",
    "SI","ES","SE","CH","TR","UA","GB","VA"
]
DEVICES = ["mobile", "desktop", "tablet"]

EMAIL_DOMAINS = [
    "gmail.com",
    "yahoo.com",
    "outlook.com",
    "hotmail.com",
    "example.com",
]


def random_username(length: int = 10) -> str:
    chars = string.ascii_lowercase + string.digits
    return "".join(random.choices(chars, k=length))


def random_email(username: str) -> str:
    return f"{username}@{random.choice(EMAIL_DOMAINS)}"


def generate_signup_event() -> dict:
    username = random_username()
    user_id = random.randint(USER_ID_MIN, USER_ID_MAX)

    return {
        "signup_id": str(uuid.uuid4()),
        "user_id": user_id,
        "username": username,
        "email": random_email(username),
        "registration_method": random.choice(REGISTRATION_METHODS),
        "country_code": random.choice(EUROPE_COUNTRY_CODES),
        "device_type": random.choice(DEVICES),
        "is_verified": random.choices([True, False], weights=[75, 25], k=1)[0],
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered to topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def main() -> None:
    print(f"Producing signup events to topic: {KAFKA_TOPIC}")

    try:
        while True:
            event = generate_signup_event()

            producer.produce(
                topic=KAFKA_TOPIC,
                key=str(event["user_id"]),
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)

            print(json.dumps(event, ensure_ascii=False))
            time.sleep(random.uniform(0.5, 2.0))

    except KeyboardInterrupt:
        print("Stopping signup producer...")

    finally:
        producer.flush()


if __name__ == "__main__":
    main()