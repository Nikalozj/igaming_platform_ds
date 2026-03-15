from __future__ import annotations

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_USER_LOGINS")

producer = Producer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "user-logins-producer",
    }
)

USER_ID_MIN = 1
USER_ID_MAX = 100_000

LOGIN_STATUSES = ["success", "failed"]
LOGIN_METHODS = ["password", "google", "apple", "facebook"]
AUTH_TYPES = ["password_only", "2fa"]

DEVICES = ["mobile", "desktop", "tablet"]

OS_TYPES = {
    "mobile": ["android", "ios"],
    "desktop": ["windows", "macos", "linux"],
    "tablet": ["android", "ios"],
}

BROWSERS = {
    "mobile": ["chrome", "safari"],
    "desktop": ["chrome", "firefox", "edge", "safari"],
    "tablet": ["chrome", "safari"],
}

EUROPE_COUNTRY_CODES = [
    "AL","AD","AT","BY","BE","BA","BG","HR","CY","CZ",
    "DK","EE","FI","FR","DE","GI","GR","HU","IS","IE",
    "IT","XK","LV","LI","LT","LU","MT","MD","MC","ME",
    "NL","MK","NO","PL","PT","RO","RU","SM","RS","SK",
    "SI","ES","SE","CH","TR","UA","GB","VA"
]

FAILURE_REASONS = [
    "invalid_password",
    "otp_failed",
    "blocked_user",
    "too_many_attempts",
]


def random_ip() -> str:
    return ".".join(str(random.randint(1, 254)) for _ in range(4))


def generate_login_event() -> dict:
    user_id = random.randint(USER_ID_MIN, USER_ID_MAX)
    login_status = random.choices(LOGIN_STATUSES, weights=[92, 8], k=1)[0]

    device = random.choice(DEVICES)
    os_name = random.choice(OS_TYPES[device])
    browser = random.choice(BROWSERS[device])

    failure_reason = None
    session_id = str(uuid.uuid4())

    if login_status == "failed":
        failure_reason = random.choice(FAILURE_REASONS)
        session_id = None

    return {
        "login_id": str(uuid.uuid4()),
        "user_id": user_id,
        "session_id": session_id,
        "login_status": login_status,
        "failure_reason": failure_reason,
        "login_method": random.choice(LOGIN_METHODS),
        "auth_type": random.choice(AUTH_TYPES),
        "ip_address": random_ip(),
        "device_type": device,
        "os_name": os_name,
        "browser_name": browser,
        "country_code": random.choice(EUROPE_COUNTRY_CODES),
        "event_time": datetime.now(timezone.utc).isoformat(),
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
    print(f"Producing login events to topic: {KAFKA_TOPIC}")

    try:
        while True:
            event = generate_login_event()

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
        print("Stopping login producer...")

    finally:
        producer.flush()


if __name__ == "__main__":
    main()