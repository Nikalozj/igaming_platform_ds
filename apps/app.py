from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Optional, Literal

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from confluent_kafka import Producer


load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = "slots_bets"

producer: Optional[Producer] = None


class SlotsBetEvent(BaseModel):
    bet_id: str
    user_id: int
    game_id: int
    provider_id: int

    stake_amount: float = Field(gt=0)
    win_amount: float = Field(ge=0)

    currency: str
    bet_status: Literal["settled", "cancelled"]

    device_type: Literal["mobile", "desktop", "tablet"]
    country_code: str = Field(min_length=2, max_length=2)

    event_time: str


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"[KAFKA ERROR] topic={msg.topic()} key={msg.key()} error={err}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "slots-bets-api",
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "linger.ms": 10,
        }
    )

    print("Kafka producer created")
    print(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")

    yield

    if producer is not None:
        producer.flush(10)
        print("Kafka producer flushed and closed")


app = FastAPI(
    title="Slots Bets API",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "topic": KAFKA_TOPIC,
    }


@app.post("/slots-bets")
def ingest_slots_bet(event: SlotsBetEvent):
    global producer

    if producer is None:
        raise HTTPException(status_code=500, detail="Kafka producer not initialized")

    try:
        payload = event.model_dump_json()

        producer.produce(
            topic=KAFKA_TOPIC,
            key=event.bet_id,
            value=payload.encode("utf-8"),
            on_delivery=delivery_report,
        )

        producer.poll(0)

        return {
            "status": "accepted",
            "bet_id": event.bet_id,
            "topic": KAFKA_TOPIC,
        }

    except BufferError as e:
        raise HTTPException(status_code=503, detail=f"Kafka queue full: {e}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))