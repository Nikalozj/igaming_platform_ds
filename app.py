from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Optional, Literal

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from confluent_kafka import Producer


# Load .env file
load_dotenv()


# Kafka settings from .env
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

producer: Optional[Producer] = None


class RouletteEvent(BaseModel):
    event_id: str
    event_name: str
    event_ts: str

    user_id: str
    session_id: str

    table_id: str
    round_id: str
    bet_id: str

    bet_type: Literal["RED", "BLACK", "ODD", "EVEN", "STRAIGHT"]
    selection: Optional[int] = Field(default=None, ge=0, le=36)
    stake: float

    currency: str
    country: str
    device: Literal["MOBILE", "DESKTOP"]


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"[KAFKA ERROR] topic={msg.topic()} key={msg.key()} error={err}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "roulette-events-api",
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
    title="Roulette Events API",
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


@app.post("/events")
def ingest_event(event: RouletteEvent):
    global producer

    if producer is None:
        raise HTTPException(status_code=500, detail="Kafka producer not initialized")

    try:
        payload = event.model_dump_json()

        producer.produce(
            topic=KAFKA_TOPIC,
            key=event.event_id,
            value=payload.encode("utf-8"),
            on_delivery=delivery_report,
        )

        producer.poll(0)

        return {
            "status": "accepted",
            "event_id": event.event_id,
            "topic": KAFKA_TOPIC,
        }

    except BufferError as e:
        raise HTTPException(status_code=503, detail=f"Kafka queue full: {e}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))