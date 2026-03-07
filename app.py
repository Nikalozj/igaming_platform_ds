from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from typing import Optional, Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from confluent_kafka import Producer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "casino.roulette.events.v1")

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
    else:
        # optional success log
        # print(f"[KAFKA OK] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "roulette-events-api",
            # reliability-oriented defaults
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
        # deliver queued/in-flight messages before shutdown
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
        raise HTTPException(status_code=500, detail="Kafka producer is not initialized")

    try:
        payload = event.model_dump_json()

        # use event_id as key so duplicates for the same event stay keyed consistently
        producer.produce(
            topic=KAFKA_TOPIC,
            key=event.event_id,
            value=payload.encode("utf-8"),
            on_delivery=delivery_report,
        )

        # serves delivery callbacks and internal queue processing
        producer.poll(0)

        return {
            "status": "accepted",
            "topic": KAFKA_TOPIC,
            "event_id": event.event_id,
        }

    except BufferError as e:
        raise HTTPException(status_code=503, detail=f"Kafka queue is full: {e}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to publish to Kafka: {e}")