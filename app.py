from __future__ import annotations

import json
import os
from dotenv import load_dotenv
from typing import Any, Dict
from contextlib import asynccontextmanager

import asyncpg
import uvicorn
from fastapi import FastAPI, HTTPException

load_dotenv()

DB_HOST = os.getenv("POSTGRES_HOST")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool

    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=10
    )

    print("Database pool created")

    yield

    await pool.close()
    print("Database pool closed")


app = FastAPI(
    title="Raw Events Ingest",
    version="0.1.0",
    lifespan=lifespan
)


@app.get("/health")
async def health() -> Dict[str, Any]:
    return {"status": "ok"}


@app.post("/events")
async def ingest_event(payload: Dict[str, Any]) -> Dict[str, Any]:
    global pool
    if pool is None:
        raise HTTPException(status_code=500, detail="DB pool not ready")

    payload_str = json.dumps(payload, ensure_ascii=False)

    async with pool.acquire() as conn:
        row_id = await conn.fetchval(
            "INSERT INTO raw_events (payload) VALUES ($1::jsonb) RETURNING id",
            payload_str
        )

    return {"inserted": True, "id": row_id}


if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host="127.0.0.1",
        port=8000,
        reload=True
    )