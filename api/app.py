"""
api/app.py
-----------
FastAPI REST API for the Highway Toll Collection System.

Endpoints:
  GET  /health                    — liveness probe
  GET  /plazas                    — list all toll plazas
  GET  /transactions/summary      — revenue summary for a date
  GET  /vehicles/{tag_id}/history — trip history for a FASTag
  POST /transactions/simulate     — push a test event to Kafka (dev only)

Author : Mayukh Ghosh | Roll No : 23052334
Batch  : Data Engineering 2025-2026, KIIT University

Run:
    uvicorn api.app:app --reload --port 8000
"""

import json
import os
import uuid
from datetime import date, datetime
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer
from pydantic import BaseModel

# ── App setup ──────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Highway Toll Collection System API",
    description="REST API for querying toll transaction data and triggering test events.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── DB config ──────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("REDSHIFT_HOST",     "toll-redshift-cluster.redshift.amazonaws.com"),
    "port":     int(os.getenv("REDSHIFT_PORT", "5439")),
    "dbname":   os.getenv("REDSHIFT_DB",       "tolldb"),
    "user":     os.getenv("REDSHIFT_USER",     "etl_user"),
    "password": os.getenv("REDSHIFT_PASSWORD", "os.getenv(...)"),
}

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC  = "toll-transactions"


def get_conn():
    return psycopg2.connect(cursor_factory=psycopg2.extras.RealDictCursor, **DB_CONFIG)


# ── Pydantic models ────────────────────────────────────────────────────────────

class SimulateRequest(BaseModel):
    tag_id:    str
    plaza_id:  str
    class_id:  str = "C2"
    amount:    float = 75.0


class TransactionSummary(BaseModel):
    date:            str
    total_revenue:   float
    tx_count:        int
    unique_vehicles: int
    flagged_count:   int


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@app.get("/plazas")
def list_plazas():
    """Return all active toll plazas."""
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT * FROM mart.dim_plaza WHERE active = 1 ORDER BY plaza_id")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {"plazas": [dict(r) for r in rows]}


@app.get("/transactions/summary", response_model=TransactionSummary)
def transaction_summary(query_date: Optional[str] = Query(
    default=None, description="Date in YYYY-MM-DD format (default: today)"
)):
    """Revenue and transaction summary for a given date."""
    if query_date is None:
        query_date = date.today().isoformat()

    try:
        dt = datetime.strptime(query_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, detail="Invalid date format. Use YYYY-MM-DD.")

    date_id = int(dt.strftime("%Y%m%d"))
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(
        """
        SELECT
            COALESCE(SUM(f.amount), 0)        AS total_revenue,
            COUNT(*)                           AS tx_count,
            COUNT(DISTINCT f.vehicle_id)       AS unique_vehicles,
            SUM(CASE WHEN py.status = 'FLAGGED' THEN 1 ELSE 0 END) AS flagged_count
        FROM mart.fact_transaction f
        JOIN mart.dim_payment py ON f.payment_id = py.payment_id
        WHERE f.date_id = %s
        """,
        (date_id,)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    return {
        "date":            query_date,
        "total_revenue":   float(row["total_revenue"]),
        "tx_count":        int(row["tx_count"]),
        "unique_vehicles": int(row["unique_vehicles"]),
        "flagged_count":   int(row["flagged_count"]),
    }


@app.get("/vehicles/{tag_id}/history")
def vehicle_history(
    tag_id: str,
    limit:  int = Query(default=20, le=100),
):
    """Retrieve recent trip history for a given FASTag ID."""
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(
        """
        SELECT
            f.transaction_id,
            f.entry_time,
            p.name    AS plaza,
            p.highway,
            c.name    AS vehicle_class,
            f.amount,
            py.status
        FROM mart.fact_transaction f
        JOIN mart.dim_vehicle v  ON f.vehicle_id = v.vehicle_id
        JOIN mart.dim_plaza   p  ON f.plaza_id   = p.plaza_id
        JOIN mart.dim_class   c  ON f.class_id   = c.class_id
        JOIN mart.dim_payment py ON f.payment_id = py.payment_id
        WHERE v.tag_id = %s
        ORDER BY f.entry_time DESC
        LIMIT %s
        """,
        (tag_id, limit)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        raise HTTPException(404, detail=f"No records found for tag_id={tag_id}")

    return {"tag_id": tag_id, "trips": [dict(r) for r in rows]}


@app.post("/transactions/simulate", status_code=201)
def simulate_transaction(req: SimulateRequest):
    """
    Push a synthetic toll event directly to Kafka.
    Intended for development / integration testing only.
    """
    event = {
        "transaction_id": str(uuid.uuid4()),
        "timestamp":      datetime.utcnow().isoformat() + "Z",
        "plaza_id":       req.plaza_id,
        "tag_id":         req.tag_id,
        "class_id":       req.class_id,
        "amount":         req.amount,
        "status":         "SUCCESS",
        "lane":           1,
    }
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        producer.send(KAFKA_TOPIC, value=event)
        producer.flush()
        producer.close()
    except Exception as e:
        raise HTTPException(503, detail=f"Kafka unavailable: {e}")

    return {"message": "Event published", "transaction_id": event["transaction_id"]}
