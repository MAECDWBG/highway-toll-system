"""
kafka/producer.py
-----------------
Simulates RFID FASTag toll events and publishes them to the
Kafka topic `toll-transactions`.

Author : Mayukh Ghosh | Roll No : 23052334
Batch  : Data Engineering 2025-2026, KIIT University
"""

import json
import random
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_BROKER  = "localhost:9092"
TOPIC         = "toll-transactions"
EVENTS_PER_SEC = 10          # adjust to simulate load
RUN_DURATION_SEC = 300       # run for 5 minutes then stop (0 = infinite)

# ── Sample data ────────────────────────────────────────────────────────────────
PLAZAS = [
    {"plaza_id": "PL001", "name": "Delhi-Meerut Expressway KM 12", "highway": "NH-58"},
    {"plaza_id": "PL002", "name": "Mumbai-Pune Expressway KM 45",  "highway": "NH-48"},
    {"plaza_id": "PL003", "name": "Bengaluru-Mysuru Expressway KM 8", "highway": "NH-275"},
    {"plaza_id": "PL004", "name": "Chennai-Vijayawada KM 60",      "highway": "NH-16"},
    {"plaza_id": "PL005", "name": "Kolkata-Dhanbad KM 30",         "highway": "NH-19"},
]

VEHICLE_CLASSES = [
    {"class_id": "C1", "name": "2-Wheeler",     "base_rate": 35},
    {"class_id": "C2", "name": "Car/Jeep/Van",  "base_rate": 75},
    {"class_id": "C3", "name": "Light Commercial","base_rate": 120},
    {"class_id": "C4", "name": "Bus/Truck",     "base_rate": 215},
    {"class_id": "C5", "name": "Multi-Axle",    "base_rate": 330},
    {"class_id": "C6", "name": "Oversized",     "base_rate": 455},
]

STATES = ["DL", "MH", "KA", "TN", "WB", "UP", "RJ", "GJ", "AP", "TS"]

BLACKLISTED_TAGS = {"TAG-00042", "TAG-00099", "TAG-00315"}   # small demo set


def random_tag() -> str:
    return f"TAG-{random.randint(1, 9999):05d}"


def generate_event() -> dict:
    plaza   = random.choice(PLAZAS)
    vclass  = random.choice(VEHICLE_CLASSES)
    tag_id  = random_tag()
    balance = round(random.uniform(0, 500), 2)
    amount  = vclass["base_rate"] * random.choice([1, 2])        # single / return
    status  = "FLAGGED" if tag_id in BLACKLISTED_TAGS else (
              "LOW_BALANCE" if balance < amount else "SUCCESS")

    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp":      datetime.utcnow().isoformat() + "Z",
        "plaza_id":       plaza["plaza_id"],
        "plaza_name":     plaza["name"],
        "highway":        plaza["highway"],
        "tag_id":         tag_id,
        "vehicle_no":     f"{random.choice(STATES)}{random.randint(1,99):02d}{random.choice('ABCDEFGH')}{random.randint(1000,9999)}",
        "class_id":       vclass["class_id"],
        "class_name":     vclass["name"],
        "amount":         amount,
        "balance_before": balance,
        "balance_after":  max(0.0, round(balance - amount, 2)),
        "status":         status,
        "lane":           random.randint(1, 6),
    }


def delivery_report(err, msg):
    if err:
        print(f"[ERROR] Delivery failed: {err}")
    # else:
    #     print(f"[OK] {msg.topic()} partition {msg.partition()} offset {msg.offset()}")


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=10,
        batch_size=16384,
    )

    print(f"🚦 Toll producer started — sending ~{EVENTS_PER_SEC} events/sec to '{TOPIC}'")
    start   = time.time()
    sent    = 0
    interval = 1.0 / EVENTS_PER_SEC

    try:
        while True:
            if RUN_DURATION_SEC and (time.time() - start) >= RUN_DURATION_SEC:
                break
            event = generate_event()
            producer.send(TOPIC, value=event)
            sent += 1
            if sent % 100 == 0:
                print(f"  → {sent} events sent | last status: {event['status']}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n⛔ Producer stopped by user.")
    finally:
        producer.flush()
        producer.close()
        elapsed = round(time.time() - start, 1)
        print(f"✅ Done. {sent} events sent in {elapsed}s.")


if __name__ == "__main__":
    main()
