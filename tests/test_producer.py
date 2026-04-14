"""
tests/test_producer.py
-----------------------
Unit tests for the Kafka producer event generation logic.

Author : Mayukh Ghosh | Roll No : 23052334
"""

import sys
import os
import uuid
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from kafka.producer import generate_event, BLACKLISTED_TAGS, VEHICLE_CLASSES, PLAZAS


def test_event_has_required_fields():
    event = generate_event()
    required = [
        "transaction_id", "timestamp", "plaza_id", "tag_id",
        "class_id", "amount", "status", "lane"
    ]
    for field in required:
        assert field in event, f"Missing field: {field}"


def test_transaction_id_is_uuid():
    event = generate_event()
    # Should not raise
    uuid.UUID(event["transaction_id"])


def test_timestamp_is_iso_format():
    event = generate_event()
    ts = event["timestamp"].replace("Z", "")
    datetime.fromisoformat(ts)   # should not raise


def test_amount_in_valid_range():
    for _ in range(50):
        event = generate_event()
        assert 0 <= event["amount"] <= 5000, f"Amount out of range: {event['amount']}"


def test_status_values_are_valid():
    valid_statuses = {"SUCCESS", "LOW_BALANCE", "FLAGGED"}
    for _ in range(50):
        event = generate_event()
        assert event["status"] in valid_statuses, f"Unexpected status: {event['status']}"


def test_blacklisted_tags_are_flagged():
    """Force a blacklisted tag and check the status."""
    import unittest.mock as mock

    blacklisted_tag = list(BLACKLISTED_TAGS)[0]

    with mock.patch("kafka.producer.random_tag", return_value=blacklisted_tag):
        event = generate_event()
        assert event["status"] == "FLAGGED", (
            f"Expected FLAGGED for blacklisted tag but got {event['status']}"
        )


def test_balance_after_not_negative():
    for _ in range(100):
        event = generate_event()
        assert event["balance_after"] >= 0, (
            f"balance_after went negative: {event['balance_after']}"
        )


def test_plaza_id_in_known_plazas():
    known_ids = {p["plaza_id"] for p in PLAZAS}
    for _ in range(20):
        event = generate_event()
        assert event["plaza_id"] in known_ids


def test_class_id_in_known_classes():
    known_ids = {c["class_id"] for c in VEHICLE_CLASSES}
    for _ in range(20):
        event = generate_event()
        assert event["class_id"] in known_ids


def test_lane_is_positive_integer():
    for _ in range(20):
        event = generate_event()
        assert isinstance(event["lane"], int) and event["lane"] >= 1
