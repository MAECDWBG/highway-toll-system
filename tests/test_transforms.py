"""
tests/test_transforms.py
-------------------------
Unit tests for Spark transformation logic using a local SparkSession.

Author : Mayukh Ghosh | Roll No : 23052334
"""

import sys
import os
import pytest
from datetime import datetime

# Requires: pip install pyspark pytest
try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not PYSPARK_AVAILABLE, reason="PySpark not installed"
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("TollTransformTests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def make_sample_df(spark, rows):
    """Helper: create a DataFrame from a list of dicts."""
    return spark.createDataFrame(rows)


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_amount_filter_removes_negatives(spark):
    rows = [
        {"transaction_id": "T1", "amount": 75.0,   "tag_id": "TAG-001", "status": "SUCCESS"},
        {"transaction_id": "T2", "amount": -10.0,  "tag_id": "TAG-002", "status": "SUCCESS"},
        {"transaction_id": "T3", "amount": 6000.0, "tag_id": "TAG-003", "status": "SUCCESS"},
        {"transaction_id": "T4", "amount": 215.0,  "tag_id": "TAG-004", "status": "SUCCESS"},
    ]
    df = make_sample_df(spark, rows)
    filtered = df.filter((F.col("amount") >= 0) & (F.col("amount") <= 5000))
    assert filtered.count() == 2


def test_blacklist_flag_override(spark):
    rows = [
        {"transaction_id": "T1", "tag_id": "TAG-00042", "status": "SUCCESS", "amount": 75.0},
        {"transaction_id": "T2", "tag_id": "TAG-00099", "status": "SUCCESS", "amount": 75.0},
        {"transaction_id": "T3", "tag_id": "TAG-99999", "status": "SUCCESS", "amount": 75.0},
    ]
    blacklist = ["TAG-00042", "TAG-00099", "TAG-00315"]
    blacklist_col = F.array(*[F.lit(t) for t in blacklist])

    df = make_sample_df(spark, rows)
    result = df.withColumn(
        "status",
        F.when(F.array_contains(blacklist_col, F.col("tag_id")), F.lit("FLAGGED"))
         .otherwise(F.col("status"))
    )

    flagged = result.filter(F.col("status") == "FLAGGED").count()
    normal  = result.filter(F.col("status") == "SUCCESS").count()
    assert flagged == 2
    assert normal  == 1


def test_null_transaction_id_dropped(spark):
    rows = [
        {"transaction_id": "T1",  "tag_id": "TAG-001", "amount": 75.0},
        {"transaction_id": None,  "tag_id": "TAG-002", "amount": 75.0},
        {"transaction_id": "T3",  "tag_id": "TAG-003", "amount": 75.0},
    ]
    df = make_sample_df(spark, rows)
    filtered = df.filter(F.col("transaction_id").isNotNull())
    assert filtered.count() == 2


def test_deduplicate_by_transaction_id(spark):
    rows = [
        {"transaction_id": "T1", "amount": 75.0},
        {"transaction_id": "T1", "amount": 75.0},   # duplicate
        {"transaction_id": "T2", "amount": 120.0},
    ]
    df = make_sample_df(spark, rows)
    deduped = df.dropDuplicates(["transaction_id"])
    assert deduped.count() == 2


def test_hourly_aggregation(spark):
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("plaza_id",       StringType(), True),
        StructField("amount",         DoubleType(), True),
        StructField("event_ts",       TimestampType(), True),
    ])
    rows = [
        ("T1", "PL001", 75.0,  datetime(2025, 7, 15, 8, 10)),
        ("T2", "PL001", 215.0, datetime(2025, 7, 15, 8, 45)),
        ("T3", "PL001", 35.0,  datetime(2025, 7, 15, 9, 5)),
        ("T4", "PL002", 120.0, datetime(2025, 7, 15, 8, 20)),
    ]
    df = spark.createDataFrame(rows, schema=schema)

    agg = (
        df
        .groupBy("plaza_id", F.hour("event_ts").alias("hour"))
        .agg(
            F.count("transaction_id").alias("tx_count"),
            F.sum("amount").alias("total_revenue"),
        )
        .orderBy("plaza_id", "hour")
    )

    results = {(r["plaza_id"], r["hour"]): r for r in agg.collect()}
    assert results[("PL001", 8)]["tx_count"]      == 2
    assert results[("PL001", 8)]["total_revenue"]  == 290.0
    assert results[("PL001", 9)]["tx_count"]      == 1
    assert results[("PL002", 8)]["total_revenue"]  == 120.0


def test_partition_columns_added(spark):
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
    schema = StructType([
        StructField("transaction_id", StringType(),   True),
        StructField("amount",         DoubleType(),   True),
        StructField("event_ts",       TimestampType(), True),
    ])
    rows = [("T1", 75.0, datetime(2025, 7, 15, 14, 30))]
    df = spark.createDataFrame(rows, schema=schema)

    result = (
        df
        .withColumn("year",  F.year("event_ts"))
        .withColumn("month", F.month("event_ts"))
        .withColumn("day",   F.dayofmonth("event_ts"))
        .withColumn("hour",  F.hour("event_ts"))
    )

    row = result.first()
    assert row["year"]  == 2025
    assert row["month"] == 7
    assert row["day"]   == 15
    assert row["hour"]  == 14
