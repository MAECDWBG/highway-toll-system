"""
spark/batch_transform.py
-------------------------
Nightly batch job triggered by Airflow.

Reads raw Parquet from S3 for the previous day, runs aggregations,
and writes results to the Redshift star-schema mart via JDBC.

Author : Mayukh Ghosh | Roll No : 23052334
Batch  : Data Engineering 2025-2026, KIIT University

Run (manual):
    spark-submit --jars redshift-jdbc42.jar spark/batch_transform.py --date 2025-07-15
"""

import argparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── Config ─────────────────────────────────────────────────────────────────────
S3_RAW_PATH      = "s3a://highway-toll-raw/transactions/"
REDSHIFT_URL     = "jdbc:redshift://toll-redshift-cluster.redshift.amazonaws.com:5439/tolldb"
REDSHIFT_USER    = "etl_user"
REDSHIFT_PASS    = "os.getenv(...)"          # use AWS Secrets Manager in production
REDSHIFT_TMPDIR  = "s3a://highway-toll-tmp/redshift-staging/"


def get_spark():
    return (
        SparkSession.builder
        .appName("HighwayTollBatchTransform")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )


def load_raw(spark, date_str: str):
    """Load one day's raw Parquet partition."""
    dt   = datetime.strptime(date_str, "%Y-%m-%d")
    path = (
        f"{S3_RAW_PATH}"
        f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
    )
    print(f"📂 Reading raw data from: {path}")
    return spark.read.parquet(path)


def build_fact_transactions(df):
    """Clean and reshape into FACT_TRANSACTION rows."""
    return (
        df
        .select(
            "transaction_id",
            "plaza_id",
            "tag_id",
            "class_id",
            F.to_date("event_ts").alias("date"),
            F.date_format("event_ts", "yyyyMMdd").cast("int").alias("date_id"),
            "amount",
            "status",
            "event_ts",
            "ingested_at",
        )
        .filter(F.col("status") != "LOW_BALANCE")   # exclude failed deductions
        .dropDuplicates(["transaction_id"])
    )


def build_hourly_revenue(df):
    """Hourly revenue summary per plaza — loaded into an aggregation table."""
    return (
        df
        .groupBy(
            "plaza_id",
            F.date_format("event_ts", "yyyyMMdd").cast("int").alias("date_id"),
            F.hour("event_ts").alias("hour"),
        )
        .agg(
            F.count("transaction_id").alias("tx_count"),
            F.sum("amount").alias("total_revenue"),
            F.countDistinct("tag_id").alias("unique_vehicles"),
            F.avg("amount").alias("avg_toll"),
            F.sum(F.when(F.col("status") == "FLAGGED", 1).otherwise(0)).alias("flagged_count"),
        )
    )


def write_to_redshift(df, table: str, mode: str = "append"):
    """Write a DataFrame to Redshift via JDBC."""
    print(f"  → Writing {df.count()} rows to Redshift table: {table}")
    (
        df.write
        .format("jdbc")
        .option("url", REDSHIFT_URL)
        .option("dbtable", table)
        .option("user", REDSHIFT_USER)
        .option("password", REDSHIFT_PASS)
        .option("driver", "com.amazon.redshift.jdbc42.Driver")
        .option("tempdir", REDSHIFT_TMPDIR)
        .mode(mode)
        .save()
    )
    print(f"  ✅ {table} written successfully.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=(datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d"),
                        help="Processing date YYYY-MM-DD (default: yesterday)")
    args = parser.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"📅 Batch transform for date: {args.date}")

    raw_df = load_raw(spark, args.date)
    print(f"   Raw rows loaded: {raw_df.count():,}")

    # Fact table
    fact_df = build_fact_transactions(raw_df)
    write_to_redshift(fact_df, "mart.fact_transaction")

    # Hourly revenue aggregate
    hourly_df = build_hourly_revenue(raw_df)
    write_to_redshift(hourly_df, "mart.agg_hourly_revenue")

    print("🎉 Batch transform complete.")
    spark.stop()


if __name__ == "__main__":
    main()
