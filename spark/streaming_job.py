"""
spark/streaming_job.py
-----------------------
PySpark Structured Streaming pipeline.

Flow:
  Kafka topic (toll-transactions)
      → parse & validate JSON
      → enrich with vehicle class rate
      → flag blacklisted / low-balance vehicles
      → write raw Parquet to S3
      → write aggregated micro-batch summary to console (or Redshift)

Author : Mayukh Ghosh | Roll No : 23052334
Batch  : Data Engineering 2025-2026, KIIT University

Run:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
                 spark/streaming_job.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

# ── Config ─────────────────────────────────────────────────────────────────────
KAFKA_BROKER     = "localhost:9092"
TOPIC            = "toll-transactions"
S3_RAW_PATH      = "s3a://highway-toll-raw/transactions/"      # change to your bucket
CHECKPOINT_PATH  = "s3a://highway-toll-raw/checkpoints/stream/"
TRIGGER_INTERVAL = "10 seconds"

BLACKLISTED_TAGS = ["TAG-00042", "TAG-00099", "TAG-00315"]

# ── Event schema ───────────────────────────────────────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("transaction_id", StringType(),    False),
    StructField("timestamp",      StringType(),    False),
    StructField("plaza_id",       StringType(),    False),
    StructField("plaza_name",     StringType(),    True),
    StructField("highway",        StringType(),    True),
    StructField("tag_id",         StringType(),    False),
    StructField("vehicle_no",     StringType(),    True),
    StructField("class_id",       StringType(),    False),
    StructField("class_name",     StringType(),    True),
    StructField("amount",         DoubleType(),    False),
    StructField("balance_before", DoubleType(),    True),
    StructField("balance_after",  DoubleType(),    True),
    StructField("status",         StringType(),    True),
    StructField("lane",           IntegerType(),   True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HighwayTollStreaming")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(raw_df):
    """Deserialise Kafka value bytes → structured columns."""
    return (
        raw_df
        .select(F.from_json(
            F.col("value").cast("string"), EVENT_SCHEMA
        ).alias("data"))
        .select("data.*")
        .withColumn("event_ts", F.to_timestamp("timestamp"))
        .drop("timestamp")
    )


def validate_and_enrich(df):
    """
    1. Drop rows missing critical fields.
    2. Validate amount range (₹0 – ₹5000).
    3. Flag blacklisted tags.
    4. Add ingestion metadata columns.
    """
    blacklist_col = F.array(*[F.lit(t) for t in BLACKLISTED_TAGS])

    return (
        df
        # --- drop nulls on key columns ---
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("tag_id").isNotNull())
        .filter(F.col("amount").isNotNull())
        # --- amount range check ---
        .filter((F.col("amount") >= 0) & (F.col("amount") <= 5000))
        # --- override status for blacklisted tags ---
        .withColumn(
            "status",
            F.when(F.array_contains(blacklist_col, F.col("tag_id")), F.lit("FLAGGED"))
             .otherwise(F.col("status"))
        )
        # --- partition columns for S3 ---
        .withColumn("year",  F.year("event_ts"))
        .withColumn("month", F.month("event_ts"))
        .withColumn("day",   F.dayofmonth("event_ts"))
        .withColumn("hour",  F.hour("event_ts"))
        # --- ingestion timestamp ---
        .withColumn("ingested_at", F.current_timestamp())
    )


def write_raw_to_s3(df, path: str, checkpoint: str):
    """Write validated events to S3 in Parquet, partitioned by date/hour."""
    return (
        df.writeStream
        .format("parquet")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .partitionBy("year", "month", "day", "hour")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .outputMode("append")
        .start()
    )


def write_summary_to_console(df):
    """
    Micro-batch aggregation: total revenue & transaction count per plaza.
    Replace with Redshift JDBC sink for production.
    """
    summary = (
        df
        .groupBy("plaza_id", "highway", "year", "month", "day", "hour")
        .agg(
            F.count("transaction_id").alias("tx_count"),
            F.sum("amount").alias("total_revenue"),
            F.countDistinct("tag_id").alias("unique_vehicles"),
            F.sum(F.when(F.col("status") == "FLAGGED", 1).otherwise(0)).alias("flagged_count"),
        )
    )

    return (
        summary.writeStream
        .format("console")
        .option("truncate", False)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .outputMode("complete")
        .start()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("🚀 Starting Highway Toll Structured Streaming job...")

    raw_df      = read_kafka_stream(spark)
    parsed_df   = parse_events(raw_df)
    enriched_df = validate_and_enrich(parsed_df)

    # ── Sink 1: raw Parquet on S3 ──────────────────────────────────────────────
    s3_query = write_raw_to_s3(
        enriched_df,
        path=S3_RAW_PATH,
        checkpoint=CHECKPOINT_PATH,
    )

    # ── Sink 2: console summary (replace with JDBC Redshift sink in prod) ─────
    console_query = write_summary_to_console(enriched_df)

    print("✅ Streaming queries running. Awaiting termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
