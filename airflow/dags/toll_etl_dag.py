"""
airflow/dags/toll_etl_dag.py
-----------------------------
Nightly ETL DAG for the Highway Toll Collection System.

Schedule : 01:00 UTC daily (after midnight data is complete)
Tasks    : validate_raw → spark_batch_transform → load_redshift_dims
           → run_dq_checks → notify_success

Author : Mayukh Ghosh | Roll No : 23052334
Batch  : Data Engineering 2025-2026, KIIT University
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.utils.dates import days_ago

# ── Default args ───────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "mayukh.ghosh",
    "depends_on_past":  False,
    "email":            ["23052334@kiit.ac.in"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout":timedelta(hours=2),
}

EMR_CLUSTER_ID  = "j-TOLLCLUSTER01"          # replace with your EMR cluster ID
SNS_TOPIC_ARN   = "arn:aws:sns:ap-south-1:123456789:toll-alerts"
S3_SCRIPT_PATH  = "s3://highway-toll-raw/scripts/batch_transform.py"

# ── Spark EMR step ─────────────────────────────────────────────────────────────
def spark_step(date_str: str) -> list:
    return [
        {
            "Name": f"TollBatchTransform-{date_str}",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--conf", "spark.sql.shuffle.partitions=16",
                    S3_SCRIPT_PATH,
                    "--date", date_str,
                ],
            },
        }
    ]


# ── Python callables ───────────────────────────────────────────────────────────
def validate_raw_data(**context):
    """
    Check that S3 raw partition for the execution date is non-empty.
    Raises if missing — halts the DAG before any compute costs.
    """
    import boto3
    date_str = context["ds"]                     # YYYY-MM-DD from Airflow
    dt       = datetime.strptime(date_str, "%Y-%m-%d")
    prefix   = (
        f"transactions/year={dt.year}/"
        f"month={dt.month:02d}/day={dt.day:02d}/"
    )
    s3 = boto3.client("s3")
    resp = s3.list_objects_v2(Bucket="highway-toll-raw", Prefix=prefix, MaxKeys=1)
    if resp.get("KeyCount", 0) == 0:
        raise FileNotFoundError(f"No raw data found at s3://highway-toll-raw/{prefix}")
    print(f"✅ Raw data present for {date_str}")


def run_great_expectations_checks(**context):
    """Run GE checkpoint to validate loaded Redshift data."""
    import subprocess
    date_str = context["ds"]
    result = subprocess.run(
        ["python", "/opt/airflow/dags/../great_expectations/checkpoint.py",
         "--date", date_str],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"Data quality check failed:\n{result.stderr}")
    print(result.stdout)


def refresh_redshift_dimensions(**context):
    """
    Upsert dimension tables (DIM_DATE etc.) using psycopg2.
    In production, use a Redshift MERGE statement.
    """
    import psycopg2
    date_str = context["ds"]
    conn = psycopg2.connect(
        host="toll-redshift-cluster.redshift.amazonaws.com",
        port=5439, dbname="tolldb",
        user="etl_user", password="os.getenv(...)"
    )
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO mart.dim_date (date_id, date, year, month, day, day_of_week, is_weekend)
        SELECT
            TO_CHAR('{date_str}'::date, 'YYYYMMDD')::int,
            '{date_str}'::date,
            EXTRACT(year  FROM '{date_str}'::date),
            EXTRACT(month FROM '{date_str}'::date),
            EXTRACT(day   FROM '{date_str}'::date),
            TO_CHAR('{date_str}'::date, 'Day'),
            CASE WHEN EXTRACT(dow FROM '{date_str}'::date) IN (0,6) THEN 1 ELSE 0 END
        WHERE NOT EXISTS (
            SELECT 1 FROM mart.dim_date
            WHERE date_id = TO_CHAR('{date_str}'::date, 'YYYYMMDD')::int
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ dim_date refreshed for {date_str}")


# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="highway_toll_etl",
    default_args=DEFAULT_ARGS,
    description="Nightly ETL: raw S3 → Redshift mart for Highway Toll System",
    schedule_interval="0 1 * * *",          # 01:00 UTC daily
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["toll", "etl", "data-engineering"],
) as dag:

    # Task 1: Validate raw S3 data exists
    t_validate = PythonOperator(
        task_id="validate_raw_data",
        python_callable=validate_raw_data,
        provide_context=True,
    )

    # Task 2: Submit Spark batch transform to EMR
    t_spark_submit = EmrAddStepsOperator(
        task_id="spark_batch_transform",
        job_flow_id=EMR_CLUSTER_ID,
        steps="{{ task_instance.xcom_pull(task_ids='validate_raw_data') or [] }}"
              if False else spark_step("{{ ds }}"),
        aws_conn_id="aws_default",
    )

    # Task 3: Wait for Spark step to complete
    t_spark_wait = EmrStepSensor(
        task_id="wait_for_spark",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull('spark_batch_transform', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=7200,
    )

    # Task 4: Refresh dimension tables
    t_refresh_dims = PythonOperator(
        task_id="refresh_redshift_dimensions",
        python_callable=refresh_redshift_dimensions,
        provide_context=True,
    )

    # Task 5: Run Great Expectations data quality checks
    t_dq_checks = PythonOperator(
        task_id="run_dq_checks",
        python_callable=run_great_expectations_checks,
        provide_context=True,
    )

    # Task 6: Notify success via SNS
    t_notify = SnsPublishOperator(
        task_id="notify_success",
        target_arn=SNS_TOPIC_ARN,
        message=(
            "✅ Highway Toll ETL completed successfully for {{ ds }}. "
            "Data is available in mart.fact_transaction."
        ),
        aws_conn_id="aws_default",
    )

    # ── Dependencies ───────────────────────────────────────────────────────────
    t_validate >> t_spark_submit >> t_spark_wait >> t_refresh_dims >> t_dq_checks >> t_notify
