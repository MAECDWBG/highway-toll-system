"""
great_expectations/checkpoint.py
----------------------------------
Standalone data quality validation script.
Called by the Airflow DAG after the Spark batch transform.

Validates mart.fact_transaction in Redshift for a given date.

Author : Mayukh Ghosh | Roll No : 23052334
Batch  : Data Engineering 2025-2026, KIIT University

Run:
    python great_expectations/checkpoint.py --date 2025-07-15
"""

import argparse
import sys
from datetime import datetime

import psycopg2

# ── Redshift connection config ─────────────────────────────────────────────────
DB_CONFIG = {
    "host":     "toll-redshift-cluster.redshift.amazonaws.com",
    "port":     5439,
    "dbname":   "tolldb",
    "user":     "etl_user",
    "password": "os.getenv(...)",       # use AWS Secrets Manager in production
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


# ── Expectation helpers ────────────────────────────────────────────────────────

def expect_row_count_gt(cur, date_str: str, min_rows: int = 1) -> dict:
    """Expect at least min_rows rows loaded for the given date."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_id = int(dt.strftime("%Y%m%d"))
    cur.execute(
        "SELECT COUNT(*) FROM mart.fact_transaction WHERE date_id = %s",
        (date_id,)
    )
    count = cur.fetchone()[0]
    passed = count >= min_rows
    return {
        "check": "row_count_gt",
        "expected": f">= {min_rows}",
        "observed": count,
        "passed": passed,
    }


def expect_no_null_transaction_ids(cur, date_str: str) -> dict:
    """Expect zero NULL transaction_ids."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_id = int(dt.strftime("%Y%m%d"))
    cur.execute(
        "SELECT COUNT(*) FROM mart.fact_transaction "
        "WHERE date_id = %s AND transaction_id IS NULL",
        (date_id,)
    )
    null_count = cur.fetchone()[0]
    return {
        "check": "no_null_transaction_ids",
        "expected": 0,
        "observed": null_count,
        "passed": null_count == 0,
    }


def expect_amount_in_range(cur, date_str: str,
                           min_val: float = 0, max_val: float = 5000) -> dict:
    """Expect all amounts to be within [min_val, max_val]."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_id = int(dt.strftime("%Y%m%d"))
    cur.execute(
        "SELECT COUNT(*) FROM mart.fact_transaction "
        "WHERE date_id = %s AND (amount < %s OR amount > %s)",
        (date_id, min_val, max_val)
    )
    out_of_range = cur.fetchone()[0]
    return {
        "check": "amount_in_range",
        "expected": f"all amounts in [{min_val}, {max_val}]",
        "observed": f"{out_of_range} out-of-range rows",
        "passed": out_of_range == 0,
    }


def expect_no_duplicate_transactions(cur, date_str: str) -> dict:
    """Expect no duplicate transaction_ids within the day."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_id = int(dt.strftime("%Y%m%d"))
    cur.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT transaction_id, COUNT(*) AS cnt
            FROM mart.fact_transaction
            WHERE date_id = %s
            GROUP BY transaction_id
            HAVING COUNT(*) > 1
        ) dups
        """,
        (date_id,)
    )
    dups = cur.fetchone()[0]
    return {
        "check": "no_duplicate_transactions",
        "expected": 0,
        "observed": dups,
        "passed": dups == 0,
    }


def expect_valid_plaza_ids(cur, date_str: str) -> dict:
    """Expect all plaza_ids to exist in DIM_PLAZA."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_id = int(dt.strftime("%Y%m%d"))
    cur.execute(
        """
        SELECT COUNT(*) FROM mart.fact_transaction f
        LEFT JOIN mart.dim_plaza p ON f.plaza_id = p.plaza_id
        WHERE f.date_id = %s AND p.plaza_id IS NULL
        """,
        (date_id,)
    )
    orphans = cur.fetchone()[0]
    return {
        "check": "valid_plaza_ids",
        "expected": "all plaza_ids in dim_plaza",
        "observed": f"{orphans} orphan rows",
        "passed": orphans == 0,
    }


# ── Runner ─────────────────────────────────────────────────────────────────────

CHECKS = [
    expect_row_count_gt,
    expect_no_null_transaction_ids,
    expect_amount_in_range,
    expect_no_duplicate_transactions,
    expect_valid_plaza_ids,
]


def run_checkpoint(date_str: str) -> bool:
    print(f"\n🔍 Running data quality checks for {date_str}...\n")
    conn = get_conn()
    cur  = conn.cursor()

    results = []
    for check_fn in CHECKS:
        try:
            result = check_fn(cur, date_str)
        except Exception as e:
            result = {"check": check_fn.__name__, "passed": False, "error": str(e)}
        results.append(result)
        status = "✅ PASS" if result["passed"] else "❌ FAIL"
        print(f"  {status}  {result['check']}")
        if not result["passed"]:
            print(f"         Expected : {result.get('expected', 'N/A')}")
            print(f"         Observed : {result.get('observed', 'N/A')}")

    cur.close()
    conn.close()

    failed = [r for r in results if not r["passed"]]
    print(f"\n{'─'*50}")
    print(f"  Total checks : {len(results)}")
    print(f"  Passed       : {len(results) - len(failed)}")
    print(f"  Failed       : {len(failed)}")
    print(f"{'─'*50}\n")

    if failed:
        print("🚨 Data quality validation FAILED. Halting pipeline.")
        return False

    print("🎉 All data quality checks PASSED.")
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Processing date YYYY-MM-DD")
    args = parser.parse_args()

    ok = run_checkpoint(args.date)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
