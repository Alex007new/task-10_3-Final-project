# etl_customer_feature_matrix.py
# NovaData — Customer Feature Matrix (PySpark local -> ClickHouse via HTTP JSONEachRow)
#
# Run (PowerShell):
#   $env:PYSPARK_PYTHON="D:\Course-DE-fall_2025\retail_project\.venv\Scripts\python.exe"
#   $env:PYSPARK_DRIVER_PYTHON="D:\Course-DE-fall_2025\retail_project\.venv\Scripts\python.exe"
#   spark-submit --jars ".\jars\clickhouse-jdbc.jar" .\etl_customer_feature_matrix.py
#
# Env (optional):
#   CH_HOST=localhost
#   CH_PORT=8124
#   CH_USER=etl_user
#   CH_PASS=etl_pass
#   SRC_DB=retail_mart
#   SRC_CUSTOMERS=customers_mart
#   SRC_PURCHASES=purchases_mart
#   TGT_DB=retail_analytics
#   TGT_TABLE=customer_feature_matrix
#   TRUNCATE_BEFORE_LOAD=1  (or 0)
#   HIGH_SPENDER_7D=1000
#   LOW_AVG_CHECK_30D=200
#   MORNING_START=6
#   MORNING_END=12
#   NIGHT_START=20
#
# Notes:
# - We DO NOT send etl_loaded_at from Spark. ClickHouse fills it with now64(6) by DEFAULT.
# - Insert uses HTTP + JSONEachRow (stable, avoids DateTime microseconds mismatch).

import os
import sys
import json
import requests
from typing import Optional

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


# -----------------------------
# CONFIG
# -----------------------------

CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = os.getenv("CH_PORT", "8124")  # ВАЖНО: HTTP порт ClickHouse (у тебя 8124)
CH_USER = os.getenv("CH_USER", "etl_user")
CH_PASS = os.getenv("CH_PASS", "etl_pass")

def ch_jdbc_url(db: str) -> str:
    # для clickhouse-jdbc (HTTP)
    return f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{db}"


CH_HTTP_URL = f"http://{CH_HOST}:{CH_PORT}"
CH_HTTP_USER = CH_USER
CH_HTTP_PASSWORD = CH_PASS

SRC_DB = os.getenv("SRC_DB", "retail_mart")
SRC_CUSTOMERS = os.getenv("SRC_CUSTOMERS", "customers_mart")
SRC_PURCHASES = os.getenv("SRC_PURCHASES", "purchases_mart")

TGT_DB = os.getenv("TGT_DB", "retail_analytics")
TGT_TABLE_NAME = os.getenv("TGT_TABLE", "customer_feature_matrix")
TGT_TABLE = f"{TGT_DB}.{TGT_TABLE_NAME}"

TRUNCATE_BEFORE_LOAD = os.getenv("TRUNCATE_BEFORE_LOAD", "0") == "1"

# Thresholds (business rules)
HIGH_SPENDER_7D = float(os.getenv("HIGH_SPENDER_7D", "1000"))
LOW_AVG_CHECK_30D = float(os.getenv("LOW_AVG_CHECK_30D", "200"))

# Time-of-day buckets
MORNING_START = int(os.getenv("MORNING_START", "6"))   # 06:00 inclusive
MORNING_END = int(os.getenv("MORNING_END", "12"))     # 12:00 exclusive
NIGHT_START = int(os.getenv("NIGHT_START", "20"))     # 20:00 inclusive

# Spark JDBC
JDBC_URL = f"jdbc:clickhouse://{CH_HOST}:9004/{SRC_DB}"  # your Native port is 9004
JDBC_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"


# -----------------------------
# ClickHouse HTTP helpers
# -----------------------------
def ch_exec(sql: str, database: Optional[str] = None, timeout_s: int = 60) -> str:
    """
    Execute SQL via ClickHouse HTTP interface.
    """
    params = {
        "user": CH_HTTP_USER,
        "password": CH_HTTP_PASSWORD,
        "query": sql,
    }
    if database:
        params["database"] = database

    r = requests.post(CH_HTTP_URL, params=params, timeout=timeout_s)
    r.raise_for_status()
    return r.text


def ch_insert_json_each_row(df, table: str, timeout_s: int = 300) -> None:
    """
    Insert Spark DataFrame -> ClickHouse using JSONEachRow over HTTP.
    We stream data from driver (ok for your current scale).
    """
    # Collect rows to driver. For your dataset size (~45 customers), this is safe.
    rows = df.toJSON().collect()
    if not rows:
        return

    data = "\n".join(rows) + "\n"
    sql = f"INSERT INTO {table} FORMAT JSONEachRow"

    params = {"user": CH_HTTP_USER, "password": CH_HTTP_PASSWORD, "query": sql}
    r = requests.post(CH_HTTP_URL, params=params, data=data.encode("utf-8"), timeout=timeout_s)
    r.raise_for_status()


def ch_ensure_db_and_table() -> None:
    """
    Create target DB and table with correct MergeTree ORDER BY.
    IMPORTANT: etl_loaded_at is DEFAULT now64(6) and we don't send it from Spark.
    """
    ch_exec(f"CREATE DATABASE IF NOT EXISTS {TGT_DB}")

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TGT_TABLE}
    (
        customer_id String,

        new_customer UInt8,
        loyal_customer UInt8,
        no_purchases UInt8,
        inactive_14_30 UInt8,
        recurrent_buyer UInt8,

        delivery_user UInt8,
        prefers_cash UInt8,
        prefers_card UInt8,

        weekend_shopper UInt8,
        weekday_shopper UInt8,
        morning_shopper UInt8,
        night_shopper UInt8,

        store_loyal UInt8,
        switching_store UInt8,

        recent_high_spender UInt8,
        low_cost_buyer UInt8,

        sum_7d Float64,
        sum_30d Float64,

        etl_loaded_at DateTime64(6) DEFAULT now64(6)
    )
    ENGINE = MergeTree
    ORDER BY customer_id
    """
    ch_exec(ddl)


# -----------------------------
# Spark helpers
# -----------------------------
def read_ch_table(spark: SparkSession, db: str, table: str):
    """
    Read ClickHouse table via JDBC into Spark DataFrame.
    """
    return (
        spark.read.format("jdbc")
        .option("url", f"jdbc:clickhouse://{CH_HOST}:8124/{db}")
        .option("driver", JDBC_DRIVER)
        .option("user", CH_USER)
        .option("password", CH_PASS)
        .option("dbtable", f"{db}.{table}")
        .load()
    )


# -----------------------------
# Main ETL
# -----------------------------
def main() -> None:
    spark = (
        SparkSession.builder
        .appName("NovaData_Customer_Feature_Matrix")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1) Read MART
    customers = read_ch_table(spark, SRC_DB, SRC_CUSTOMERS).select(
        F.col("customer_id").cast("string").alias("customer_id"),
        F.col("registration_dt").cast("timestamp").alias("registration_dt"),
        F.coalesce(F.col("is_loyalty_member"), F.lit(0)).cast("int").alias("is_loyalty_member"),
    )

    purchases = read_ch_table(spark, SRC_DB, SRC_PURCHASES).select(
        F.col("purchase_id").cast("string").alias("purchase_id"),
        F.col("customer_id").cast("string").alias("customer_id"),
        F.col("store_id").cast("string").alias("store_id"),
        F.col("purchase_datetime").cast("timestamp").alias("purchase_ts"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.lower(F.col("payment_method")).alias("payment_method"),
        F.coalesce(F.col("is_delivery"), F.lit(0)).cast("int").alias("is_delivery"),
    )

    # 2) Build base aggregates
    now_ts = F.current_timestamp()

    purchases_enriched = (
        purchases
        .withColumn("days_ago", F.datediff(now_ts, F.col("purchase_ts")))
        .withColumn("is_14d", (F.col("days_ago") >= 0) & (F.col("days_ago") <= 13))
        .withColumn("is_15_30d", (F.col("days_ago") >= 14) & (F.col("days_ago") <= 29))
        .withColumn("is_7d", (F.col("days_ago") >= 0) & (F.col("days_ago") <= 6))
        .withColumn("is_30d", (F.col("days_ago") >= 0) & (F.col("days_ago") <= 29))
        .withColumn("is_cash", (F.col("payment_method") == F.lit("cash")))
        .withColumn("is_card", (F.col("payment_method") == F.lit("card")))
        .withColumn("dow", F.dayofweek(F.col("purchase_ts")))  # 1=Sun .. 7=Sat
        .withColumn("is_weekend", F.col("dow").isin([1, 7]))
        .withColumn("is_weekday", ~F.col("dow").isin([1, 7]))
        .withColumn("hour", F.hour(F.col("purchase_ts")))
        .withColumn("is_morning", (F.col("hour") >= F.lit(MORNING_START)) & (F.col("hour") < F.lit(MORNING_END)))
        .withColumn("is_night", (F.col("hour") >= F.lit(NIGHT_START)) | (F.col("hour") < F.lit(MORNING_START)))
    )

    agg = (
        purchases_enriched
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("purchases_cnt"),
            F.max("purchase_ts").alias("last_purchase_ts"),

            F.sum(F.when(F.col("is_14d"), 1).otherwise(0)).alias("cnt_14d"),
            F.sum(F.when(F.col("is_15_30d"), 1).otherwise(0)).alias("cnt_15_30d"),

            (F.sum(F.col("is_delivery")) / F.count("*")).alias("share_delivery"),

            (F.sum(F.when(F.col("is_cash"), 1).otherwise(0)) / F.count("*")).alias("share_cash"),
            (F.sum(F.when(F.col("is_card"), 1).otherwise(0)) / F.count("*")).alias("share_card"),

            (F.sum(F.when(F.col("is_weekend"), 1).otherwise(0)) / F.count("*")).alias("share_weekend"),
            (F.sum(F.when(F.col("is_weekday"), 1).otherwise(0)) / F.count("*")).alias("share_weekday"),

            (F.sum(F.when(F.col("is_morning"), 1).otherwise(0)) / F.count("*")).alias("share_morning"),
            (F.sum(F.when(F.col("is_night"), 1).otherwise(0)) / F.count("*")).alias("share_night"),

            F.sum(F.when(F.col("is_7d"), F.col("total_amount")).otherwise(F.lit(0.0))).alias("sum_7d"),
            F.sum(F.when(F.col("is_30d"), F.col("total_amount")).otherwise(F.lit(0.0))).alias("sum_30d"),

            F.avg(F.when(F.col("is_30d"), F.col("total_amount"))).alias("avg_check_30d"),

            F.countDistinct(F.when(F.col("is_30d"), F.col("store_id"))).alias("distinct_stores_30d"),
        )
    )

    # Top store share (last 30d)
    by_store_30d = (
        purchases_enriched
        .where(F.col("is_30d"))
        .groupBy("customer_id", "store_id")
        .agg(F.count("*").alias("cnt_store_30d"))
    )

    w = Window.partitionBy("customer_id").orderBy(F.col("cnt_store_30d").desc(), F.col("store_id").asc())
    top_store = (
        by_store_30d
        .withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") == 1)
        .select("customer_id", F.col("cnt_store_30d").alias("top_store_cnt_30d"))
    )

    total_30d = (
        purchases_enriched
        .where(F.col("is_30d"))
        .groupBy("customer_id")
        .agg(F.count("*").alias("total_cnt_30d"))
    )

    top_store_share = (
        total_30d.join(top_store, "customer_id", "left")
        .withColumn("top_store_share_30d",
                    F.when(F.col("total_cnt_30d") > 0, F.col("top_store_cnt_30d") / F.col("total_cnt_30d"))
                     .otherwise(F.lit(0.0)))
        .select("customer_id", "top_store_share_30d")
    )

    # 3) Join features to customers
    feat = (
        customers
        .join(agg, "customer_id", "left")
        .join(top_store_share, "customer_id", "left")
        .na.fill({
            "purchases_cnt": 0,
            "cnt_14d": 0,
            "cnt_15_30d": 0,
            "share_delivery": 0.0,
            "share_cash": 0.0,
            "share_card": 0.0,
            "share_weekend": 0.0,
            "share_weekday": 0.0,
            "share_morning": 0.0,
            "share_night": 0.0,
            "sum_7d": 0.0,
            "sum_30d": 0.0,
            "avg_check_30d": 0.0,
            "distinct_stores_30d": 0,
            "top_store_share_30d": 0.0,
        })
    )

    # 4) Final feature matrix (SAFE ORDER + SAFE TYPES)
    result = (
        feat
        .withColumn("new_customer", (F.col("registration_dt") >= (now_ts - F.expr("INTERVAL 30 DAYS"))).cast("int"))
        .withColumn("loyal_customer", (F.col("is_loyalty_member") == 1).cast("int"))

        .withColumn("no_purchases", (F.col("purchases_cnt") == 0).cast("int"))
        .withColumn("inactive_14_30", ((F.col("cnt_14d") == 0) & (F.col("cnt_15_30d") > 0)).cast("int"))
        .withColumn("recurrent_buyer", (F.col("purchases_cnt") >= 2).cast("int"))

        .withColumn("delivery_user", (F.col("share_delivery") > 0.5).cast("int"))
        .withColumn("prefers_cash", (F.col("share_cash") > 0.7).cast("int"))
        .withColumn("prefers_card", (F.col("share_card") > 0.7).cast("int"))

        .withColumn("weekend_shopper", (F.col("share_weekend") > 0.5).cast("int"))
        .withColumn("weekday_shopper", (F.col("share_weekday") > 0.5).cast("int"))
        .withColumn("morning_shopper", (F.col("share_morning") > 0.5).cast("int"))
        .withColumn("night_shopper", (F.col("share_night") > 0.5).cast("int"))

        .withColumn("store_loyal", (F.col("top_store_share_30d") >= 0.7).cast("int"))
        .withColumn("switching_store", (F.col("distinct_stores_30d") >= 2).cast("int"))

        .withColumn("recent_high_spender", (F.col("sum_7d") >= F.lit(HIGH_SPENDER_7D)).cast("int"))
        .withColumn("low_cost_buyer", (F.col("avg_check_30d") <= F.lit(LOW_AVG_CHECK_30D)).cast("int"))

        # HARD ORDER (exactly as ClickHouse table, WITHOUT etl_loaded_at)
        .select(
            F.col("customer_id").cast("string").alias("customer_id"),

            F.col("new_customer").cast("int").alias("new_customer"),
            F.col("loyal_customer").cast("int").alias("loyal_customer"),
            F.col("no_purchases").cast("int").alias("no_purchases"),
            F.col("inactive_14_30").cast("int").alias("inactive_14_30"),
            F.col("recurrent_buyer").cast("int").alias("recurrent_buyer"),

            F.col("delivery_user").cast("int").alias("delivery_user"),
            F.col("prefers_cash").cast("int").alias("prefers_cash"),
            F.col("prefers_card").cast("int").alias("prefers_card"),

            F.col("weekend_shopper").cast("int").alias("weekend_shopper"),
            F.col("weekday_shopper").cast("int").alias("weekday_shopper"),
            F.col("morning_shopper").cast("int").alias("morning_shopper"),
            F.col("night_shopper").cast("int").alias("night_shopper"),

            F.col("store_loyal").cast("int").alias("store_loyal"),
            F.col("switching_store").cast("int").alias("switching_store"),

            F.col("recent_high_spender").cast("int").alias("recent_high_spender"),
            F.col("low_cost_buyer").cast("int").alias("low_cost_buyer"),

            F.col("sum_7d").cast("double").alias("sum_7d"),
            F.col("sum_30d").cast("double").alias("sum_30d"),
        )
    )

    # 5) Ensure target & load
    ch_ensure_db_and_table()

    if TRUNCATE_BEFORE_LOAD:
        ch_exec(f"TRUNCATE TABLE {TGT_TABLE}")

    # Insert
    ch_insert_json_each_row(result, TGT_TABLE)

    # Small console checks
    cnt = result.count()
    print(f"OK -> inserted rows (Spark): {cnt}")
    print(f"OK -> target: {TGT_TABLE}")

    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("FAILED:", repr(e), file=sys.stderr)
        raise