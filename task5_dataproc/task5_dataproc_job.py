import argparse
import hashlib
import json
import statistics
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def result_signature(rows, columns):
    normalized_rows = []

    for row in rows:
        values = []
        for col in columns:
            value = row[col]
            if isinstance(value, float):
                value = round(value, 4)
            values.append(str(value))
        normalized_rows.append("|".join(values))

    normalized_rows.sort()
    payload = "\n".join(normalized_rows).encode("utf-8")
    checksum = hashlib.md5(payload).hexdigest()[:16]

    return f"rows={len(rows)};cols={len(columns)};checksum={checksum}"


def benchmark_query(
    query_name,
    query_func,
    rows,
    input_size_mb,
    layout,
    repeats,
    notes,
):
    times = []
    last_rows = None
    last_columns = None

    for _ in range(repeats):
        start = time.perf_counter()
        result_df = query_func()
        collected = result_df.collect()
        elapsed = time.perf_counter() - start

        times.append(elapsed)
        last_rows = collected
        last_columns = result_df.columns

    return {
        "library_engine": "pyspark",
        "mode": "dataproc-cluster",
        "query_name": query_name,
        "data_format": "parquet",
        "layout": layout,
        "rows": int(rows),
        "median_time_s": round(float(statistics.median(times)), 4),
        "peak_memory_mb": -1.0,
        "input_size_mb": round(float(input_size_mb), 2),
        "result_check": result_signature(last_rows, last_columns),
        "notes": notes,
    }


parser = argparse.ArgumentParser()
parser.add_argument("--events-path", required=True)
parser.add_argument("--partitioned-events-path", required=True)
parser.add_argument("--dimension-path", required=True)
parser.add_argument("--output-path", required=True)
parser.add_argument("--rows", required=True, type=int)
parser.add_argument("--events-size-mb", required=True, type=float)
parser.add_argument("--partitioned-size-mb", required=True, type=float)
parser.add_argument("--repeats", default=3, type=int)
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("TBDPhase2Task5DataprocBenchmark")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", "8")


def q1_price_stats_dataproc():
    df = spark.read.parquet(args.partitioned_events_path).select(
        "event_date",
        "country",
        "listing_status",
        "marketplace_category",
        "final_price",
    )

    return (
        df.filter(
            (F.col("listing_status") == "active")
            & (F.col("country").isin("PL", "DE", "FR"))
            & (F.col("event_date") >= F.lit("2026-03-01"))
        )
        .groupBy("marketplace_category")
        .agg(
            F.count("*").alias("listings"),
            F.avg("final_price").alias("avg_price"),
            F.expr("percentile_approx(final_price, 0.5)").alias("median_price"),
            F.expr("percentile_approx(final_price, 0.9)").alias("p90_price"),
        )
        .orderBy(F.desc("listings"))
    )


def q2_top_sellers_dataproc():
    df = spark.read.parquet(args.events_path).select(
        "seller_id",
        "views_7d",
        "final_price",
    )

    return (
        df.groupBy("seller_id")
        .agg(
            F.count("*").alias("listings"),
            F.sum("views_7d").alias("total_views"),
            F.avg("final_price").alias("avg_price"),
        )
        .orderBy(F.desc("total_views"), F.desc("listings"))
        .limit(20)
    )


def q3_commission_dataproc():
    events_df = spark.read.parquet(args.events_path).select(
        "category_id",
        "listing_status",
        "final_price",
    )

    dimension_df = spark.read.parquet(args.dimension_path)

    return (
        events_df.join(dimension_df, on="category_id", how="left")
        .filter(F.col("listing_status") == "sold")
        .withColumn("commission_value", F.col("final_price") * F.col("commission_rate"))
        .groupBy("category_group")
        .agg(
            F.count("*").alias("sold_listings"),
            F.sum("final_price").alias("gross_value"),
            F.sum("commission_value").alias("commission_value"),
        )
        .orderBy(F.desc("commission_value"))
    )


results = [
    benchmark_query(
        query_name="q1_price_stats_active_listings",
        query_func=q1_price_stats_dataproc,
        rows=args.rows,
        input_size_mb=args.partitioned_size_mb,
        layout="gcs_partitioned_by_event_date",
        repeats=args.repeats,
        notes=(
            "Dataproc PySpark. Query uses date filter over partitioned GCS layout. "
            "Remote executor memory is not measured inside this notebook."
        ),
    ),
    benchmark_query(
        query_name="q2_top_sellers_by_views",
        query_func=q2_top_sellers_dataproc,
        rows=args.rows,
        input_size_mb=args.events_size_mb,
        layout="gcs_default",
        repeats=args.repeats,
        notes=(
            "Dataproc PySpark. High-cardinality groupBy over seller_id causes shuffle. "
            "Remote executor memory is not measured inside this notebook."
        ),
    ),
    benchmark_query(
        query_name="q3_commission_by_category_group",
        query_func=q3_commission_dataproc,
        rows=args.rows,
        input_size_mb=args.events_size_mb,
        layout="gcs_default",
        repeats=args.repeats,
        notes=(
            "Dataproc PySpark. Join with dimension table followed by aggregation. "
            "Remote executor memory is not measured inside this notebook."
        ),
    ),
]

spark.sparkContext.parallelize(
    [json.dumps(row) for row in results],
    1,
).saveAsTextFile(args.output_path)

spark.stop()