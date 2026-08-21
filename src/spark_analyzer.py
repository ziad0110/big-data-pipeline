# -*- coding: utf-8 -*-
"""
spark_analyzer.py — محرك التحليل الموزع لملف الـ 30 مليون سجل بـ PySpark
Distributed Analytics Engine for 30M Big Data Dataset
مقرر البيانات الضخمة (العملي) | جامعة الرازي | إشراف: م. عمر أبو سند
"""

import os
import sys
import time
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

from config.settings import (
    SPARK_APP_NAME,
    SPARK_DRIVER_MEMORY,
    SPARK_TARGET_PARTITIONS,
    FILE_ENCODING,
    PHONE_REGEX,
)


def get_spark_session():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName(f"{SPARK_APP_NAME}-Analyzer-30M")
        .master("local[*]")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.default.parallelism", str(SPARK_TARGET_PARTITIONS))
        .config("spark.sql.shuffle.partitions", str(SPARK_TARGET_PARTITIONS))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def analyze_dataset(input_file: str, reports_dir: str = "reports") -> dict:
    from pyspark.sql.functions import (
        col, count, countDistinct, sum as _sum, avg, min as _min, max as _max,
        trim, desc, when, regexp_replace, translate
    )
    from src.spark_loader import _get_raw_schema

    input_path = Path(input_file).resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"الملف غير موجود: {input_path}")

    file_size_bytes = os.path.getsize(str(input_path))
    file_size_mb = file_size_bytes / (1024 * 1024)
    file_size_gb = file_size_bytes / (1024 * 1024 * 1024)

    print("\n" + "=" * 70)
    print("   Apache PySpark Distributed Big Data Analytics Engine")
    print(f"   Analyzing Full Dataset: {input_path.name}")
    print(f"   File Size: {file_size_gb:.2f} GB ({file_size_mb:.2f} MB)")
    print(f"   Driver Memory: {SPARK_DRIVER_MEMORY} | Partitions: {SPARK_TARGET_PARTITIONS}")
    print("=" * 70 + "\n")

    start_time = time.perf_counter()

    spark = get_spark_session()
    schema = _get_raw_schema()

    print("[Step 1/5] Loading DataFrame with distributed partitions...")
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("encoding", "UTF-8")
        .schema(schema)
        .load(str(input_path))
    )

    num_partitions = df.rdd.getNumPartitions()
    print(f"  [OK] DataFrame loaded across {num_partitions} distributed partitions.")

    print("\n[Step 2/5] Computing Volume & Customer Metrics across partitions...")
    total_records = df.count()
    print(f"  [OK] Total Raw Records: {total_records:,}")

    print("\n[Step 3/5] Aggregating Business Dimensions (Status, Payments, Cities)...")
    status_df = df.groupBy("status").agg(count("*").alias("count")).orderBy(desc("count"))
    status_counts = {row["status"] or "UNKNOWN": int(row["count"]) for row in status_df.collect()}

    pm_df = df.groupBy("payment_method").agg(count("*").alias("count")).orderBy(desc("count"))
    payment_methods = {row["payment_method"] or "UNKNOWN": int(row["count"]) for row in pm_df.collect()}

    ps_df = df.groupBy("payment_status").agg(count("*").alias("count")).orderBy(desc("count"))
    payment_statuses = {row["payment_status"] or "UNKNOWN": int(row["count"]) for row in ps_df.collect()}

    cities_df = df.groupBy("city").agg(count("*").alias("count")).orderBy(desc("count")).limit(12)
    top_cities = {row["city"] or "UNKNOWN": int(row["count"]) for row in cities_df.collect()}

    del_df = df.groupBy("delivery_type").agg(count("*").alias("count")).orderBy(desc("count"))
    delivery_types = {row["delivery_type"] or "UNKNOWN": int(row["count"]) for row in del_df.collect()}

    print("\n[Step 4/5] Profiling Data Quality Anomalies & Quarantine Triggers...")
    
    anomaly_agg = df.select(
        _sum(when(col("order_id").isNull() | (trim(col("order_id")) == ""), 1).otherwise(0)).alias("missing_order_id"),
        _sum(when(col("customer_id").isNull() | (trim(col("customer_id")) == ""), 1).otherwise(0)).alias("missing_customer_id"),
        _sum(when(col("total_amount").contains("???"), 1).otherwise(0)).alias("symbolic_total"),
        _sum(when(col("items_json") == "not-json", 1).otherwise(0)).alias("corrupted_items_json"),
        _sum(when(col("customer_phone").isNull() | ~col("customer_phone").rlike(PHONE_REGEX), 1).otherwise(0)).alias("invalid_phone"),
        _sum(when(col("customer_email").isNull() | col("customer_email").contains("@@") | ~col("customer_email").contains("@"), 1).otherwise(0)).alias("invalid_email"),
        _sum(when(col("currency").isNotNull() & (col("currency") != "YER"), 1).otherwise(0)).alias("non_standard_currency")
    ).collect()[0]

    anomalies = {
        "MISSING_ORDER_ID": int(anomaly_agg["missing_order_id"] or 0),
        "MISSING_CUSTOMER_ID": int(anomaly_agg["missing_customer_id"] or 0),
        "SYMBOLIC_VALUE": int(anomaly_agg["symbolic_total"] or 0),
        "CORRUPTED_ITEMS_JSON": int(anomaly_agg["corrupted_items_json"] or 0),
        "INVALID_PHONE": int(anomaly_agg["invalid_phone"] or 0),
        "INVALID_EMAIL": int(anomaly_agg["invalid_email"] or 0),
        "NON_STANDARD_CURRENCY": int(anomaly_agg["non_standard_currency"] or 0),
    }

    numeric_clean_col = translate(col("total_amount"), "٠١٢٣٤٥٦٧٨٩٫٬", "0123456789.,")
    numeric_clean_col = regexp_replace(numeric_clean_col, r"[\,\s]", "")
    numeric_val = when(numeric_clean_col.rlike(r"^[0-9]+(\.[0-9]+)?$"), numeric_clean_col.cast("double")).otherwise(None)
    
    rev_agg = df.filter(~col("total_amount").contains("???") & col("total_amount").isNotNull()).select(
        _sum(numeric_val).alias("total_revenue"),
        avg(numeric_val).alias("avg_order_value")
    ).collect()[0]

    total_revenue = float(rev_agg["total_revenue"] or 0.0)
    avg_order_value = float(rev_agg["avg_order_value"] or 0.0)

    elapsed = time.perf_counter() - start_time
    throughput = total_records / elapsed if elapsed > 0 else 0.0

    print(f"\n[Step 5/5] Analysis Complete in {elapsed:.2f}s! ({throughput:,.2f} records/s)")

    results = {
        "dataset_name": input_path.name,
        "file_size_gb": round(file_size_gb, 2),
        "file_size_mb": round(file_size_mb, 2),
        "total_records": total_records,
        "num_spark_partitions": num_partitions,
        "spark_driver_memory": SPARK_DRIVER_MEMORY,
        "elapsed_seconds": round(elapsed, 2),
        "throughput_records_per_sec": round(throughput, 2),
        "financial_summary": {
            "estimated_total_gmv_yer": round(total_revenue, 2),
            "average_order_value_yer": round(avg_order_value, 2),
        },
        "status_distribution": status_counts,
        "payment_methods": payment_methods,
        "payment_statuses": payment_statuses,
        "top_cities": top_cities,
        "delivery_types": delivery_types,
        "data_quality_anomalies": anomalies,
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
    }

    rep_path = Path(reports_dir)
    rep_path.mkdir(parents=True, exist_ok=True)
    json_path = rep_path / "spark_analysis_30m.json"
    md_path = rep_path / "spark_analysis_30m.md"

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"# تقرير التحليل الموزع لملف الـ 30 مليون سجل بـ Apache Spark\n\n")
        f.write(f"- **اسم الملف:** `{input_path.name}` ({file_size_gb:.2f} GB)\n")
        f.write(f"- **إجمالي السجلات المحللة:** `{total_records:,}` سجل\n")
        f.write(f"- **عدد الـ Partitions في سبارك:** `{num_partitions}` بارتشن متوازي\n")
        f.write(f"- **الوقت المستغرق:** `{elapsed:.2f}` ثانية\n")
        f.write(f"- **معدل السرعة (Throughput):** `{throughput:,.2f}` سجل/ثانية\n\n")
        f.write(f"## 1. المؤشرات المالية\n")
        f.write(f"- **إجمالي المبيعات التقديرية (GMV):** `{total_revenue:,.2f} YER`\n")
        f.write(f"- **متوسط قيمة الطلب (AOV):** `{avg_order_value:,.2f} YER`\n\n")
        f.write(f"## 2. توزيع المبيعات حسب المدن\n\n| المدينة | عدد الطلبات |\n| :--- | :--- |\n")
        for city, cnt in top_cities.items():
            f.write(f"| {city} | {cnt:,} |\n")
        f.write(f"\n## 3. بروفايل جودة البيانات والشذوذ (Anomalies)\n\n| رمز الشذوذ / الخطأ | العدد المرصود |\n| :--- | :--- |\n")
        for anom, cnt in anomalies.items():
            f.write(f"| `{anom}` | {cnt:,} |\n")

    print(f"  [Saved] Reports generated successfully:\n    - {json_path}\n    - {md_path}\n")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark 30M Big Data Dataset Distributed Analyzer")
    parser.add_argument("--input", "-i", default=r"..\big data\orders_huge_mixed_quality.csv", help="مسار ملف الـ 13GB")
    parser.add_argument("--reports", "-r", default="reports", help="مجلد حفظ التقارير")
    args = parser.parse_args()

    analyze_dataset(args.input, args.reports)
