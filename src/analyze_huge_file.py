"""
analyze_huge_file.py — تحليل شامل للبيانات الضخمة (13.26 GB) باستخدام PySpark
Distributed Big Data Analyzer for the entire 13.26GB Dataset
مقرر البيانات الضخمة (العملي) | جامعة الرازي | إشراف: م. عمر أبو سند
"""

import os
import sys
import time
import json
from pathlib import Path
from datetime import datetime

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from config.settings import (
    FILE_ENCODING, SPARK_APP_NAME, SPARK_DRIVER_MEMORY, SPARK_TARGET_PARTITIONS
)


def run_huge_file_analysis(file_path=None, output_dir="reports"):
    """
    تحليل الملف الكبير (13.26 GB) كاملاً باستخدام Apache PySpark بالتوازي.
    يقوم بحساب الإحصائيات الشاملة دون استهلاك القرص أو إرهاق الجهاز.
    """
    if file_path is None:
        file_path = PROJECT_ROOT.parent / "big data" / "orders_huge_mixed_quality.csv"

    file_path = Path(file_path).resolve()
    if not file_path.exists():
        print(f"\n[ERROR] Huge file not found at: {file_path}")
        return

    file_size_bytes = file_path.stat().st_size
    file_size_gb = file_size_bytes / (1024 ** 3)

    print("\n" + "=" * 70)
    print("  +------------------------------------------------------------------+")
    print("  |   PySpark Distributed Big Data Analyzer (13.26 GB Full Scan)    |")
    print("  +------------------------------------------------------------------+")
    print(f"  File Source  : {file_path.name}")
    print(f"  File Size    : {file_size_gb:.2f} GB ({file_size_bytes:,} bytes)")
    print(f"  Partitions   : {SPARK_TARGET_PARTITIONS}")
    print(f"  Memory Limit : {SPARK_DRIVER_MEMORY}")
    print("=" * 70 + "\n")

    print("[Step 1/4] Initializing Apache Spark Session...")
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType

    spark = (
        SparkSession.builder
        .appName("BigData-HugeFile-Analyzer")
        .master("local[*]")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.default.parallelism", str(SPARK_TARGET_PARTITIONS))
        .config("spark.sql.shuffle.partitions", str(SPARK_TARGET_PARTITIONS))
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Define StringType Schema for all fields
    raw_schema = StructType([
        StructField("order_id",       StringType(), True),
        StructField("order_date",     StringType(), True),
        StructField("status",         StringType(), True),
        StructField("customer_id",    StringType(), True),
        StructField("customer_name",  StringType(), True),
        StructField("customer_phone", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("city",           StringType(), True),
        StructField("district",       StringType(), True),
        StructField("delivery_type",  StringType(), True),
        StructField("delivery_cost",  StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("payment_amount", StringType(), True),
        StructField("currency",       StringType(), True),
        StructField("total_amount",   StringType(), True),
        StructField("items_json",     StringType(), True),
    ])

    print("[Step 2/4] Reading Dataset with Distributed Schema...")
    start_time = time.perf_counter()

    df = (
        spark.read
        .option("header", "true")
        .option("encoding", "UTF-8")
        .schema(raw_schema)
        .csv(str(file_path))
    )

    print("[Step 3/4] Running Distributed Aggregations & Quality Checks across all CPU cores...")
    
    # 1. Total Count
    total_records = df.count()
    elapsed_count = time.perf_counter() - start_time
    print(f"  -> Total Rows Counted: {total_records:,} in {elapsed_count:.2f}s")

    # 2. Status Breakdown
    status_df = df.groupBy("status").count().orderBy(F.desc("count")).collect()
    status_dist = {r["status"] or "NULL": r["count"] for r in status_df}

    # 3. Payment Method Breakdown
    pm_df = df.groupBy("payment_method").count().orderBy(F.desc("count")).collect()
    pm_dist = {r["payment_method"] or "NULL": r["count"] for r in pm_df}

    # 4. Currency Breakdown
    curr_df = df.groupBy("currency").count().orderBy(F.desc("count")).collect()
    curr_dist = {r["currency"] or "NULL": r["count"] for r in curr_df}

    # 5. Top Cities Breakdown
    city_df = df.groupBy("city").count().orderBy(F.desc("count")).limit(10).collect()
    city_dist = {r["city"] or "NULL": r["count"] for r in city_df}

    # 6. Quality Anomalies Detection
    missing_orders = df.filter(F.col("order_id").isNull() | (F.trim(F.col("order_id")) == "")).count()
    missing_customers = df.filter(F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == "")).count()
    symbolic_totals = df.filter(F.col("total_amount").contains("???")).count()
    corrupted_json = df.filter(F.col("items_json") == "not-json").count()
    arabic_numerals_count = df.filter(F.col("total_amount").rlike("[٠-٩]")).count()
    comma_numbers_count = df.filter(F.col("total_amount").contains(",")).count()

    total_elapsed = time.perf_counter() - start_time
    throughput = total_records / total_elapsed if total_elapsed > 0 else 0

    print("\n[Step 4/4] Generating Big Data Analytics Report...")
    report_dict = {
        "dataset_name": file_path.name,
        "file_size_gb": round(file_size_gb, 2),
        "file_size_bytes": file_size_bytes,
        "total_records": total_records,
        "scan_time_seconds": round(total_elapsed, 2),
        "throughput_records_per_sec": round(throughput, 2),
        "spark_partitions": SPARK_TARGET_PARTITIONS,
        "status_distribution": status_dist,
        "payment_method_distribution": pm_dist,
        "currency_distribution": curr_dist,
        "top_cities": city_dist,
        "data_quality_anomalies": {
            "missing_order_ids": missing_orders,
            "missing_customer_ids": missing_customers,
            "symbolic_total_amount_question_marks": symbolic_totals,
            "corrupted_items_not_json": corrupted_json,
            "arabic_numerals_detected": arabic_numerals_count,
            "thousand_separator_commas_detected": comma_numbers_count
        },
        "timestamp": datetime.now().isoformat()
    }

    # Save JSON Report
    out_dir = PROJECT_ROOT / output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    
    json_path = out_dir / "huge_file_analysis.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report_dict, f, ensure_ascii=False, indent=2)

    # Save Markdown Report
    md_path = out_dir / "huge_file_analysis.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"""# 🚀 تقرير التحليل الشامل للبيانات الضخمة (13.26 GB) عبر Apache PySpark

**المقرر:** البيانات الضخمة (العملي) | **جامعة الرازي**  
**إشراف:** م. عمر أبو سند | **إعداد الطالب:** زياد

---

## 📊 1. ملخص الأداء والحجم (Performance & Scale)

| المقياس | القيمة |
| :--- | :--- |
| **اسم الملف** | `{file_path.name}` |
| **حجم الملف** | **{file_size_gb:.2f} GB** ({file_size_bytes:,} بايت) |
| **إجمالي السجلات المفحوصة** | **{total_records:,} سجل** |
| **الزمن المستغرق** | **{total_elapsed:.2f} ثانية** |
| **معدل السرعة (Throughput)** | **{throughput:,.2f} سجل / ثانية** |
| **عدد مسارات التوازي (Partitions)** | `{SPARK_TARGET_PARTITIONS}` مسارات متوازية |

---

## 🔍 2. كشف شوائب وجودة البيانات (Data Quality Anomalies)

| نوع الشائبة / الخطأ | عدد السجلات المتأثرة | النسبة من الإجمالي |
| :--- | :--- | :--- |
| **معرف العميل مفقود (Missing Customer ID)** | `{missing_customers:,}` | `{(missing_customers/total_records)*100:.2f}%` |
| **قائمة منتجات تالفة (`items_json = 'not-json'`)** | `{corrupted_json:,}` | `{(corrupted_json/total_records)*100:.2f}%` |
| **مبالغ تحتوي على أرقام عربية مشرقية (`٠-٩`)** | `{arabic_numerals_count:,}` | `{(arabic_numerals_count/total_records)*100:.2f}%` |
| **مبالغ تحتوي على فواصل آلاف (`,`)** | `{comma_numbers_count:,}` | `{(comma_numbers_count/total_records)*100:.2f}%` |
| **مبالغ تحتوي على رموز `???`** | `{symbolic_totals:,}` | `{(symbolic_totals/total_records)*100:.2f}%` |
| **معرف الطلب مفقود (Missing Order ID)** | `{missing_orders:,}` | `{(missing_orders/total_records)*100:.2f}%` |

---

## 🏙️ 3. توزيع المدن الأكثر طلباً (Top Cities)

| المدينة | عدد الطلبات | النسبة |
| :--- | :--- | :--- |
""" + "
".join([f"| **{city}** | {cnt:,} | {(cnt/total_records)*100:.2f}% |" for city, cnt in city_dist.items()]) + f"""

---

## 💳 4. طرق الدفع المعتمدة (Payment Methods)

| طريقة الدفع | عدد العمليات | النسبة |
| :--- | :--- | :--- |
""" + "
".join([f"| **{pm}** | {cnt:,} | {(cnt/total_records)*100:.2f}% |" for pm, cnt in pm_dist.items()]) + f"""

---
*تم توليد هذا التقرير آلياً عبر محرك Apache PySpark الموزع.*
""")

    print(f"  [SUCCESS] Reports saved successfully to:")
    print(f"   - {json_path}")
    print(f"   - {md_path}")
    print("=" * 70 + "\n")
    spark.stop()


if __name__ == '__main__':
    run_huge_file_analysis()
