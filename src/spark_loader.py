"""
spark_loader.py — PySpark Parallel Loader
محرك PySpark للملفات الكبيرة (> 200MB)

يستخدم SparkSession مع Schema ثابتة (جميع الحقول StringType)
لقراءة CSV بشكل متوازي، ثم يُدخل البيانات في MongoDB على مراحل:
  1. Raw Ingestion → orders_raw (الكل كما هو)
  2. Cleaning & Validation → orders_validated / orders_quarantine
"""

import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from config.settings import (
    MONGO_URI, DB_NAME, BATCH_SIZE, FILE_ENCODING,
    SPARK_APP_NAME, SPARK_DRIVER_MEMORY, SPARK_TARGET_PARTITIONS,
    RAW_COLLECTION, VALIDATED_COLLECTION, QUARANTINE_COLLECTION,
)
from src.mongo_setup import get_collections
from src.quality_rules import apply_rules
from src.metrics import PipelineMetrics


def _get_spark_session():
    """إنشاء أو استرجاع SparkSession مع الإعدادات المطلوبة."""
    import sys
    from pyspark.sql import SparkSession

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master("local[*]")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.python.worker.reuse", "true")
        .config("spark.python.worker.timeout", "300")
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.default.parallelism", str(SPARK_TARGET_PARTITIONS))
        .config("spark.sql.shuffle.partitions", str(SPARK_TARGET_PARTITIONS))
        .config("spark.sql.adaptive.enabled", "true")
        .config(
            "spark.sql.files.maxPartitionBytes",
            str(128 * 1024 * 1024)
        )
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def _get_raw_schema():
    """
    Schema ثابتة — جميع الحقول StringType.
    هذا يمنع فقدان القيم المشوّهة مثل ٧٠٦٠٠٠٫٠ و ??? و 135,000.00
    """
    from pyspark.sql.types import StructType, StructField, StringType

    return StructType([
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


def _process_partition(partition_iter, run_id, file_name):
    """
    معالجة بارتشن واحد: Raw ingestion + Cleaning + Classification.
    يتم استدعاؤها عبر foreachPartition لمعالجة كل بارتشن بشكل مستقل.

    Returns:
        Iterator of dicts with counts: raw, valid, corrected, quarantine, errors
    """
    from pymongo import MongoClient

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    raw_col = db[RAW_COLLECTION]
    validated_col = db[VALIDATED_COLLECTION]
    quarantine_col = db[QUARANTINE_COLLECTION]

    raw_batch = []
    validated_batch = []
    quarantine_batch = []

    counts = {
        "raw": 0, "valid": 0, "corrected": 0,
        "quarantine": 0, "errors": {}
    }
    batch_size = BATCH_SIZE

    for row in partition_iter:
        row_dict = row.asDict()
        counts["raw"] += 1

        # ─── Stage 1: Raw Ingestion ───
        raw_doc = {
            "run_id": run_id,
            "file_source": file_name,
            "source_row_number": counts["raw"],
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "engine_used": "pyspark",
            "raw_record": row_dict,
        }
        raw_batch.append(raw_doc)

        # ─── Stage 2: Cleaning & Classification ───
        status, record, details = apply_rules(row_dict)

        if status == "quarantine":
            error_codes = [d.get("error_code", "UNKNOWN") for d in details]
            quarantine_doc = {
                "run_id": run_id,
                "file_source": file_name,
                "source_row_number": counts["raw"],
                "raw_record": row_dict,
                "error_codes": error_codes,
                "error_details": details,
                "quarantined_at": datetime.now(timezone.utc).isoformat(),
            }
            quarantine_batch.append(quarantine_doc)
            counts["quarantine"] += 1
            for code in error_codes:
                counts["errors"][code] = counts["errors"].get(code, 0) + 1
        else:
            record["run_id"] = run_id
            record["validated_at"] = datetime.now(timezone.utc).isoformat()
            if status == "corrected":
                record["corrections"] = details
                counts["corrected"] += 1
            else:
                counts["valid"] += 1
            validated_batch.append(record)

        # ─── Flush when full ───
        if len(raw_batch) >= batch_size:
            try:
                raw_col.insert_many(raw_batch, ordered=False)
            except BulkWriteError:
                pass
            raw_batch.clear()

        if len(validated_batch) >= batch_size:
            ops = [
                UpdateOne(
                    {"order_id": doc["order_id"]},
                    {"$set": doc},
                    upsert=True
                )
                for doc in validated_batch
            ]
            try:
                validated_col.bulk_write(ops, ordered=False)
            except BulkWriteError:
                pass
            validated_batch.clear()

        if len(quarantine_batch) >= batch_size:
            try:
                quarantine_col.insert_many(quarantine_batch, ordered=False)
            except BulkWriteError:
                pass
            quarantine_batch.clear()

    # ─── Flush remaining ───
    if raw_batch:
        try:
            raw_col.insert_many(raw_batch, ordered=False)
        except BulkWriteError:
            pass

    if validated_batch:
        ops = [
            UpdateOne(
                {"order_id": doc["order_id"]},
                {"$set": doc},
                upsert=True
            )
            for doc in validated_batch
        ]
        try:
            validated_col.bulk_write(ops, ordered=False)
        except BulkWriteError:
            pass

    if quarantine_batch:
        try:
            quarantine_col.insert_many(quarantine_batch, ordered=False)
        except BulkWriteError:
            pass

    client.close()
    yield counts


def load(file_path, run_id=None, metrics=None):
    """
    تحميل ملف CSV كبير (> 200MB) إلى MongoDB باستخدام PySpark.

    Args:
        file_path: مسار ملف CSV
        run_id: معرف التشغيل
        metrics: كائن PipelineMetrics

    Returns:
        PipelineMetrics: كائن المقاييس بعد الانتهاء
    """
    if run_id is None:
        run_id = str(uuid.uuid4())[:8]

    if metrics is None:
        metrics = PipelineMetrics(
            run_id=run_id,
            file_source=str(file_path),
            engine_used="pyspark"
        )

    file_name = Path(file_path).name

    print(f"\n{'='*60}")
    print(f"  PySpark Loader — Parallel Distributed Mode")
    print(f"  File: {file_name}")
    print(f"  Run ID: {run_id}")
    print(f"{'='*60}\n")

    # ─── إنشاء SparkSession ───
    spark = _get_spark_session()

    print(f"  Spark version       : {spark.version}")
    print(f"  Master              : {spark.sparkContext.master}")
    print(f"  Default parallelism : {spark.sparkContext.defaultParallelism}")
    print(f"  Spark UI            : {spark.sparkContext.uiWebUrl}")
    print()

    # ─── قراءة CSV مع Schema ثابتة ───
    df = (
        spark.read
        .schema(_get_raw_schema())
        .option("header", True)
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .csv(str(file_path))
    )

    num_partitions = df.rdd.getNumPartitions()
    print(f"  Input partitions: {num_partitions}")
    df.printSchema()

    # ─── إعادة التقسيم إذا لزم الأمر ───
    if num_partitions < SPARK_TARGET_PARTITIONS:
        df = df.repartition(SPARK_TARGET_PARTITIONS)
        print(f"  Repartitioned to: {df.rdd.getNumPartitions()}")

    # ─── معالجة كل بارتشن بشكل مستقل ───
    print("\n  Processing partitions...")

    # جمع النتائج من كل بارتشن
    results_rdd = df.rdd.mapPartitions(
        lambda part: _process_partition(part, run_id, file_name)
    )

    # تنفيذ المعالجة وجمع الإحصائيات
    partition_results = results_rdd.collect()

    # ─── تجميع النتائج ───
    total_errors = {}
    for result in partition_results:
        # تسجيل Valid
        for _ in range(result["valid"]):
            metrics.record_result("valid")
        # تسجيل Corrected
        for _ in range(result["corrected"]):
            metrics.record_result("corrected")
        # تسجيل Quarantine السجلات الفعلية
        for _ in range(result["quarantine"]):
            metrics.record_result("quarantine")
        # تسجيل تفصيل أكواد الأخطاء
        for code, count in result["errors"].items():
            metrics.error_breakdown[code] = metrics.error_breakdown.get(code, 0) + count
            total_errors[code] = total_errors.get(code, 0) + count

    # ─── ملخص ───
    total_raw = sum(r["raw"] for r in partition_results)
    total_valid = sum(r["valid"] for r in partition_results)
    total_corrected = sum(r["corrected"] for r in partition_results)
    total_quarantine = sum(r["quarantine"] for r in partition_results)

    print(f"\n{'─'*60}")
    print(f"  PySpark Loader Summary")
    print(f"{'─'*60}")
    print(f"  Total partitions processed : {len(partition_results)}")
    print(f"  Raw records                : {total_raw:,}")
    print(f"  Valid records              : {total_valid:,}")
    print(f"  Corrected records          : {total_corrected:,}")
    print(f"  Quarantined records        : {total_quarantine:,}")
    print(f"{'─'*60}\n")

    # إيقاف Spark
    spark.stop()

    return metrics
