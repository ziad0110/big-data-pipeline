"""
batch_loader.py — Python Batch Loader (Streaming)
محرك الدفعات لقراءة ملفات CSV الصغيرة (<= 200MB) بتقنية التدفق

يقرأ الملف سطراً بسطر عبر csv.DictReader (بدون تحميل كامل في الذاكرة)،
ويُدخل السجلات في MongoDB على ثلاث مراحل:
  1. Raw Ingestion → orders_raw
  2. Cleaning & Validation → orders_validated / orders_quarantine
  3. Metrics tracking
"""

import csv
import uuid
from datetime import datetime, timezone

from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from config.settings import FILE_ENCODING, BATCH_SIZE
from src.mongo_setup import get_database, get_collections
from src.quality_rules import apply_rules
from src.metrics import PipelineMetrics


def _insert_batch_raw(collection, batch):
    """إدخال دفعة من السجلات الخام في orders_raw."""
    if not batch:
        return 0
    try:
        result = collection.insert_many(batch, ordered=False)
        return len(result.inserted_ids)
    except BulkWriteError as e:
        details = e.details or {}
        return details.get("nInserted", 0)


def _upsert_batch_validated(collection, batch):
    """
    إدخال/تحديث دفعة في orders_validated باستخدام Idempotent Upsert.
    يمنع التكرار عند إعادة تشغيل نفس الملف.
    """
    if not batch:
        return {"inserted": 0, "updated": 0, "unchanged": 0}

    operations = []
    for doc in batch:
        order_id = doc["order_id"]
        operations.append(
            UpdateOne(
                {"order_id": order_id},        # فلتر: البحث بمعرف الطلب
                {"$set": doc},                  # تحديث: استبدال كل الحقول
                upsert=True                     # إدخال إذا لم يكن موجوداً
            )
        )

    try:
        result = collection.bulk_write(operations, ordered=False)
        return {
            "inserted": result.upserted_count,
            "updated": result.modified_count,
            "unchanged": result.matched_count - result.modified_count,
        }
    except BulkWriteError as e:
        details = e.details or {}
        return {
            "inserted": details.get("nUpserted", 0),
            "updated": details.get("nModified", 0),
            "unchanged": 0,
        }


def _insert_batch_quarantine(collection, batch):
    """إدخال دفعة من السجلات المعزولة في orders_quarantine."""
    if not batch:
        return 0
    try:
        result = collection.insert_many(batch, ordered=False)
        return len(result.inserted_ids)
    except BulkWriteError as e:
        details = e.details or {}
        return details.get("nInserted", 0)


def load(file_path, run_id=None, metrics=None, progress_callback=None, cancel_check=None):
    """
    تحميل ملف CSV صغير (<= 200MB) إلى MongoDB باستخدام Python Batch Streaming.

    المراحل:
      1. قراءة تدفقية → إدخال خام في orders_raw
      2. تطبيق قواعد التنظيف → orders_validated أو orders_quarantine
      3. تتبع المقاييس وتحديث شريط التقدم والوقت المتبقي

    Args:
        file_path: مسار ملف CSV
        run_id: معرف التشغيل (يُولَّد تلقائياً إذا لم يُعطَ)
        metrics: كائن PipelineMetrics (يُنشأ تلقائياً إذا لم يُعطَ)
        progress_callback: دالة رد اتصال لتحديث واجهة المستخدم (current, total, speed, eta)
        cancel_check: دالة تفحص ما إذا طلب المستخدم إيقاف المعالجة (ترجع True للإيقاف)

    Returns:
        PipelineMetrics: كائن المقاييس بعد الانتهاء
    """
    if run_id is None:
        run_id = str(uuid.uuid4())[:8]

    if metrics is None:
        metrics = PipelineMetrics(
            run_id=run_id,
            file_source=str(file_path),
            engine_used="python_batch"
        )

    collections = get_collections()
    raw_col = collections["raw"]
    validated_col = collections["validated"]
    quarantine_col = collections["quarantine"]

    # مصفوفات الدفعات المؤقتة
    raw_batch = []
    validated_batch = []
    quarantine_batch = []

    # عدادات النتائج التراكمية
    total_inserted_raw = 0
    total_upsert_results = {"inserted": 0, "updated": 0, "unchanged": 0}
    total_quarantined = 0

    file_name = str(file_path).split("\\")[-1].split("/")[-1]

    print(f"\n{'='*60}")
    print(f"  Python Batch Loader — Streaming Mode")
    print(f"  File: {file_name}")
    print(f"  Run ID: {run_id}")
    print(f"  Batch Size: {BATCH_SIZE:,}")
    print(f"{'='*60}\n")

    # ─── القراءة التدفقية ───
    with open(file_path, encoding=FILE_ENCODING, newline="") as f:
        reader = csv.DictReader(f)

        # تنظيف أسماء الأعمدة من BOM المتبقي
        if reader.fieldnames:
            reader.fieldnames = [
                col.replace("\ufeff", "").strip()
                for col in reader.fieldnames
            ]

        for row_num, row in enumerate(reader, start=1):
            # ─── Stage 1: Raw Ingestion ───
            raw_doc = {
                "run_id": run_id,
                "file_source": file_name,
                "source_row_number": row_num,
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "engine_used": "python_batch",
                "raw_record": dict(row),  # نسخة من السجل كما هو
            }
            raw_batch.append(raw_doc)

            # ─── Stage 2: Cleaning & Classification ───
            status, record, details = apply_rules(dict(row))

            if status == "quarantine":
                error_codes = [d.get("error_code", "UNKNOWN") for d in details]
                quarantine_doc = {
                    "run_id": run_id,
                    "file_source": file_name,
                    "source_row_number": row_num,
                    "raw_record": dict(row),
                    "error_codes": error_codes,
                    "error_details": details,
                    "quarantined_at": datetime.now(timezone.utc).isoformat(),
                }
                quarantine_batch.append(quarantine_doc)
                metrics.record_result("quarantine", error_codes)
            else:
                # valid أو corrected
                record["run_id"] = run_id
                record["validated_at"] = datetime.now(timezone.utc).isoformat()
                if status == "corrected":
                    record["corrections"] = details  # Audit Trail
                validated_batch.append(record)
                metrics.record_result(status)

            # ─── Flush batches when full ───
            if len(raw_batch) >= BATCH_SIZE:
                total_inserted_raw += _insert_batch_raw(raw_col, raw_batch)
                raw_batch.clear()

            if len(validated_batch) >= BATCH_SIZE:
                result = _upsert_batch_validated(validated_col, validated_batch)
                for k in total_upsert_results:
                    total_upsert_results[k] += result[k]
                validated_batch.clear()

            if len(quarantine_batch) >= BATCH_SIZE:
                total_quarantined += _insert_batch_quarantine(
                    quarantine_col, quarantine_batch
                )
                quarantine_batch.clear()

            # طباعة وتحديث التقدم
            metrics.print_progress(interval=BATCH_SIZE)
            
            # فحص طلب الإلغاء
            if cancel_check and cancel_check():
                print("[WARN] User requested pipeline cancellation!")
                break

            # رد اتصال تحديث واجهة المستخدم
            if progress_callback and (row_num % 2500 == 0 or row_num == 1):
                elapsed_now = time.perf_counter() - metrics.start_time
                speed_now = row_num / elapsed_now if elapsed_now > 0 else 0
                progress_callback(row_num, speed_now, elapsed_now)

    # ─── Flush remaining batches ───
    if raw_batch:
        total_inserted_raw += _insert_batch_raw(raw_col, raw_batch)
        raw_batch.clear()

    if validated_batch:
        result = _upsert_batch_validated(validated_col, validated_batch)
        for k in total_upsert_results:
            total_upsert_results[k] += result[k]
        validated_batch.clear()

    if quarantine_batch:
        total_quarantined += _insert_batch_quarantine(
            quarantine_col, quarantine_batch
        )
        quarantine_batch.clear()

    # ─── ملخص التحميل ───
    print(f"\n{'─'*60}")
    print(f"  Batch Loader Summary")
    print(f"{'─'*60}")
    print(f"  Raw inserted       : {total_inserted_raw:,}")
    print(f"  Validated inserted : {total_upsert_results['inserted']:,}")
    print(f"  Validated updated  : {total_upsert_results['updated']:,}")
    print(f"  Validated unchanged: {total_upsert_results['unchanged']:,}")
    print(f"  Quarantined        : {total_quarantined:,}")
    print(f"{'─'*60}\n")

    return metrics
