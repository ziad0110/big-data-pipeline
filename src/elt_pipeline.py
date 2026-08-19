"""
elt_pipeline.py — ELT Pipeline Orchestrator
المنسق الرئيسي لخط البيانات الهجين (Hybrid ELT Pipeline)

يدير دورة حياة التشغيل الكاملة:
  1. توليد run_id فريد
  2. تهيئة MongoDB (Collections + Indexes)
  3. توجيه الملف للمحرك المناسب (File Router)
  4. تشغيل المحرك (Batch أو Spark)
  5. فحص معادلة الاتساق
  6. توليد التقارير
"""

import os
import sys
import uuid
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from src.file_router import route_file
from src.mongo_setup import initialize as init_mongo
from src import batch_loader
from src import spark_loader
from src.metrics import PipelineMetrics


def run(file_path, reports_dir="reports"):
    """
    تشغيل خط ELT الكامل على ملف CSV.

    Args:
        file_path: مسار ملف CSV المُدخل
        reports_dir: مجلد حفظ التقارير

    Returns:
        dict: تقرير النتائج الكامل
    """
    file_path = Path(file_path).resolve()

    # التحقق من وجود الملف
    if not file_path.exists():
        raise FileNotFoundError(f"الملف غير موجود: {file_path}")

    # ─── توليد run_id فريد ───
    run_id = str(uuid.uuid4())[:8]

    print("\n" + "=" * 60)
    print("  +--------------------------------------------------+")
    print("  |     ELT Data Pipeline -- Hybrid Engine           |")
    print("  +--------------------------------------------------+")
    print(f"  Run ID    : {run_id}")
    print(f"  File      : {file_path.name}")
    print(f"  Full Path : {file_path}")
    print("=" * 60 + "\n")

    # ─── Step 1: تهيئة MongoDB ───
    print("[Step 1/4] MongoDB Initialization...")
    init_mongo()

    # ─── Step 2: توجيه الملف ───
    print("\n[Step 2/4] File Routing & Engine Selection...")
    engine = route_file(str(file_path))

    # ─── Step 3: إنشاء كائن المقاييس ───
    metrics = PipelineMetrics(
        run_id=run_id,
        file_source=str(file_path),
        engine_used=engine
    )

    # ─── Step 4: تشغيل المحرك ───
    print(f"\n[Step 3/4] Running Engine: {engine}...")
    if engine == "python_batch":
        metrics = batch_loader.load(
            file_path=str(file_path),
            run_id=run_id,
            metrics=metrics
        )
    elif engine == "pyspark":
        metrics = spark_loader.load(
            file_path=str(file_path),
            run_id=run_id,
            metrics=metrics
        )
    else:
        raise ValueError(f"Unknown engine: {engine}")

    # ─── Step 5: فحص الاتساق وتوليد التقرير ───
    print("\n[Step 4/4] Verifying Consistency and Generating Reports...")
    consistency_ok = metrics.verify_consistency()

    if not consistency_ok:
        print("\n  [WARN] Consistency check FAILED!")
        print("  raw_count != valid + corrected + quarantine")

    report = metrics.generate_report(reports_dir=reports_dir)

    # ─── ملخص نهائي ───
    print("\n" + "=" * 60)
    print("  +--------------------------------------------------+")
    print("  |              Pipeline Complete                   |")
    print("  +--------------------------------------------------+")
    print(f"  Run ID           : {run_id}")
    print(f"  Engine           : {engine}")
    print(f"  Total Raw        : {report['total_raw_records']:,}")
    print(f"  Valid            : {report['valid_records']:,}")
    print(f"  Corrected        : {report['corrected_records']:,}")
    print(f"  Quarantined      : {report['quarantined_records']:,}")
    print(f"  Consistency      : {report['consistency_check']}")
    print(f"  Elapsed          : {report['elapsed_seconds']:.2f}s")
    print(f"  Throughput       : {report['throughput_records_per_sec']:,.2f} rec/s")
    print(f"  Report saved to  : {reports_dir}/results.json")
    print("=" * 60 + "\n")

    return report
