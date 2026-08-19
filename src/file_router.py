"""
وحدة توجيه الملفات (File Router)
تحدد محرك المعالجة المناسب بناءً على حجم ملف البيانات:
- Python Batch للملفات الصغيرة (<= SMALL_FILE_THRESHOLD_MB)
- PySpark للملفات الكبيرة (> SMALL_FILE_THRESHOLD_MB)
"""

import os
import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import SMALL_FILE_THRESHOLD_MB


def route_file(file_path: str) -> str:
    """فحص حجم الملف وتحديد محرك المعالجة المناسب: 'python_batch' أو 'pyspark'.

    Check file size and return engine name: 'python_batch' or 'pyspark'.

    Args:
        file_path (str): المسار الكامل لملف البيانات المراد معالجته.

    Returns:
        str: اسم المحرك المختار ('python_batch' أو 'pyspark').

    Raises:
        FileNotFoundError: إذا لم يكن الملف موجوداً في المسار المحدد.
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"الملف غير موجود: {file_path}")

    # حساب حجم الملف بالميجابايت
    size_bytes = os.path.getsize(file_path)
    size_mb = size_bytes / (1024 * 1024)
    file_name = os.path.basename(file_path)

    # تحديد المحرك بناءً على الحد المحدد في الإعدادات
    if size_mb <= SMALL_FILE_THRESHOLD_MB:
        engine = "python_batch"
    else:
        engine = "pyspark"

    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    # طباعة تفاصيل القرار
    print("=" * 60)
    print(f"  File Name   : {file_name}")
    print(f"  File Size   : {size_mb:.2f} MB ({size_bytes:,} bytes)")
    print(f"  Threshold   : {SMALL_FILE_THRESHOLD_MB} MB")
    print(f"  Chosen Engine: {engine}")
    print("=" * 60)

    return engine


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    if len(sys.argv) > 1:
        target_file = sys.argv[1]
        try:
            chosen_engine = route_file(target_file)
            print(f"Final Decision: {chosen_engine}")
        except Exception as err:
            print(f"[ERROR]: {err}", file=sys.stderr)
            sys.exit(1)
    else:
        print("Usage: python src/file_router.py <path_to_csv_file>")
