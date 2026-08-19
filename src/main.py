"""
main.py — نقطة الدخول الرئيسية لخط البيانات الهجين
Main entry point for the Hybrid ELT Data Pipeline

الاستخدام:
    python src/main.py --input "data/sample_orders.csv"
    python src/main.py --input "big data/orders_huge_mixed_quality.csv"
"""

import sys
import os
import argparse
from pathlib import Path

# إضافة المجلد الجذر للمشروع إلى مسار الاستيراد
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.elt_pipeline import run as run_pipeline


def main():
    """نقطة الدخول الرئيسية."""
    parser = argparse.ArgumentParser(
        description="Hybrid ELT Data Pipeline — خط بيانات هجين لمعالجة طلبات المتاجر الإلكترونية",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
أمثلة:
  python src/main.py --input "data/sample_orders.csv"
  python src/main.py --input "big data/orders_huge_mixed_quality.csv"
  python src/main.py --input "data/sample_orders.csv" --reports "reports"
        """
    )

    parser.add_argument(
        "--input", "-i",
        required=True,
        help="مسار ملف CSV المُدخل (مطلق أو نسبي)"
    )

    parser.add_argument(
        "--reports", "-r",
        default="reports",
        help="مجلد حفظ التقارير (افتراضي: reports)"
    )

    args = parser.parse_args()

    # حل المسار النسبي بالنسبة لمجلد المشروع
    input_path = Path(args.input)
    if not input_path.is_absolute():
        input_path = PROJECT_ROOT / input_path

    reports_dir = Path(args.reports)
    if not reports_dir.is_absolute():
        reports_dir = PROJECT_ROOT / reports_dir

    # التحقق من وجود الملف
    if not input_path.exists():
        print(f"\n  [ERROR] File not found: {input_path}")
        sys.exit(1)

    # تشغيل خط البيانات
    try:
        report = run_pipeline(
            file_path=str(input_path),
            reports_dir=str(reports_dir)
        )
    except Exception as e:
        print(f"\n  [ERROR] Pipeline run failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
