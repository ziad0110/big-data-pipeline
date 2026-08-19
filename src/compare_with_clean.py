"""
compare_with_clean.py — أداة مقارنة البيانات المنظفة مع الملف المرجعي النظيف
Compares the pipeline's cleaned records in MongoDB (orders_validated)
with the instructor's ground-truth clean file (orders_huge_clean.csv).
"""

import os
import sys
import csv
import argparse
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

from pymongo import MongoClient
from config.settings import MONGO_URI, DB_NAME, VALIDATED_COLLECTION, FILE_ENCODING


def compare(clean_file_path: str, max_check: int = 1000):
    """مقارنة السجلات المنظفة في MongoDB مع الملف النظيف الأصلي."""
    clean_path = Path(clean_file_path).resolve()
    if not clean_path.exists():
        print(f"[ERROR] الملف النظيف غير موجود: {clean_path}")
        return

    print("=" * 70)
    print("      مقارنة البيانات المنظفة مع الملف النظيف المرجعي")
    print("      Clean Ground-Truth vs Pipeline Cleaned (orders_validated)")
    print("=" * 70)
    print(f"  Clean File : {clean_path.name}")
    print(f"  Max Check  : {max_check:,} records")
    print("-" * 70)

    # الاتصال بـ MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    validated_col = db[VALIDATED_COLLECTION]

    total_checked = 0
    matched_count = 0
    mismatched_count = 0
    not_in_validated = 0  # (قد تكون معزولة في quarantine)

    sample_comparisons = []

    # فتح الملف النظيف وقراءة السجلات
    with open(clean_path, mode="r", encoding=FILE_ENCODING, errors="replace") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames:
            reader.fieldnames = [c.replace("\ufeff", "").strip() for c in reader.fieldnames]

        for i, clean_row in enumerate(reader):
            if total_checked >= max_check:
                break

            order_id = clean_row.get("order_id", "").strip()
            if not order_id:
                continue

            total_checked += 1

            # البحث عن السجل في orders_validated
            doc = validated_col.find_one({"order_id": order_id})

            if not doc:
                not_in_validated += 1
                continue

            # مقارنة الحقول الأساسية
            clean_phone = clean_row.get("customer_phone", "").strip()
            our_phone = doc.get("customer", {}).get("phone", "")

            clean_email = clean_row.get("customer_email", "").strip()
            our_email = doc.get("customer", {}).get("email", "")

            clean_status = clean_row.get("status", "").strip()
            our_status = doc.get("status", "")

            clean_currency = clean_row.get("currency", "").strip()
            our_currency = doc.get("payment", {}).get("currency", "")

            try:
                clean_total = float(clean_row.get("total_amount", 0))
                our_total = float(doc.get("total_amount", 0))
                total_diff = abs(clean_total - our_total)
            except Exception:
                total_diff = 999.0

            # هل السجل متطابق؟
            is_match = (
                clean_phone in our_phone or our_phone in clean_phone
            ) and (
                clean_currency == our_currency
            ) and (
                clean_status == our_status
            ) and (
                total_diff <= 1.0
            )

            if is_match:
                matched_count += 1
            else:
                mismatched_count += 1

            if len(sample_comparisons) < 3 and is_match:
                sample_comparisons.append({
                    "order_id": order_id,
                    "clean_phone": clean_phone,
                    "our_phone": our_phone,
                    "clean_total": clean_row.get("total_amount"),
                    "our_total": our_total,
                    "clean_currency": clean_currency,
                    "our_currency": our_currency,
                    "corrections_made": len(doc.get("corrections", [])),
                })

    client.close()

    # طباعة نتائج المقارنة
    print("\n" + "=" * 70)
    print("                    نتائج المقارنة والتحقق")
    print("=" * 70)
    print(f"  إجمالي السجلات المفحوصة من الملف النظيف : {total_checked:,}")
    print(f"  [OK] سجلات مطابقة تماماً في orders_validated : {matched_count:,}")
    if total_checked > 0:
        match_rate = (matched_count / (total_checked - not_in_validated)) * 100 if (total_checked - not_in_validated) > 0 else 0
        print(f"  دقة المطابقة (Match Accuracy)          : {match_rate:.2f}%")
    print(f"  سجلات معزولة في Quarantine (غير مقبولة) : {not_in_validated:,}")
    print(f"  سجلات بها اختلاف                      : {mismatched_count:,}")
    print("-" * 70)

    # عرض نماذج للمقارنة جنباً إلى جنب
    if sample_comparisons:
        print("\n  نماذج للمقارنة جنباً إلى جنب (Side-by-Side Comparison):")
        for idx, sample in enumerate(sample_comparisons, start=1):
            print(f"\n  [Sample {idx}] الطلب: {sample['order_id']}")
            print(f"    - الهاتف  | المرجعي النظيف: {sample['clean_phone']:<15} | الناتج المنظف لدينا: {sample['our_phone']}")
            print(f"    - المجموع | المرجعي النظيف: {sample['clean_total']:<15} | الناتج المنظف لدينا: {sample['our_total']}")
            print(f"    - العملة  | المرجعي النظيف: {sample['clean_currency']:<15} | الناتج المنظف لدينا: {sample['our_currency']}")
            print(f"    - عدد التصحيحات التي تم تطبيقها على السجل (Audit Trail): {sample['corrections_made']}")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="مقارنة البيانات المنظفة مع الملف النظيف المرجعي")
    parser.add_argument(
        "--clean-file", "-c",
        default=r"..\big data\orders_huge_clean.csv",
        help="مسار ملف البيانات النظيفة (الافتراضي: ..\\big data\\orders_huge_clean.csv)"
    )
    parser.add_argument(
        "--max", "-m",
        type=int,
        default=1000,
        help="عدد السجلات المراد فحصها للمقارنة (الافتراضي: 1000)"
    )
    args = parser.parse_args()
    compare(clean_file_path=args.clean_file, max_check=args.max)
