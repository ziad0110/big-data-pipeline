"""
وحدة إنشاء عينة صغيرة من البيانات (Create Small Sample)
تقوم باستخراج عينة محددة الحجم من ملف البيانات الكبير بشكل تدفقي (Streaming)
سطراً بسطر دون تحميل الملف كاملاً في الذاكرة لتوفير استهلاك الموارد.
"""

import os
import sys
import argparse
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import FILE_ENCODING


def create_sample(input_path: str, output_path: str = "data/sample_orders.csv", max_rows: int = 50000) -> int:
    """استخراج عينة بعدد أسطر محدد من ملف CSV وكتابتها في ملف جديد بشكل تدفقي.

    Stream and write header + first max_rows from input CSV to output CSV.

    Args:
        input_path (str): مسار ملف CSV المصدر الكبير.
        output_path (str): مسار ملف CSV الناتج (الافتراضي: data/sample_orders.csv).
        max_rows (int): عدد أسطر البيانات المطلوب استخراجها (الافتراضي: 50000).

    Returns:
        int: إجمالي عدد الأسطر التي تم استخراجها بنجاح (دون حساب سطر الترويسة).

    Raises:
        FileNotFoundError: إذا لم يكن الملف المصدر موجوداً.
    """
    if not os.path.isfile(input_path):
        raise FileNotFoundError(f"الملف المصدر غير موجود: {input_path}")

    # التأكد من وجود المجلد الوجهة وإنشائه إذا لزم الأمر
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    print("=" * 60)
    print("[INFO] Start Streaming CSV Extraction...")
    print(f"  Input File  : {input_path}")
    print(f"  Output File : {output_path}")
    print(f"  Target Rows : {max_rows:,}")
    print("=" * 60)

    rows_written = 0

    # فتح الملفات بترميز utf-8-sig للتعامل السليم مع BOM واللغة العربية
    with open(input_path, mode="r", encoding=FILE_ENCODING, errors="replace") as infile, \
         open(output_path, mode="w", encoding=FILE_ENCODING, newline="") as outfile:

        # قراءة وكتابة سطر الترويسة
        header = infile.readline()
        if not header:
            print("[WARN] Input file is empty!")
            return 0

        outfile.write(header)

        # قراءة تدفقية سطراً بسطر
        for line in infile:
            outfile.write(line)
            rows_written += 1

            if rows_written % 10000 == 0:
                print(f"  [Progress] Extracted {rows_written:,} rows...")

            if rows_written >= max_rows:
                break

    print("=" * 60)
    print(f"[SUCCESS] Finished! Extracted: {rows_written:,} rows.")
    print(f"  Saved to: {output_path}")
    print("=" * 60)

    return rows_written


def main():
    """معالجة معاملات سطر الأوامر وتشغيل عملية استخراج العينة."""
    parser = argparse.ArgumentParser(
        description="استخراج عينة صغيرة من ملف CSV كبير بشكل تدفقي (Streaming CSV Sampler)"
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="مسار ملف CSV المصدر الكبير (مطلوب)",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="data/sample_orders.csv",
        help="مسار حفظ ملف العينة (الافتراضي: data/sample_orders.csv)",
    )
    parser.add_argument(
        "--rows",
        "-r",
        type=int,
        default=50000,
        help="عدد الأسطر المراد استخراجها (الافتراضي: 50000)",
    )

    args = parser.parse_args()

    try:
        create_sample(
            input_path=args.input,
            output_path=args.output,
            max_rows=args.rows,
        )
    except Exception as exc:
        print(f"[ERROR] Error during sample extraction: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
