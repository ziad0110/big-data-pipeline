"""
وحدة تتبع مؤشرات الأداء والتحقق من الاتساق وتوليد التقارير
Performance Metrics, Consistency Verification, and Report Generation Module.
مقرر البيانات الضخمة - العملي | جامعة الرازي
"""

import os
import sys
import time
import json
import psutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

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
    DB_NAME,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
)


def get_memory_mb() -> float:
    """
    حساب استهلاك الذاكرة الحالي للعملية بالميجابايت (RSS).
    Returns current process RSS memory in megabytes (MB) using psutil.

    Returns:
        float: حجم الذاكرة المستهلكة بالميجابايت.
    """
    process = psutil.Process(os.getpid())
    return round(process.memory_info().rss / (1024 * 1024), 2)


class PipelineMetrics:
    """
    فئة لتتبع ومراقبة مؤشرات أداء خط الأنابيب والتحقق من سلامة واتساق البيانات.
    Tracks execution metrics, data counts, error breakdown, throughput,
    memory consumption, and generates execution reports (JSON & Markdown).
    """

    def __init__(self, run_id: str, file_source: str, engine_used: str):
        """
        تهيئة متتبع مؤشرات الأداء.

        Args:
            run_id (str): معرف فريد لجلسة التشغيل.
            file_source (str): مسار ملف البيانات المصدر.
            engine_used (str): محرك المعالجة المستخدم ('python_batch' أو 'pyspark').
        """
        self.run_id = run_id
        self.file_source = str(file_source)
        self.engine_used = engine_used

        # تسجيل وقت البدء واستهلاك الذاكرة المبدئي
        self.start_time = time.perf_counter()
        self.start_memory = get_memory_mb()

        # عدادات السجلات
        self.raw_count: int = 0
        self.valid_count: int = 0
        self.corrected_count: int = 0
        self.quarantine_count: int = 0

        # قاموس تفصيل الأخطاء (رمز الخطأ -> العدد)
        self.error_breakdown: Dict[str, int] = {}

    def record_result(self, status: str, error_codes: Optional[List[str]] = None) -> None:
        """
        تسجيل نتيجة معالجة سجل فردي وتحديث العدادات وتفاصيل الأخطاء.

        Args:
            status (str): حالة السجل بعد التدقيق ('valid', 'corrected', 'quarantine').
            error_codes (list, optional): قائمة برموز الأخطاء في حال العزل.
        """
        status_clean = str(status).strip().lower()

        if status_clean == "valid":
            self.valid_count += 1
        elif status_clean == "corrected":
            self.corrected_count += 1
        elif status_clean == "quarantine":
            self.quarantine_count += 1
            if error_codes:
                for code in error_codes:
                    code_str = str(code)
                    self.error_breakdown[code_str] = self.error_breakdown.get(code_str, 0) + 1
        else:
            # في حال وجود حالة غير متوقعة، يتم تسجيلها ضمن المعزولة افتراضياً
            self.quarantine_count += 1
            unknown_code = f"UNKNOWN_STATUS_{status}"
            self.error_breakdown[unknown_code] = self.error_breakdown.get(unknown_code, 0) + 1

        # زيادة إجمالي عدد السجلات الخام دائماً
        self.raw_count += 1

    def verify_consistency(self) -> bool:
        """
        التحقق من الاتساق الرياضي لأعداد السجلات:
        raw_count == valid_count + corrected_count + quarantine_count

        Returns:
            bool: True إذا كان التحقق ناجحاً، False إذا كان هناك خلل في الاتساق.
        """
        sum_parts = self.valid_count + self.corrected_count + self.quarantine_count
        passed = (self.raw_count == sum_parts)

        print("\n" + "=" * 70)
        print("          Consistency Verification")
        print("=" * 70)
        if passed:
            print(f"  [PASSED] Consistency Check: PASSED")
            print(f"   Raw Total   : {self.raw_count:,}")
            print(f"   = Valid     : {self.valid_count:,}")
            print(f"   + Corrected : {self.corrected_count:,}")
            print(f"   + Quarantine: {self.quarantine_count:,}")
            print(f"   Calculated Sum: {sum_parts:,}")
        else:
            print(f"  [FAILED] Consistency Check: FAILED")
            print(f"   Raw Total   : {self.raw_count:,}")
            print(f"   != Valid + Corrected + Quarantine: {sum_parts:,}")
            print(f"   Difference  : {abs(self.raw_count - sum_parts):,}")
        print("=" * 70 + "\n")

        return passed

    def print_progress(self, interval: int = 10000) -> None:
        """
        طباعة سطر تقدم المعالجة كل فترة محددة من السجلات.

        Args:
            interval (int): عدد السجلات التي يتم طباعة التقدم عندها (الافتراضي: 10,000).
        """
        if self.raw_count > 0 and self.raw_count % interval == 0:
            elapsed = time.perf_counter() - self.start_time
            throughput = self.raw_count / elapsed if elapsed > 0 else 0.0
            current_mem = get_memory_mb()
            print(
                f"  [Progress] {self.raw_count:,} records processed | "
                f"Elapsed: {elapsed:.2f}s | "
                f"Speed: {throughput:,.1f} rec/s | "
                f"Memory: {current_mem:.1f} MB"
            )

    def generate_report(self, reports_dir: str = "reports") -> Dict[str, Any]:
        """
        حساب مؤشرات الأداء النهائية، إنشاء وحفظ ملفي التقرير (JSON & Markdown)،
        وطباعة الملخص على شاشة الأوامر.

        Args:
            reports_dir (str): المسار الذي سيتم حفظ التقارير فيه (الافتراضي: 'reports').

        Returns:
            dict: قاموس يحتوي على كامل بيانات التقرير.
        """
        elapsed_seconds = round(time.perf_counter() - self.start_time, 2)
        throughput = round(self.raw_count / elapsed_seconds, 2) if elapsed_seconds > 0 else 0.0
        memory_end = get_memory_mb()
        memory_delta = round(memory_end - self.start_memory, 2)

        sum_parts = self.valid_count + self.corrected_count + self.quarantine_count
        consistency_status = "PASSED" if (self.raw_count == sum_parts) else "FAILED"

        iso_timestamp = datetime.now().isoformat()

        report: Dict[str, Any] = {
            "run_id": self.run_id,
            "file_source": self.file_source,
            "engine_used": self.engine_used,
            "total_raw_records": self.raw_count,
            "valid_records": self.valid_count,
            "corrected_records": self.corrected_count,
            "quarantined_records": self.quarantine_count,
            "consistency_check": consistency_status,
            "elapsed_seconds": elapsed_seconds,
            "throughput_records_per_sec": throughput,
            "memory_start_mb": self.start_memory,
            "memory_end_mb": memory_end,
            "memory_delta_mb": memory_delta,
            "error_breakdown": self.error_breakdown,
            "timestamp": iso_timestamp,
        }

        # التأكد من وجود مجلد التقارير
        reports_path = Path(reports_dir)
        reports_path.mkdir(parents=True, exist_ok=True)

        # 1. حفظ التقرير كملف JSON
        json_file_path = reports_path / "results.json"
        with open(json_file_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=4, ensure_ascii=False)

        # 2. إنشاء تقرير Markdown منسق
        md_file_path = reports_path / "results.md"
        md_content = self._build_markdown_report(report)
        with open(md_file_path, "w", encoding="utf-8") as f:
            f.write(md_content)

        # 3. طباعة ملخص شامل في وحدة التحكم
        self._print_console_summary(report)

        return report

    def _build_markdown_report(self, report: Dict[str, Any]) -> str:
        """
        بناء محتوى تقرير Markdown بتنسيق احترافي وجداول مفصلة.

        Args:
            report (dict): قاموس بيانات التقرير.

        Returns:
            str: نص التقرير بتنسيق Markdown.
        """
        raw_total = report["total_raw_records"]
        valid_pct = (report["valid_records"] / raw_total * 100) if raw_total > 0 else 0.0
        corr_pct = (report["corrected_records"] / raw_total * 100) if raw_total > 0 else 0.0
        quar_pct = (report["quarantined_records"] / raw_total * 100) if raw_total > 0 else 0.0

        consistency_icon = "✅ PASSED" if report["consistency_check"] == "PASSED" else "❌ FAILED"

        # بناء جدول تفصيل الأخطاء
        error_rows = []
        if self.error_breakdown:
            sorted_errors = sorted(self.error_breakdown.items(), key=lambda x: x[1], reverse=True)
            for code, count in sorted_errors:
                pct = (count / raw_total * 100) if raw_total > 0 else 0.0
                error_rows.append(f"| `{code}` | {count:,} | {pct:.2f}% |")
            error_table = "\n".join(error_rows)
        else:
            error_table = "| - | 0 | 0.0% | *(لا توجد أخطاء مسجلة)* |"

        md = f"""# تقرير تنفيذ خط الأنابيب | Pipeline Execution Report

**تاريخ التقرير:** `{report['timestamp']}`  
**قاعدة البيانات المستهدفة:** `{DB_NAME}` (`{RAW_COLLECTION}`, `{VALIDATED_COLLECTION}`, `{QUARANTINE_COLLECTION}`)

---

## 1. معلومات التشغيل | Execution Info

| الخاصية / Property | القيمة / Value |
| :--- | :--- |
| **معرف التشغيل (Run ID)** | `{report['run_id']}` |
| **الملف المصدر (Source File)** | `{report['file_source']}` |
| **المحرك المستخدم (Engine)** | `{report['engine_used']}` |
| **وقت التنفيذ (Timestamp)** | `{report['timestamp']}` |

---

## 2. إحصائيات السجلات | Data Records Breakdown

| نوع السجل / Record Type | العدد / Count | النسبة / Percentage |
| :--- | :--- | :--- |
| **إجمالي السجلات الخام (Total Raw)** | **{report['total_raw_records']:,}** | **100.00%** |
| ✅ السجلات السليمة (Valid Records) | {report['valid_records']:,} | {valid_pct:.2f}% |
| 🔄 السجلات المصححة (Corrected Records) | {report['corrected_records']:,} | {corr_pct:.2f}% |
| ⚠️ السجلات المعزولة (Quarantined Records) | {report['quarantined_records']:,} | {quar_pct:.2f}% |
| **فحص الاتساق الرياضي (Consistency Check)** | **{consistency_icon}** | `Raw == Valid + Corrected + Quarantine` |

---

## 3. مقاييس الأداء والموارد | Performance & Resource Metrics

| المقياس / Metric | القيمة / Value |
| :--- | :--- |
| ⏱️ **الوقت المستغرق الكلي (Elapsed Time)** | `{report['elapsed_seconds']:.2f}` ثانية (seconds) |
| 🚀 **معدل المعالجة (Throughput)** | `{report['throughput_records_per_sec']:,.2f}` سجل/ثانية (records/sec) |
| 💾 **الذاكرة عند البدء (Start Memory)** | `{report['memory_start_mb']:.2f}` MB |
| 💾 **الذاكرة عند الانتهاء (End Memory)** | `{report['memory_end_mb']:.2f}` MB |
| 📈 **فارق استهلاك الذاكرة (Memory Delta)** | `{report['memory_delta_mb']:+.2f}` MB |

---

## 4. تفاصيل أخطاء العزل | Quarantine Error Breakdown

| رمز الخطأ / Error Code | عدد التكرار / Count | النسبة من الإجمالي / % of Total |
| :--- | :--- | :--- |
{error_table}

---
*تم توليد هذا التقرير آلياً بواسطة وحدة `src/metrics.py` - مقرر البيانات الضخمة (جامعة الرازي)*
"""
        return md

    def _print_console_summary(self, report: Dict[str, Any]) -> None:
        """
        طباعة ملخص نهائي جذاب ومفصل على سطر الأوامر.

        Args:
            report (dict): قاموس بيانات التقرير.
        """
        print("\n" + "=" * 70)
        print("          ملخص تشغيل خط الأنابيب | Pipeline Run Summary")
        print("=" * 70)
        print(f"  Run ID            : {report['run_id']}")
        print(f"  Engine Used       : {report['engine_used']}")
        print(f"  File Source       : {report['file_source']}")
        print(f"  Timestamp         : {report['timestamp']}")
        print("-" * 70)
        print(f"  Total Raw Records : {report['total_raw_records']:,}")
        print(f"  - Valid Records   : {report['valid_records']:,}")
        print(f"  - Corrected       : {report['corrected_records']:,}")
        print(f"  - Quarantined     : {report['quarantined_records']:,}")
        print(f"  Consistency Check : {report['consistency_check']}")
        print("-" * 70)
        print(f"  Elapsed Time      : {report['elapsed_seconds']:.2f} s")
        print(f"  Throughput        : {report['throughput_records_per_sec']:,.2f} records/s")
        print(f"  Memory Start      : {report['memory_start_mb']:.2f} MB")
        print(f"  Memory End        : {report['memory_end_mb']:.2f} MB")
        print(f"  Memory Delta      : {report['memory_delta_mb']:+.2f} MB")
        if self.error_breakdown:
            print("-" * 70)
            print("  Top Quarantine Errors:")
            sorted_errors = sorted(self.error_breakdown.items(), key=lambda x: x[1], reverse=True)
            for code, count in sorted_errors[:5]:
                print(f"    * {code}: {count:,}")
        print("=" * 70 + "\n")
