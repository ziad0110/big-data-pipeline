# تقرير تنفيذ خط الأنابيب | Pipeline Execution Report

**تاريخ التقرير:** `2026-08-25T19:06:57.633302`  
**قاعدة البيانات المستهدفة:** `ecommerce_store` (`orders_raw`, `orders_validated`, `orders_quarantine`)

---

## 1. معلومات التشغيل | Execution Info

| الخاصية / Property | القيمة / Value |
| :--- | :--- |
| **معرف التشغيل (Run ID)** | `57323342` |
| **الملف المصدر (Source File)** | `C:\Users\ziad\Desktop\4th year\tkalef\omar abu sanad\تكاليف\مشروع نهائي\big data\orders_huge_mixed_quality.csv` |
| **المحرك المستخدم (Engine)** | `pyspark` |
| **وقت التنفيذ (Timestamp)** | `2026-08-25T19:06:57.633302` |

---

## 2. إحصائيات السجلات | Data Records Breakdown

| نوع السجل / Record Type | العدد / Count | النسبة / Percentage |
| :--- | :--- | :--- |
| **إجمالي السجلات الخام (Total Raw)** | **30,209,432** | **100.00%** |
| ✅ السجلات السليمة (Valid Records) | 22,886,416 | 75.76% |
| 🔄 السجلات المصححة (Corrected Records) | 4,345,804 | 14.39% |
| ⚠️ السجلات المعزولة (Quarantined Records) | 2,977,212 | 9.86% |
| **فحص الاتساق الرياضي (Consistency Check)** | **✅ PASSED** | `Raw == Valid + Corrected + Quarantine` |

---

## 3. مقاييس الأداء والموارد | Performance & Resource Metrics

| المقياس / Metric | القيمة / Value |
| :--- | :--- |
| ⏱️ **الوقت المستغرق الكلي (Elapsed Time)** | `2339.06` ثانية (seconds) |
| 🚀 **معدل المعالجة (Throughput)** | `12,915.20` سجل/ثانية (records/sec) |
| 💾 **الذاكرة عند البدء (Start Memory)** | `38.96` MB |
| 💾 **الذاكرة عند الانتهاء (End Memory)** | `46.84` MB |
| 📈 **فارق استهلاك الذاكرة (Memory Delta)** | `+7.88` MB |

---

## 4. تفاصيل أخطاء العزل | Quarantine Error Breakdown

| رمز الخطأ / Error Code | عدد التكرار / Count | النسبة من الإجمالي / % of Total |
| :--- | :--- | :--- |
| `CORRUPTED_ITEMS_JSON` | 419,906 | 1.39% |
| `INVALID_NUMERIC` | 333,888 | 1.11% |
| `INVALID_PAYMENT_STATUS` | 333,786 | 1.10% |
| `IMPOSSIBLE_DATE` | 210,524 | 0.70% |
| `INVALID_STATUS` | 210,194 | 0.70% |
| `INVALID_CURRENCY` | 210,190 | 0.70% |
| `INVALID_PHONE` | 210,042 | 0.70% |
| `INVALID_QUANTITY` | 210,021 | 0.70% |
| `UNKNOWN_PRICE` | 210,018 | 0.70% |
| `EMPTY_ITEMS` | 209,934 | 0.69% |
| `SYMBOLIC_VALUE` | 209,432 | 0.69% |
| `INVALID_EMAIL` | 209,277 | 0.69% |

---
*تم توليد هذا التقرير آلياً بواسطة وحدة `src/metrics.py` - مقرر البيانات الضخمة (جامعة الرازي)*
