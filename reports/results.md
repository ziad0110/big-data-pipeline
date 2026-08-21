# تقرير تنفيذ خط الأنابيب | Pipeline Execution Report

**تاريخ التقرير:** `2026-08-21T13:56:44.307630`  
**قاعدة البيانات المستهدفة:** `ecommerce_store` (`orders_raw`, `orders_validated`, `orders_quarantine`)

---

## 1. معلومات التشغيل | Execution Info

| الخاصية / Property | القيمة / Value |
| :--- | :--- |
| **معرف التشغيل (Run ID)** | `614959f4` |
| **الملف المصدر (Source File)** | `C:\Users\ziad\Desktop\4th year\tkalef\omar abu sanad\تكاليف\مشروع نهائي\midterm-data-pipeline\data\sample_orders.csv` |
| **المحرك المستخدم (Engine)** | `python_batch` |
| **وقت التنفيذ (Timestamp)** | `2026-08-21T13:56:44.307630` |

---

## 2. إحصائيات السجلات | Data Records Breakdown

| نوع السجل / Record Type | العدد / Count | النسبة / Percentage |
| :--- | :--- | :--- |
| **إجمالي السجلات الخام (Total Raw)** | **100,000** | **100.00%** |
| ✅ السجلات السليمة (Valid Records) | 74,747 | 74.75% |
| 🔄 السجلات المصححة (Corrected Records) | 14,590 | 14.59% |
| ⚠️ السجلات المعزولة (Quarantined Records) | 10,663 | 10.66% |
| **فحص الاتساق الرياضي (Consistency Check)** | **✅ PASSED** | `Raw == Valid + Corrected + Quarantine` |

---

## 3. مقاييس الأداء والموارد | Performance & Resource Metrics

| المقياس / Metric | القيمة / Value |
| :--- | :--- |
| ⏱️ **الوقت المستغرق الكلي (Elapsed Time)** | `25.14` ثانية (seconds) |
| 🚀 **معدل المعالجة (Throughput)** | `3,977.72` سجل/ثانية (records/sec) |
| 💾 **الذاكرة عند البدء (Start Memory)** | `175.18` MB |
| 💾 **الذاكرة عند الانتهاء (End Memory)** | `234.06` MB |
| 📈 **فارق استهلاك الذاكرة (Memory Delta)** | `+58.88` MB |

---

## 4. تفاصيل أخطاء العزل | Quarantine Error Breakdown

| رمز الخطأ / Error Code | عدد التكرار / Count | النسبة من الإجمالي / % of Total |
| :--- | :--- | :--- |
| `MISSING_CUSTOMER_ID` | 1,411 | 1.41% |
| `CORRUPTED_ITEMS_JSON` | 1,338 | 1.34% |
| `INVALID_PAYMENT_STATUS` | 1,158 | 1.16% |
| `INVALID_NUMERIC` | 1,146 | 1.15% |
| `IMPOSSIBLE_DATE` | 722 | 0.72% |
| `MISSING_ORDER_ID` | 721 | 0.72% |
| `INVALID_STATUS` | 718 | 0.72% |
| `INVALID_CURRENCY` | 713 | 0.71% |
| `INVALID_PHONE` | 707 | 0.71% |
| `UNKNOWN_PRICE` | 683 | 0.68% |
| `EMPTY_ITEMS` | 677 | 0.68% |
| `INVALID_QUANTITY` | 674 | 0.67% |
| `SYMBOLIC_VALUE` | 674 | 0.67% |
| `INVALID_EMAIL` | 669 | 0.67% |

---
*تم توليد هذا التقرير آلياً بواسطة وحدة `src/metrics.py` - مقرر البيانات الضخمة (جامعة الرازي)*
