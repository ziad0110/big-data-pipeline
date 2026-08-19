# وثيقة معمارية النظام وتصميم المكونات | Architecture & Technical Design Document

## مشروع خط البيانات الهجين (Hybrid ELT Data Pipeline)
**المقرر:** البيانات الضخمة (العملي) | **الجامعة:** جامعة الرازي  
**إشراف:** م. عمر أبو سند | **إعداد الطالب:** زياد

---

## 1. نظرة عامة على معمارية النظام | System Overview

يعتمد المشروع على نمط معمارية **ELT (Extract, Load, Transform)** الحديث لمعالجة تدفقات وسجلات طلبات التجارة الإلكترونية ذات الجودة المتفاوتة وغير المنتظمة. يجمع النظام بين سرعتين للمعالجة عبر معمارية هجينة (Hybrid Architecture) توازن بين الكفاءة العالية في استهلاك الموارد للملفات الصغيرة والتوسع الأفقي المتوازي للملفات الضخمة.

### مخطط تدفق البيانات ومعمارية النظام (System Architecture Flowchart):

```text
 ┌─────────────────────────────────────────────────────────────────────────┐
 │                            CSV Data Source                              │
 │                 (UTF-8 with BOM Encoding, 17 Columns)                   │
 └────────────────────────────────────┬────────────────────────────────────┘
                                      │
                                      ▼
                      ┌───────────────────────────────┐
                      │    File Router (src/router)   │
                      │  Inspects File Size in MB     │
                      └───────┬───────────────┬───────┘
                              │               │
            Size <= 200 MB    │               │  Size > 200 MB
     ┌────────────────────────┘               └────────────────────────┐
     ▼                                                                 ▼
┌──────────────────────────────────────┐             ┌───────────────────────────────────┐
│     Python Batch Loader (Streaming)  │             │     PySpark Distributed Loader    │
│  - Line-by-line csv.DictReader       │             │  - Resilient Distributed Dataset  │
│  - O(1) Memory Footprint             │             │  - Multi-Core Partitioning (16)   │
│  - Dynamic Batch Buffering (10,000)  │             │  - All-StringType Schema          │
└──────────────────┬───────────────────┘             └─────────────────┬─────────────────┘
                   │                                                   │
                   └─────────────────────┬─────────────────────────────┘
                                         │
                                         ▼
                     ┌───────────────────────────────────────┐
                     │          MongoDB Initialization       │
                     │  - Ensure Collections Exist           │
                     │  - Build Indexes (Unique / Secondary) │
                     │  - Apply $jsonSchema Validation Rules │
                     └───────────────────┬───────────────────┘
                                         │
                                         ▼
                     ┌───────────────────────────────────────┐
                     │        Stage 1: Raw Ingestion         │
                     │  - Insert full raw record directly    │
                     │  - Append metadata (run_id, row_num)  │
                     │  ──> MongoDB: `orders_raw`            │
                     └───────────────────┬───────────────────┘
                                         │
                                         ▼
                     ┌───────────────────────────────────────┐
                     │   Stage 2: 10 Cleaning Rules Engine   │
                     │  - Arabic Digits & Number Words Fix   │
                     │  - Yemeni Phone & Email Regex Audit   │
                     │  - Date & Currency ISO Normalization  │
                     │  - Recompute Totals & Price Integrity │
                     └───────────────────┬───────────────────┘
                                         │
                         ┌───────────────┴───────────────┐
                         │      Record Classification    │
                         └───────┬───────────────┬───────┘
                                 │               │
                   Valid / Corrected             │ Unrecoverable Error
     ┌───────────────────────────┘               └───────────────────────────┐
     ▼                                                                       ▼
┌──────────────────────────────────────┐                   ┌───────────────────────────────────┐
│      Stage 3A: Validated Orders      │                   │      Stage 3B: Quarantined Orders │
│  - Nested BSON Document Structure    │                   │  - Preserve Original Raw Record   │
│  - Full Audit Trail (Corrections)    │                   │  - Error Codes & Detailed Reasons │
│  - Idempotent Upsert (UpdateOne)     │                   │  - Bulk Batch Ingestion           │
│  ──> MongoDB: `orders_validated`     │                   │  ──> MongoDB: `orders_quarantine` │
└──────────────────┬───────────────────┘                   └─────────────────┬─────────────────┘
                   │                                                         │
                   └─────────────────────┬───────────────────────────────────┘
                                         │
                                         ▼
                     ┌───────────────────────────────────────┐
                     │   Stage 4: Verification & Reporting   │
                     │  - Check: Raw == Valid + Corr + Quar  │
                     │  - Track Memory RSS, Time, Throughput │
                     │  - Generate results.json & results.md │
                     └───────────────────────────────────────┘
```

---

## 2. منطق موجه الملفات | File Router Logic

تتولى وحدة `src/file_router.py` فحص ملف الإدخال قبل بدء أي معالجة لاتخاذ قرار التوجيه:

$$\text{Engine} = \begin{cases} \text{python\_batch}, & \text{FileSize} \le \text{SMALL\_FILE\_THRESHOLD\_MB } (200\text{ MB}) \\ \text{pyspark}, & \text{FileSize} > \text{SMALL\_FILE\_THRESHOLD\_MB } (200\text{ MB}) \end{cases}$$

### مبررات التصميم:
1. **تجنب الحمل الزائد لـ JVM و Spark في الملفات الصغيرة:** عملية تشغيل سياق Spark (`SparkSession`) تستهلك بضع ثوانٍ وحوالي $300 - 500\text{ MB}$ من الذاكرة كحد أدنى. بالنسبة للملفات الصغيرة ($\le 200\text{ MB}$)، يكون محرك بايثون التدفقي المباشر أسرع بكثير وأقل استهلاكاً للموارد.
2. **التوسع الأفقي للملفات الكبيرة:** عندما يتجاوز حجم الملف $200\text{ MB}$ أو يصل لعشرات الجيجابايتات، يتفوق محرك PySpark بفضل قدرته على تجزئة الملف إلى كتل (Partitions) وتوزيع المعالجة على جميع الأنوية المتاحة (`master("local[*]")`).

---

## 3. محرك التدفق والدفعات | Python Batch Streaming Loader

تعمل وحدة `src/batch_loader.py` وفق المبادئ التالية:

- **القراءة التدفقية (True Streaming):** استخدام `csv.DictReader` كـ Generator في بايثون، مما يعني قراءة سطر واحد فقط في الذاكرة في اللحظة الزمنية الواحدة، مما يحافظ على ثبات استهلاك الذاكرة العشوائية ($O(1)\text{ Memory Space Complexity}$) مهما بلغ حجم الملف.
- **تجميع الدفعات (Batch Buffering):** لتجنب بطء استدعاءات الشبكة مع قاعدة البيانات في كل سجل منفرد، يقوم المحرك بتجميع السجلات في مصفوفات بذاكرة مؤقتة حتى تصل إلى `BATCH_SIZE` (المحدد بـ 10,000 سجل في `config/settings.py`).
- **التنفيذ الجماعي (Bulk Operations):**
  - مجموعة `orders_raw`: إدخال جماعي عبر `collection.insert_many(batch, ordered=False)`.
  - مجموعة `orders_validated`: دمج وتحديث جماعي غير متسلسل عبر `collection.bulk_write(operations, ordered=False)` باستخدام `UpdateOne`.
  - مجموعة `orders_quarantine`: إدخال جماعي عبر `collection.insert_many(batch, ordered=False)`.

---

## 4. محرك المعالجة الموزعة | PySpark Parallel Loader

تعتمد وحدة `src/spark_loader.py` على Apache Spark لمعالجة البيانات المتوازية:

### أ. مخطط البيانات الثابت (All-StringType Schema)
يتم فرض Schema محددة تتضمن جميع الحقول الـ 17 بنوع `StringType()`:
```python
StructType([
    StructField("order_id",       StringType(), True),
    StructField("order_date",     StringType(), True),
    StructField("status",         StringType(), True),
    ...
    StructField("total_amount",   StringType(), True),
    StructField("items_json",     StringType(), True),
])
```
> **لماذا All-StringType؟** الاستنتاج التلقائي للمخطط (`inferSchema=True`) في Spark يفشل أو يضع قيم `null` عند مواجهة بيانات مشوهة مثل أرقام مشرقية (`٧٠٦٠٠٠٫٠`) أو كلمات (`ألفان`) أو قيم رمزية (`???`). قراءة جميع الحقول كنصوص تضمن وصول كل البيانات دون بتر إلى محرك التنظيف.

### ب. التقسيم والتوازي (Partitioning)
- يتم ضبط التوازي المستهدف على `SPARK_TARGET_PARTITIONS = 16`.
- يتم استخدام `spark.sql.files.maxPartitionBytes = 128MB`.
- في حال كان عدد أقسام الملف أقل من 16، يتم تنفيذ `df.repartition(16)` لضمان استغلال كامل لقدرات المعالج.

### ج. المعالجة المستقلة للبارتشنات (`mapPartitions`)
تتم معالجة كل قسم (Partition) بشكل منعزل داخل دالة `_process_partition`؛ حيث يقوم كل عامل (Worker):
1. بإنشاء اتصال محلي بقاعدة بيانات MongoDB.
2. تدوير السجلات وتطبيق القواعد الـ 10.
3. تفريغ الدفعات إلى المجموعات الثلاث باستخدام عمليات الـ Bulk Write.
4. إرجاع إحصائيات العد والأخطاء ليتم تجميعها في المعالج الرئيسي (Driver).

---

## 5. مخطط مجموعات MongoDB وقواعد التحقق | MongoDB Collections Schema

قاعدة البيانات: **`ecommerce_store`**

### 1. مجموعة السجلات الخام (`orders_raw`)
تخزن كل السجلات الواردة كما هي لتكون سجلاً تاريخياً غير قابل للتعديل (Immutable Log):
```json
{
  "_id": ObjectId("..."),
  "run_id": "a1b2c3d4",
  "file_source": "sample_orders.csv",
  "source_row_number": 1042,
  "ingested_at": "2026-08-15T19:30:00.000Z",
  "engine_used": "python_batch",
  "raw_record": {
    "order_id": "طلب-100001",
    "order_date": "2025/02/24 21:29:00",
    "status": "مؤكد",
    "customer_id": "عميل-1",
    "customer_name": "محمد علي",
    "customer_phone": "77 557 8449",
    "customer_email": "user123@@example.com",
    "city": "صنعاء",
    "district": "التحرير",
    "delivery_type": "سريع",
    "delivery_cost": "٥٠٠٠٫٠",
    "payment_method": "بطاقة",
    "payment_status": "تم الدفع",
    "payment_amount": "١٠٠٠٠٠٫٠",
    "currency": "ريال يمني",
    "total_amount": "100,000.00",
    "items_json": "[{\"sku\":\"SKU-1001\",\"name\":\"لابتوب\",\"qty\":1,\"unit_price\":95000.0,\"total\":95000.0}]"
  }
}
```

### 2. مجموعة السجلات السليمة والمصححة (`orders_validated`)
تخزن السجلات المنظفة بعد تحويلها لهيكل JSON شجري احترافي (Nested Structure)، محكومة بمخطط تحقق صارم عبر `$jsonSchema`:
```json
{
  "_id": ObjectId("..."),
  "order_id": "طلب-100001",
  "order_date": "2025-02-24T21:29:00",
  "status": "مؤكد",
  "run_id": "a1b2c3d4",
  "validated_at": "2026-08-15T19:30:01.000Z",
  "customer": {
    "customer_id": "عميل-1",
    "name": "محمد علي",
    "phone": "775578449",
    "email": "user123@example.com",
    "address": {
      "city": "صنعاء",
      "district": "التحرير"
    }
  },
  "items": [
    {
      "sku": "SKU-1001",
      "name": "لابتوب",
      "qty": 1,
      "unit_price": 95000.0,
      "total": 95000.0
    }
  ],
  "delivery": {
    "type": "سريع",
    "cost": 5000.0
  },
  "payment": {
    "method": "بطاقة",
    "status": "تم الدفع",
    "amount": 100000.0,
    "currency": "YER"
  },
  "total_amount": 100000.0,
  "corrections": [
    {
      "field": "customer_phone",
      "rule": "PHONE_FORMAT",
      "original": "77 557 8449",
      "corrected": "775578449"
    },
    {
      "field": "customer_email",
      "rule": "EMAIL_FIX",
      "original": "user123@@example.com",
      "corrected": "user123@example.com"
    },
    {
      "field": "delivery_cost",
      "rule": "ARABIC_NUMERALS",
      "original": "٥٠٠٠٫٠",
      "corrected": "5000.0"
    },
    {
      "field": "payment_amount",
      "rule": "ARABIC_NUMERALS",
      "original": "١٠٠٠٠٠٫٠",
      "corrected": "100000.0"
    },
    {
      "field": "currency",
      "rule": "CURRENCY_TEXT",
      "original": "ريال يمني",
      "corrected": "YER"
    },
    {
      "field": "total_amount",
      "rule": "THOUSAND_SEPARATOR",
      "original": "100,000.00",
      "corrected": "100000.00"
    }
  ]
}
```

### 3. مجموعة السجلات المعزولة (`orders_quarantine`)
تخزن السجلات المرفوضة التي تحتوي على أخطاء حرجة مع توثيق كود الخطأ وتفاصيله:
```json
{
  "_id": ObjectId("..."),
  "run_id": "a1b2c3d4",
  "file_source": "sample_orders.csv",
  "source_row_number": 2105,
  "quarantined_at": "2026-08-15T19:30:02.000Z",
  "raw_record": {
    "order_id": "",
    "customer_id": "عميل-99",
    "total_amount": "???",
    "items_json": "not-json"
  },
  "error_codes": [
    "MISSING_ORDER_ID",
    "SYMBOLIC_VALUE",
    "CORRUPTED_ITEMS_JSON"
  ],
  "error_details": [
    {"error_code": "MISSING_ORDER_ID", "error_detail": "order_id is missing or empty", "field": "order_id"},
    {"error_code": "SYMBOLIC_VALUE", "error_detail": "total_amount contains ???", "field": "total_amount"},
    {"error_code": "CORRUPTED_ITEMS_JSON", "error_detail": "items_json is strictly not-json", "field": "items_json"}
  ]
}
```

### 4. الفهارس المطبقة (Database Indexes)
| المجموعة / Collection | الحقل المفهرس / Indexed Field | نوع الفهرس / Index Type | الهدف / Purpose |
| :--- | :--- | :--- | :--- |
| `orders_validated` | `order_id` | **Unique Index (1)** | منع التكرار وضمان خاصية الـ Idempotency |
| `orders_raw` | `run_id` | **Secondary Index (1)** | تسريع تتبع واستعلام جلسات التشغيل المحددة |
| `orders_validated` | `run_id` | **Secondary Index (1)** | تتبع السجلات المنقحة حسب جلسة التشغيل |
| `orders_quarantine` | `run_id` | **Secondary Index (1)** | تتبع السجلات المعزولة حسب جلسة التشغيل |
| `orders_quarantine` | `error_codes` | **Multikey Index (1)** | تسريع استعلامات تصنيف وتكرار أنواع الأخطاء |

---

## 6. جدول قواعد التنظيف العشر | 10 Cleaning Rules Summary Table

تنفذ وحدة `src/quality_rules.py` عشر قواعد تدقيق وتنظيف متسلسلة:

| # | اسم القاعدة / Rule Name | وصف القاعدة / Description | مثال الإدخال / Input Example | النتيجة بعد التنظيف / Output | الإجراء / Action |
| :-: | :--- | :--- | :--- | :--- | :-: |
| **1** | `ARABIC_NUMERALS` | تحويل الأرقام المشرقية (`٠-٩`) والفواصل العربية (`٫`, `٬`) إلى أرقام لاتينية قياسية | `"٧٠٦٠٠٠٫٠"` | `706000.0` | تصحيح (Corrected) |
| **2** | `CURRENCY_TEXT` | توحيد صيغ العملة اليمنية النصية المختلفة ("ريال يمني", "ريال", "لاير", "ر.ي") إلى المعيار الدولي | `"ريال يمني"` | `"YER"` | تصحيح (Corrected) |
| **3** | `THOUSAND_SEPARATOR` | إزالة فواصل الآلاف الإنجليزية لتفادي أخطاء التحويل الرقمي | `"135,000.00"` | `135000.00` | تصحيح (Corrected) |
| **4** | `PRICE_WORDS` | تحويل الكلمات العربية الدالة على المبالغ إلى قيم عددية مكافئة | `"ألفان"` / `"خمسة آلاف"` | `2000.0` / `5000.0` | تصحيح (Corrected) |
| **5** | `PHONE_FORMAT` | تنظيف الرموز الزائدة وتوحيد أرقام الهواتف اليمنية وفق النمط `^(967)?(77\|73\|70\|71)\d{7}$` | `"+967 77-557-8449"` | `"967775578449"` | تصحيح (Corrected) |
| **6** | `EMAIL_FIX` | معالجة الرموز المكررة الخاطئة في البريد الإلكتروني (`@@` $\rightarrow$ `@`, `..` $\rightarrow$ `.`) | `"user@@domain..com"` | `"user@domain.com"` | تصحيح (Corrected) |
| **7** | `DATE_NORMALIZE` | توحيد صيغ التواريخ المختلفة (`YYYY/MM/DD` أو `DD-MM-YYYY`) إلى صيغة ISO القياسية | `"17-01-2025 04:50:00"` | `"2025-01-17T04:50:00"` | تصحيح (Corrected) |
| **8** | `TRIM_NORMALIZE` | إزالة المسافات الزائدة وتوحيد مرادفات حالات الطلبات ("ملغى" $\rightarrow$ "ملغي", "مأكد" $\rightarrow$ "مؤكد") | `"  ملغى  "` | `"ملغي"` | تصحيح (Corrected) |
| **9** | `TOTAL_RECALC` | إعادة احتساب إجمالي الطلب ومقارنته بمجموع العناصر وتكلفة الشحن إذا تجاوز الفارق 1 ريال | $\text{Items Total} + \text{Cost}$ | تصحيح المبلغ الإجمالي | تصحيح (Corrected) |
| **10** | `SYMBOLIC_VALUES` | كشف القيم الرمزية المفقودة (`???`, غياب المعرفات، تلف JSON، كميات $\le 0$) | `order_id=""` / `qty=-2` | تحويل السجل للعزل | **عزل (Quarantine)** |

---

## 7. بنية سجل التدقيق | Audit Trail Structure

يضمن خط البيانات الشفافية الكاملة لأي عملية تحويل للبيانات؛ حيث يُرفق مع كل مستند تم تعديله حقل مصفوفة باسم `corrections`.

### هيكل كائن التصحيح (Correction Item Schema):
```typescript
interface CorrectionDetail {
  field: string;      // اسم الحقل الذي تم تعديله (مثل: 'customer_phone', 'currency')
  rule: string;       // اسم القاعدة المنفذة (مثل: 'PHONE_FORMAT', 'ARABIC_NUMERALS')
  original: any;      // القيمة الأصلية المشوهة كما وردت في ملف المصدر
  corrected: any;     // القيمة المنقحة والنهائية بعد تطبيق القاعدة
}
```

---

## 8. معادلة الاتساق الرياضي | Consistency Equation

لضمان عدم فقدان أو إسقاط أي سجل أثناء المعالجة، يطبق النظام معادلة اتساق رياضية حتمية:

$$\text{Total Raw Records} \equiv \text{Valid Records} + \text{Corrected Records} + \text{Quarantined Records}$$

يتم فحص هذه المعادلة آلياً بواسطة الدالة `metrics.verify_consistency()` في نهاية كل دورة تشغيل؛ وإذا حدث أي عدم تطابق ولو بسجل واحد ($|\Delta| > 0$) يتم إطلاق تحذير فوري ووسم التقرير بـ `FAILED`.

---

## 9. آلية عدم التكرار | Idempotent Upsert Mechanism

لتحقيق متطلبات التشغيل الآمن في بيئات الإنتاج، تم تصميم عملية التخزين في مجموعة `orders_validated` لتكون **Idempotent**.

```python
operations.append(
    UpdateOne(
        filter={"order_id": doc["order_id"]},
        update={"$set": doc},
        upsert=True
    )
)
```

### سلوك المعالجة:
- **التشغيل الأولي (First Run):** يُنشئ المستند كإدخال جديد (`upserted_count = N`).
- **إعادة التشغيل (Re-run):** يعثر على المستند عبر `order_id` ويقوم بتحديث حقوله فقط (`matched_count = N, modified_count = 0`).
- **الفائدة:** تجنب حدوث استثناءات تكرار المفاتيح (`DuplicateKeyError`) مع المحافظة على ثبات عدد السجلات الإجمالي في قاعدة البيانات.

---

## 10. معالجة ترميز النصوص العربية | Encoding & UTF-8 with BOM

تحتوي ملفات الـ CSV الصادرة من بيئات Windows وبرنامج Excel على علامة ترتيب البايتات **BOM (Byte Order Mark)** ذات التسلسل البايتي `\xef\xbb\xbf`.

- **المشكلة:** عند قراءة الملف بترميز `utf-8` العادي، تلتصق علامة BOM باسم أول عمود في الملف ليصبح `\ufefforder_id` بدلاً من `order_id`، مما يؤدي إلى فشل الوصول لبيانات المعرف في قواميس بايثون.
- **الحل المعتمد:**
  1. ضبط ترميز القراءة دائماً على `FILE_ENCODING = "utf-8-sig"`.
  2. تنظيف ترويسة الأعمدة في القارئ التدفقي تلقائياً:
     ```python
     reader.fieldnames = [col.replace("\ufeff", "").strip() for col in reader.fieldnames]
     ```

---

## 11. مقاييس ومؤشرات الأداء المتبعة | Performance Metrics Tracked

تقوم وحدة `src/metrics.py` بمراقبة وتسجيل المؤشرات التالية في كل جلسة تشغيل:

1. **مؤشرات الزمن والسرعة:**
   - وقت التنفيذ الكلي (`elapsed_seconds`) عبر `time.perf_counter()`.
   - معدل التدفق والإنتاجية (`throughput_records_per_sec = total_records / elapsed_seconds`).
2. **مؤشرات استهلاك الذاكرة العشوائية:**
   - حجم الذاكرة المستهلكة عند البدء (`memory_start_mb`) عبر `psutil.Process().memory_info().rss`.
   - حجم الذاكرة عند الانتهاء (`memory_end_mb`).
   - صافي التغير في الذاكرة (`memory_delta_mb`).
3. **مؤشرات جودة البيانات:**
   - عدد ونسبة السجلات السليمة والمصححة والمعزولة.
   - التوزيع التكراري لأكواد أخطاء العزل (`error_breakdown`).
4. **توليد المخرجات:**
   - كتابة تقرير JSON مهيكل في `reports/results.json`.
   - كتابة تقرير Markdown تحليلي في `reports/results.md`.

---

<div align="center">
  <b>تم إعداد هذا المستند كمرجع معمارية وهندسة برمجيات لمشروع خط البيانات الهجين</b><br>
  جامعة الرازي — كلية الحاسوب وتكنولوجيا المعلومات — 2026
</div>
