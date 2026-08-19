# Configuration settings for the ELT Data Pipeline
# مقرر البيانات الضخمة - العملي | جامعة الرازي

import os

# ─────────────────────────────────────────────
# MongoDB Connection
# ─────────────────────────────────────────────
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "ecommerce_store")

# Collection names
RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "orders_quarantine"

# ─────────────────────────────────────────────
# File Router Threshold
# ─────────────────────────────────────────────
SMALL_FILE_THRESHOLD_MB = 200  # <= 200MB → python_batch, > 200MB → pyspark

# ─────────────────────────────────────────────
# Batch Processing
# ─────────────────────────────────────────────
BATCH_SIZE = 10_000

# ─────────────────────────────────────────────
# File Encoding
# ─────────────────────────────────────────────
FILE_ENCODING = "utf-8-sig"  # الملف يحتوي على BOM (Byte Order Mark)

# ─────────────────────────────────────────────
# Spark Settings
# ─────────────────────────────────────────────
SPARK_APP_NAME = "YemenMart-ELT-Pipeline"
SPARK_DRIVER_MEMORY = "6g"
SPARK_TARGET_PARTITIONS = 16

# ─────────────────────────────────────────────
# Allowed Values (Business Rules)
# ─────────────────────────────────────────────
ALLOWED_STATUSES = {
    "قيد الانتظار", "مؤكد", "قيد الشحن",
    "تم التسليم", "مرتجع", "ملغي"
}

ALLOWED_PAYMENT_METHODS = {
    "نقدًا عند التسليم", "بطاقة", "محفظة إلكترونية"
}

ALLOWED_PAYMENT_STATUSES = {
    "بانتظار الدفع", "تم الدفع", "مرفوض"
}

# ─────────────────────────────────────────────
# Phone & Email Validation Patterns
# ─────────────────────────────────────────────
PHONE_REGEX = r"^(967)?(77|73|70|71)\d{7}$"
EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

# ─────────────────────────────────────────────
# Currency Normalization Map
# ─────────────────────────────────────────────
CURRENCY_MAP = {
    "ريال يمني": "YER",
    "ريال": "YER",
    "لاير": "YER",
    "ر.ي": "YER",
    "YER": "YER",
}

# ─────────────────────────────────────────────
# Arabic-Indic Digit Map
# ─────────────────────────────────────────────
ARABIC_DIGIT_MAP = {
    "٠": "0", "١": "1", "٢": "2", "٣": "3", "٤": "4",
    "٥": "5", "٦": "6", "٧": "7", "٨": "8", "٩": "9",
    "٫": ".",  # Arabic decimal separator
    "٬": ",",  # Arabic thousands separator
}

# ─────────────────────────────────────────────
# Word-to-Number Map (Arabic price words)
# ─────────────────────────────────────────────
WORD_PRICE_MAP = {
    "ألف": 1000,
    "ألفان": 2000,
    "ثلاثة آلاف": 3000,
    "أربعة آلاف": 4000,
    "خمسة آلاف": 5000,
    "ستة آلاف": 6000,
    "سبعة آلاف": 7000,
    "ثمانية آلاف": 8000,
    "تسعة آلاف": 9000,
    "عشرة آلاف": 10000,
    "مائة": 100,
    "مئة": 100,
    "خمسمائة": 500,
    "خمسمئة": 500,
}

# ─────────────────────────────────────────────
# Status Synonym Map (for normalization)
# ─────────────────────────────────────────────
STATUS_SYNONYM_MAP = {
    "مؤكد": "مؤكد",
    "مأكد": "مؤكد",
    "مئكد": "مؤكد",
    "قيد الانتظار": "قيد الانتظار",
    "انتظار": "قيد الانتظار",
    "قيد الشحن": "قيد الشحن",
    "شحن": "قيد الشحن",
    "تم التسليم": "تم التسليم",
    "تسليم": "تم التسليم",
    "مرتجع": "مرتجع",
    "ملغي": "ملغي",
    "ملغى": "ملغي",
}

# ─────────────────────────────────────────────
# Delivery Type Synonym Map
# ─────────────────────────────────────────────
DELIVERY_TYPE_MAP = {
    "سريع": "سريع",
    "عادي": "عادي",
}

# ─────────────────────────────────────────────
# CSV Column Names (as they appear in the file)
# ─────────────────────────────────────────────
CSV_COLUMNS = [
    "order_id", "order_date", "status", "customer_id",
    "customer_name", "customer_phone", "customer_email",
    "city", "district", "delivery_type", "delivery_cost",
    "payment_method", "payment_status", "payment_amount",
    "currency", "total_amount", "items_json",
]
