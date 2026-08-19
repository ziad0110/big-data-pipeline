"""
إعداد وتهيئة قاعدة بيانات MongoDB لمشروع خط معالجة البيانات (ELT Data Pipeline).
يتولى هذا الملف إنشاء الاتصال، وضبط الفهارس، وتطبيق قواعد التحقق من المخطط (Schema Validation).
"""

import sys
from pathlib import Path
from typing import Dict, Any, Optional

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from pymongo import MongoClient, ASCENDING
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import OperationFailure, PyMongoError

from config.settings import (
    MONGO_URI,
    DB_NAME,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
    PHONE_REGEX,
    EMAIL_REGEX,
    ALLOWED_STATUSES,
    ALLOWED_PAYMENT_METHODS,
    ALLOWED_PAYMENT_STATUSES,
)

# مخطط التحقق من صحة المستندات لمجموعة orders_validated
VALIDATION_SCHEMA: Dict[str, Any] = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": [
            "order_id",
            "order_date",
            "status",
            "customer",
            "items",
            "payment",
            "total_amount",
        ],
        "properties": {
            "order_id": {
                "bsonType": ["string", "int", "long"],
                "description": "معرف الطلب - حقل مطلوب",
            },
            "order_date": {
                "bsonType": ["string", "date"],
                "description": "تاريخ الطلب - حقل مطلوب",
            },
            "status": {
                "enum": list(ALLOWED_STATUSES),
                "description": "حالة الطلب - يجب أن تكون إحدى القيم المسموحة",
            },
            "customer": {
                "bsonType": "object",
                "required": ["customer_id", "name", "phone", "email", "address"],
                "properties": {
                    "customer_id": {
                        "bsonType": ["string", "int", "long"],
                        "description": "معرف العميل",
                    },
                    "name": {
                        "bsonType": "string",
                        "description": "اسم العميل",
                    },
                    "phone": {
                        "bsonType": "string",
                        "pattern": PHONE_REGEX,
                        "description": "رقم هاتف يمني مطابق للنمط القياسي",
                    },
                    "email": {
                        "bsonType": "string",
                        "pattern": EMAIL_REGEX,
                        "description": "البريد الإلكتروني للعميل",
                    },
                    "address": {
                        "bsonType": "object",
                        "required": ["city", "district"],
                        "properties": {
                            "city": {
                                "bsonType": "string",
                                "description": "المدينة",
                            },
                            "district": {
                                "bsonType": "string",
                                "description": "المديرية أو الحي",
                            },
                        },
                    },
                },
            },
            "items": {
                "bsonType": "array",
                "minItems": 1,
                "description": "قائمة المنتجات المشتراة (عنصر واحد على الأقل)",
                "items": {
                    "bsonType": "object",
                    "required": ["sku", "name", "qty", "unit_price", "total"],
                    "properties": {
                        "sku": {
                            "bsonType": "string",
                            "description": "رمز وحدة حفظ المخزون (SKU)",
                        },
                        "name": {
                            "bsonType": "string",
                            "description": "اسم المنتج",
                        },
                        "qty": {
                            "bsonType": ["int", "long"],
                            "minimum": 1,
                            "description": "الكمية المطلوبة (عدد صحيح لا يقل عن 1)",
                        },
                        "unit_price": {
                            "bsonType": ["double", "decimal", "int", "long"],
                            "minimum": 0,
                            "description": "سعر الوحدة (قيمة رقمية أكبر من أو تساوي 0)",
                        },
                        "total": {
                            "bsonType": ["double", "decimal", "int", "long"],
                            "minimum": 0,
                            "description": "إجمالي سعر العنصر (قيمة رقمية أكبر من أو تساوي 0)",
                        },
                    },
                },
            },
            "payment": {
                "bsonType": "object",
                "required": ["method", "status", "amount", "currency"],
                "properties": {
                    "method": {
                        "enum": list(ALLOWED_PAYMENT_METHODS),
                        "description": "طريقة الدفع المعتمدة",
                    },
                    "status": {
                        "enum": list(ALLOWED_PAYMENT_STATUSES),
                        "description": "حالة عملية الدفع",
                    },
                    "amount": {
                        "bsonType": ["double", "decimal", "int", "long"],
                        "minimum": 0,
                        "description": "مبلغ الدفع المسجل",
                    },
                    "currency": {
                        "enum": ["YER"],
                        "description": "العملة المعتمدة (YER)",
                    },
                },
            },
            "total_amount": {
                "bsonType": ["double", "decimal", "int", "long"],
                "minimum": 0,
                "description": "الإجمالي النهائي للطلب (أكبر من أو يساوي 0)",
            },
        },
    }
}


def get_client() -> MongoClient:
    """إنشاء وإرجاع كائن MongoClient للاتصال بقاعدة بيانات MongoDB.

    Returns:
        MongoClient: كائن العميل للاتصال بـ MongoDB.
    """
    return MongoClient(MONGO_URI)


def get_database(client: Optional[MongoClient] = None) -> Database:
    """إرجاع كائن قاعدة البيانات المحددة في الإعدادات.

    Args:
        client (Optional[MongoClient]): كائن العميل، وفي حال عدم تمريره يتم إنشاء عميل جديد.

    Returns:
        Database: كائن قاعدة البيانات (ecommerce_store).
    """
    if client is None:
        client = get_client()
    return client[DB_NAME]


def get_collections(db: Optional[Database] = None) -> Dict[str, Collection]:
    """إرجاع قاموس يحتوي على كائنات المجموعات الثلاث في MongoDB.

    Args:
        db (Optional[Database]): كائن قاعدة البيانات.

    Returns:
        Dict[str, Collection]: قاموس بالمفاتيح ('raw', 'validated', 'quarantine').
    """
    if db is None:
        db = get_database()
    return {
        "raw": db[RAW_COLLECTION],
        "validated": db[VALIDATED_COLLECTION],
        "quarantine": db[QUARANTINE_COLLECTION],
    }


def initialize(client: Optional[MongoClient] = None, db: Optional[Database] = None) -> Dict[str, Collection]:
    """تهيئة قاعدة بيانات MongoDB وتجهيز المجموعات والفهارس وقواعد التحقق.

    يقوم التابع بالخطوات التالية:
    1. التأكد من إنشاء المجموعات الثلاث (orders_raw, orders_validated, orders_quarantine).
    2. إنشاء فهرس فريد (Unique Index) على 'order_id' في orders_validated.
    3. إنشاء فهرس على 'run_id' في جميع المجموعات الثلاث لتتبع دورات المعالجة.
    4. إنشاء فهرس على 'error_codes' في مجموعة orders_quarantine.
    5. تطبيق قواعد التحقق من المخطط (Schema Validation) على orders_validated
       بمستوى 'moderate' وإجراء 'error'.
    6. طباعة رسائل تأكيد لكل خطوة.

    Args:
        client (Optional[MongoClient]): كائن العميل للاتصال.
        db (Optional[Database]): كائن قاعدة البيانات.

    Returns:
        Dict[str, Collection]: قاموس يحتوي على المجموعات الثلاث بعد تهيئتها.
    """
    print("=" * 70)
    print("[INFO] Start MongoDB Database Initialization...")
    print(f"  URI      : {MONGO_URI}")
    print(f"  Database : {DB_NAME}")
    print("=" * 70)

    if client is None:
        client = get_client()
    if db is None:
        db = client[DB_NAME]

    existing_collections = db.list_collection_names()
    target_collections = [RAW_COLLECTION, VALIDATED_COLLECTION, QUARANTINE_COLLECTION]

    # أ. إنشاء/التأكد من وجود المجموعات الثلاث
    print("\n[1/4] Checking and creating collections:")
    for col_name in target_collections:
        if col_name not in existing_collections:
            db.create_collection(col_name)
            print(f"  [OK] Created collection: '{col_name}'")
        else:
            print(f"  [OK] Collection exists: '{col_name}'")

    # ب، ج، د. إنشاء الفهارس (Indexes)
    print("\n[2/4] Creating indexes:")

    # فهرس فريد على order_id في orders_validated
    val_col = db[VALIDATED_COLLECTION]
    val_col.create_index([("order_id", ASCENDING)], unique=True, name="uniq_order_id")
    print(f"  [OK] Created Unique Index on 'order_id' in '{VALIDATED_COLLECTION}'")

    # فهرس على run_id في المجموعات الثلاث
    for col_name in target_collections:
        db[col_name].create_index([("run_id", ASCENDING)], name=f"idx_{col_name}_run_id")
        print(f"  [OK] Created Index on 'run_id' in '{col_name}'")

    # فهرس على error_codes في orders_quarantine
    quar_col = db[QUARANTINE_COLLECTION]
    quar_col.create_index([("error_codes", ASCENDING)], name="idx_quarantine_error_codes")
    print(f"  [OK] Created Index on 'error_codes' in '{QUARANTINE_COLLECTION}'")

    # هـ. تطبيق قواعد التحقق من المخطط (Schema Validation)
    print("\n[3/4] Applying Schema Validation:")
    try:
        db.command(
            "collMod",
            VALIDATED_COLLECTION,
            validator=VALIDATION_SCHEMA,
            validationLevel="moderate",
            validationAction="error",
        )
        print(f"  [OK] Schema Validation applied on '{VALIDATED_COLLECTION}' successfully!")
        print("       Level: moderate | Action: error")
    except OperationFailure as err:
        print(f"  [WARN] collMod note: {err}")

    print("\n[4/4] Initialization Complete! All collections and indexes ready.")
    print("=" * 70)

    return {
        "raw": db[RAW_COLLECTION],
        "validated": db[VALIDATED_COLLECTION],
        "quarantine": db[QUARANTINE_COLLECTION],
    }


if __name__ == "__main__":
    try:
        initialize()
    except Exception as exc:
        print(f"[ERROR] Database initialization failed: {exc}", file=sys.stderr)
        sys.exit(1)
