"""
test_classification.py — اختبارات التصنيف الشامل والـ Idempotency
Integration tests for record classification and pipeline behavior
"""

import sys
from pathlib import Path

# إضافة المجلد الجذر للمشروع إلى مسار الاستيراد
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pytest
from src.quality_rules import apply_rules


# ─────────────────────────────────────────────
# مساعدات
# ─────────────────────────────────────────────

def _make_record(**overrides):
    """بناء سجل CSV كامل مع إمكانية تعديل حقول."""
    record = {
        "order_id": "طلب-200001",
        "order_date": "2025-03-15T10:30:00",
        "status": "تم التسليم",
        "customer_id": "عميل-50",
        "customer_name": "خالد سالم",
        "customer_phone": "734480824",
        "customer_email": "user295679@example.com",
        "city": "عدن",
        "district": "كريتر",
        "delivery_type": "عادي",
        "delivery_cost": "2000.0",
        "payment_method": "نقدًا عند التسليم",
        "payment_status": "تم الدفع",
        "payment_amount": "32000.0",
        "currency": "YER",
        "total_amount": "32000.0",
        "items_json": '[{"sku":"SKU-1009","name":"شاحن سريع","qty":3,"unit_price":10000.0,"total":30000.0}]',
    }
    record.update(overrides)
    return record


# ═══════════════════════════════════════════════
# اختبارات التصنيف
# ═══════════════════════════════════════════════

class TestClassification:
    """اختبارات تصنيف السجلات إلى valid/corrected/quarantine."""

    def test_clean_record_is_valid(self):
        """سجل نظيف تماماً يجب أن يكون valid."""
        record = _make_record()
        status, doc, details = apply_rules(record)
        assert status == "valid"
        assert isinstance(doc, dict)
        assert details == []

    def test_corrected_record_has_audit_trail(self):
        """سجل مصحح يجب أن يحتوي على audit trail."""
        record = _make_record(
            customer_phone="77 448 0824",
            status="  تم التسليم  "
        )
        status, doc, details = apply_rules(record)
        assert status == "corrected"
        assert len(details) > 0
        # التحقق من بنية audit trail
        for correction in details:
            assert "field" in correction
            assert "rule" in correction
            assert "original" in correction
            assert "corrected" in correction

    def test_quarantine_has_error_codes(self):
        """سجل معزول يجب أن يحتوي على error_codes واضحة."""
        record = _make_record(order_id="", items_json="not-json")
        status, doc, details = apply_rules(record)
        assert status == "quarantine"
        assert len(details) > 0
        for error in details:
            assert "error_code" in error
            assert "error_detail" in error

    def test_multiple_errors_all_captured(self):
        """سجل به أخطاء متعددة يجب أن تُلتقط جميعها."""
        record = _make_record(
            order_id="",
            customer_id="",
        )
        status, doc, details = apply_rules(record)
        assert status == "quarantine"
        error_codes = [d.get("error_code") for d in details]
        assert "MISSING_ORDER_ID" in error_codes
        assert "MISSING_CUSTOMER_ID" in error_codes


# ═══════════════════════════════════════════════
# اختبارات بنية المستند النهائي
# ═══════════════════════════════════════════════

class TestDocumentStructure:
    """اختبارات بنية المستند المنظف النهائي."""

    def test_customer_nested_structure(self):
        """التأكد من بنية customer المتفرعة."""
        record = _make_record()
        status, doc, details = apply_rules(record)
        assert "customer" in doc
        customer = doc["customer"]
        assert "customer_id" in customer
        assert "name" in customer
        assert "phone" in customer
        assert "email" in customer
        assert "address" in customer
        assert "city" in customer["address"]
        assert "district" in customer["address"]

    def test_payment_nested_structure(self):
        """التأكد من بنية payment المتفرعة."""
        record = _make_record()
        status, doc, details = apply_rules(record)
        assert "payment" in doc
        payment = doc["payment"]
        assert "method" in payment
        assert "status" in payment
        assert "amount" in payment
        assert "currency" in payment
        assert payment["currency"] == "YER"

    def test_delivery_nested_structure(self):
        """التأكد من بنية delivery المتفرعة."""
        record = _make_record()
        status, doc, details = apply_rules(record)
        assert "delivery" in doc
        delivery = doc["delivery"]
        assert "type" in delivery
        assert "cost" in delivery

    def test_items_parsed_from_json(self):
        """التأكد من أن items تم تحليلها من JSON."""
        record = _make_record()
        status, doc, details = apply_rules(record)
        assert "items" in doc
        assert isinstance(doc["items"], list)
        assert len(doc["items"]) > 0
        item = doc["items"][0]
        assert "sku" in item
        assert "name" in item
        assert "qty" in item
        assert "unit_price" in item
        assert "total" in item

    def test_total_amount_is_float(self):
        """total_amount يجب أن يكون float."""
        record = _make_record()
        status, doc, details = apply_rules(record)
        assert isinstance(doc["total_amount"], float)


# ═══════════════════════════════════════════════
# اختبارات الحافات (Edge Cases)
# ═══════════════════════════════════════════════

class TestEdgeCases:
    """اختبارات حالات حافة."""

    def test_multiple_items_in_order(self):
        """طلب بعدة أصناف."""
        items = '[{"sku":"SKU-1010","name":"هاتف","qty":2,"unit_price":200000.0,"total":400000.0},{"sku":"SKU-1002","name":"فأرة","qty":1,"unit_price":10000.0,"total":10000.0}]'
        record = _make_record(
            items_json=items,
            total_amount="415000.0",
            payment_amount="415000.0",
            delivery_cost="5000.0"
        )
        status, doc, details = apply_rules(record)
        assert status in ("valid", "corrected")
        assert len(doc["items"]) == 2

    def test_combined_corrections(self):
        """سجل يحتاج تصحيحات متعددة في وقت واحد."""
        record = _make_record(
            customer_phone="+967 734480824",
            customer_email="user295679@@example.com",
            currency="ريال يمني",
            status="  مؤكد  "
        )
        status, doc, details = apply_rules(record)
        assert status == "corrected"
        # يجب أن يكون هناك عدة تصحيحات
        assert len(details) >= 3
        # جميع التصحيحات يجب أن تكون ناجحة
        assert doc["customer"]["email"] == "user295679@example.com"
        assert doc["payment"]["currency"] == "YER"
        assert doc["status"] == "مؤكد"
