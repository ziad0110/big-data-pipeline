"""
test_cleaning_rules.py — اختبارات وحدة لقواعد التنظيف الـ 10
Unit tests for all 10 data cleaning rules in quality_rules.py
"""

import sys
import os
import pytest
from pathlib import Path

# إضافة المجلد الجذر للمشروع إلى مسار الاستيراد
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.quality_rules import apply_rules


# ─────────────────────────────────────────────
# مساعدات لبناء سجلات اختبارية
# ─────────────────────────────────────────────

def _make_valid_record(**overrides):
    """بناء سجل CSV سليم 100% مع إمكانية تعديل حقول معينة."""
    record = {
        "order_id": "طلب-100001",
        "order_date": "2025-02-24T21:29:00",
        "status": "مؤكد",
        "customer_id": "عميل-1",
        "customer_name": "محمد علي",
        "customer_phone": "775578449",
        "customer_email": "user123@example.com",
        "city": "صنعاء",
        "district": "التحرير",
        "delivery_type": "سريع",
        "delivery_cost": "5000.0",
        "payment_method": "بطاقة",
        "payment_status": "تم الدفع",
        "payment_amount": "100000.0",
        "currency": "YER",
        "total_amount": "100000.0",
        "items_json": '[{"sku":"SKU-1001","name":"لابتوب","qty":1,"unit_price":95000.0,"total":95000.0}]',
    }
    record.update(overrides)
    return record


# ═══════════════════════════════════════════════
# Rule 1: الأرقام العربية (المشرقية)
# ═══════════════════════════════════════════════

class TestArabicNumerals:
    """اختبارات تحويل الأرقام العربية-المشرقية إلى لاتينية."""

    def test_arabic_total_amount(self):
        """٧٠٦٠٠٠٫٠ يجب أن يتحول إلى 706000.0"""
        record = _make_valid_record(total_amount="٧٠٦٠٠٠٫٠", payment_amount="706000.0")
        # تحديث items_json ليتوافق مع المجموع
        record["items_json"] = '[{"sku":"SKU-1010","name":"هاتف","qty":1,"unit_price":701000.0,"total":701000.0}]'
        status, doc, details = apply_rules(record)
        assert status in ("valid", "corrected")
        assert doc["total_amount"] == 706000.0

    def test_arabic_payment_amount(self):
        """أرقام عربية في payment_amount."""
        record = _make_valid_record(payment_amount="١٢٣٤٥٫٠")
        record["total_amount"] = "12345.0"
        record["items_json"] = '[{"sku":"SKU-1001","name":"شيء","qty":1,"unit_price":7345.0,"total":7345.0}]'
        status, doc, details = apply_rules(record)
        assert status in ("valid", "corrected")
        assert doc["payment"]["amount"] == 12345.0

    def test_arabic_delivery_cost(self):
        """أرقام عربية في delivery_cost."""
        record = _make_valid_record(delivery_cost="٥٠٠٠٫٠")
        status, doc, details = apply_rules(record)
        assert status in ("valid", "corrected")
        assert doc["delivery"]["cost"] == 5000.0


# ═══════════════════════════════════════════════
# Rule 2: العملة النصية
# ═══════════════════════════════════════════════

class TestCurrencyText:
    """اختبارات تحويل العملة النصية إلى YER."""

    def test_riyal_yemeni(self):
        """'ريال يمني' → 'YER'"""
        record = _make_valid_record(currency="ريال يمني")
        status, doc, details = apply_rules(record)
        assert status in ("valid", "corrected")
        assert doc["payment"]["currency"] == "YER"

    def test_yer_unchanged(self):
        """'YER' تبقى كما هي."""
        record = _make_valid_record(currency="YER")
        status, doc, details = apply_rules(record)
        assert doc["payment"]["currency"] == "YER"


# ═══════════════════════════════════════════════
# Rule 3: فواصل الآلاف
# ═══════════════════════════════════════════════

class TestThousandSeparator:
    """اختبارات إزالة فواصل الآلاف."""

    def test_thousand_comma_in_total(self):
        """'135,000.00' → 135000.0"""
        record = _make_valid_record(total_amount="135,000.00", payment_amount="135000.0")
        record["items_json"] = '[{"sku":"SKU-1001","name":"شيء","qty":1,"unit_price":130000.0,"total":130000.0}]'
        status, doc, details = apply_rules(record)
        assert status in ("valid", "corrected")
        assert doc["total_amount"] == 135000.0


# ═══════════════════════════════════════════════
# Rule 4: السعر بالكلمات
# ═══════════════════════════════════════════════

class TestPriceWords:
    """اختبارات تحويل الكلمات العربية إلى أرقام."""

    def test_alfan(self):
        """'ألفان' → 2000"""
        record = _make_valid_record(total_amount="ألفان", payment_amount="2000.0")
        record["delivery_cost"] = "0"
        record["items_json"] = '[{"sku":"SKU-1001","name":"شيء","qty":1,"unit_price":2000.0,"total":2000.0}]'
        status, doc, details = apply_rules(record)
        assert status in ("valid", "corrected")
        assert doc["total_amount"] == 2000.0

    def test_khamset_alaf(self):
        """'خمسة آلاف' → 5000"""
        record = _make_valid_record(total_amount="خمسة آلاف", payment_amount="5000.0")
        record["delivery_cost"] = "0"
        record["items_json"] = '[{"sku":"SKU-1001","name":"شيء","qty":1,"unit_price":5000.0,"total":5000.0}]'
        status, doc, details = apply_rules(record)
        assert status in ("valid", "corrected")
        assert doc["total_amount"] == 5000.0


# ═══════════════════════════════════════════════
# Rule 5: أرقام الهواتف
# ═══════════════════════════════════════════════

class TestPhoneFormat:
    """اختبارات توحيد أرقام الهواتف اليمنية."""

    def test_phone_with_spaces(self):
        """'77 557 8449' → '775578449'"""
        record = _make_valid_record(customer_phone="77 557 8449")
        status, doc, details = apply_rules(record)
        assert status in ("valid", "corrected")
        assert doc["customer"]["phone"] == "775578449"

    def test_phone_with_country_code(self):
        """'+967 777559764' → '967777559764'"""
        record = _make_valid_record(customer_phone="+967 777559764")
        status, doc, details = apply_rules(record)
        assert status in ("valid", "corrected")
        assert doc["customer"]["phone"] == "967777559764"

    def test_valid_phone_unchanged(self):
        """رقم سليم يبقى كما هو."""
        record = _make_valid_record(customer_phone="775578449")
        status, doc, details = apply_rules(record)
        assert doc["customer"]["phone"] == "775578449"


# ═══════════════════════════════════════════════
# Rule 6: البريد الإلكتروني
# ═══════════════════════════════════════════════

class TestEmailFix:
    """اختبارات إصلاح البريد الإلكتروني."""

    def test_double_at(self):
        """'user@@example.com' → 'user@example.com'"""
        record = _make_valid_record(customer_email="user@@example.com")
        status, doc, details = apply_rules(record)
        assert status in ("valid", "corrected")
        assert doc["customer"]["email"] == "user@example.com"

    def test_double_dot(self):
        """'user@example..com' → 'user@example.com'"""
        record = _make_valid_record(customer_email="user@example..com")
        status, doc, details = apply_rules(record)
        assert status in ("valid", "corrected")
        assert doc["customer"]["email"] == "user@example.com"

    def test_corrupted_email_quarantine(self):
        """'@@' يجب عزله."""
        record = _make_valid_record(customer_email="@@")
        status, doc, details = apply_rules(record)
        assert status == "quarantine"
        error_codes = [d.get("error_code") for d in details]
        assert "CORRUPTED_EMAIL" in error_codes


# ═══════════════════════════════════════════════
# Rule 7: التواريخ
# ═══════════════════════════════════════════════

class TestDateNormalize:
    """اختبارات توحيد صيغ التواريخ."""

    def test_slash_format(self):
        """'2025/04/11 13:41:00' → '2025-04-11T13:41:00'"""
        record = _make_valid_record(order_date="2025/04/11 13:41:00")
        status, doc, details = apply_rules(record)
        assert status in ("valid", "corrected")
        assert doc["order_date"] == "2025-04-11T13:41:00"

    def test_reversed_dd_mm_yyyy(self):
        """'17-01-2025 04:50:00' → '2025-01-17T04:50:00'"""
        record = _make_valid_record(order_date="17-01-2025 04:50:00")
        status, doc, details = apply_rules(record)
        assert status in ("valid", "corrected")
        assert doc["order_date"] == "2025-01-17T04:50:00"

    def test_impossible_date_quarantine(self):
        """'2025-19-45 99:70:00' تاريخ مستحيل — يجب عزله."""
        record = _make_valid_record(order_date="2025-19-45 99:70:00")
        status, doc, details = apply_rules(record)
        assert status == "quarantine"
        error_codes = [d.get("error_code") for d in details]
        assert "IMPOSSIBLE_DATE" in error_codes

    def test_valid_iso_unchanged(self):
        """تاريخ ISO سليم يبقى كما هو."""
        record = _make_valid_record(order_date="2025-02-24T21:29:00")
        status, doc, details = apply_rules(record)
        assert doc["order_date"] == "2025-02-24T21:29:00"


# ═══════════════════════════════════════════════
# Rule 8: المسافات والمرادفات
# ═══════════════════════════════════════════════

class TestTrimNormalize:
    """اختبارات تنظيف المسافات وتوحيد الحالات."""

    def test_extra_spaces_status(self):
        """'  ملغي  ' → 'ملغي'"""
        record = _make_valid_record(status="  ملغي  ")
        status, doc, details = apply_rules(record)
        assert status in ("valid", "corrected")
        assert doc["status"] == "ملغي"

    def test_status_synonym(self):
        """'ملغى' → 'ملغي' (مرادف)."""
        record = _make_valid_record(status="ملغى")
        status, doc, details = apply_rules(record)
        assert status in ("valid", "corrected")
        assert doc["status"] == "ملغي"


# ═══════════════════════════════════════════════
# Rule 9: إجمالي الطلب
# ═══════════════════════════════════════════════

class TestTotalRecalc:
    """اختبارات إعادة حساب إجمالي الطلب."""

    def test_correct_total_unchanged(self):
        """إجمالي صحيح يبقى بدون تعديل."""
        record = _make_valid_record(
            total_amount="100000.0",
            delivery_cost="5000.0",
            items_json='[{"sku":"SKU-1001","name":"لابتوب","qty":1,"unit_price":95000.0,"total":95000.0}]'
        )
        status, doc, details = apply_rules(record)
        assert doc["total_amount"] == 100000.0


# ═══════════════════════════════════════════════
# Rule 10: القيم الرمزية والمفقودة
# ═══════════════════════════════════════════════

class TestSymbolicValues:
    """اختبارات القيم الرمزية والمفقودة."""

    def test_missing_order_id_quarantine(self):
        """order_id فارغ يجب عزله."""
        record = _make_valid_record(order_id="")
        status, doc, details = apply_rules(record)
        assert status == "quarantine"
        error_codes = [d.get("error_code") for d in details]
        assert "MISSING_ORDER_ID" in error_codes

    def test_missing_customer_id_quarantine(self):
        """customer_id فارغ يجب عزله."""
        record = _make_valid_record(customer_id="")
        status, doc, details = apply_rules(record)
        assert status == "quarantine"
        error_codes = [d.get("error_code") for d in details]
        assert "MISSING_CUSTOMER_ID" in error_codes

    def test_symbolic_total_quarantine(self):
        """total_amount = '???' يجب عزله."""
        record = _make_valid_record(total_amount="???")
        status, doc, details = apply_rules(record)
        assert status == "quarantine"

    def test_not_json_quarantine(self):
        """items_json = 'not-json' يجب عزله."""
        record = _make_valid_record(items_json="not-json")
        status, doc, details = apply_rules(record)
        assert status == "quarantine"
        error_codes = [d.get("error_code") for d in details]
        assert "CORRUPTED_ITEMS_JSON" in error_codes

    def test_negative_qty_quarantine(self):
        """qty سالب يجب عزله."""
        record = _make_valid_record(
            items_json='[{"sku":"SKU-1001","name":"لابتوب","qty":-2,"unit_price":95000.0,"total":95000.0}]'
        )
        status, doc, details = apply_rules(record)
        assert status == "quarantine"
        error_codes = [d.get("error_code") for d in details]
        assert "INVALID_QUANTITY" in error_codes


# ═══════════════════════════════════════════════
# اختبار السجل السليم الكامل
# ═══════════════════════════════════════════════

class TestValidRecord:
    """اختبار أن السجل السليم يمر بدون أخطاء."""

    def test_fully_valid_record(self):
        """سجل سليم 100% يجب أن يكون status='valid' بدون أخطاء."""
        record = _make_valid_record()
        status, doc, details = apply_rules(record)
        assert status == "valid"
        assert details == []
        assert doc["order_id"] == "طلب-100001"
        assert doc["customer"]["name"] == "محمد علي"
        assert doc["payment"]["currency"] == "YER"
