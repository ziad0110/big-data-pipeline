"""
quality_rules.py — محرك قواعد جودة البيانات والتنظيف والتدقيق
Quality Rules & Audit Trail Module for Hybrid ELT Pipeline
مقرر البيانات الضخمة (العملي) | جامعة الرازي | إشراف: م. عمر أبو سند

يحتوي هذا الملف على:
1. تطبيق 10 قواعد تنظيف متقدمة وفق متطلبات التكليف وفحص البيانات الفعلية.
2. نظام توثيق التعديلات (Audit Trail) عبر مصفوفة corrections لكل سجل مصحح.
3. نظام عزل الأخطاء (Quarantine System) عبر مصفوفة error_codes و error_details.
"""

import json
import re
from datetime import datetime
from config.settings import (
    ALLOWED_STATUSES,
    ALLOWED_PAYMENT_METHODS,
    ALLOWED_PAYMENT_STATUSES,
    PHONE_REGEX,
    EMAIL_REGEX,
    CURRENCY_MAP,
    ARABIC_DIGIT_MAP,
    WORD_PRICE_MAP,
    STATUS_SYNONYM_MAP,
)


def _clean_numeric_field(value: str) -> tuple[float | None, list[dict]]:
    """
    تنظيف الحقول الرقمية بتطبيق القواعد: 1 (الأرقام العربية)، 3 (فواصل الآلاف)، 4 (الكلمات النصية).
    Applies rules 1, 3, 4 in sequence to any numeric field.
    Returns: (cleaned_float, corrections_list) or (None, errors) if unrecoverable.
    """
    if value is None:
        return None, [{'error_code': 'MISSING_NUMERIC', 'error_detail': 'القيمة مفقودة (None)'}]
    
    original = str(value).strip()
    if original == "":
        return 0.0, []
        
    corrections = []
    current = original
    
    # ─── القاعدة 4: تحويل الأسعار المكتوبة بالكلمات (PRICE_WORDS) ───
    if current in WORD_PRICE_MAP:
        corrected = str(WORD_PRICE_MAP[current])
        corrections.append({
            'rule': 'PRICE_WORDS',
            'original': current,
            'corrected': corrected
        })
        current = corrected

    # ─── القاعدة 1: تحويل الأرقام العربية المشرقية (ARABIC_NUMERALS: ٠١٢٣٤٥٦٧٨٩) ───
    has_arabic = any(char in ARABIC_DIGIT_MAP for char in current)
    if has_arabic:
        corrected_arabic = "".join(ARABIC_DIGIT_MAP.get(c, c) for c in current)
        corrections.append({
            'rule': 'ARABIC_NUMERALS',
            'original': current,
            'corrected': corrected_arabic
        })
        current = corrected_arabic

    # ─── القاعدة 3: إزالة فواصل الآلاف (THOUSAND_SEPARATOR: 135,000.00 -> 135000.00) ───
    if ',' in current:
        corrected_comma = current.replace(',', '')
        corrections.append({
            'rule': 'THOUSAND_SEPARATOR',
            'original': current,
            'corrected': corrected_comma
        })
        current = corrected_comma

    try:
        return float(current), corrections
    except ValueError:
        return None, [{'error_code': 'INVALID_NUMERIC', 'error_detail': f'لا يمكن تحويل القيمة إلى رقم: {current}'}]


def apply_rules(raw_record: dict) -> tuple[str, dict, list]:
    """
    تطبيق جميع قواعد التنظيف العشر على السجل الخام.
    Apply all 10 cleaning rules to a raw CSV record.
    
    Returns:
        tuple: (status, record, details)
        - status: 'valid' | 'corrected' | 'quarantine'
        - record: المستند المنظف أو السجل الخام في حال العزل
        - details: قائمة التعديلات (Audit Trail) أو قائمة أسباب العزل
    """
    errors = []
    corrections = []

    # =========================================================================
    # القاعدة 10: فحص القيم الرمزية والمفقودة (SYMBOLIC_VALUES & MISSING PRE-CHECK)
    # =========================================================================
    order_id = str(raw_record.get('order_id', '')).strip()
    customer_id = str(raw_record.get('customer_id', '')).strip()
    total_amount_raw = str(raw_record.get('total_amount', ''))
    items_json_raw = str(raw_record.get('items_json', ''))

    # فحص معرف الطلب
    if not order_id:
        errors.append({'error_code': 'MISSING_ORDER_ID', 'error_detail': 'معرف الطلب order_id مفقود أو فارغ', 'field': 'order_id'})
    # فحص معرف العميل
    if not customer_id:
        errors.append({'error_code': 'MISSING_CUSTOMER_ID', 'error_detail': 'معرف العميل customer_id مفقود أو فارغ', 'field': 'customer_id'})
    # فحص القيم الرمزية المجهولة
    if '???' in total_amount_raw:
        errors.append({'error_code': 'SYMBOLIC_VALUE', 'error_detail': 'إجمالي المبلغ يحتوي على قيمة رمزية ???', 'field': 'total_amount'})

    # فحص وتحليل عناصر الطلب items_json
    items_parsed = []
    if items_json_raw == 'not-json':
        errors.append({'error_code': 'CORRUPTED_ITEMS_JSON', 'error_detail': 'حقل items_json تالف ويحتوي not-json', 'field': 'items_json'})
    elif items_json_raw:
        try:
            items_parsed = json.loads(items_json_raw)
            if not isinstance(items_parsed, list) or len(items_parsed) == 0:
                errors.append({'error_code': 'EMPTY_ITEMS', 'error_detail': 'قائمة المنتجات فارغة أو غير صحيحة', 'field': 'items_json'})
            else:
                for idx, item in enumerate(items_parsed):
                    if not isinstance(item, dict):
                        errors.append({'error_code': 'CORRUPTED_ITEMS_JSON', 'error_detail': f'العنصر [{idx}] ليس كائناً صحيحاً', 'field': 'items_json'})
                        continue

                    # التحقق من الكمية (يجب أن تكون عدداً موجباً أكبر من الصفر)
                    qty_raw = item.get('qty', 0)
                    try:
                        qty_num, _ = _clean_numeric_field(str(qty_raw))
                        if qty_num is None or qty_num <= 0:
                            errors.append({'error_code': 'INVALID_QUANTITY', 'error_detail': f'الكمية [{idx}] سالبة أو صفرية: {qty_raw}', 'field': 'items_json'})
                            continue
                        item['qty'] = int(qty_num)
                    except Exception:
                        errors.append({'error_code': 'INVALID_QUANTITY', 'error_detail': f'الكمية [{idx}] غير صالحة: {qty_raw}', 'field': 'items_json'})
                        continue

                    # تنظيف والتحقق من سعر الوحدة والإجمالي
                    up_raw = item.get('unit_price')
                    up_val, _ = _clean_numeric_field(str(up_raw)) if up_raw is not None else (None, [])
                    
                    tot_raw = item.get('total')
                    tot_val, _ = _clean_numeric_field(str(tot_raw)) if tot_raw is not None else (None, [])

                    if up_val is not None and up_val >= 0 and (tot_val is None or tot_val < 0):
                        # استنتاج الإجمالي من سعر الوحدة * الكمية
                        tot_val = round(up_val * item['qty'], 2)
                    elif tot_val is not None and tot_val >= 0 and (up_val is None or up_val < 0):
                        # استنتاج سعر الوحدة من الإجمالي / الكمية
                        up_val = round(tot_val / item['qty'], 2)
                    elif (up_val is None or up_val < 0) and (tot_val is None or tot_val < 0):
                        # السعر مجهول ولا يمكن استنتاجه بأمان -> عزل
                        errors.append({'error_code': 'UNKNOWN_PRICE', 'error_detail': f'سعر العنصر [{idx}] مجهول وغير قابل للاستنتاج', 'field': 'items_json'})
                        continue

                    item['unit_price'] = float(up_val)
                    item['total'] = float(tot_val)
        except json.JSONDecodeError:
            errors.append({'error_code': 'CORRUPTED_ITEMS_JSON', 'error_detail': 'فشل قراءة كود JSON لعناصر الطلب', 'field': 'items_json'})
            
    # في حال وجود أخطاء عزل مبكرة -> عزل فوري
    if errors:
        return ('quarantine', raw_record, errors)

    # =========================================================================
    # القاعدة 8: إزالة المسافات وتوحيد الحالات (TRIM_NORMALIZE & STATUS SYNONYMS)
    # =========================================================================
    cleaned_fields = {}
    for key, val in raw_record.items():
        if isinstance(val, str):
            cleaned_fields[key] = val.strip()
        else:
            cleaned_fields[key] = val

    # توحيد حالة الطلب
    raw_status = cleaned_fields.get('status', '')
    norm_status = STATUS_SYNONYM_MAP.get(raw_status, raw_status)
    if norm_status != raw_status:
        corrections.append({'field': 'status', 'rule': 'TRIM_NORMALIZE_STATUS', 'original': raw_status, 'corrected': norm_status})
    if norm_status not in ALLOWED_STATUSES:
        errors.append({'error_code': 'INVALID_STATUS', 'error_detail': f'حالة الطلب غير مسموحة: {norm_status}', 'field': 'status'})
    cleaned_fields['status'] = norm_status

    # توحيد طريقة الدفع
    pm = cleaned_fields.get('payment_method', '')
    if pm not in ALLOWED_PAYMENT_METHODS:
        errors.append({'error_code': 'INVALID_PAYMENT_METHOD', 'error_detail': f'طريقة الدفع غير مسموحة: {pm}', 'field': 'payment_method'})
    
    # توحيد حالة الدفع
    ps = cleaned_fields.get('payment_status', '')
    if ps not in ALLOWED_PAYMENT_STATUSES:
        errors.append({'error_code': 'INVALID_PAYMENT_STATUS', 'error_detail': f'حالة الدفع غير مسموحة: {ps}', 'field': 'payment_status'})

    # =========================================================================
    # القاعدة 7: توحيد صيغ التواريخ وعزل المستحيلة (DATE_NORMALIZE & ISO-8601)
    # =========================================================================
    date_val = cleaned_fields.get('order_date', '')
    if date_val:
        orig_date = date_val
        # استبدال الفواصل المائلة بشرطات
        normalized = date_val.replace('/', '-')
        
        # فصل التاريخ والوقت
        if ' ' in normalized:
            date_part, time_part = normalized.split(' ', 1)
        elif 'T' in normalized:
            date_part, time_part = normalized.split('T', 1)
        else:
            date_part, time_part = normalized, None

        # معالجة التاريخ المعكوس DD-MM-YYYY وتحويله إلى YYYY-MM-DD
        d_parts = date_part.split('-')
        if len(d_parts) == 3 and len(d_parts[0]) == 2 and len(d_parts[2]) == 4:
            date_part = f"{d_parts[2]}-{d_parts[1]}-{d_parts[0]}"

        if time_part:
            date_val = f"{date_part}T{time_part}"
        else:
            date_val = date_part

        if orig_date != date_val:
            corrections.append({'field': 'order_date', 'rule': 'DATE_NORMALIZE', 'original': orig_date, 'corrected': date_val})

        try:
            # التحقق من صحة التاريخ حسب المعيار القياسي ISO
            datetime.fromisoformat(date_val)
            cleaned_fields['order_date'] = date_val
        except ValueError:
            errors.append({'error_code': 'IMPOSSIBLE_DATE', 'error_detail': f'تاريخ غير صالح أو مستحيل: {date_val}', 'field': 'order_date'})
    
    # =========================================================================
    # القواعد 1+3+4: تنظيف المبالغ الرقمية (تكلفة التوصيل، المدفوع، الإجمالي)
    # =========================================================================
    numerics = {'delivery_cost': 0.0, 'payment_amount': 0.0, 'total_amount': 0.0}
    for field in numerics:
        val, num_corrs = _clean_numeric_field(cleaned_fields.get(field, '0'))
        if val is None:
            for e in num_corrs:
                e['field'] = field
                errors.append(e)
        else:
            for c in num_corrs:
                c['field'] = field
                corrections.append(c)
            numerics[field] = val
    
    # =========================================================================
    # القاعدة 2: توحيد العملة النصية إلى YER (CURRENCY_TEXT)
    # =========================================================================
    currency = cleaned_fields.get('currency', '')
    norm_currency = CURRENCY_MAP.get(currency, currency)
    if norm_currency != currency:
        corrections.append({'field': 'currency', 'rule': 'CURRENCY_TEXT', 'original': currency, 'corrected': norm_currency})
    
    if norm_currency not in CURRENCY_MAP.values() and norm_currency != 'YER':
        errors.append({'error_code': 'INVALID_CURRENCY', 'error_detail': f'عملة غير مسموحة: {norm_currency}', 'field': 'currency'})
    
    # =========================================================================
    # القاعدة 5: توحيد وفحص رقم الهاتف اليمني (PHONE_FORMAT)
    # =========================================================================
    phone = cleaned_fields.get('customer_phone', '')
    clean_phone = re.sub(r'[\s\+\-]', '', phone)
    if phone != clean_phone:
        corrections.append({'field': 'customer_phone', 'rule': 'PHONE_FORMAT', 'original': phone, 'corrected': clean_phone})
        
    if not re.match(PHONE_REGEX, clean_phone):
        errors.append({'error_code': 'INVALID_PHONE', 'error_detail': f'رقم الهاتف لا يطابق النمط القياسي اليمني: {clean_phone}', 'field': 'customer_phone'})
        
    # =========================================================================
    # القاعدة 6: إصلاح وتصحيح البريد الإلكتروني (EMAIL_FIX)
    # =========================================================================
    email = cleaned_fields.get('customer_email', '')
    if not email or email == '@@':
        errors.append({'error_code': 'CORRUPTED_EMAIL', 'error_detail': 'البريد الإلكتروني مفقود أو تالف تماماً (@@)', 'field': 'customer_email'})
    else:
        fixed_email = email.replace('@@', '@').replace('..', '.')
        if email != fixed_email:
            corrections.append({'field': 'customer_email', 'rule': 'EMAIL_FIX', 'original': email, 'corrected': fixed_email})
        
        if not re.match(EMAIL_REGEX, fixed_email):
            errors.append({'error_code': 'INVALID_EMAIL', 'error_detail': f'البريد الإلكتروني غير مطابق للصيغة القياسية: {fixed_email}', 'field': 'customer_email'})
        cleaned_fields['customer_email'] = fixed_email
            
    # في حال تراكم أي أخطاء أثناء التدقيق -> عزل السجل فوراً
    if errors:
        return ('quarantine', raw_record, errors)

    # =========================================================================
    # القاعدة 9: إعادة حساب إجمالي الطلب ومطابقته (TOTAL_RECALC)
    # =========================================================================
    delivery_cost = numerics['delivery_cost']
    payment_amount = numerics['payment_amount']
    total_amount = numerics['total_amount']
    
    expected_total = sum(float(item.get('total', 0.0)) for item in items_parsed) + delivery_cost
    
    if abs(total_amount - expected_total) > 1.0:
        corrections.append({
            'field': 'total_amount', 
            'rule': 'TOTAL_RECALC', 
            'original': total_amount, 
            'corrected': expected_total
        })
        total_amount = expected_total

    # =========================================================================
    # بناء المستند المنظف النهائي (Validated Document Structure)
    # =========================================================================
    validated_doc = {
        'order_id': cleaned_fields['order_id'],
        'order_date': cleaned_fields.get('order_date', ''),
        'status': cleaned_fields['status'],
        'customer': {
            'customer_id': cleaned_fields['customer_id'],
            'name': cleaned_fields.get('customer_name', ''),
            'phone': clean_phone,
            'email': cleaned_fields.get('customer_email', ''),
            'address': {
                'city': cleaned_fields.get('city', ''),
                'district': cleaned_fields.get('district', '')
            }
        },
        'items': items_parsed,
        'delivery': {
            'type': cleaned_fields.get('delivery_type', ''),
            'cost': delivery_cost
        },
        'payment': {
            'method': cleaned_fields.get('payment_method', ''),
            'status': cleaned_fields.get('payment_status', ''),
            'amount': payment_amount,
            'currency': norm_currency
        },
        'total_amount': total_amount
    }

    # إرجاع النتيجة مع سجل التدقيق
    if corrections:
        return ('corrected', validated_doc, corrections)
    else:
        return ('valid', validated_doc, [])
